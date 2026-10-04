"""端到端测试：上传/续传/去重、manifest CAS、GC dry-run 与并发安全。"""
import contextlib
import hashlib
import json
import multiprocessing
import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.errors import OCIError
from app.main import create_app
from app.registry import Registry

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"


def digest_of(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def make_manifest(config: bytes, layers: list) -> bytes:
    return json.dumps({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST,
        "config": {"mediaType": OCI_CONFIG, "digest": digest_of(config), "size": len(config)},
        "layers": [
            {"mediaType": OCI_LAYER, "digest": digest_of(l), "size": len(l)}
            for l in layers
        ],
    }).encode()


@pytest.fixture()
def settings(tmp_path):
    return Settings(data_dir=tmp_path / "data", upload_lease_seconds=3600,
                    gc_grace_seconds=3600)


@pytest.fixture()
def client(settings):
    return TestClient(create_app(settings))


def push_blob(client: TestClient, repo: str, data: bytes) -> str:
    d = digest_of(data)
    r = client.post(f"/v2/{repo}/blobs/uploads?digest={d}", content=data)
    assert r.status_code == 201, r.text
    assert r.headers["Docker-Content-Digest"] == d
    return d


def push_manifest(client: TestClient, repo: str, tag: str, manifest: bytes,
                  expected: str = None, status: int = 201):
    headers = {"Content-Type": OCI_MANIFEST}
    if expected:
        headers["If-Match"] = expected
    r = client.put(f"/v2/{repo}/manifests/{tag}", content=manifest, headers=headers)
    assert r.status_code == status, r.text
    return r


# ---------------- 基础 ----------------

def test_v2_ping(client):
    r = client.get("/v2/")
    assert r.status_code == 200
    assert r.headers["Docker-Distribution-API-Version"] == "registry/2.0"


def test_invalid_repo_name(client):
    r = client.post("/v2/UPPER/case/blobs/uploads")
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "NAME_INVALID"


# ---------------- 上传 ----------------

def test_monolithic_upload_and_pull(client):
    data = b"hello registry" * 100
    d = push_blob(client, "team/app", data)
    r = client.get(f"/v2/team/app/blobs/{d}")
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["Docker-Content-Digest"] == d
    h = client.head(f"/v2/team/app/blobs/{d}")
    assert h.headers["Content-Length"] == str(len(data))


def test_chunked_upload_resume_and_verify(client):
    chunks = [b"aaa" * 100, b"bbb" * 100, b"ccc" * 100]
    data = b"".join(chunks)

    r = client.post("/v2/app/blobs/uploads")
    assert r.status_code == 202
    loc = r.headers["Location"]
    uid = r.headers["Docker-Upload-UUID"]
    assert r.headers["Range"] == "0-0"

    r = client.patch(loc, content=chunks[0])
    assert r.status_code == 202
    assert r.headers["Range"] == f"0-{len(chunks[0]) - 1}"

    # 续传探测：GET 返回当前进度
    r = client.get(loc)
    assert r.status_code == 204
    assert r.headers["Range"] == f"0-{len(chunks[0]) - 1}"
    assert r.headers["Docker-Upload-UUID"] == uid

    # 带 Content-Range 续传第二块
    r = client.patch(loc, content=chunks[1],
                     headers={"Content-Range": f"{len(chunks[0])}-{len(chunks[0]) + len(chunks[1]) - 1}"})
    assert r.status_code == 202

    # 最后一块随 PUT 一起提交并校验摘要
    r = client.put(f"{loc}?digest={digest_of(data)}", content=chunks[2])
    assert r.status_code == 201
    assert r.headers["Docker-Content-Digest"] == digest_of(data)

    assert client.get(f"/v2/app/blobs/{digest_of(data)}").content == data


def test_chunked_upload_wrong_content_range(client):
    client.post("/v2/app/blobs/uploads")
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    client.patch(loc, content=b"abc")
    r = client.patch(loc, content=b"def", headers={"Content-Range": "0-2"})
    assert r.status_code == 416
    assert r.json()["errors"][0]["code"] == "RANGE_INVALID"


def test_finalize_digest_mismatch_cancels_upload(client):
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    client.patch(loc, content=b"some data")
    bad = "sha256:" + "0" * 64
    r = client.put(f"{loc}?digest={bad}")
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "DIGEST_INVALID"
    assert client.get(loc).status_code == 404  # 会话已取消


def test_finalize_idempotent_retry(client):
    data = b"retry me"
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    client.patch(loc, content=data)
    d = digest_of(data)
    assert client.put(f"{loc}?digest={d}").status_code == 201
    # 客户端没收到响应而重试：同 digest 幂等返回 201
    assert client.put(f"{loc}?digest={d}").status_code == 201


def test_cancel_upload(client):
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    assert client.delete(loc).status_code == 204
    assert client.get(loc).status_code == 404


def test_blob_dedup_across_repos(client, settings):
    data = b"shared layer" * 50
    d = push_blob(client, "repo/one", data)
    push_blob(client, "repo/two", data)
    push_blob(client, "repo/one", data)  # 重复推送
    blobs = list(settings.blobs_dir.rglob("*"))
    blob_files = [p for p in blobs if p.is_file()]
    assert len(blob_files) == 1  # 全局只有一份内容
    assert client.get(f"/v2/repo/one/blobs/{d}").status_code == 200
    assert client.get(f"/v2/repo/two/blobs/{d}").status_code == 200


def test_cross_repo_mount(client):
    data = b"mountable"
    d = push_blob(client, "src/repo", data)
    r = client.post(f"/v2/dst/repo/blobs/uploads?mount={d}&from=src/repo")
    assert r.status_code == 201
    assert client.get(f"/v2/dst/repo/blobs/{d}").status_code == 200


def test_blob_namespace_isolation(client):
    d = push_blob(client, "repo/a", b"private")
    r = client.get(f"/v2/repo/b/blobs/{d}")
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "BLOB_UNKNOWN"


# ---------------- manifest ----------------

def test_manifest_requires_existing_layers(client):
    m = make_manifest(b"cfg", [b"missing-layer"])
    r = client.put("/v2/app/manifests/v1", content=m,
                   headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "MANIFEST_BLOB_UNKNOWN"


def test_manifest_publish_and_pull(client):
    cfg, layer = b'{"cfg":1}', b"layer-data"
    push_blob(client, "app", cfg)
    push_blob(client, "app", layer)
    m = make_manifest(cfg, [layer])
    r = push_manifest(client, "app", "v1", m)
    d = digest_of(m)
    assert r.headers["Docker-Content-Digest"] == d

    by_tag = client.get("/v2/app/manifests/v1")
    assert by_tag.content == m
    assert by_tag.headers["Content-Type"].startswith(OCI_MANIFEST)
    by_digest = client.get(f"/v2/app/manifests/{d}")
    assert by_digest.content == m
    assert client.head("/v2/app/manifests/v1").headers["Content-Length"] == str(len(m))
    assert client.get("/v2/app/tags/list").json()["tags"] == ["v1"]


def test_manifest_digest_ref_is_immutable(client):
    cfg = b"cfg"
    push_blob(client, "app", cfg)
    m1 = make_manifest(cfg, [])
    other = make_manifest(b"cfg2", [])
    # digest 引用与内容不符 -> 拒绝
    r = client.put(f"/v2/app/manifests/{digest_of(m1)}", content=other,
                   headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "DIGEST_INVALID"


def test_tag_move_requires_cas(client):
    cfg_a, cfg_b = b"config-a", b"config-b"
    push_blob(client, "app", cfg_a)
    push_blob(client, "app", cfg_b)
    m_a = make_manifest(cfg_a, [])
    m_b = make_manifest(cfg_b, [])
    d_a, d_b = digest_of(m_a), digest_of(m_b)

    push_manifest(client, "app", "latest", m_a)

    # 移动标签必须携带期望摘要
    r = push_manifest(client, "app", "latest", m_b, status=409)
    err = r.json()["errors"][0]
    assert err["code"] == "TAG_CONFLICT"
    assert err["detail"]["current"] == d_a

    # 期望摘要错误 -> 409
    r = push_manifest(client, "app", "latest", m_b,
                      expected="sha256:" + "1" * 64, status=409)
    assert r.json()["errors"][0]["code"] == "TAG_CONFLICT"

    # 期望摘要正确 -> CAS 成功
    push_manifest(client, "app", "latest", m_b, expected=d_a)
    assert client.get("/v2/app/manifests/latest").content == m_b

    # 重复推送同一 manifest 到同一标签：当前值==内容 digest，仍需 If-Match
    push_manifest(client, "app", "latest", m_b, expected=d_b)


def test_tag_cas_on_absent_tag(client):
    cfg = b"cfg"
    push_blob(client, "app", cfg)
    m = make_manifest(cfg, [])
    # 标签不存在却声称期望某个摘要 -> 冲突
    r = push_manifest(client, "app", "v1", m, expected="sha256:" + "2" * 64, status=409)
    assert r.json()["errors"][0]["detail"]["current"] is None


def test_manifest_unknown(client):
    r = client.get("/v2/app/manifests/nope")
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "MANIFEST_UNKNOWN"


# ---------------- GC ----------------

def _push_image(client, repo, tag, cfg, layers, expected=None):
    push_blob(client, repo, cfg)
    for l in layers:
        push_blob(client, repo, l)
    m = make_manifest(cfg, layers)
    push_manifest(client, repo, tag, m, expected=expected)
    return m


def test_gc_dry_run_then_execute(client):
    shared, layer1, layer2 = b"shared", b"layer-1", b"layer-2"
    cfg_a, cfg_b = b"cfg-a", b"cfg-b"
    m_a = _push_image(client, "app", "keep", cfg_a, [shared, layer1])
    m_b = _push_image(client, "app", "temp", cfg_b, [shared, layer2])
    # 用 CAS 把 temp 移走，使 m_b 成为无标签垃圾
    m_c = _push_image(client, "app", "temp", cfg_a, [shared], expected=digest_of(m_b))

    # grace_seconds=0：跳过宽限期，立刻回收无引用对象
    report = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert report["dry_run"] is True
    assert report["plan_id"]  # dry-run 返回持久化计划 ID
    assert report["expires_at"] > report["generated_at"]
    dead_blobs = {b["digest"] for b in report["delete"]["blobs"]}
    assert digest_of(layer2) in dead_blobs
    assert digest_of(cfg_b) in dead_blobs
    assert digest_of(shared) not in dead_blobs  # 共享层可达
    dead_manifests = {m["digest"] for m in report["delete"]["manifests"]}
    assert digest_of(m_b) in dead_manifests
    assert digest_of(m_a) not in dead_manifests
    assert digest_of(m_c) not in dead_manifests

    # dry-run 不删除任何东西
    assert client.get(f"/v2/app/blobs/{digest_of(layer2)}").status_code == 200

    # 缺 plan_id 被拒绝（400）且不删数据
    missing = client.post("/gc", json={"dry_run": False})
    assert missing.status_code == 400
    assert missing.json()["errors"][0]["code"] == "GC_PLAN_REQUIRED"
    assert client.get(f"/v2/app/blobs/{digest_of(layer2)}").status_code == 200

    # 执行必须携带 dry-run 返回的 plan_id；响应列出实际删除项
    r = client.post("/gc", json={"dry_run": False, "plan_id": report["plan_id"]})
    assert r.status_code == 200, r.text
    result = r.json()
    assert result["executed"] is True
    assert result["plan_id"] == report["plan_id"]
    assert result["summary"]["blobs_deleted"] == 2
    assert result["summary"]["manifests_deleted"] == 1
    assert result["summary"]["bytes_reclaimed"] > 0
    deleted_blobs = {b["digest"] for b in result["deleted"]["blobs"]}
    assert digest_of(layer2) in deleted_blobs
    assert digest_of(cfg_b) in deleted_blobs
    assert client.get(f"/v2/app/blobs/{digest_of(layer2)}").status_code == 404
    assert client.get(f"/v2/app/blobs/{digest_of(shared)}").status_code == 200
    assert client.get(f"/v2/app/manifests/{digest_of(m_b)}").status_code == 404
    assert client.get("/v2/app/manifests/keep").status_code == 200
    assert client.get("/v2/app/manifests/temp").status_code == 200

    # 重复执行同一计划：409 拒绝，不会再次删除
    dup = client.post("/gc", json={"dry_run": False, "plan_id": report["plan_id"]})
    assert dup.status_code == 409
    assert dup.json()["errors"][0]["code"] == "GC_PLAN_ALREADY_EXECUTED"
    assert client.get(f"/v2/app/blobs/{digest_of(shared)}").status_code == 200


def test_gc_respects_grace_period(client):
    d = push_blob(client, "app", b"fresh unreferenced blob")
    report = client.post("/gc", json={"dry_run": True}).json()
    assert report["summary"]["blobs_to_delete"] == 0
    assert report["summary"]["blobs_kept_by_grace"] == 1

    report = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert d in {b["digest"] for b in report["delete"]["blobs"]}

    client.post("/gc", json={"dry_run": False, "plan_id": report["plan_id"]})
    assert client.get(f"/v2/app/blobs/{d}").status_code == 404


def test_gc_upload_leases(client, settings):
    live = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    stale = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    # 手动把 stale 的租约改到过去
    registry = client.app.state.registry
    with registry.lock:
        registry.db.update_upload_progress(stale, 0, time.time() - 1)

    report = client.post("/gc", json={"dry_run": True}).json()
    assert stale in {u["id"] for u in report["delete"]["uploads"]}
    assert live in {u["id"] for u in report["keep"]["uploads_leased"]}

    client.post("/gc", json={"dry_run": False, "plan_id": report["plan_id"]})
    assert client.get(f"/v2/app/blobs/uploads/{stale}").status_code == 404
    assert client.get(f"/v2/app/blobs/uploads/{live}").status_code == 204


def test_gc_concurrent_pull_and_publish(client, settings):
    """GC 与并发拉取/发布同时进行：可达数据不丢、新发布不被误删。"""
    shared = b"hot shared layer" * 100
    m = _push_image(client, "app", "live", b"cfg", [shared])
    # 制造一些垃圾
    push_blob(client, "app", b"garbage-old")
    registry = client.app.state.registry
    with registry.lock:  # 让垃圾老过宽限期
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()

    errors = []

    def puller():
        try:
            for _ in range(30):
                r = client.get(f"/v2/app/blobs/{digest_of(shared)}")
                assert r.status_code == 200 and r.content == shared
                time.sleep(0.001)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def publisher():
        try:
            for i in range(10):
                _push_image(client, "app", f"v{i}", f"cfg-{i}".encode(), [shared])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def collector():
        # 每次循环生成自己的计划再执行；并发新发布/标签移动产生的新候选
        # 不在任何计划内，绝不能被误删
        try:
            for _ in range(10):
                p = client.post("/gc", json={"dry_run": True}).json()
                r = client.post("/gc", json={"dry_run": False,
                                             "plan_id": p["plan_id"]})
                assert r.status_code == 200, r.text
                time.sleep(0.001)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = ([threading.Thread(target=puller) for _ in range(3)]
               + [threading.Thread(target=publisher)]
               + [threading.Thread(target=collector)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert client.get(f"/v2/app/blobs/{digest_of(shared)}").status_code == 200
    assert client.get("/v2/app/manifests/live").content == m
    assert client.get("/v2/app/manifests/v9").status_code == 200
    # 垃圾已被回收
    assert client.get(f"/v2/app/blobs/{digest_of(b'garbage-old')}").status_code == 404


# ---------------- GC 复核计划：修复 dry-run 后新增候选被误删 ----------------

def _age_all_blobs(registry):
    with registry.lock:  # 让所有 blob 老过宽限期
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()


def test_gc_plan_unknown_and_bad_request(client):
    assert client.post("/gc", json={"dry_run": False,
                                   "plan_id": "deadbeef"}).status_code == 404
    r = client.post("/gc", json={"dry_run": False, "plan_id": "   "})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "GC_PLAN_REQUIRED"
    r = client.post("/gc", json={"dry_run": True, "grace_seconds": -5})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "GC_INVALID"


def test_gc_new_candidates_after_dry_run_are_left_for_next_review(client):
    """核心漏洞：dry-run 之后标签移动产生的新不可达对象不得被本次执行删除。"""
    registry = client.app.state.registry

    # dry-run 前就存在的、已被复核的垃圾
    old_garbage = push_blob(client, "app", b"old garbage")
    # 一张随后会被移动的标签：v1 -> m1
    cfg1, cfg2 = b"cfg-1", b"cfg-2"
    layer1, layer2 = b"layer-1", b"layer-2"
    m1 = _push_image(client, "app", "v1", cfg1, [layer1])
    _age_all_blobs(registry)

    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert old_garbage in {b["digest"] for b in plan["delete"]["blobs"]}

    # ---- dry-run 之后、执行之前：标签移动制造新的不可达 manifest/blob ----
    _push_image(client, "app", "v1", cfg2, [layer2], expected=digest_of(m1))
    # 再凭空产生一个不可达 blob（同样老过宽限期，专门用来验证“不在计划内”）
    new_garbage = push_blob(client, "app", b"new garbage after plan")
    _age_all_blobs(registry)

    result = client.post("/gc", json={"dry_run": False,
                                      "plan_id": plan["plan_id"]})
    assert result.status_code == 200, result.text
    rep = result.json()

    # 只删除计划内仍符合条件的 old garbage
    deleted = {b["digest"] for b in rep["deleted"]["blobs"]}
    assert deleted == {old_garbage}
    assert client.get(f"/v2/app/blobs/{old_garbage}").status_code == 404

    # 标签移动新产生的不可达对象（m1 及其 cfg/layer、new_garbage）一律保留，
    # 即使它们老过宽限期——等待下次 dry-run 审阅
    assert client.get(f"/v2/app/blobs/{digest_of(cfg1)}").status_code == 200
    assert client.get(f"/v2/app/blobs/{digest_of(layer1)}").status_code == 200
    assert client.get(f"/v2/app/manifests/{digest_of(m1)}").status_code == 200
    assert client.get(f"/v2/app/blobs/{new_garbage}").status_code == 200

    # 下一次 dry-run 才把这些新候选列出来
    next_plan = client.post("/gc", json={"dry_run": True,
                                         "grace_seconds": 0}).json()
    next_blob_candidates = {b["digest"] for b in next_plan["delete"]["blobs"]}
    assert new_garbage in next_blob_candidates
    assert digest_of(cfg1) in next_blob_candidates
    assert digest_of(layer1) in next_blob_candidates
    assert digest_of(m1) in {m["digest"] for m in next_plan["delete"]["manifests"]}


def test_gc_execution_skips_blob_become_reachable_again(client):
    registry = client.app.state.registry
    d = push_blob(client, "app", b"will be re-referenced")
    _age_all_blobs(registry)
    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}

    # 执行前发布一个带标签的 manifest 引用该 blob，使其重新可达
    push_blob(client, "app", b"cfg")
    _age_all_blobs(registry)
    m = make_manifest(b"cfg", [b"will be re-referenced"])
    push_manifest(client, "app", "v1", m)

    rep = client.post("/gc", json={"dry_run": False,
                                   "plan_id": plan["plan_id"]}).json()
    assert rep["summary"]["blobs_deleted"] == 0
    assert rep["summary"]["blobs_skipped"] == 1
    assert rep["skipped"]["blobs"][0]["digest"] == d
    assert "reachable again" in rep["skipped"]["blobs"][0]["reason"]
    assert client.get(f"/v2/app/blobs/{d}").status_code == 200


def test_gc_execution_skips_manifest_retagged_again(client):
    registry = client.app.state.registry
    cfg_a, cfg_b = b"cfg-a", b"cfg-b"
    m_a = _push_image(client, "app", "v1", cfg_a, [])
    _push_image(client, "app", "v1", cfg_b, [], expected=digest_of(m_a))
    _age_all_blobs(registry)

    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert digest_of(m_a) in {m["digest"] for m in plan["delete"]["manifests"]}

    # 执行前把标签移回 m_a：m_a 重新可达，本次跳过
    cur = client.get("/v2/app/manifests/v1").headers["Docker-Content-Digest"]
    push_manifest(client, "app", "v1", m_a, expected=cur)

    rep = client.post("/gc", json={"dry_run": False,
                                   "plan_id": plan["plan_id"]}).json()
    assert rep["summary"]["manifests_deleted"] == 0
    assert rep["summary"]["manifests_skipped"] == 1
    assert rep["skipped"]["manifests"][0]["digest"] == digest_of(m_a)
    assert client.get(f"/v2/app/manifests/{digest_of(m_a)}").status_code == 200


def test_gc_execution_skips_renewed_upload(client):
    # 会话租约在 dry-run 时刚好到期（被列为候选），执行前被续租
    registry = client.app.state.registry
    near = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    # 手动把到期时间放到“dry-run 之后、执行之前”之间
    with registry.lock:
        registry.db.update_upload_progress(near, 0, time.time() + 0.3)
    time.sleep(0.5)  # 现在租约已过期 -> 进入 dry-run 候选

    plan = client.post("/gc", json={"dry_run": True}).json()
    assert near in {u["id"] for u in plan["delete"]["uploads"]}

    # 执行前续租：模拟客户端在复核窗口内完成读写，租约被延长
    with registry.lock:
        registry.db.update_upload_progress(near, 0, time.time() + 3600)

    rep = client.post("/gc", json={"dry_run": False,
                                   "plan_id": plan["plan_id"]}).json()
    assert rep["summary"]["uploads_deleted"] == 0
    assert rep["summary"]["uploads_skipped"] == 1
    assert rep["skipped"]["uploads"][0]["id"] == near
    assert "renewed" in rep["skipped"]["uploads"][0]["reason"]
    assert client.get(f"/v2/app/blobs/uploads/{near}").status_code == 204


def test_gc_expired_plan_is_rejected_without_deleting(client):
    registry = client.app.state.registry
    garbage = push_blob(client, "app", b"expired plan garbage")
    _age_all_blobs(registry)
    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()

    # 把计划的有效期改到过去（模拟生成超过 1 小时）
    with registry.lock:
        registry.db._conn.execute(
            "UPDATE gc_plans SET expires_at = ? WHERE id = ?",
            (time.time() - 1, plan["plan_id"]),
        )
        registry.db._conn.commit()

    r = client.post("/gc", json={"dry_run": False, "plan_id": plan["plan_id"]})
    assert r.status_code == 410
    assert r.json()["errors"][0]["code"] == "GC_PLAN_EXPIRED"
    # 未删除任何数据，需重新 dry-run
    assert client.get(f"/v2/app/blobs/{garbage}").status_code == 200

    new_plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert client.post("/gc", json={"dry_run": False,
                                    "plan_id": new_plan["plan_id"]}).status_code == 200
    assert client.get(f"/v2/app/blobs/{garbage}").status_code == 404


def test_gc_plan_survives_restart_and_executes_once(settings):
    """计划持久化在 SQLite：跨进程重启仍可执行，且只能执行一次。"""
    c1 = TestClient(create_app(settings))
    garbage = push_blob(c1, "app", b"plan across restart")
    registry = c1.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = c1.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()

    # 重启：同一数据目录的新应用实例读取到同一份计划
    c2 = TestClient(create_app(settings))
    r = c2.post("/gc", json={"dry_run": False, "plan_id": plan["plan_id"]})
    assert r.status_code == 200, r.text
    assert c2.get(f"/v2/app/blobs/{garbage}").status_code == 404

    # 重启后重复执行同样被拒绝
    dup = c2.post("/gc", json={"dry_run": False, "plan_id": plan["plan_id"]})
    assert dup.status_code == 409
    assert dup.json()["errors"][0]["code"] == "GC_PLAN_ALREADY_EXECUTED"


# ---------------- GC 跨进程认领：同一 plan_id 只允许一个进程执行 ----------------

def _gc_claim_worker(data_dir, plan_id, digest, mode, queue):
    """子进程：独立 Registry + 独立 SQLite 连接，与父进程共享数据目录。"""
    try:
        settings = Settings(data_dir=data_dir, upload_lease_seconds=3600,
                            gc_grace_seconds=3600)
        registry = Registry(settings)

        if mode == "slow":
            # 在认领之后、任何复核/删除之前挂起：保证另一进程在其执行窗口内
            # 竞争认领，且此刻候选尚未被触碰
            orig_reachable = registry._reachable_sets

            def _slow_reachable(*a, **k):
                time.sleep(1.5)
                return orig_reachable(*a, **k)

            registry._reachable_sets = _slow_reachable

        try:
            registry.gc_execute(plan_id)
            status, code = 200, None
        except OCIError as exc:
            status, code = exc.status_code, exc.code

        # 竞争失败者必须确认自己没有碰任何候选（失败时候选依然存在）
        blob_gone = registry.db.get_blob(digest) is None
        queue.put({"mode": mode, "status": status, "code": code,
                   "blob_gone": blob_gone})
    except Exception as exc:  # noqa: BLE001
        import traceback
        queue.put({"mode": mode, "fatal": f"{exc!r}\n{traceback.format_exc()}"})


def test_gc_same_plan_across_two_processes_only_one_deletes(settings):
    """两个独立进程同时以 dry_run:false 和同一 plan_id 请求：只有认领成功的
    进程删除；竞争失败进程收到 OCI 409 GC_PLAN_IN_PROGRESS 且不碰候选。"""
    data_dir = str(settings.data_dir)
    client = TestClient(create_app(settings))
    d = push_blob(client, "app", b"cross process garbage")
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    plan_id = plan["plan_id"]
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}
    client.app.state.registry.db._conn.commit()  # 确保计划已落盘

    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    slow = ctx.Process(target=_gc_claim_worker,
                       args=(data_dir, plan_id, d, "slow", queue))
    fast = ctx.Process(target=_gc_claim_worker,
                       args=(data_dir, plan_id, d, "fast", queue))
    slow.start()
    time.sleep(0.4)  # 等慢进程完成原子认领进入 executing
    fast.start()
    results = []
    for p in (fast, slow):
        p.join(timeout=15)
        assert not p.is_alive(), f"{p.name} timed out"
        p.close()
    while not queue.empty():
        results.append(queue.get_nowait())

    assert "fatal" not in results[0] and "fatal" not in results[1], results
    by_mode = {r["mode"]: r for r in results}
    winner, loser = by_mode["slow"], by_mode["fast"]
    assert winner["status"] == 200, winner
    assert winner["blob_gone"] is True                      # 只有赢家删除候选
    assert loser["status"] == 409, loser
    assert loser["code"] == "GC_PLAN_IN_PROGRESS", loser
    assert loser["blob_gone"] is False                     # 输家不碰任何候选

    # 赢家完成后，落后的旧重试变成 GC_PLAN_ALREADY_EXECUTED
    done = TestClient(create_app(settings))
    dup = done.post("/gc", json={"dry_run": False, "plan_id": plan_id})
    assert dup.status_code == 409
    assert dup.json()["errors"][0]["code"] == "GC_PLAN_ALREADY_EXECUTED"
    assert done.get(f"/v2/app/blobs/{d}").status_code == 404

    # 旧计划不能再用；剩余候选（如果有）只能走新的 dry-run
    fresh_garbage = push_blob(done, "app", b"leftover after race")
    with done.app.state.registry.lock:
        done.app.state.registry.db._conn.execute(
            "UPDATE blobs SET created_at = 0")
        done.app.state.registry.db._conn.commit()
    next_plan = done.post("/gc", json={"dry_run": True,
                                      "grace_seconds": 0}).json()
    assert fresh_garbage in {b["digest"]
                             for b in next_plan["delete"]["blobs"]}


def test_gc_crash_after_claim_leaves_non_replayable_executing_plan(
        client, settings, monkeypatch):
    """认领成功后、删除过程中崩溃：计划停留在 executing，重试返回 409
    GC_PLAN_IN_PROGRESS（不重放）；部分删除生效，剩余候选重新 dry-run 审阅。"""
    registry = client.app.state.registry

    # 一个过期上传 + 一个垃圾 blob；让删除在上传删除后、blob 删除前崩溃
    stale = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    garbage = push_blob(client, "app", b"partially executed garbage")
    with registry.lock:
        registry.db.update_upload_progress(stale, 0, time.time() - 1)
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()

    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert stale in {u["id"] for u in plan["delete"]["uploads"]}
    assert garbage in {b["digest"] for b in plan["delete"]["blobs"]}

    real_delete_manifests = registry.db.delete_manifests

    def _crash_then_restore(keys):
        # 上传已删（部分删除生效）；在 manifest 删除边界模拟进程崩溃，
        # 此后的 blob 一个都不能动
        registry.db.delete_manifests = real_delete_manifests
        raise RuntimeError("simulated crash mid-GC")

    monkeypatch.setattr(registry.db, "delete_manifests", _crash_then_restore)

    with pytest.raises(RuntimeError, match="simulated crash"):
        registry.gc_execute(plan["plan_id"])

    # 计划保留在 executing（不可重放的执行中状态），认领信息已落盘
    row = registry.db.get_gc_plan(plan["plan_id"])
    assert row["state"] == "executing"
    assert row["claimed_at"] is not None
    assert row["claimer"]
    assert row["executed_at"] is None

    # 崩溃前已完成的删除（上传）保留，未执行到的 blob 仍在
    assert registry.db.get_upload(stale) is None
    assert client.get(f"/v2/app/blobs/{garbage}").status_code == 200

    # 同一计划重试（即使跨进程的新实例）：409 GC_PLAN_IN_PROGRESS，不碰候选
    retry = client.post("/gc", json={"dry_run": False,
                                     "plan_id": plan["plan_id"]})
    assert retry.status_code == 409
    err = retry.json()["errors"][0]
    assert err["code"] == "GC_PLAN_IN_PROGRESS"
    assert err["detail"]["stale"] is False  # 刚认领，不算僵死
    assert client.get(f"/v2/app/blobs/{garbage}").status_code == 200

    other = TestClient(create_app(settings))
    retry2 = other.post("/gc", json={"dry_run": False,
                                     "plan_id": plan["plan_id"]})
    assert retry2.status_code == 409
    assert retry2.json()["errors"][0]["code"] == "GC_PLAN_IN_PROGRESS"
    assert other.get(f"/v2/app/blobs/{garbage}").status_code == 200

    # 旧计划不可重放；剩余候选可重新 dry-run 审阅并由新计划回收
    next_plan = other.post("/gc", json={"dry_run": True,
                                        "grace_seconds": 0}).json()
    assert garbage in {b["digest"] for b in next_plan["delete"]["blobs"]}
    r = other.post("/gc", json={"dry_run": False,
                                "plan_id": next_plan["plan_id"]})
    assert r.status_code == 200
    assert other.get(f"/v2/app/blobs/{garbage}").status_code == 404

    # 旧计划仍是 executing 且再试仍是 409
    assert registry.db.get_gc_plan(plan["plan_id"])["state"] == "executing"
    assert other.post("/gc", json={"dry_run": False,
                                   "plan_id": plan["plan_id"]}).status_code == 409


def test_gc_stale_executing_plan_reports_stale_hint(client):
    """认领后长期未完成（进程早已崩溃）：重试 409，detail.stale=true。"""
    registry = client.app.state.registry
    garbage = push_blob(client, "app", b"zombie plan garbage")
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    with registry.lock:
        registry.db._conn.execute(
            "UPDATE gc_plans SET state = 'executing', claimed_at = ?,"
            " claimer = 'dead-host:1:deadbeef' WHERE id = ?",
            (time.time() - registry.CLAIM_STALE_SECONDS - 10, plan["plan_id"]),
        )
        registry.db._conn.commit()

    r = client.post("/gc", json={"dry_run": False,
                                 "plan_id": plan["plan_id"]})
    assert r.status_code == 409
    err = r.json()["errors"][0]
    assert err["code"] == "GC_PLAN_IN_PROGRESS"
    assert err["detail"]["stale"] is True
    assert "crashed" in err["message"]
    assert client.get(f"/v2/app/blobs/{garbage}").status_code == 200


def test_gc_plan_schema_migrates_from_two_state_layout(tmp_path):
    """旧版本数据库（gc_plans 只有 pending/executed 两列）可平滑升级。"""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)
    conn = sqlite3.connect(str(data_dir / "registry.db"))
    conn.executescript(
        """
        CREATE TABLE gc_plans (
            id TEXT PRIMARY KEY, state TEXT NOT NULL, grace_seconds INTEGER NOT NULL,
            created_at REAL NOT NULL, expires_at REAL NOT NULL, executed_at REAL
        );
        """
    )
    conn.execute("INSERT INTO gc_plans VALUES ('oldplan', 'pending', 0, 0, 9.9e18, NULL)")
    conn.commit()
    conn.close()

    registry = Registry(Settings(data_dir=data_dir))
    row = registry.db.get_gc_plan("oldplan")
    assert row["state"] == "pending"
    assert row["claimed_at"] is None and row["claimer"] is None
    # 旧 pending 计划仍能被原子认领
    assert registry.db.claim_gc_plan("oldplan", 1.0, "h:1:x") == "claimed"
    assert registry.db.get_gc_plan("oldplan")["state"] == "executing"


# ---------------- GC vs 发布：跨进程引用/删除竞态 ----------------

def _layer_only_manifest(layer_digest: str, size: int) -> bytes:
    """无 config、单 layer 的最小合法 image manifest。"""
    return json.dumps({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST,
        "layers": [{"mediaType": OCI_LAYER, "digest": layer_digest, "size": size}],
    }).encode()


def _publish_worker(data_dir, repo, tag, manifest, started: "multiprocessing.Event",
                    queue):
    """独立进程：发起一次标签发布。started 在其进入跨进程排他临界区
    （引用校验->标签提交）时置位，用于与 GC 进程确定性地排序。"""
    try:
        settings = Settings(data_dir=data_dir, upload_lease_seconds=3600,
                            gc_grace_seconds=3600)
        registry = Registry(settings)
        _exclusive_orig = registry.xlock.exclusive

        @contextlib.contextmanager
        def _exclusive_signaled():
            with _exclusive_orig():
                started.set()
                yield

        registry.xlock.exclusive = _exclusive_signaled

        try:
            digest = registry.put_manifest(repo, tag, manifest, OCI_MANIFEST, None)
            queue.put({"status": 201, "digest": digest})
        except OCIError as exc:
            queue.put({"status": exc.status_code, "code": exc.code,
                       "detail": exc.detail})
    except Exception as exc:  # noqa: BLE001
        import traceback
        queue.put({"fatal": f"{exc!r}\n{traceback.format_exc()}"})


def _gc_worker_enter_event(data_dir, plan_id, entered: "multiprocessing.Event",
                           digest, queue, hold_seconds: float = 0.0):
    """独立进程：认领计划后，在进入 GC 跨进程排他临界区时置位 entered；
    hold_seconds>0 时在临界区内停留，便于确定性地观察竞争方阻塞。"""
    try:
        settings = Settings(data_dir=data_dir, upload_lease_seconds=3600,
                            gc_grace_seconds=3600)
        registry = Registry(settings)
        _exclusive_orig = registry.xlock.exclusive

        @contextlib.contextmanager
        def _exclusive_signaled():
            with _exclusive_orig():
                entered.set()
                if hold_seconds:
                    time.sleep(hold_seconds)
                yield

        registry.xlock.exclusive = _exclusive_signaled

        try:
            registry.gc_execute(plan_id)
            queue.put({"status": 200,
                       "blob_gone": registry.db.get_blob(digest) is None})
        except OCIError as exc:
            queue.put({"status": exc.status_code, "code": exc.code})
    except Exception as exc:  # noqa: BLE001
        import traceback
        queue.put({"fatal": f"{exc!r}\n{traceback.format_exc()}"})


def test_cross_process_publish_commits_before_gc_candidate_is_skipped(settings):
    """发布先完成：标签在 GC 复核前提交，候选 blob/manifest 重新可达，
    GC 跳过删除；标签可读且引用可拉取。"""
    data_dir = str(settings.data_dir)
    client = TestClient(create_app(settings))
    repo, tag = "race", "v1"
    layer = b"candidate published into a tag"
    d = push_blob(client, repo, layer)
    registry = client.app.state.registry
    with registry.lock:  # 候选要老过宽限期，才会进计划
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    manifest = _layer_only_manifest(d, len(layer))

    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}
    client.app.state.registry.db._conn.commit()  # 确保计划已落盘

    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    in_publish = ctx.Event()
    gc_entered = ctx.Event()
    publisher = ctx.Process(
        target=_publish_worker,
        args=(data_dir, repo, tag, manifest, in_publish, queue),
    )
    gc = ctx.Process(
        target=_gc_worker_enter_event,
        args=(data_dir, plan["plan_id"], gc_entered, d, queue),
    )

    publisher.start()
    assert in_publish.wait(10), "publisher never entered its critical section"
    # 此刻发布已持有排他 flock（标签即将/已经提交）；GC 必须在锁外等待，
    # 等发布提交完成后才复核，因此看到候选重新可达而跳过
    gc.start()
    for p in (publisher, gc):
        p.join(timeout=15)
        assert not p.is_alive(), f"{p.name} timed out"
        p.close()

    results = []
    while not queue.empty():
        results.append(queue.get_nowait())
    assert all("fatal" not in r for r in results), results

    pub = next(r for r in results if "digest" in r)
    gcr = next(r for r in results if "blob_gone" in r)
    assert pub["status"] == 201, results
    assert pub["digest"] == digest_of(manifest)
    assert gcr["status"] == 200, results
    assert gcr["blob_gone"] is False  # GC 跳过已可达候选，没有删 blob

    # 用第三个独立进程复核对外可见状态：标签可读，blob 与 manifest 可拉取
    verifier = TestClient(create_app(settings))
    r = verifier.get(f"/v2/{repo}/manifests/{tag}")
    assert r.status_code == 200
    assert r.headers["Docker-Content-Digest"] == digest_of(manifest)
    assert r.content == manifest
    assert verifier.get(f"/v2/{repo}/blobs/{d}").status_code == 200
    assert verifier.get(f"/v2/{repo}/blobs/{d}").content == layer
    assert tag in verifier.get(f"/v2/{repo}/tags/list").json()["tags"]

    # 该计划已执行完成，不可重放
    assert verifier.post("/gc", json={"dry_run": False,
                                     "plan_id": plan["plan_id"]}).status_code == 409


def test_cross_process_gc_deletes_before_publish_put_rejected_no_tag(settings):
    """GC 先完成：候选 blob 的元数据与文件都已删除后发布才拿到锁，引用
    校验失败 -> 400 MANIFEST_BLOB_UNKNOWN，且没有任何可见标签；其它未变
    标签的引用依然可拉取。"""
    data_dir = str(settings.data_dir)
    client = TestClient(create_app(settings))
    repo, tag, other_tag = "race", "v1", "keep"
    layer, keep_layer, keep_cfg = b"candidate deleted first", b"keep-layer", b"keep-cfg"
    d = push_blob(client, repo, layer)
    keep_manifest = _push_image(client, repo, other_tag, keep_cfg, [keep_layer])
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    manifest = _layer_only_manifest(d, len(layer))

    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}
    client.app.state.registry.db._conn.commit()

    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    in_gc = ctx.Event()
    publisher_ready = ctx.Event()
    gc = ctx.Process(
        target=_gc_worker_enter_event,
        args=(data_dir, plan["plan_id"], in_gc, d, queue, 1.0),
    )
    publisher = ctx.Process(
        target=_publish_worker,
        args=(data_dir, repo, tag, manifest, publisher_ready, queue),
    )

    gc.start()
    assert in_gc.wait(10), "GC never entered its critical section"
    # GC 正在排他临界区内复核/删除。发布进程此刻启动并尝试 PUT：它会在
    # flock 上阻塞，直到 GC 完成元数据与文件删除、释放锁后才进行引用校验
    publisher.start()
    assert publisher_ready.wait(10), "publisher never queued on the lock"
    assert publisher.is_alive(), "publisher must be blocked while GC holds the lock"

    for p in (gc, publisher):
        p.join(timeout=15)
        assert not p.is_alive(), f"{p.name} timed out"
        p.close()

    results = []
    while not queue.empty():
        results.append(queue.get_nowait())
    assert all("fatal" not in r for r in results), results

    gcr = next(r for r in results if "blob_gone" in r)
    pub = next(r for r in results if "code" in r or r.get("status") == 201)
    assert gcr["status"] == 200, results
    assert gcr["blob_gone"] is True                # GC 先删除候选
    assert pub["status"] == 400, results           # 发布被拒
    assert pub["code"] == "MANIFEST_BLOB_UNKNOWN", results
    assert d in pub["detail"]["missing"]

    # 用第三个独立进程复核：新标签不可见；blob 元数据和文件都已消失
    verifier = TestClient(create_app(settings))
    tags = verifier.get(f"/v2/{repo}/tags/list").json()["tags"]
    assert tag not in tags
    assert other_tag in tags
    r = verifier.get(f"/v2/{repo}/manifests/{tag}")
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "MANIFEST_UNKNOWN"
    assert verifier.get(f"/v2/{repo}/blobs/{d}").status_code == 404
    # blob 文件也已从共享存储删除（不只是元数据）
    assert not (settings.blobs_dir / "sha256" / d.split(":")[1]).exists()

    # 标签未变：既有标签的引用（config/layer/manifest）全部仍可拉取
    r = verifier.get(f"/v2/{repo}/manifests/{other_tag}")
    assert r.status_code == 200 and r.content == keep_manifest
    assert verifier.get(f"/v2/{repo}/blobs/{digest_of(keep_layer)}").status_code == 200
    assert verifier.get(f"/v2/{repo}/blobs/{digest_of(keep_cfg)}").status_code == 200


# ---------------- 同摘要重传/挂载 vs GC：跨进程删除窗口竞态 ----------------

def _gc_worker_window_between_metadata_and_file(data_dir, plan_id, digest,
                                                entered, proceed, queue):
    """独立进程：认领计划后，在「已删 blob 元数据、未删 blob 文件」的
    窗口内置位 entered 并阻塞在 proceed 事件上，确定性地把同摘要重传方
    放进这段窗口。整个窗口位于跨进程排他 flock 之内。"""
    try:
        settings = Settings(data_dir=data_dir, upload_lease_seconds=3600,
                            gc_grace_seconds=3600)
        registry = Registry(settings)
        real_delete_blobs = registry.db.delete_blobs

        def _delete_blobs_then_pause(digests):
            real_delete_blobs(digests)
            if digest in digests:
                entered.set()
                proceed.wait(timeout=30)

        registry.db.delete_blobs = _delete_blobs_then_pause
        try:
            registry.gc_execute(plan_id)
            queue.put({"ok": True,
                       "blob_gone": registry.db.get_blob(digest) is None})
        except OCIError as exc:
            queue.put({"ok": False, "status": exc.status_code, "code": exc.code})
    except Exception as exc:  # noqa: BLE001
        import traceback
        queue.put({"fatal": f"{exc!r}\n{traceback.format_exc()}"})


def _reupload_worker(data_dir, repo, digest, payload, mode, queue):
    """独立进程：在 GC 的删除窗口内发起同摘要重传或跨仓库挂载。

    mode:
      - "mono":    POST .../uploads?digest= 单块整体重传；
      - "chunked": POST 会话 -> PATCH -> PUT?digest= 分块提交；
      - "mount":   POST .../uploads?mount=&from= 跨仓库挂载。
    """
    try:
        settings = Settings(data_dir=data_dir, upload_lease_seconds=3600,
                            gc_grace_seconds=3600)
        registry = Registry(settings)
        if mode == "mono":
            registry.monolithic_upload(repo, payload, digest)
            queue.put({"mode": mode, "committed": True})
        elif mode == "chunked":
            u = registry.start_upload(repo)
            registry.append_chunk(repo, u["id"], payload, None)
            d, size = registry.finalize_upload(repo, u["id"], digest, None)
            queue.put({"mode": mode, "committed": True, "digest": d, "size": size})
        elif mode == "mount":
            from_repo = "src/repo"
            mounted = registry.mount_blob(repo, digest)
            queue.put({"mode": mode, "committed": False, "mounted": mounted,
                       "src_has": registry.db.repo_has_blob(from_repo, digest)})
        else:  # pragma: no cover
            raise AssertionError(mode)
    except OCIError as exc:
        queue.put({"mode": mode, "fatal_code": exc.code, "status": exc.status_code})
    except Exception as exc:  # noqa: BLE001
        import traceback
        queue.put({"mode": mode, "fatal": f"{exc!r}\n{traceback.format_exc()}"})


def _run_reupload_during_gc_window(settings, plan_id, digest, repo, payload,
                                   mode, queue):
    data_dir = str(settings.data_dir)
    ctx = multiprocessing.get_context("fork")
    entered, proceed = ctx.Event(), ctx.Event()
    gc = ctx.Process(target=_gc_worker_window_between_metadata_and_file,
                     args=(data_dir, plan_id, digest, entered, proceed, queue))
    other = ctx.Process(target=_reupload_worker,
                        args=(data_dir, repo, digest, payload, mode, queue))
    gc.start()
    assert entered.wait(15), "GC never entered the metadata/file deletion window"
    # 此刻 GC 已在排他 flock 内删掉 blob 元数据、文件仍在；重传方启动后
    # 阻塞在同一把 flock 上
    other.start()
    time.sleep(0.8)
    assert other.is_alive(), "reupload must be blocked by GC critical section"
    proceed.set()
    for p in (gc, other):
        p.join(timeout=20)
        assert not p.is_alive(), f"{p.name} timed out"
        p.close()
    results = []
    while not queue.empty():
        results.append(queue.get_nowait())
    assert all("fatal" not in r and "fatal_code" not in r for r in results), results
    return results


def _gc_candidate(client, settings, repo: str, payload: bytes,
                  src_repo: "str | None" = None):
    """推送一个只在 src_repo（默认 repo）关联、无标签引用且老过宽限期的
    blob，dry-run 出包含它的计划，返回 (plan_id, digest)。"""
    push_from = src_repo or repo
    d = push_blob(client, push_from, payload)
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = client.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}
    registry.db._conn.commit()  # 确保计划已落盘
    return plan["plan_id"], d


def test_same_digest_monolithic_reupload_during_gc_window_lands_intact(settings):
    """GC 先删：单块同摘要重传被 flock 挡到元数据与文件都删完之后，落下
    完整新文件并重建关联；随后发布标签 201，GET blob 200（修复前为
    commit_blob 见旧文件丢弃新内容、文件被 GC 删除的悬空记录）。"""
    client = TestClient(create_app(settings))
    repo = "race/reup"
    payload = b"same digest retransmitted in gc window"
    plan_id, d = _gc_candidate(client, settings, repo, payload)
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()

    results = _run_reupload_during_gc_window(
        settings, plan_id, d, repo, payload, "mono", queue
    )
    gcr = next(r for r in results if "blob_gone" in r)
    reup = next(r for r in results if r.get("mode") == "mono")
    assert gcr["ok"] is True and gcr["blob_gone"] is True, results
    assert reup["committed"] is True

    # 重传落下完整文件：记录与文件都在，GET/HEAD blob 成功
    verifier = TestClient(create_app(settings))
    r = verifier.get(f"/v2/{repo}/blobs/{d}")
    assert r.status_code == 200, r.text
    assert r.content == payload
    assert verifier.head(f"/v2/{repo}/blobs/{d}").headers["Content-Length"] \
        == str(len(payload))

    # 随后发布引用它的标签：201，且标签与 blob 都可拉取（不再 201/404）
    manifest = _layer_only_manifest(d, len(payload))
    r = verifier.put(f"/v2/{repo}/manifests/v1", content=manifest,
                     headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 201, r.text
    assert verifier.get(f"/v2/{repo}/manifests/v1").content == manifest
    assert verifier.get(f"/v2/{repo}/blobs/{d}").content == payload

    # 重传产生的是新记录（受宽限期保护）：紧接着的 GC 不删它
    p2 = verifier.post("/gc", json={"dry_run": True, "grace_seconds": 0}).json()
    assert d not in {b["digest"] for b in p2["delete"]["blobs"]}


def test_same_digest_chunked_finalize_during_gc_window_lands_intact(settings):
    """同上，但是分块提交（PUT ?digest=）：finalize 的摘要复核与提交在
    GC 释放锁之后完成，新文件与关联一致，标签可发布、blob 可拉取。"""
    client = TestClient(create_app(settings))
    repo = "race/chunk"
    payload = b"chunked finalize racing gc " * 7
    plan_id, d = _gc_candidate(client, settings, repo, payload)
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()

    results = _run_reupload_during_gc_window(
        settings, plan_id, d, repo, payload, "chunked", queue
    )
    gcr = next(r for r in results if "blob_gone" in r)
    reup = next(r for r in results if r.get("mode") == "chunked")
    assert gcr["ok"] is True and gcr["blob_gone"] is True, results
    assert reup["committed"] is True
    assert reup["digest"] == d and reup["size"] == len(payload)

    verifier = TestClient(create_app(settings))
    r = verifier.get(f"/v2/{repo}/blobs/{d}")
    assert r.status_code == 200 and r.content == payload
    manifest = _layer_only_manifest(d, len(payload))
    r = verifier.put(f"/v2/{repo}/manifests/v1", content=manifest,
                     headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 201, r.text
    assert verifier.get(f"/v2/{repo}/manifests/v1").content == manifest
    assert verifier.get(f"/v2/{repo}/blobs/{d}").content == payload


def test_cross_repo_mount_during_gc_window_falls_back_to_upload(settings):
    """GC 先删：跨仓库挂载方等锁后观察到 blob（元数据与文件）已不存在，
    mount_blob 返回 False，路由按 OCI 规范回退为普通上传会话（202）。"""
    client = TestClient(create_app(settings))
    src, dst = "src/repo", "dst/repo"
    payload = b"mount candidate racing gc"
    plan_id, d = _gc_candidate(client, settings, dst, payload, src_repo=src)
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()

    results = _run_reupload_during_gc_window(
        settings, plan_id, d, dst, payload, "mount", queue
    )
    gcr = next(r for r in results if "blob_gone" in r)
    mounter = next(r for r in results if r.get("mode") == "mount")
    assert gcr["ok"] is True and gcr["blob_gone"] is True, results
    assert mounter["mounted"] is False and mounter["src_has"] is False

    verifier = TestClient(create_app(settings))
    # HTTP 语义：挂载来源已不存在 -> 202 普通上传会话（而非 201 悬空挂载）
    r = verifier.post(f"/v2/{dst}/blobs/uploads?mount={d}&from={src}")
    assert r.status_code == 202, r.text
    assert r.headers["Docker-Upload-UUID"]
    assert verifier.get(f"/v2/{dst}/blobs/{d}").status_code == 404

    # 回退会话可正常完成上传，随后标签发布与拉取成功
    loc = r.headers["Location"]
    r = verifier.put(f"{loc}?digest={d}", content=payload)
    assert r.status_code == 201, r.text
    assert verifier.get(f"/v2/{dst}/blobs/{d}").content == payload
    manifest = _layer_only_manifest(d, len(payload))
    r = verifier.put(f"/v2/{dst}/manifests/v1", content=manifest,
                     headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 201
    assert verifier.get(f"/v2/{dst}/manifests/v1").content == manifest


def test_reupload_then_publish_first_gc_skips_reachable(client, settings):
    """重传/标签先发布：候选重新可达，随后 GC 复核跳过，blob 可拉取。"""
    repo = "race/first"
    payload = b"reupload and publish before gc executes"
    plan_id, d = _gc_candidate(client, settings, repo, payload)

    # 复核窗口内重传同摘要（去重，不丢内容）并发布标签
    r = client.post(f"/v2/{repo}/blobs/uploads?digest={d}", content=payload)
    assert r.status_code == 201
    manifest = _layer_only_manifest(d, len(payload))
    r = client.put(f"/v2/{repo}/manifests/v1", content=manifest,
                   headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 201

    rep = client.post("/gc", json={"dry_run": False, "plan_id": plan_id}).json()
    assert rep["summary"]["blobs_deleted"] == 0
    assert rep["skipped"]["blobs"][0]["digest"] == d
    assert client.get(f"/v2/{repo}/blobs/{d}").content == payload
    assert client.get(f"/v2/{repo}/manifests/v1").content == manifest


# ---------------- 重启恢复 ----------------

def test_upload_survives_restart(settings):
    data = b"resume after restart" * 10
    c1 = TestClient(create_app(settings))
    r = c1.post("/v2/app/blobs/uploads")
    loc = r.headers["Location"]
    c1.patch(loc, content=data[:50])

    # 模拟重启：同一数据目录创建新应用实例
    c2 = TestClient(create_app(settings))
    r = c2.get(loc)
    assert r.status_code == 204
    assert r.headers["Range"] == "0-49"  # 进度从磁盘恢复

    c2.patch(loc, content=data[50:], headers={"Content-Range": f"50-{len(data) - 1}"})
    r = c2.put(f"{loc}?digest={digest_of(data)}")
    assert r.status_code == 201
    assert c2.get(f"/v2/app/blobs/{digest_of(data)}").content == data
