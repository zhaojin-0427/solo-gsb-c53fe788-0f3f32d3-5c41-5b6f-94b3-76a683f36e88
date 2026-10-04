"""真实双进程竞态演示：GC 在「已删 blob 元数据、未删文件」窗口内暂停，
另一独立进程（uvicorn 服务，等价于另一个容器）发起同摘要重传（单块与
分块）及跨仓库挂载，裁决三种交错结果。

用法：
    REGISTRY_DATA_DIR=/tmp/race-data \
        uvicorn app.main:app --host 127.0.0.1 --port 5000 &
    python3 tests/live_race_demo.py /tmp/race-data

脚本本身作为共享同一数据目录的第二个进程：以独立的 Registry/SQLite 连接
执行 dry-run 与带删除窗口暂停的 GC；所有上传/挂载/发布/拉取均经 5000
端口上的真实 HTTP API 完成。
"""
import hashlib
import json
import sys
import threading
import time
import urllib.error
import urllib.request

from app.config import Settings
from app.registry import Registry

DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else "/tmp/race-data"
BASE = "http://127.0.0.1:5000"   # 对端实例：重传/挂载/发布/拉取

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"


def digest_of(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def http(method, url, data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read(), {k.lower(): v for k, v in r.headers.items()}
    except urllib.error.HTTPError as e:
        return e.code, e.read(), {k.lower(): v for k, v in e.headers.items()}


def scenario(name, repo, payload, mode, tag):
    d = digest_of(payload)
    print(f"\n=== scenario: {name} (digest {d[:19]}..., {len(payload)} bytes) ===")

    # 1) 经端口 A 初次推送
    st, _, h = http("POST",
                    f"{BASE}/v2/{repo}/blobs/uploads?digest={d}",
                    data=payload)
    assert st == 201, (st, h)
    print(f"[A] initial push                 -> {st}")

    # 2) 独立 Registry（即端口 B 那个进程的替身）做 dry-run，grace=0
    settings = Settings(data_dir=DATA_DIR, gc_grace_seconds=0)
    reg = Registry(settings)
    with reg.db._lock:
        reg.db._conn.execute("UPDATE blobs SET created_at = 0")
        reg.db._conn.commit()
    plan = reg.gc_dry_run(grace_seconds=0)
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}
    plan_id = plan["plan_id"]
    print(f"[B] dry-run plan {plan_id[:12]}  -> candidate contains blob")

    # 3) 给 B 的删除路径装窗口钩子：删掉元数据后、删文件前暂停
    real_delete_blobs = reg.db.delete_blobs
    state = {"released": False}
    gate = threading.Event()

    def patched(digests):
        real_delete_blobs(digests)
        if d in digests:
            print("[B] deleted DB rows, file still present; holding window...")
            gate.wait(timeout=30)

    reg.db.delete_blobs = patched

    def run_gc():
        reg.gc_execute(plan_id)

    t = threading.Thread(target=run_gc)
    t.start()
    time.sleep(0.6)  # 等 GC 进入窗口

    # 4) 在窗口内经端口 A（另一个进程）发起提交
    if mode == "mono":
        st, body, h = http("POST",
                           f"{BASE}/v2/{repo}/blobs/uploads?digest={d}",
                           data=payload)
        print(f"[A] same-digest retransmit      -> {st} "
              f"(blocks on flock until GC finishes)")
        assert st == 201, body
    elif mode == "chunked":
        st, body, h = http("POST", f"{BASE}/v2/{repo}/blobs/uploads")
        assert st == 202, body
        loc = h["location"]
        st, _, h = http("PATCH", f"{BASE}{loc}", data=payload)
        assert st == 202, body
        st, body, h = http("PUT", f"{BASE}{loc}?digest={d}")
        print(f"[A] chunked finalize             -> {st} "
              f"(blocks on flock until GC finishes)")
        assert st == 201, body
    elif mode == "mount":
        st, body, h = http(
            "POST",
            f"{BASE}/v2/mount/dst/blobs/uploads?mount={d}&from={repo}")
        print(f"[A] cross-repo mount             -> {st} (202 = fell back to a"
              f" normal upload session)")
        assert st == 202, body
        gate.set()
        state["released"] = True
        t.join(timeout=15)
        print(f"[A] blob absent after fallback?  -> "
              f"{http('GET', f'{BASE}/v2/mount/dst/blobs/{d}')[0] == 404}")
        # 用回退会话完成上传
        loc = h["location"]
        st, body, _ = http("PUT", f"{BASE}{loc}?digest={d}", data=payload)
        assert st == 201, body
        d_final = d
    else:
        raise AssertionError(mode)

    if mode in ("mono", "chunked"):
        gate.set()
        state["released"] = True
        t.join(timeout=15)
        d_final = d

    # 5) GC 已删完旧文件；重传落下的完整新文件必须可读
    st, body, _ = http("GET", f"{BASE}/v2/{'mount/dst' if mode == 'mount' else repo}/blobs/{d_final}")
    assert st == 200 and body == payload, (st, body[:32])
    print(f"[A] GET retransmitted blob        -> {st}, "
          f"{len(body)} bytes, content intact")

    # 6) 发布标签 -> 201，且标签与 blob 都可拉取（修复前为 201 / 404）
    target_repo = "mount/dst" if mode == "mount" else repo
    manifest = json.dumps({
        "schemaVersion": 2, "mediaType": OCI_MANIFEST,
        "layers": [{"mediaType": OCI_LAYER, "digest": d_final,
                    "size": len(payload)}],
    }).encode()
    st, body, h = http(
        "PUT", f"{BASE}/v2/{target_repo}/manifests/{tag}", data=manifest,
        headers={"Content-Type": OCI_MANIFEST})
    assert st == 201, body
    m_digest = h.get("docker-content-digest")
    print(f"[A] PUT manifest tag {tag:<11}      -> {st} ({m_digest[:19]}...)")
    st_m, _, _ = http("GET", f"{BASE}/v2/{target_repo}/manifests/{tag}")
    st_b, _, _ = http("GET", f"{BASE}/v2/{target_repo}/blobs/{d_final}")
    assert (st_m, st_b) == (200, 200)
    print(f"[A] GET manifest / GET blob       -> {st_m} / {st_b}")
    assert state["released"]


if __name__ == "__main__":
    scenario("monolithic retransmit", "race/mono",
             b"same digest retransmitted in gc window", "mono",
             "t-" + str(int(time.time() * 1000)))
    scenario("chunked finalize", "race/chunk",
             b"chunked finalize racing gc " * 7, "chunked",
             "t-" + str(int(time.time() * 1000)))
    scenario("cross-repo mount", "race/mountsrc",
             b"mount candidate racing gc", "mount",
             "t-" + str(int(time.time() * 1000)))
    print("\nALL SCENARIOS OK: GC-first ordering leaves a complete pullable"
          " blob and a published tag; mount falls back to a normal upload.")
