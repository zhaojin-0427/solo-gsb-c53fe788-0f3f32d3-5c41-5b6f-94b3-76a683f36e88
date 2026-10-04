"""核心仓库逻辑：分块上传、manifest 发布（CAS）、垃圾回收。

并发模型（两层协调）：

1. 进程内一把 threading.RLock 串行化同一进程内的元数据写事务与 GC；拉取
   blob 时在锁内打开文件句柄，锁外流式发送——即使 GC 随后删除文件，
   POSIX 下已打开的 fd 依然可读，因此并发拉取不会被误删打断。
2. 跨进程（多个进程挂载同一数据目录）时进程锁互不相见，由一把
   CrossProcessLock（fcntl.flock 文件锁）串行化三类关键区间：

   - 发布 manifest：从 config/layers/子 manifest 引用存在性校验、
     manifest 落库到标签 CAS 提交，全程持排他锁；
   - blob 提交：单块整体上传（POST ?digest=）、分块上传完成
     （PUT ?digest=）以及跨仓库挂载（?mount=）的「内容就位 ->
     blobs/repo_blobs 关联落库」全程持排他锁；分块的最后落盘与大文件
     sha256 摘要复核在会话独占临时文件上、跨进程锁外完成，不阻塞其它
     仓库的并发提交；
   - GC 执行：gc_plans 计划原子认领（pending -> executing）之后，
     从可达性/租约复核、元数据删除到 blob 文件删除，全程持排他锁。

   三个区间因此跨进程严格串行。发布与 GC 之间杜绝「发布引用校验已过、
   标签未提交，GC 先删了 blob；随后标签可见但 blob 404」的悬空引用：

   * 发布先完成 -> 标签已提交，GC 复核时 blob/候选 manifest 重新可达，
     进入 skipped 而不是删除；
   * GC 先完成 -> 引用的 blob 元数据与文件都已删除，发布校验失败，
     PUT 返回 400 MANIFEST_BLOB_UNKNOWN 且标签未写入；既有标签未变，
     其引用全部可拉取。

   同摘要重传（单块/分块提交）与跨仓库挂载和 GC 之间同理，杜绝
   「GC 已删 blob 元数据、文件未删，重传方 commit_blob 见旧文件而丢弃
   新内容却重建关联，GC 随后删文件，标签发布 201 而 blob 404」：

   * GC 先完成 -> 重传方在锁外等到元数据与文件都删完才继续，
     os.replace 把刚校验过的完整内容落为新文件，并重建 blobs/
     repo_blobs 记录（新记录受宽限期保护），随后发布标签 201、
     拉取 blob 200；挂载方则观察到 blob 已不存在，按规范回退为
     普通上传会话（202）；
   * 重传/挂载先完成 -> 元数据关联先于 GC 成为既成事实；若此时标签
     已发布，GC 复核到候选重新可达而跳过；若始终没有标签引用，GC
     按可达性/宽限期正常复核，不会出现有记录无文件的悬空 blob。

   flock 随进程死亡由内核自动释放，不会形成僵死锁；dry-run 持共享锁
   拍摄快照，多个 dry-run 可并行，但都不会看到发布进行到一半的状态。

GC 执行本身还不靠进程锁防重：gc_plans 表的 pending -> executing 原子认领
在 SQLite 写事务内完成并立即提交，跨进程只有一个赢家；认领失败者返回
409，不复核、不删除任何候选。认领成功后若异常或崩溃，计划永久停留在
executing，不可重放（再次提交同样 409），只能重新 dry-run。
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import errors as E
from .config import Settings
from .coord import CrossProcessLock
from .db import Database
from .digest import is_digest, is_tag, sha256_bytes, sha256_file
from .storage import FileStorage

DEFAULT_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def parse_manifest_refs(content: bytes):
    """解析 manifest，返回 (blob_digests, manifest_digests, media_type|None)。

    blob 引用：config、layers[]；manifest 引用：manifests[]（index）、subject（referrers）。
    结构非法时抛 ValueError。
    """
    doc = json.loads(content.decode("utf-8"))
    if not isinstance(doc, dict):
        raise ValueError("manifest must be a JSON object")
    blobs, manifests = [], []

    def _digest_of(descriptor, field):
        if not isinstance(descriptor, dict) or "digest" not in descriptor:
            raise ValueError(f"invalid {field} descriptor: missing digest")
        d = descriptor["digest"]
        if not is_digest(d):
            raise ValueError(f"invalid digest in {field}: {d!r}")
        return d

    if "config" in doc:
        blobs.append(_digest_of(doc["config"], "config"))
    layers = doc.get("layers")
    if layers is not None:
        if not isinstance(layers, list):
            raise ValueError("layers must be a list")
        for layer in layers:
            blobs.append(_digest_of(layer, "layers[]"))
    children = doc.get("manifests")
    if children is not None:
        if not isinstance(children, list):
            raise ValueError("manifests must be a list")
        for child in children:
            manifests.append(_digest_of(child, "manifests[]"))
    if "subject" in doc:
        manifests.append(_digest_of(doc["subject"], "subject"))
    return blobs, manifests, doc.get("mediaType")


def parse_content_range_start(value: str) -> int:
    """接受 '0-511' / 'bytes 0-511' / 'bytes 0-511/1024'，返回起始偏移。"""
    text = value.strip()
    if text.lower().startswith("bytes "):
        text = text[6:]
    text = text.split("/", 1)[0]
    start = text.split("-", 1)[0].strip()
    if not start.isdigit():
        raise E.upload_invalid(f"invalid Content-Range header: {value!r}")
    return int(start)


class Registry:
    def __init__(self, settings: Settings):
        self.settings = settings
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.storage = FileStorage(settings)
        self.db = Database(settings.db_path)
        self.lock = threading.RLock()
        # 跨进程协调：发布临界区（引用校验->标签提交）与 GC 临界区
        # （复核->元数据/文件删除）在同一把 flock 上串行
        self.xlock = CrossProcessLock(settings.lock_path)
        # 跨进程认领计划时的认领者标识（仅写入 gc_plans.claimer 供诊断）
        self.instance_id = (
            f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )
        self._reconcile()

    # ---------------- 启动恢复 ----------------

    def _reconcile(self):
        """重启后恢复：以上传临时文件为准修正会话进度；清理孤儿 blob 文件。"""
        with self.lock:
            for u in self.db.list_uploads(state="active"):
                path = self.storage.upload_path(u["id"])
                if not path.exists():
                    self.db.set_upload_state(u["id"], "cancelled")
                    continue
                actual = path.stat().st_size
                if actual != u["size"]:
                    self.db.update_upload_progress(u["id"], actual, u["expires_at"])
            known = {b["digest"] for b in self.db.all_blobs()}
            self.storage.sweep_orphan_blobs(
                known, older_than_ts=time.time() - self.settings.gc_grace_seconds
            )

    # ---------------- 上传会话 ----------------

    def start_upload(self, repo: str) -> dict:
        with self.lock:
            upload_id = uuid.uuid4().hex
            now = time.time()
            self.db.create_upload(
                upload_id, repo, now, now + self.settings.upload_lease_seconds
            )
            self.storage.upload_path(upload_id).touch()
            return self.db.get_upload(upload_id)

    def _active_upload(self, repo: str, upload_id: str) -> dict:
        u = self.db.get_upload(upload_id)
        if u is None or u["repo"] != repo or u["state"] != "active":
            raise E.upload_unknown(upload_id)
        if u["expires_at"] <= time.time():
            raise E.upload_unknown(upload_id)  # 租约已过期，等 GC 回收
        return u

    def _renew_lease(self, upload_id: str) -> float:
        return time.time() + self.settings.upload_lease_seconds

    def get_upload_status(self, repo: str, upload_id: str) -> dict:
        with self.lock:
            u = self._active_upload(repo, upload_id)
            self.db.update_upload_progress(u["id"], u["size"], self._renew_lease(u["id"]))
            return self.db.get_upload(upload_id)

    def append_chunk(self, repo: str, upload_id: str, chunk: bytes,
                     content_range: "str | None" = None) -> int:
        with self.lock:
            u = self._active_upload(repo, upload_id)
            path = self.storage.upload_path(upload_id)
            if not path.exists():
                raise E.upload_unknown(upload_id)
            if content_range:
                start = parse_content_range_start(content_range)
                if start != u["size"]:
                    raise E.range_invalid(u["size"], content_range)
            with open(path, "ab") as f:
                f.write(chunk)
            new_size = u["size"] + len(chunk)
            self.db.update_upload_progress(upload_id, new_size, self._renew_lease(upload_id))
            return new_size

    def finalize_upload(self, repo: str, upload_id: str, digest: str,
                        chunk: "bytes | None" = None):
        """校验摘要并落库。已 committed 的会话允许同 digest 幂等重试。"""
        with self.lock:
            u = self.db.get_upload(upload_id)
            if u is None or u["repo"] != repo:
                raise E.upload_unknown(upload_id)
            if u["state"] == "committed":
                if u["digest"] == digest and self.db.repo_has_blob(repo, digest):
                    blob = self.db.get_blob(digest)
                    return digest, blob["size"] if blob else 0
                raise E.upload_invalid(
                    "upload already committed with a different digest",
                    {"upload_id": upload_id},
                )
            if u["state"] != "active" or u["expires_at"] <= time.time():
                raise E.upload_unknown(upload_id)
            path = self.storage.upload_path(upload_id)
            if not path.exists():
                raise E.upload_unknown(upload_id)

            # 最后分块落盘与摘要复核在进程锁内、跨进程锁外完成：临时文件
            # 为本会话独占（其它进程/GC 都不写它），大文件 sha256 可能耗时，
            # 不持全局 flock 以免阻塞其它仓库的并发上传与 GC。进程锁一直持有，
            # 保证同进程的 PATCH 追加/DELETE 取消不会插入到复核与提交之间。
            if chunk:
                with open(path, "ab") as f:
                    f.write(chunk)
            computed = sha256_file(path)
            if computed != digest:
                # 数据不可信，取消会话让客户端重新上传
                self.storage.delete_upload_file(upload_id)
                self.db.set_upload_state(upload_id, "cancelled")
                raise E.digest_mismatch(expected=digest, computed=computed)

            # 跨进程临界区（只包住原子就位 -> 关联落库 -> committed）：
            # 与 GC 的「复核 -> 元数据删除 -> 文件删除」严格串行。等锁期间
            # 另一进程可能已用同一会话幂等提交，故拿锁后重新判定状态。
            # GC 先删完时这里目标文件已不存在，os.replace 必然把上面刚复核
            # 过的完整内容落为新文件并重建关联（而非见旧文件丢弃新内容）。
            with self.xlock.exclusive():
                u = self.db.get_upload(upload_id)
                if u is not None and u["repo"] == repo and u["state"] == "committed":
                    if u["digest"] == digest and self.db.repo_has_blob(repo, digest):
                        blob = self.db.get_blob(digest)
                        return digest, blob["size"] if blob else 0
                    raise E.upload_invalid(
                        "upload already committed with a different digest",
                        {"upload_id": upload_id},
                    )
                if (u is None or u["repo"] != repo or u["state"] != "active"
                        or u["expires_at"] <= time.time()
                        or not self.storage.upload_path(upload_id).exists()):
                    raise E.upload_unknown(upload_id)

                size = self.storage.commit_blob(path, digest)  # 按摘要去重
                self.db.create_blob(digest, size)
                self.db.link_repo_blob(repo, digest)
                self.db.set_upload_state(upload_id, "committed", digest)
                return digest, size

    def monolithic_upload(self, repo: str, payload: bytes, digest: str):
        computed = sha256_bytes(payload)
        if computed != digest:
            raise E.digest_mismatch(expected=digest, computed=computed)
        # 摘要已在内存复核。写入独占临时文件（随机 uuid，无其它请求会碰它）
        # 在跨进程锁外完成；flock 只包住「就位 -> 关联落库」这一小段，与 GC
        # 的「删元数据 -> 删文件」原子互斥。否则 GC 可能在「已删 blobs 记录、
        # 未删文件」的窗口里与本请求交错：commit_blob 见旧文件丢弃新内容，却
        # 重建关联，GC 随后删文件 -> 记录悬空（标签发布 201、blob 404）。
        with self.lock:
            tmp = self.storage.upload_path(uuid.uuid4().hex)
            try:
                with open(tmp, "wb") as f:
                    f.write(payload)
                with self.xlock.exclusive():
                    size = self.storage.commit_blob(tmp, digest)
                    self.db.create_blob(digest, size)
                    self.db.link_repo_blob(repo, digest)
            except BaseException:
                tmp.unlink(missing_ok=True)
                raise
            return digest, size

    def mount_blob(self, repo: str, digest: str) -> bool:
        """跨仓库挂载：blob 已存在则直接关联，实现秒级去重推送。

        检查与关联在跨进程排他临界区内完成，与 GC 删除串行：GC 先删完
        时本方法观察到 blob 不存在而返回 False（路由按规范回退为普通
        上传会话）；本方法先关联成功时，随后的 GC 复核若候选仍无标签
        引用则按宽限期正常处理，不会产生有记录无文件的悬空关联。
        """
        with self.lock, self.xlock.exclusive():
            if not self.db.blob_exists(digest) or not self.storage.has_blob(digest):
                return False
            self.db.link_repo_blob(repo, digest)
            return True

    def cancel_upload(self, repo: str, upload_id: str):
        with self.lock:
            u = self.db.get_upload(upload_id)
            if u is None or u["repo"] != repo:
                raise E.upload_unknown(upload_id)
            self.storage.delete_upload_file(upload_id)
            self.db.delete_upload(upload_id)

    # ---------------- blob 读取 ----------------

    def open_blob(self, repo: str, digest: str):
        """返回 (file, size)。文件句柄在锁内打开，GC 之后删文件不影响读取。"""
        with self.lock:
            if not self.db.repo_has_blob(repo, digest):
                raise E.blob_unknown(repo, digest)
            try:
                f = self.storage.open_blob(digest)
            except FileNotFoundError:
                raise E.blob_unknown(repo, digest)
            return f, os.fstat(f.fileno()).st_size

    def stat_blob(self, repo: str, digest: str) -> int:
        with self.lock:
            f, size = self.open_blob(repo, digest)
            f.close()
            return size

    # ---------------- manifest 发布 / 读取 ----------------

    def put_manifest(self, repo: str, ref: str, content: bytes,
                     content_type: "str | None", expected: "str | None") -> str:
        if not content:
            raise E.manifest_invalid("empty manifest body")
        digest = sha256_bytes(content)
        if is_digest(ref):
            if ref != digest:  # digest 寻址的 manifest 不可伪造
                raise E.digest_mismatch(expected=ref, computed=digest)
            tag = None
        elif is_tag(ref):
            tag = ref
        else:
            raise E.tag_invalid(ref)

        try:
            blob_refs, child_refs, doc_media_type = parse_manifest_refs(content)
        except (ValueError, UnicodeDecodeError) as exc:
            raise E.manifest_invalid(str(exc))
        media_type = (
            (content_type or "").split(";")[0].strip()
            or doc_media_type
            or DEFAULT_MANIFEST_MEDIA_TYPE
        )

        with self.lock, self.xlock.exclusive():
            # 跨进程临界区：引用存在性校验 -> manifest 落库 -> 标签 CAS 提交
            # 全程与 GC 的复核/删除串行。GC 先删掉候选 blob 时，这里的校验
            # 立刻失败（400 MANIFEST_BLOB_UNKNOWN），manifest 与标签都不会
            # 写入；本发布先提交标签时，GC 复核会发现候选重新可达而跳过。
            #
            # 校验层引用：config/layers 必须是本 repo 已存在的 blob，
            # index 子 manifest / subject 必须已存在，杜绝悬空引用
            missing_blobs = [d for d in blob_refs if not self.db.repo_has_blob(repo, d)]
            if missing_blobs:
                raise E.manifest_blob_unknown(repo, missing_blobs)
            missing_manifests = [d for d in child_refs if not self.db.manifest_exists(repo, d)]
            if missing_manifests:
                raise E.manifest_blob_unknown(repo, missing_manifests)

            self.db.put_manifest(repo, digest, media_type, content)
            if tag is not None:
                status, current = self.db.cas_tag(repo, tag, expected, digest)
                if status == "conflict":
                    raise E.tag_conflict(repo, tag, current, expected)
            return digest

    def get_manifest(self, repo: str, ref: str) -> dict:
        with self.lock:
            if is_digest(ref):
                row = self.db.get_manifest_by_digest(repo, ref)
            elif is_tag(ref):
                row = self.db.get_manifest_by_tag(repo, ref)
            else:
                raise E.tag_invalid(ref)
            if row is None:
                raise E.manifest_unknown(repo, ref)
            return row

    def list_tags(self, repo: str) -> list:
        with self.lock:
            return self.db.list_tags(repo)

    # ---------------- 垃圾回收 ----------------
    #
    # 两阶段复核 + 跨进程串行：
    #   dry-run  -> 持共享 flock 快照候选清单并持久化为 plan（plan_id），
    #               只出报告不删除；
    #   execute  -> 必须携带 dry-run 返回的 plan_id；先在 SQLite 中把计划
    #               从 pending 原子认领为 executing 并提交（跨进程防重，只有
    #               认领成功者继续），再持排他 flock：重算可达性/租约，只删除
    #               “计划内且仍符合条件”的对象（先元数据后文件），全部删完才
    #               标记 executed。
    # 排他 flock 与 manifest 发布临界区（引用校验->标签提交）及 blob 提交
    # 临界区（单块重传/分块完成/跨仓库挂载的内容就位->关联落库）互斥，因此：
    # dry-run 之后新产生的垃圾（如标签移动刚制造的不可达对象）不在计划内，
    # 绝不会被未审阅地删除；重新可达或续租的对象自动跳过；发布先提交标签时
    # 候选重新可达被跳过，GC 先删完时发布的引用校验失败（400，标签不写入）。
    # 同摘要重传/挂载同样被串行：重传先完成则按可达性/宽限期正常复核，GC
    # 先删完则重传落下完整新文件并重建关联（挂载方按规范回退普通上传），
    # 成功发布的标签引用必然可拉取。
    # 两个进程同时执行同一计划时，只有认领成功的一方删除，另一方 409 且不碰
    # 任何候选。认领后异常或崩溃，计划保持 executing（不可重放，flock 由内核
    # 自动释放），剩余候选可重新 dry-run 审阅。

    PLAN_TTL_SECONDS = 3600              # 计划生成后 1 小时内有效
    # executing 认领被视为“正在执行”的宽限：超过该时长仍未完成，认领失败方
    # 的报错提示认领方可能已崩溃；无论新旧，计划都不会重放
    CLAIM_STALE_SECONDS = 5 * 60
    EXECUTED_PLAN_RETENTION_SECONDS = 7 * 24 * 3600
    EXPIRED_PLAN_RETENTION_SECONDS = 30 * 24 * 3600

    def _reachable_sets(self) -> "tuple[set, set]":
        """以所有 tag 为根遍历 manifest 图，返回 (可达 manifest 键集, 可达 blob 集)。"""
        reachable_manifests: set = set()
        reachable_blobs: set = set()
        stack = [(t["repo"], t["digest"]) for t in self.db.all_tags()]
        while stack:
            key = stack.pop()
            if key in reachable_manifests:
                continue
            row = self.db.get_manifest_by_digest(*key)
            if row is None:
                continue
            reachable_manifests.add(key)
            try:
                blobs, children, _ = parse_manifest_refs(row["content"])
            except (ValueError, UnicodeDecodeError):
                continue  # 发布时已校验，理论上不会到这里
            reachable_blobs.update(blobs)
            stack.extend((key[0], c) for c in children)
        return reachable_manifests, reachable_blobs

    def _collect_candidates(self, now: float, grace: int) -> dict:
        """在当前状态下计算 GC 候选与受保护对象（调用方持锁）。"""
        reachable_manifests, reachable_blobs = self._reachable_sets()

        dead_manifests = [
            m for m in self.db.all_manifests()
            if (m["repo"], m["digest"]) not in reachable_manifests
        ]

        grace_cutoff = now - grace
        dead_blobs, grace_protected = [], []
        for b in self.db.all_blobs():
            if b["digest"] in reachable_blobs:
                continue
            (grace_protected if b["created_at"] > grace_cutoff else dead_blobs).append(b)

        active_uploads = self.db.list_uploads(state="active")
        expired_uploads = [u for u in active_uploads if u["expires_at"] <= now]
        leased_uploads = [u for u in active_uploads if u["expires_at"] > now]

        return {
            "reachable_manifests": reachable_manifests,
            "reachable_blobs": reachable_blobs,
            "dead_blobs": dead_blobs,
            "grace_protected": grace_protected,
            "dead_manifests": dead_manifests,
            "expired_uploads": expired_uploads,
            "leased_uploads": leased_uploads,
        }

    def gc_dry_run(self, grace_seconds: "int | None" = None) -> dict:
        """生成复核计划：候选快照持久化（重启有效），不删除任何数据。"""
        grace = self.settings.gc_grace_seconds if grace_seconds is None else grace_seconds
        if grace < 0:
            raise E.gc_invalid_request(
                "grace_seconds must be >= 0", {"grace_seconds": grace}
            )
        with self.lock, self.xlock.shared():
            now = time.time()
            cand = self._collect_candidates(now, grace)
            dead_blobs, dead_manifests = cand["dead_blobs"], cand["dead_manifests"]
            expired_uploads = cand["expired_uploads"]
            repos_map = self.db.repos_for_blobs([b["digest"] for b in dead_blobs])

            plan_id = uuid.uuid4().hex
            self.db.prune_gc_plans(
                executed_before=now - self.EXECUTED_PLAN_RETENTION_SECONDS,
                pending_before=now - self.EXPIRED_PLAN_RETENTION_SECONDS,
            )
            self.db.create_gc_plan(
                plan_id, grace, now, self.PLAN_TTL_SECONDS,
                blobs=[{"digest": b["digest"], "size": b["size"],
                        "repos": sorted(repos_map.get(b["digest"], []))}
                       for b in dead_blobs],
                manifests=[{"repo": m["repo"], "digest": m["digest"],
                            "media_type": m["media_type"]} for m in dead_manifests],
                uploads=[u["id"] for u in expired_uploads],
            )

            return {
                "dry_run": True,
                "executed": False,
                "plan_id": plan_id,
                "grace_seconds": grace,
                "generated_at": _iso(now),
                "expires_at": _iso(now + self.PLAN_TTL_SECONDS),
                "summary": {
                    "blobs_to_delete": len(dead_blobs),
                    "manifests_to_delete": len(dead_manifests),
                    "uploads_to_delete": len(expired_uploads),
                    "bytes_to_reclaim": sum(b["size"] for b in dead_blobs)
                    + sum(u["size"] for u in expired_uploads),
                    "manifests_reachable": len(cand["reachable_manifests"]),
                    "blobs_reachable": len(cand["reachable_blobs"]),
                    "blobs_kept_by_grace": len(cand["grace_protected"]),
                    "uploads_kept_by_lease": len(cand["leased_uploads"]),
                },
                "delete": {
                    "blobs": [
                        {"digest": b["digest"], "size": b["size"],
                         "repos": sorted(repos_map.get(b["digest"], [])),
                         "reason": "unreferenced and older than grace period"}
                        for b in sorted(dead_blobs, key=lambda x: x["digest"])
                    ],
                    "manifests": [
                        {"repo": m["repo"], "digest": m["digest"],
                         "media_type": m["media_type"], "reason": "untagged"}
                        for m in sorted(dead_manifests,
                                        key=lambda x: (x["repo"], x["digest"]))
                    ],
                    "uploads": [
                        {"id": u["id"], "repo": u["repo"], "size": u["size"],
                         "expired_at": _iso(u["expires_at"]), "reason": "lease expired"}
                        for u in sorted(expired_uploads, key=lambda x: x["id"])
                    ],
                },
                "keep": {
                    "blobs_grace_protected": [
                        {"digest": b["digest"], "size": b["size"],
                         "age_seconds": round(now - b["created_at"], 3)}
                        for b in sorted(cand["grace_protected"],
                                        key=lambda x: x["digest"])
                    ],
                    "uploads_leased": [
                        {"id": u["id"], "repo": u["repo"], "size": u["size"],
                         "expires_at": _iso(u["expires_at"])}
                        for u in sorted(cand["leased_uploads"], key=lambda x: x["id"])
                    ],
                },
            }

    def gc_execute(self, plan_id: "str | None") -> dict:
        """按已复核计划执行 GC。

        拒绝条件（不删任何数据）：plan_id 缺失（400）、未知（404）、生成已
        超过 1 小时（410）、已执行（409）。跨进程并发执行同一计划时，先在
        SQLite 中原子认领（pending -> executing，立即提交）：只有认领成功者
        继续复核与删除，竞争失败的一方收到 409 GC_PLAN_IN_PROGRESS，不复核、
        不碰任何候选。认领成功后在跨进程排他 flock 内以计划记录的宽限期重算
        可达性与租约，只删除计划内仍符合条件的对象（先元数据后文件）；该
        flock 与 manifest 发布临界区互斥——发布先提交标签则候选重新可达被
        跳过，本区间先删完则发布的引用校验失败（400 MANIFEST_BLOB_UNKNOWN，
        标签不写入）；计划外的新候选、重新可达或续租的对象一律跳过；全部
        删除完成后才把计划标记为 executed。

        认领之后若抛异常或进程崩溃，计划保持 executing——这是不可重放的
        执行中状态：再次提交同一 plan_id 仍返回 409，已删部分不会重复执行，
        剩余候选只能重新 dry-run 生成新计划审阅；flock 由内核随进程死亡
        自动释放。
        """
        if not plan_id or not isinstance(plan_id, str) or not plan_id.strip():
            raise E.gc_plan_required()
        plan_id = plan_id.strip()

        # ---- 0. 保留未知/过期/已执行计划的原有错误（认领前读一次） ----
        plan = self.db.get_gc_plan(plan_id)
        if plan is None:
            raise E.gc_plan_unknown(plan_id)
        if plan["state"] == "executed":
            raise E.gc_plan_already_executed(
                plan_id, _iso(plan["executed_at"]) if plan["executed_at"] else None
            )
        now = time.time()
        if plan["state"] == "pending" and plan["expires_at"] <= now:
            raise E.gc_plan_expired(plan_id, _iso(plan["expires_at"]))

        # ---- 1. 跨进程原子认领：pending + 未过期 -> executing，并立即提交。
        #         此后任何异常/崩溃都不回滚认领，保证计划只能被执行一次。 ----
        outcome = self.db.claim_gc_plan(plan_id, now, self.instance_id)
        if outcome != "claimed":
            # 竞争失败：另一进程正在执行（或认领方已崩溃留下 executing）；
            # 也可能在上面读取后刚好过期/被清理——重新读取以套用原有错误。
            latest = self.db.get_gc_plan(plan_id)
            if latest is None:
                raise E.gc_plan_unknown(plan_id)
            if latest["state"] == "executed":
                raise E.gc_plan_already_executed(
                    plan_id, _iso(latest["executed_at"]) if latest["executed_at"] else None
                )
            if latest["state"] == "pending":
                # 极窄窗口：claim 因过期未命中而状态仍为 pending
                if latest["expires_at"] <= time.time():
                    raise E.gc_plan_expired(
                        plan_id, _iso(latest["expires_at"])
                    )
                raise E.gc_plan_in_progress(plan_id, None, None)
            # state == executing：不碰候选，直接 409
            claimed_at = latest["claimed_at"]
            stale = claimed_at is None or now - claimed_at > self.CLAIM_STALE_SECONDS
            raise E.gc_plan_in_progress(
                plan_id,
                _iso(claimed_at) if claimed_at else None,
                latest["claimer"],
                stale=stale,
            )

        # ---- 2. 认领成功：以下区间同时持有进程锁与跨进程排他 flock。
        #         可达性复核 -> 元数据删除 -> blob 文件删除 -> 标记 executed
        #         与任何并发发布（引用校验->标签提交）及 blob 提交
        #         （单块重传/分块完成/跨仓库挂载的内容就位->关联落库）
        #         跨进程严格串行：
        #         本区间内发布无法完成标签提交，因此复核时看不到的标签在
        #         删除完成前不会出现；区间结束后才发布的请求会重新校验并
        #         发现 blob 已不存在（400 MANIFEST_BLOB_UNKNOWN，标签不变）；
        #         同摘要重传在「已删元数据、未删文件」窗口内无法插入，
        #         本区间先完成时它拿到锁后文件也已删除，os.replace 落下
        #         完整新文件并重建关联（新记录受宽限期保护），重传先完成
        #         时则按复核结果可达跳过/超期正常删除。
        #         异常时计划留在 executing，不回滚、不重放；flock 由内核
        #         在进程死亡时自动释放，不会僵死。 ----
        with self.lock, self.xlock.exclusive():
            plan = self.db.get_gc_plan(plan_id)
            grace = plan["grace_seconds"]
            now = time.time()
            grace_cutoff = now - grace
            reachable_manifests, reachable_blobs = self._reachable_sets()

            # ---- 逐候选复核：只认计划内对象的当前状态 ----
            plan_blobs = self.db.get_gc_plan_blobs(plan_id)
            plan_manifests = self.db.get_gc_plan_manifests(plan_id)
            plan_upload_ids = self.db.get_gc_plan_uploads(plan_id)

            delete_blobs, skip_blobs = [], []
            for entry in sorted(plan_blobs, key=lambda x: x["digest"]):
                digest = entry["digest"]
                row = self.db.get_blob(digest)
                if row is None:
                    skip_blobs.append({"digest": digest,
                                       "reason": "no longer present"})
                elif digest in reachable_blobs:
                    skip_blobs.append({"digest": digest,
                                       "reason": "reachable again: referenced by a"
                                                 " tagged manifest"})
                elif row["created_at"] > grace_cutoff:
                    skip_blobs.append({"digest": digest,
                                       "reason": "within grace period again"})
                else:
                    delete_blobs.append({"digest": digest, "size": row["size"]})

            delete_manifests, skip_manifests = [], []
            for m in sorted(plan_manifests, key=lambda x: (x["repo"], x["digest"])):
                key = (m["repo"], m["digest"])
                row = self.db.get_manifest_by_digest(*key)
                if row is None:
                    skip_manifests.append({"repo": key[0], "digest": key[1],
                                           "reason": "no longer present"})
                elif key in reachable_manifests:
                    skip_manifests.append({"repo": key[0], "digest": key[1],
                                           "reason": "reachable again: a tag (or a"
                                                     " tagged parent) points to it"})
                else:
                    delete_manifests.append({"repo": key[0], "digest": key[1],
                                             "media_type": m["media_type"]})

            delete_uploads, skip_uploads = [], []
            for upload_id in sorted(plan_upload_ids):
                u = self.db.get_upload(upload_id)
                if u is None:
                    skip_uploads.append({"id": upload_id,
                                         "reason": "no longer present"})
                elif u["state"] != "active":
                    skip_uploads.append({"id": upload_id, "repo": u["repo"],
                                         "reason": f"upload {u['state']}"})
                elif u["expires_at"] > now:
                    skip_uploads.append({"id": upload_id, "repo": u["repo"],
                                         "reason": "lease renewed or still active"})
                else:
                    delete_uploads.append(u)

            # ---- 实际删除（先元数据后文件）：进程锁 + 跨进程 flock 与
            #      并发发布互斥；拉取在打开文件句柄后流式发送，随后删文件
            #      不影响进行中的下载 ----
            for u in delete_uploads:
                self.storage.delete_upload_file(u["id"])
                self.db.delete_upload(u["id"])
            self.db.delete_manifests(
                [(m["repo"], m["digest"]) for m in delete_manifests]
            )
            digests = [b["digest"] for b in delete_blobs]
            repos_map = self.db.repos_for_blobs(digests)
            self.db.delete_blobs(digests)
            for d in digests:
                self.storage.delete_blob(d)  # 崩溃残留由启动时 sweep 兜底

            # 全部候选处理完毕才允许标记 executed；若标记未命中（状态被外部
            # 改走），不回滚删除，也不返回成功——计划留在非 executed 状态。
            if not self.db.mark_gc_plan_executed(plan_id, now):
                raise RuntimeError(
                    f"gc plan {plan_id!r} vanished from executing state during run"
                )

            return {
                "dry_run": False,
                "executed": True,
                "plan_id": plan_id,
                "grace_seconds": grace,
                "generated_at": _iso(plan["created_at"]),
                "executed_at": _iso(now),
                "summary": {
                    "blobs_deleted": len(delete_blobs),
                    "manifests_deleted": len(delete_manifests),
                    "uploads_deleted": len(delete_uploads),
                    "blobs_skipped": len(skip_blobs),
                    "manifests_skipped": len(skip_manifests),
                    "uploads_skipped": len(skip_uploads),
                    "bytes_reclaimed": sum(b["size"] for b in delete_blobs)
                    + sum(u["size"] for u in delete_uploads),
                },
                "deleted": {
                    "blobs": [
                        {"digest": b["digest"], "size": b["size"],
                         "repos": sorted(repos_map.get(b["digest"], [])),
                         "reason": "still unreferenced at execution"}
                        for b in delete_blobs
                    ],
                    "manifests": [
                        {"repo": m["repo"], "digest": m["digest"],
                         "media_type": m["media_type"],
                         "reason": "still untagged at execution"}
                        for m in delete_manifests
                    ],
                    "uploads": [
                        {"id": u["id"], "repo": u["repo"], "size": u["size"],
                         "reason": "lease still expired at execution"}
                        for u in delete_uploads
                    ],
                },
                "skipped": {
                    "blobs": skip_blobs,
                    "manifests": skip_manifests,
                    "uploads": skip_uploads,
                },
            }
