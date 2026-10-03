"""SQLite 元数据层。

每个进程持有一个连接，进程内访问通过一把 RLock 串行化；SQLite 开启 WAL，
写事务由 `with conn:` 保证提交/回滚，并设置 busy_timeout——多个进程挂载同一
数据目录时，写锁冲突会等待而非立刻报错。表设计：

- blobs       全局内容寻址 blob（按摘要去重）
- repo_blobs  repo -> blob 的命名空间关联（同一 blob 可被多个 repo 可见）
- uploads     上传会话（落盘持久化，重启后可恢复/续传）
- manifests   repo 内按摘要不可变存储的 manifest
- tags        repo 内 tag -> manifest digest（CAS 更新）
- gc_plans    GC 复核计划（dry-run 快照，持久化、1h 有效、只能执行一次）。
              state: pending -> executing（原子认领，已提交，跨进程互斥）
                     -> executed；认领后崩溃则永久停留在 executing，
                     不可重放，只能重新 dry-run
- gc_plan_blobs / gc_plan_manifests / gc_plan_uploads  计划内候选清单
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    digest     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS repo_blobs (
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (repo, digest)
);
CREATE TABLE IF NOT EXISTS uploads (
    id         TEXT PRIMARY KEY,
    repo       TEXT NOT NULL,
    state      TEXT NOT NULL,              -- active | committed | cancelled
    size       INTEGER NOT NULL DEFAULT 0,
    digest     TEXT,                       -- committed 时记录最终 digest（幂等重试用）
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    media_type TEXT NOT NULL,
    content    BLOB NOT NULL,
    size       INTEGER NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (repo, digest)
);
CREATE TABLE IF NOT EXISTS tags (
    repo       TEXT NOT NULL,
    name       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (repo, name)
);
CREATE TABLE IF NOT EXISTS gc_plans (
    id           TEXT PRIMARY KEY,
    state        TEXT NOT NULL,          -- pending | executing | executed
    grace_seconds INTEGER NOT NULL,      -- 生成时记录，执行复核以此为准
    created_at   REAL NOT NULL,
    expires_at   REAL NOT NULL,          -- created_at + 1h，过期计划拒绝执行
    executed_at  REAL,
    claimed_at   REAL,                   -- 认领为 executing 的时刻（跨进程互斥）
    claimer      TEXT                    -- 认领者标识（host:pid:instance），仅诊断用
);
CREATE TABLE IF NOT EXISTS gc_plan_blobs (
    plan_id    TEXT NOT NULL,
    digest     TEXT NOT NULL,
    size       INTEGER NOT NULL,
    repos      TEXT NOT NULL,            -- JSON 数组：候选时关联的仓库
    PRIMARY KEY (plan_id, digest)
);
CREATE TABLE IF NOT EXISTS gc_plan_manifests (
    plan_id    TEXT NOT NULL,
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    media_type TEXT NOT NULL,
    PRIMARY KEY (plan_id, repo, digest)
);
CREATE TABLE IF NOT EXISTS gc_plan_uploads (
    plan_id    TEXT NOT NULL,
    upload_id  TEXT NOT NULL,
    PRIMARY KEY (plan_id, upload_id)
);
CREATE INDEX IF NOT EXISTS idx_gc_plans_state_created ON gc_plans (state, created_at);
"""


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        # timeout + busy_timeout：跨进程共享同一 SQLite 文件时，写者之间
        # 排队等待而不是立刻抛 database is locked
        self._conn = sqlite3.connect(
            str(path), check_same_thread=False, timeout=15.0
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=15000")
            self._conn.executescript(SCHEMA)
            self._migrate_gc_plans()

    def _migrate_gc_plans(self):
        """旧库 gc_plans 只有 pending/executed 两列，补齐认领列。"""
        cols = {r["name"] for r in self._conn.execute(
            "PRAGMA table_info(gc_plans)"
        ).fetchall()}
        if "claimed_at" not in cols:
            self._conn.execute("ALTER TABLE gc_plans ADD COLUMN claimed_at REAL")
        if "claimer" not in cols:
            self._conn.execute("ALTER TABLE gc_plans ADD COLUMN claimer TEXT")

    # ---------------- uploads ----------------

    def create_upload(self, upload_id: str, repo: str, now: float, expires_at: float):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO uploads (id, repo, state, size, digest, created_at, updated_at, expires_at)"
                " VALUES (?, ?, 'active', 0, NULL, ?, ?, ?)",
                (upload_id, repo, now, now, expires_at),
            )

    def get_upload(self, upload_id: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM uploads WHERE id = ?", (upload_id,)
            ).fetchone()
            return dict(row) if row else None

    def update_upload_progress(self, upload_id: str, size: int, expires_at: float):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE uploads SET size = ?, updated_at = ?, expires_at = ? WHERE id = ?",
                (size, time.time(), expires_at, upload_id),
            )

    def set_upload_state(self, upload_id: str, state: str, digest: "str | None" = None):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE uploads SET state = ?, digest = COALESCE(?, digest), updated_at = ? WHERE id = ?",
                (state, digest, time.time(), upload_id),
            )

    def delete_upload(self, upload_id: str):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM uploads WHERE id = ?", (upload_id,))

    def list_uploads(self, state: "str | None" = None) -> list:
        with self._lock:
            if state:
                rows = self._conn.execute(
                    "SELECT * FROM uploads WHERE state = ?", (state,)
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM uploads").fetchall()
            return [dict(r) for r in rows]

    # ---------------- blobs ----------------

    def create_blob(self, digest: str, size: int):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO blobs (digest, size, created_at) VALUES (?, ?, ?)",
                (digest, size, time.time()),
            )

    def get_blob(self, digest: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM blobs WHERE digest = ?", (digest,)
            ).fetchone()
            return dict(row) if row else None

    def blob_exists(self, digest: str) -> bool:
        return self.get_blob(digest) is not None

    def all_blobs(self) -> list:
        with self._lock:
            return [dict(r) for r in self._conn.execute("SELECT * FROM blobs").fetchall()]

    def link_repo_blob(self, repo: str, digest: str):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO repo_blobs (repo, digest, created_at) VALUES (?, ?, ?)",
                (repo, digest, time.time()),
            )

    def repo_has_blob(self, repo: str, digest: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM repo_blobs WHERE repo = ? AND digest = ?", (repo, digest)
            ).fetchone()
            return row is not None

    def repos_for_blobs(self, digests: list) -> dict:
        if not digests:
            return {}
        marks = ",".join("?" for _ in digests)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT digest, repo FROM repo_blobs WHERE digest IN ({marks})",
                digests,
            ).fetchall()
        out: dict = {d: [] for d in digests}
        for r in rows:
            out.setdefault(r["digest"], []).append(r["repo"])
        return out

    def delete_blobs(self, digests: list):
        """删除 blob 记录及其 repo 关联，并清理任何悬空关联。"""
        if not digests:
            return
        marks = ",".join("?" for _ in digests)
        with self._lock, self._conn:
            self._conn.execute(f"DELETE FROM repo_blobs WHERE digest IN ({marks})", digests)
            self._conn.execute(f"DELETE FROM blobs WHERE digest IN ({marks})", digests)
            self._conn.execute(
                "DELETE FROM repo_blobs WHERE digest NOT IN (SELECT digest FROM blobs)"
            )

    # ---------------- manifests / tags ----------------

    def put_manifest(self, repo: str, digest: str, media_type: str, content: bytes):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO manifests (repo, digest, media_type, content, size, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (repo, digest, media_type, content, len(content), time.time()),
            )

    def get_manifest_by_digest(self, repo: str, digest: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM manifests WHERE repo = ? AND digest = ?", (repo, digest)
            ).fetchone()
            return dict(row) if row else None

    def get_manifest_by_tag(self, repo: str, tag: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT m.* FROM manifests m JOIN tags t"
                " ON t.repo = m.repo AND t.digest = m.digest"
                " WHERE t.repo = ? AND t.name = ?",
                (repo, tag),
            ).fetchone()
            return dict(row) if row else None

    def manifest_exists(self, repo: str, digest: str) -> bool:
        return self.get_manifest_by_digest(repo, digest) is not None

    def all_manifests(self) -> list:
        with self._lock:
            return [dict(r) for r in self._conn.execute(
                "SELECT repo, digest, media_type, size, created_at FROM manifests"
            ).fetchall()]

    def delete_manifests(self, keys: list):
        """keys: [(repo, digest), ...]"""
        if not keys:
            return
        marks = ",".join("(?, ?)" for _ in keys)
        params = [v for key in keys for v in key]
        with self._lock, self._conn:
            self._conn.execute(
                f"DELETE FROM manifests WHERE (repo, digest) IN ({marks})", params
            )

    def cas_tag(self, repo: str, name: str, expected: "str | None", new_digest: str):
        """比较并交换 tag。返回 (status, current_digest)。

        - created: 标签不存在且未提供 expected -> 创建
        - updated: 标签存在且 expected == 当前值 -> 移动
        - conflict: 其余情况，current_digest 为当前实际值（不存在则为 None）
        """
        now = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT digest FROM tags WHERE repo = ? AND name = ?", (repo, name)
            ).fetchone()
            if row is None:
                if expected is not None:
                    return "conflict", None
                self._conn.execute(
                    "INSERT INTO tags (repo, name, digest, updated_at) VALUES (?, ?, ?, ?)",
                    (repo, name, new_digest, now),
                )
                return "created", None
            current = row["digest"]
            if expected is None or expected != current:
                return "conflict", current
            self._conn.execute(
                "UPDATE tags SET digest = ?, updated_at = ? WHERE repo = ? AND name = ?",
                (new_digest, now, repo, name),
            )
            return "updated", current

    def all_tags(self) -> list:
        with self._lock:
            return [dict(r) for r in self._conn.execute("SELECT * FROM tags").fetchall()]

    def list_tags(self, repo: str) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM tags WHERE repo = ? ORDER BY name", (repo,)
            ).fetchall()
            return [r["name"] for r in rows]

    # ---------------- GC 复核计划 ----------------

    def create_gc_plan(self, plan_id: str, grace_seconds: int, now: float,
                       ttl_seconds: int, blobs: list, manifests: list,
                       uploads: list):
        """原子写入一份 GC 计划及其全部候选（与下次 GC 互斥，持锁调用）。"""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO gc_plans (id, state, grace_seconds, created_at,"
                " expires_at, executed_at) VALUES (?, 'pending', ?, ?, ?, NULL)",
                (plan_id, grace_seconds, now, now + ttl_seconds),
            )
            self._conn.executemany(
                "INSERT INTO gc_plan_blobs (plan_id, digest, size, repos)"
                " VALUES (?, ?, ?, ?)",
                [(plan_id, b["digest"], b["size"], json.dumps(b["repos"]))
                 for b in blobs],
            )
            self._conn.executemany(
                "INSERT INTO gc_plan_manifests (plan_id, repo, digest, media_type)"
                " VALUES (?, ?, ?, ?)",
                [(plan_id, m["repo"], m["digest"], m["media_type"])
                 for m in manifests],
            )
            self._conn.executemany(
                "INSERT INTO gc_plan_uploads (plan_id, upload_id) VALUES (?, ?)",
                [(plan_id, u) for u in uploads],
            )

    def get_gc_plan(self, plan_id: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM gc_plans WHERE id = ?", (plan_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_gc_plan_blobs(self, plan_id: str) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM gc_plan_blobs WHERE plan_id = ?", (plan_id,)
            ).fetchall()
            return [{"digest": r["digest"], "size": r["size"],
                     "repos": json.loads(r["repos"])} for r in rows]

    def get_gc_plan_manifests(self, plan_id: str) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM gc_plan_manifests WHERE plan_id = ?", (plan_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_gc_plan_uploads(self, plan_id: str) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT upload_id FROM gc_plan_uploads WHERE plan_id = ?",
                (plan_id,),
            ).fetchall()
            return [r["upload_id"] for r in rows]

    def claim_gc_plan(self, plan_id: str, now: float, claimer: str) -> "str | None":
        """跨进程原子认领计划：仅当计划处于 pending 且未过期时，将其置为
        executing 并提交。返回认领后的状态：

        - "claimed"   ：本调用认领成功（唯一允许删除候选的一方）
        - "executing" ：已被其他进程认领（并发竞争失败或认领方已崩溃）
        - "executed"  ：已经执行完成
        - None        ：计划不存在（未知或已被清理）

        过期的 pending 计划不在此认领，由调用方先读取 expires_at 判定为
        410 GC_PLAN_EXPIRED；其余状态原样返回。单条 UPDATE 的写事务在
        SQLite WAL 下天然串行化，跨进程也只有一个赢家。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE gc_plans SET state = 'executing', claimed_at = ?,"
                " claimer = ? WHERE id = ? AND state = 'pending' AND expires_at > ?",
                (now, claimer, plan_id, now),
            )
            if cur.rowcount == 1:
                return "claimed"
        row = self.get_gc_plan(plan_id)
        return None if row is None else row["state"]

    def mark_gc_plan_executed(self, plan_id: str, now: float) -> bool:
        """认领方在删除全部完成后把 executing 标记为 executed，提交。

        正常路径下该计划必然处于 executing，返回 True；返回 False 只可能
        来自外部干预（状态被人为改走），调用方不回滚删除。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE gc_plans SET state = 'executed', executed_at = ?"
                " WHERE id = ? AND state = 'executing'",
                (now, plan_id),
            )
            return cur.rowcount == 1

    def prune_gc_plans(self, executed_before: float, pending_before: float):
        """删除历史计划，防止表无限增长：

        - executed 且 executed_at 超过已执行保留期；
        - pending / executing 且有效期早已超过（executing 说明认领方已崩溃，
          计划不可重放，超过保留期后同样清掉）。
        """
        with self._lock, self._conn:
            ids = [r["id"] for r in self._conn.execute(
                "SELECT id FROM gc_plans"
                " WHERE (state = 'executed' AND executed_at < ?)"
                "    OR (state IN ('pending', 'executing') AND expires_at < ?)",
                (executed_before, pending_before),
            ).fetchall()]
            if not ids:
                return
            marks = ",".join("?" for _ in ids)
            for table in ("gc_plan_blobs", "gc_plan_manifests", "gc_plan_uploads"):
                self._conn.execute(
                    f"DELETE FROM {table} WHERE plan_id IN ({marks})", ids
                )
            self._conn.execute(f"DELETE FROM gc_plans WHERE id IN ({marks})", ids)
