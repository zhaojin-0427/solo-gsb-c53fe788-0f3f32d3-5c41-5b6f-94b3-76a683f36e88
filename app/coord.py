"""跨进程协调：基于 POSIX flock 的咨询式读写锁。

进程内 threading.RLock 无法协调挂载同一数据目录的多个进程（例如同一卷上的
两个容器、滚动升级时并存的新旧实例）。这里用一把 fcntl.flock 文件锁串行化
manifest 发布、blob 提交与 GC 之间的跨进程临界区：

- 发布（PUT /v2/.../manifests/<ref>）：从引用存在性校验、manifest 落库到
  标签 CAS 提交，全程持有排他锁（EX）；
- blob 提交（POST .../uploads?digest= 单块、PUT .../uploads/<id>?digest=
  分块完成、POST .../uploads?mount=&from= 跨仓库挂载）：从「blob 记录与
  物理文件是否都在」的一致性判定，到内容原子安装（install_blob）、blob
  记录 upsert（刷新 created_at 宽限时间戳）、repo 关联写入，全程持有
  排他锁（EX）；
- GC 执行（POST /gc dry_run:false）：计划原子认领之后，从可达性复核、
  元数据删除到 blob 文件删除，全程持有排他锁（EX）；
- GC dry-run：持共享锁（SH）拍摄候选快照，与发布/提交互斥（快照期间不会
  看到提交或发布到一半的状态），多个 dry-run 之间可并行。

因此三类关键区间跨进程严格串行。

发布与 GC 之间，标签指向的引用必然仍可拉取：

  * 发布先完成 -> 标签已提交，GC 复核时候选重新可达，进入 skipped，
    blob 与 manifest 都不删；
  * GC 先完成 -> blob 元数据与文件都已删除，发布的引用校验失败，返回
    400 MANIFEST_BLOB_UNKNOWN，标签不会写入——既有标签不变，其引用
    一律可拉取。

blob 同摘要重传/挂载与 GC 的复核删除之间（旧实现里 commit_blob 在 GC
「记录已删、文件未删」窗口见到旧文件便丢弃新内容、只重建关联，导致标签
201 但 blob 404）：

  * 重传/挂载先提交 -> created_at 刷新到当前时刻（宽限窗口重新起算），
    GC 复核时该 blob 仍在宽限期内（或已被并发发布的标签引用而可达），
    进入 skipped 不删除；
  * GC 先完成 -> blob 记录与文件都已删除，重传在锁后把摘要校验过的
    内容完整原子安装并重建记录/关联（201，之后必可拉取），挂载按规范
    回退为 202 普通上传会话。

flock 随进程死亡由内核自动释放（即使被 SIGKILL），不会留下僵死锁；锁文件
本身常驻数据目录，可被任意数量的进程重复打开。仅在 Linux/macOS 等支持
fcntl 的平台上多进程部署；单进程部署不依赖它（进程内 RLock 已足够）。
"""
from __future__ import annotations

import contextlib
import fcntl
import os
import threading
from pathlib import Path


class CrossProcessLock:
    """单文件 flock 读写锁；**线程安全**，每线程可重入。

    跨进程互斥由内核 `fcntl.flock`（按打开文件描述的咨询锁）提供；但同进程
    不同线程之间内核 flock 会互相放行（进程持有的锁不阻塞本进程其它线程），
    因此同进程内 EX 对 SH/EX 的互斥由一把 `threading.Condition` 计数门补齐。
    采用**两阶段**获取以避免「跨进程循环等待」死锁：

      阶段 1（跨进程）：每个线程各自打开一个锁文件 fd 并按模式 flock
        （SH/EX），阻塞直到**其它进程**释放；
      阶段 2（进程内）：拿到 flock 后，再在条件变量门上等待本进程名额——
        EX 要等到本进程没有其它 SH/EX；SH 在有 EX 持有者或 EX 等待者时排队
        （防止写者饿死）。

    顺序至关重要：必须先 flock 再进进程内门。若反过来「先在进程内占名额再
    等 flock」，可能出现 P1 的 SH 线程已占名额、正等 EX 的跨进程锁，而 P2
    持 EX 又在等 P1 其余 SH 释放的循环等待。两阶段顺序下所有线程都在 flock
    上排队（跨进程不构成环），flock 到手后只与**同进程**线程竞争进程内门，
    不会跨进程死锁。

    语义：

    - 同线程同模式可重入（depth 计数，复用本线程 fd）；同线程 EX/SH 不可
      互相嵌套；
    - 不同线程：EX 与一切互斥；多个 SH 并行；EX 到达后新 SH 排队防饿死；
    - 跨进程：flock 阻塞直到其它进程释放，EX/SH 语义与同进程一致。

    任何线程异常退出临界区都会释放自己的 fd；flock 随 close/进程死亡由内核
    自动释放，不会僵死。
    """

    def __init__(self, path: "str | Path"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._gate = threading.Condition()
        self._ex_owner: "int | None" = None   # 进程内持有 EX 的线程 ident
        self._sh = {}                          # {thread_ident: 重入深度}
        self._ex_waiters = 0                   # 进程内等待 EX 的线程数
        self._local = threading.local()

    @property
    def _mode(self) -> "str | None":
        return getattr(self._local, "mode", None)

    @contextlib.contextmanager
    def _hold(self, mode: str):
        ident = threading.get_ident()
        if self._mode is not None and self._mode != mode:
            raise RuntimeError(
                f"nested cross-process lock mode conflict: "
                f"{self._mode} -> {mode} (same thread cannot nest SH inside EX"
                " or vice versa)"
            )
        if self._mode == mode:
            # 同线程同模式重入：复用 fd，只加深度
            self._bump_reentrant(mode, ident, +1)
            try:
                yield
            finally:
                self._bump_reentrant(mode, ident, -1)
            return

        # 阶段 1：跨进程 flock（阻塞只可能来自其它进程，不持进程内名额）
        fd = self._open_and_flock(mode)
        gate_held = False
        try:
            # 阶段 2：进程内读写门
            self._enter_gate(mode, ident)
            gate_held = True
            self._local.mode = mode
            self._local.depth = 1
            self._local.fd = fd
            yield
        finally:
            if gate_held:
                self._leave_gate(mode, ident)
                self._local.mode = None
                self._local.depth = 0
                self._local.fd = None
            # 无论是否进过门（等待时被取消等），本线程的 flock 都要释放
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _bump_reentrant(self, mode: str, ident: int, delta: int):
        depth = getattr(self._local, "depth", 0) + delta
        self._local.depth = depth
        if mode == "sh":
            if depth > 0:
                self._sh[ident] = depth
            else:
                self._sh.pop(ident, None)
        # EX 的进程内排他身份由 _ex_owner 表示，重入不改其归属

    def _open_and_flock(self, mode: str) -> int:
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
        flags = fcntl.LOCK_EX if mode == "ex" else fcntl.LOCK_SH
        try:
            fcntl.flock(fd, flags)  # 阻塞直到其它进程释放
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _enter_gate(self, mode: str, ident: int):
        with self._gate:
            if mode == "ex":
                self._ex_waiters += 1
                try:
                    while self._ex_owner is not None or self._sh:
                        self._gate.wait()
                    self._ex_owner = ident
                finally:
                    self._ex_waiters -= 1
            else:  # sh：有 EX 持有者或 EX 等待者时排队，避免写者饿死
                while self._ex_owner is not None or self._ex_waiters:
                    self._gate.wait()
                self._sh[ident] = 1

    def _leave_gate(self, mode: str, ident: int):
        with self._gate:
            if mode == "ex":
                self._ex_owner = None
            else:
                self._sh.pop(ident, None)
            self._gate.notify_all()

    @contextlib.contextmanager
    def exclusive(self):
        """排他临界区：与其它进程的发布/GC/dry-run 全部互斥。"""
        with self._hold("ex"):
            yield

    @contextlib.contextmanager
    def shared(self):
        """共享临界区：只排斥其它进程的排他区（发布/GC 执行）。"""
        with self._hold("sh"):
            yield
