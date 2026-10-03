"""跨进程协调：基于 POSIX flock 的咨询式读写锁。

进程内 threading.RLock 无法协调挂载同一数据目录的多个进程（例如同一卷上的
两个容器、滚动升级时并存的新旧实例）。这里用一把 fcntl.flock 文件锁作为
manifest 发布与 GC 之间的跨进程互斥手段：

- 发布（PUT /v2/.../manifests/<ref>）：从引用存在性校验、manifest 落库到
  标签 CAS 提交，全程持有排他锁（EX）；
- GC 执行（POST /gc dry_run:false）：计划原子认领之后，从可达性复核、
  元数据删除到 blob 文件删除，全程持有排他锁（EX）；
- GC dry-run：持共享锁（SH）拍摄候选快照，与发布互斥（快照期间不会看到
  发布到一半的状态），多个 dry-run 之间可并行。

因此两类关键区间跨进程严格串行，标签指向的引用必然仍可拉取：

  * 发布先完成 -> 标签已提交，GC 复核时候选重新可达，进入 skipped，
    blob 与 manifest 都不删；
  * GC 先完成 -> blob 元数据与文件都已删除，发布的引用校验失败，返回
    400 MANIFEST_BLOB_UNKNOWN，标签不会写入——既有标签不变，其引用
    一律可拉取。

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
    """单文件 flock 读写锁，进程内可重入（同模式）。

    每个进程第一次进入临界区时独立打开锁文件并加锁；进程内用计数器把
    同模式的嵌套请求折叠到同一把锁上，EX 与 SH 不允许互相嵌套。
    """

    def __init__(self, path: "str | Path"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._tlock = threading.RLock()   # 保护下面的计数/模式
        self._depth = 0
        self._mode: "str | None" = None  # 'sh' | 'ex'
        self._fd: "int | None" = None

    @contextlib.contextmanager
    def _hold(self, mode: str):
        with self._tlock:
            if self._depth > 0 and self._mode != mode:
                raise RuntimeError(
                    f"nested cross-process lock mode conflict: "
                    f"{self._mode} -> {mode}"
                )
            if self._depth == 0:
                fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
                flags = fcntl.LOCK_EX if mode == "ex" else fcntl.LOCK_SH
                try:
                    fcntl.flock(fd, flags)  # 阻塞直到其它进程释放
                except BaseException:
                    os.close(fd)
                    raise
                self._fd = fd
                self._mode = mode
            self._depth += 1
        try:
            yield
        finally:
            with self._tlock:
                self._depth -= 1
                if self._depth == 0:
                    try:
                        fcntl.flock(self._fd, fcntl.LOCK_UN)
                    finally:
                        os.close(self._fd)
                        self._fd = None
                        self._mode = None

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
