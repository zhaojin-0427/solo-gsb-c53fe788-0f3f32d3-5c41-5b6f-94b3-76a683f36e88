"""运行时配置，全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
from pathlib import Path


class Settings:
    def __init__(
        self,
        data_dir: "str | Path | None" = None,
        upload_lease_seconds: "int | None" = None,
        gc_grace_seconds: "int | None" = None,
    ):
        self.data_dir = Path(
            data_dir or os.environ.get("REGISTRY_DATA_DIR", "./data")
        ).resolve()
        # 上传会话租约时长：超过该时间没有任何读写，GC 将回收该上传
        self.upload_lease_seconds = int(
            upload_lease_seconds
            if upload_lease_seconds is not None
            else os.environ.get("REGISTRY_UPLOAD_LEASE_SECONDS", "3600")
        )
        # GC 宽限期：创建时间早于此秒数的未被引用 blob 才允许回收，
        # 用于保护“已上传、尚未被 manifest 引用”的新 blob
        self.gc_grace_seconds = int(
            gc_grace_seconds
            if gc_grace_seconds is not None
            else os.environ.get("REGISTRY_GC_GRACE_SECONDS", "3600")
        )

    @property
    def db_path(self) -> Path:
        return self.data_dir / "registry.db"

    @property
    def blobs_dir(self) -> Path:
        return self.data_dir / "blobs"

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def lock_path(self) -> Path:
        """跨进程 flock 文件：协调发布与 GC 的引用/删除临界区。"""
        return self.data_dir / "registry.lock"
