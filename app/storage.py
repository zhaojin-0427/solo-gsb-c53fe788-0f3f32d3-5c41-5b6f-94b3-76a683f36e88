"""本地文件存储：内容寻址 blob + 上传临时文件。

布局：
    <data>/blobs/sha256/<hex>      blob 内容，按摘要去重，全局共享
    <data>/uploads/<uuid>          进行中的上传分块数据
"""
from __future__ import annotations

import os
from pathlib import Path

from .config import Settings


class FileStorage:
    def __init__(self, settings: Settings):
        self.blobs_dir = settings.blobs_dir
        self.uploads_dir = settings.uploads_dir
        self.blobs_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)

    # ---------------- blobs ----------------

    def blob_path(self, digest: str) -> Path:
        algo, _, hexa = digest.partition(":")
        return self.blobs_dir / algo / hexa

    def has_blob(self, digest: str) -> bool:
        return self.blob_path(digest).is_file()

    def open_blob(self, digest: str):
        return open(self.blob_path(digest), "rb")

    def delete_blob(self, digest: str):
        path = self.blob_path(digest)
        path.unlink(missing_ok=True)
        try:
            path.parent.rmdir()  # 仅当目录为空时成功
        except OSError:
            pass

    def commit_blob(self, tmp_path: Path, digest: str) -> int:
        """把上传临时文件落为内容寻址 blob；已存在则去重（删临时文件）。"""
        target = self.blob_path(digest)
        if target.exists():
            Path(tmp_path).unlink(missing_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_path, target)  # 同文件系统内原子改名
        return target.stat().st_size

    def sweep_orphan_blobs(self, known_digests: set, older_than_ts: float) -> list:
        """清理不在元数据中的孤儿 blob 文件（崩溃残留），只动足够老的文件。"""
        removed = []
        if not self.blobs_dir.exists():
            return removed
        for algo_dir in self.blobs_dir.iterdir():
            if not algo_dir.is_dir():
                continue
            for f in algo_dir.iterdir():
                digest = f"{algo_dir.name}:{f.name}"
                if digest in known_digests:
                    continue
                try:
                    if f.stat().st_mtime >= older_than_ts:
                        continue
                    f.unlink()
                    removed.append(digest)
                except FileNotFoundError:
                    pass
            try:
                algo_dir.rmdir()
            except OSError:
                pass
        return removed

    # ---------------- uploads ----------------

    def upload_path(self, upload_id: str) -> Path:
        return self.uploads_dir / upload_id

    def delete_upload_file(self, upload_id: str):
        self.upload_path(upload_id).unlink(missing_ok=True)
