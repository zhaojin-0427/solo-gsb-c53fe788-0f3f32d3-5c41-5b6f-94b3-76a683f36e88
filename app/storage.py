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

    def install_blob(self, tmp_path: Path, digest: str) -> int:
        """把**摘要已校验**的上传临时文件原子落为内容寻址 blob。

        与旧的「见同名文件即去重、丢弃新内容」不同：这里始终 os.replace
        覆盖目标。内容寻址存储中同摘要必然同内容，覆盖只会得到逐字节相同
        的文件，却能在「GC 已删元数据记录、旧文件尚未删除」的残留窗口里
        用刚校验过的完整内容重建文件，而不是把新上传误判为去重后丢弃。

        调用方必须持有与 GC 删除区间互斥的跨进程排他锁——正常并发下该
        残留窗口根本观察不到；无条件安装属于崩溃/外部干预残留的防御性自愈。
        """
        target = self.blob_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_path, target)  # 同文件系统内原子改名（覆盖同名文件）
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
