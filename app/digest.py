"""digest / tag / 仓库名的校验与计算工具。"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
TAG_RE = re.compile(r"^[\w][\w.-]{0,127}$")
# OCI distribution-spec 的仓库名规则：小写字母数字 + . _ __ - 分隔，可多级路径
REPO_RE = re.compile(
    r"^[a-z0-9]+(?:(?:\.|_|__|-+)[a-z0-9]+)*"
    r"(?:/[a-z0-9]+(?:(?:\.|_|__|-+)[a-z0-9]+)*)*$"
)


def is_digest(value: "str | None") -> bool:
    return bool(value) and bool(DIGEST_RE.match(value))


def is_tag(value: "str | None") -> bool:
    return bool(value) and bool(TAG_RE.match(value)) and not is_digest(value)


def is_repo(value: "str | None") -> bool:
    return bool(value) and len(value) <= 255 and bool(REPO_RE.match(value))


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_file(path: "str | Path", chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk_size), b""):
            h.update(block)
    return "sha256:" + h.hexdigest()
