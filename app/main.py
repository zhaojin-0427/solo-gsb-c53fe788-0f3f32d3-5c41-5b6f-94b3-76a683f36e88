"""HTTP 路由：OCI 风格 /v2 API + /gc。"""
from __future__ import annotations

from typing import Optional

from fastapi import Body, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import errors as E
from .config import Settings
from .digest import is_digest, is_repo
from .errors import OCIError
from .registry import Registry

DISTRIBUTION_HEADER = "Docker-Distribution-API-Version"


def _range_header(size: int) -> str:
    return f"0-{max(size - 1, 0)}"


def _iter_file(f, chunk_size: int = 1024 * 1024):
    try:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield data
    finally:
        f.close()


class GCRequest(BaseModel):
    dry_run: bool = True
    plan_id: Optional[str] = None
    grace_seconds: Optional[int] = None


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings()
    registry = Registry(settings)
    app = FastAPI(title="Local OCI Registry", version="1.0.0")
    app.state.registry = registry

    @app.exception_handler(OCIError)
    async def _oci_error_handler(_request: Request, exc: OCIError):
        return JSONResponse(status_code=exc.status_code, content=exc.body())

    @app.middleware("http")
    async def _distribution_header(request: Request, call_next):
        resp = await call_next(request)
        resp.headers.setdefault(DISTRIBUTION_HEADER, "registry/2.0")
        return resp

    def check_repo(repo: str):
        if not is_repo(repo):
            raise E.name_invalid(repo)

    def check_digest(digest: str):
        if not is_digest(digest):
            raise E.digest_invalid(digest)

    def parse_expected(request: Request) -> Optional[str]:
        """期望摘要：优先 If-Match 头，其次 ?expected= 查询参数。"""
        raw = request.headers.get("if-match") or request.query_params.get("expected")
        if raw is None:
            return None
        value = raw.strip()
        if value.lower().startswith("w/"):  # 容忍弱校验器前缀
            value = value[2:].strip()
        value = value.strip('"').strip()
        if value in ("", "*"):
            return None
        if not is_digest(value):
            raise E.digest_invalid(value)
        return value

    def upload_location(repo: str, upload_id: str) -> str:
        return f"/v2/{repo}/blobs/uploads/{upload_id}"

    # ---------------- 基础 ----------------

    @app.get("/v2")
    @app.get("/v2/")
    def v2_base():
        return {}

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    # ---------------- blob 上传 ----------------

    @app.post("/v2/{repo:path}/blobs/uploads")
    @app.post("/v2/{repo:path}/blobs/uploads/")
    async def start_upload(
        repo: str,
        request: Request,
        digest: Optional[str] = Query(default=None),
        mount: Optional[str] = Query(default=None),
        from_repo: Optional[str] = Query(default=None, alias="from"),
    ):
        check_repo(repo)
        # 跨仓库挂载（去重推送）；失败则按规范回退为普通上传会话
        if mount:
            check_digest(mount)
            if registry.mount_blob(repo, mount):
                return Response(status_code=201, headers={
                    "Location": f"/v2/{repo}/blobs/{mount}",
                    "Docker-Content-Digest": mount,
                    "Content-Length": "0",
                })
        # 单块整体上传：POST ?digest=... 直接带 body
        if digest:
            check_digest(digest)
            payload = await request.body()
            registry.monolithic_upload(repo, payload, digest)
            return Response(status_code=201, headers={
                "Location": f"/v2/{repo}/blobs/{digest}",
                "Docker-Content-Digest": digest,
                "Content-Length": "0",
            })
        u = registry.start_upload(repo)
        return Response(status_code=202, headers={
            "Location": upload_location(repo, u["id"]),
            "Range": "0-0",
            "Docker-Upload-UUID": u["id"],
            "Content-Length": "0",
        })

    @app.get("/v2/{repo:path}/blobs/uploads/{upload_id}")
    def upload_status(repo: str, upload_id: str):
        """续传探测：返回当前已接收的字节范围（重启后依然有效）。"""
        check_repo(repo)
        u = registry.get_upload_status(repo, upload_id)
        return Response(status_code=204, headers={
            "Location": upload_location(repo, u["id"]),
            "Range": _range_header(u["size"]),
            "Docker-Upload-UUID": u["id"],
        })

    @app.patch("/v2/{repo:path}/blobs/uploads/{upload_id}")
    async def patch_upload(repo: str, upload_id: str, request: Request):
        check_repo(repo)
        chunk = await request.body()
        size = registry.append_chunk(
            repo, upload_id, chunk, request.headers.get("content-range")
        )
        return Response(status_code=202, headers={
            "Location": upload_location(repo, upload_id),
            "Range": _range_header(size),
            "Docker-Upload-UUID": upload_id,
            "Content-Length": "0",
        })

    @app.put("/v2/{repo:path}/blobs/uploads/{upload_id}")
    async def finalize_upload(repo: str, upload_id: str, request: Request,
                              digest: str = Query(...)):
        check_repo(repo)
        check_digest(digest)
        chunk = await request.body()  # 允许最后一个分块随 PUT 一起提交
        d, _size = registry.finalize_upload(repo, upload_id, digest, chunk or None)
        return Response(status_code=201, headers={
            "Location": f"/v2/{repo}/blobs/{d}",
            "Docker-Content-Digest": d,
            "Content-Length": "0",
        })

    @app.delete("/v2/{repo:path}/blobs/uploads/{upload_id}")
    def cancel_upload(repo: str, upload_id: str):
        check_repo(repo)
        registry.cancel_upload(repo, upload_id)
        return Response(status_code=204)

    # ---------------- blob 拉取 ----------------

    @app.get("/v2/{repo:path}/blobs/{digest}")
    def get_blob(repo: str, digest: str):
        check_repo(repo)
        check_digest(digest)
        f, size = registry.open_blob(repo, digest)
        return StreamingResponse(
            _iter_file(f),
            media_type="application/octet-stream",
            headers={
                "Docker-Content-Digest": digest,
                "Content-Length": str(size),
                "Accept-Ranges": "bytes",
            },
        )

    @app.head("/v2/{repo:path}/blobs/{digest}")
    def head_blob(repo: str, digest: str):
        check_repo(repo)
        check_digest(digest)
        size = registry.stat_blob(repo, digest)
        return Response(status_code=200, headers={
            "Docker-Content-Digest": digest,
            "Content-Length": str(size),
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes",
        })

    # ---------------- manifest ----------------

    @app.put("/v2/{repo:path}/manifests/{ref}")
    async def put_manifest(repo: str, ref: str, request: Request):
        check_repo(repo)
        expected = parse_expected(request)
        payload = await request.body()
        digest = registry.put_manifest(
            repo, ref, payload, request.headers.get("content-type"), expected
        )
        return Response(status_code=201, headers={
            "Location": f"/v2/{repo}/manifests/{digest}",
            "Docker-Content-Digest": digest,
            "Content-Length": "0",
        })

    @app.get("/v2/{repo:path}/manifests/{ref}")
    def get_manifest(repo: str, ref: str):
        check_repo(repo)
        row = registry.get_manifest(repo, ref)
        return Response(
            content=bytes(row["content"]),
            media_type=row["media_type"],
            headers={"Docker-Content-Digest": row["digest"]},
        )

    @app.head("/v2/{repo:path}/manifests/{ref}")
    def head_manifest(repo: str, ref: str):
        check_repo(repo)
        row = registry.get_manifest(repo, ref)
        return Response(status_code=200, headers={
            "Docker-Content-Digest": row["digest"],
            "Content-Type": row["media_type"],
            "Content-Length": str(row["size"]),
        })

    @app.get("/v2/{repo:path}/tags/list")
    def list_tags(repo: str):
        check_repo(repo)
        return {"name": repo, "tags": registry.list_tags(repo)}

    # ---------------- 垃圾回收 ----------------

    @app.post("/gc")
    def gc(req: Optional[GCRequest] = Body(default=None)):
        """两阶段复核 GC。

        - dry_run=true（默认）：生成持久化计划，返回 plan_id 与候选清单；
        - dry_run=false：必须携带 plan_id；计划缺失/未知/超过 1 小时/已执行
          均拒绝（4xx）且不删数据。执行前先在 SQLite 中把计划从 pending
          原子认领为 executing（跨进程互斥，立即提交），只有认领成功者会
          复核与删除；并发竞争失败返回 409 GC_PLAN_IN_PROGRESS 且不碰候选。
          认领后重算可达性与租约，只删计划内仍符合条件的对象，全部删完才
          标记 executed；计划外新候选一律不碰。认领后进程异常/崩溃则计划
          永久停留在 executing（重试 409、不可重放），剩余候选可重新 dry-run。
        """
        req = req or GCRequest()
        if req.dry_run:
            return registry.gc_dry_run(grace_seconds=req.grace_seconds)
        return registry.gc_execute(plan_id=req.plan_id)

    return app


app = create_app()
