"""OCI distribution-spec 风格的错误响应。

错误体格式：{"errors": [{"code": ..., "message": ..., "detail": ...}]}
"""
from __future__ import annotations


class OCIError(Exception):
    def __init__(self, status_code: int, code: str, message: str, detail=None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail

    def body(self) -> dict:
        err = {"code": self.code, "message": self.message}
        if self.detail is not None:
            err["detail"] = self.detail
        return {"errors": [err]}


def name_invalid(repo: str) -> OCIError:
    return OCIError(400, "NAME_INVALID", f"invalid repository name: {repo!r}",
                    {"repo": repo})


def digest_invalid(digest: str) -> OCIError:
    return OCIError(400, "DIGEST_INVALID", f"invalid digest: {digest!r}",
                    {"digest": digest})


def digest_mismatch(expected: str, computed: str) -> OCIError:
    return OCIError(400, "DIGEST_INVALID",
                    f"content digest mismatch: expected {expected}, computed {computed}",
                    {"expected": expected, "computed": computed})


def blob_unknown(repo: str, digest: str) -> OCIError:
    return OCIError(404, "BLOB_UNKNOWN",
                    f"blob {digest} unknown in repo '{repo}'",
                    {"repo": repo, "digest": digest})


def upload_unknown(upload_id: str) -> OCIError:
    return OCIError(404, "BLOB_UPLOAD_UNKNOWN",
                    f"blob upload unknown, expired or cancelled: {upload_id}",
                    {"upload_id": upload_id})


def upload_invalid(message: str, detail=None) -> OCIError:
    return OCIError(400, "BLOB_UPLOAD_INVALID", message, detail)


def range_invalid(expected_start: int, content_range: str) -> OCIError:
    return OCIError(416, "RANGE_INVALID",
                    f"chunk starts at wrong offset: expected {expected_start}, "
                    f"got Content-Range {content_range!r}",
                    {"expected_start": expected_start, "content_range": content_range})


def manifest_unknown(repo: str, ref: str) -> OCIError:
    return OCIError(404, "MANIFEST_UNKNOWN",
                    f"manifest unknown: {repo}:{ref}",
                    {"repo": repo, "ref": ref})


def manifest_invalid(message: str) -> OCIError:
    return OCIError(400, "MANIFEST_INVALID", f"manifest invalid: {message}")


def manifest_blob_unknown(repo: str, missing: list) -> OCIError:
    return OCIError(400, "MANIFEST_BLOB_UNKNOWN",
                    f"manifest references unknown content in repo '{repo}': "
                    + ", ".join(missing),
                    {"repo": repo, "missing": missing})


def tag_invalid(ref: str) -> OCIError:
    return OCIError(400, "TAG_INVALID", f"invalid tag or reference: {ref!r}",
                    {"ref": ref})


def tag_conflict(repo: str, tag: str, current: "str | None", expected: "str | None") -> OCIError:
    return OCIError(
        409, "TAG_CONFLICT",
        f"tag '{tag}' in repo '{repo}' currently points to "
        f"{current or '<absent>'}, not {expected or '<unspecified>'}; "
        f"re-send with header 'If-Match: <current digest>' to move the tag",
        {"repo": repo, "tag": tag, "current": current, "expected": expected},
    )


# ---------------- GC 复核计划 ----------------

def gc_invalid_request(message: str, detail=None) -> OCIError:
    return OCIError(400, "GC_INVALID", message, detail)


def gc_plan_required() -> OCIError:
    return OCIError(
        400, "GC_PLAN_REQUIRED",
        "execution (dry_run=false) requires the plan_id of a prior dry-run report; "
        "run a dry-run first and re-submit the returned plan_id",
        {"hint": "POST /gc with {\"dry_run\": true} to obtain a plan_id"},
    )


def gc_plan_unknown(plan_id: str) -> OCIError:
    return OCIError(
        404, "GC_PLAN_UNKNOWN",
        f"unknown GC plan_id: {plan_id!r}; run a new dry-run",
        {"plan_id": plan_id},
    )


def gc_plan_expired(plan_id: str, expires_at: "str | None") -> OCIError:
    return OCIError(
        410, "GC_PLAN_EXPIRED",
        f"GC plan {plan_id!r} expired at {expires_at or '<unknown>'} "
        "(plans are valid for one hour after generation); run a new dry-run",
        {"plan_id": plan_id, "expires_at": expires_at},
    )


def gc_plan_already_executed(plan_id: str, executed_at: "str | None") -> OCIError:
    return OCIError(
        409, "GC_PLAN_ALREADY_EXECUTED",
        f"GC plan {plan_id!r} was already executed at {executed_at or '<unknown>'}; "
        "repeated execution deletes nothing, run a new dry-run for a new plan",
        {"plan_id": plan_id, "executed_at": executed_at},
    )


def gc_plan_in_progress(plan_id: str, claimed_at: "str | None",
                        claimer: "str | None", stale: bool = False) -> OCIError:
    """认领失败（409）：另一进程正在执行，或上次执行在认领后崩溃。

    两种情况都不允许重放：计划已脱离 pending，本请求不触碰任何候选；
    剩余候选可重新 dry-run 生成新计划审阅。
    """
    if stale:
        message = (
            f"GC plan {plan_id!r} was claimed at {claimed_at or '<unknown>'} by "
            f"{claimer or '<unknown>'} but never finished (the executing process "
            "crashed or was killed); it cannot be replayed and no candidates were "
            "touched by this request, run a new dry-run for a new plan"
        )
    else:
        message = (
            f"GC plan {plan_id!r} is already being executed by another process "
            f"(claimed at {claimed_at or '<unknown>'} by {claimer or '<unknown>'}); "
            "concurrent execution of the same plan is rejected and no candidates "
            "were touched by this request, retry later or run a new dry-run"
        )
    return OCIError(
        409, "GC_PLAN_IN_PROGRESS", message,
        {"plan_id": plan_id, "claimed_at": claimed_at, "claimer": claimer,
         "stale": stale},
    )
