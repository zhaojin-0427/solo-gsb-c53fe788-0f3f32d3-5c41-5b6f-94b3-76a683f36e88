# 本地容器制品仓库（Local OCI Registry）

基于 **Python + FastAPI + SQLite + 本地文件存储** 的轻量 OCI 风格制品仓库。
支持分块/断点续传 blob、按摘要全局去重、重启后恢复上传、不可变 manifest
发布校验、标签 CAS 移动，以及带 dry-run 复核的可达性垃圾回收。

## 功能特性

| 能力 | 说明 |
| --- | --- |
| OCI 风格 API | `/v2/<repo>/blobs/uploads`、`/v2/<repo>/manifests/<ref>`、`/v2/<repo>/blobs/<digest>`、`/v2/<repo>/tags/list` |
| 分块上传 / 续传 | `POST → PATCH* → PUT?digest=`；`GET` 会话返回 `Range` 进度，可断点续传 |
| 摘要校验 | 上传完成时按内容计算 sha256，与客户端摘要不一致即拒绝并取消会话 |
| 按摘要去重 | blob 全局内容寻址存储，跨仓库/重复推送只占一份磁盘；支持 `?mount=&from=` 跨仓库挂载 |
| 重启恢复 | 上传会话与进度持久化在 SQLite，重启后自动对齐磁盘实际进度，可继续续传 |
| 不可变 manifest | digest 寻址的 manifest 内容与摘要不符即拒绝；发布时校验 config/layers/subject/子 manifest 引用必须已存在，杜绝悬空引用 |
| 标签 CAS | 移动已存在的标签必须携带期望摘要（`If-Match` 头或 `?expected=`），不匹配返回 `409 TAG_CONFLICT` |
| 安全 GC | 以「标签 + 未过期上传租约」为根建立可达图；**先 dry-run 生成持久化复核计划（`plan_id`，1 小时有效、重启有效），再凭 `plan_id` 执行**；执行前在 SQLite 中原子认领计划（`pending → executing`），多个进程共享同一数据目录并发执行同一计划时只有一个进程认领成功并删除，竞争方返回 `409 GC_PLAN_IN_PROGRESS` 且不碰候选；认领后进程崩溃则计划永久停留在 `executing`（不可重放），全部删除完成才标记 `executed`；执行时重算可达性/租约，只删计划内仍符合条件的对象，复核窗口内新产生的候选不会被误删；与并发拉取互斥 |
| 跨进程发布/GC 协调 | 进程内锁跨进程无效时，**manifest 发布（引用校验→标签提交）、blob 提交（单块重传/分块完成/跨仓库挂载的内容就位→关联落库）与 GC 执行（认领后可达性复核→元数据和文件删除）由一把 `flock` 文件锁跨进程串行**：发布先提交标签则 GC 跳过已重新可达的候选；GC 先删完则发布返回 `400 MANIFEST_BLOB_UNKNOWN` 且无可见标签；同摘要重传先完成则 GC 按可达性/宽限期正常复核，GC 先删完则重传落下完整新文件并重建关联（挂载按规范回退普通上传）。任何时刻「标签可见时其引用必可拉取」 |

## 快速开始（Docker Compose 一键启动）

```bash
docker compose up --build -d
```

服务监听在 **http://localhost:5000**（容器内 8000 端口），数据持久化在
`registry-data` 卷中。验证：

```bash
curl http://localhost:5000/v2/        # -> {} ，响应头含 Docker-Distribution-API-Version: registry/2.0
curl http://localhost:5000/healthz    # -> {"status":"ok"}
```

需要复现/验证**跨进程协调**时，可一键启动共享同一数据卷的第二个实例
（默认不带 profile 时仍只启动单实例）：

```bash
docker compose --profile multiproc up --build -d
# registry   -> http://localhost:5000
# registry-b -> http://localhost:5001  （与 registry 共享同一个 registry-data 卷）
```

两个实例的数据元数据在同一 SQLite（WAL + `busy_timeout` 串行写），manifest
发布与 GC 的关键区间由数据卷内的 `registry.lock`（`flock` 文件锁）互斥，
详见下文「跨进程发布/GC 竞态与协调」。

也可以本地直接运行（Python 3.11+）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 5000
```

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REGISTRY_DATA_DIR` | `./data`（镜像内 `/data`） | 数据目录（SQLite + blob 文件 + 上传临时文件） |
| `REGISTRY_UPLOAD_LEASE_SECONDS` | `3600` | 上传会话租约；超时无读写即被 GC 回收 |
| `REGISTRY_GC_GRACE_SECONDS` | `3600` | GC 宽限期；创建不足该时长的未引用 blob 不回收，保护「已推送、待发布」窗口 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v2/` | API 版本探测 |
| POST | `/v2/{repo}/blobs/uploads` | 创建上传会话（202）；`?digest=` 整体上传（201）；`?mount=&from=` 跨仓库挂载 |
| GET | `/v2/{repo}/blobs/uploads/{id}` | 查询上传进度（204 + `Range`），用于续传 |
| PATCH | `/v2/{repo}/blobs/uploads/{id}` | 追加分块，可选 `Content-Range` 校验偏移 |
| PUT | `/v2/{repo}/blobs/uploads/{id}?digest=` | 完成上传（可携带最后一块），校验摘要 |
| DELETE | `/v2/{repo}/blobs/uploads/{id}` | 取消上传 |
| GET/HEAD | `/v2/{repo}/blobs/{digest}` | 拉取 blob |
| PUT | `/v2/{repo}/manifests/{ref}` | 发布 manifest；`ref` 为标签时需 CAS（`If-Match` 或 `?expected=`） |
| GET/HEAD | `/v2/{repo}/manifests/{ref}` | 拉取 manifest（`ref` 可为标签或 digest） |
| GET | `/v2/{repo}/tags/list` | 列出标签 |
| POST | `/gc` | 垃圾回收。`{"dry_run": true, "grace_seconds": 3600}` 生成持久化复核计划并返回 `plan_id`；`{"dry_run": false, "plan_id": "..."}` 先原子认领计划（跨进程互斥），成功才执行；竞争返回 `409 GC_PLAN_IN_PROGRESS` |

错误统一为 OCI 格式：`{"errors":[{"code","message","detail"}]}`。

## curl 示例

以下假设服务在 `localhost:5000`，仓库为 `demo/app`。

### 1. 分块上传 blob（断点续传）

```bash
# 准备数据并计算摘要
printf 'hello-layer-content' > layer.bin
DIGEST="sha256:$(sha256sum layer.bin | cut -d' ' -f1)"

# 创建上传会话，记录 Location
LOC=$(curl -s -D - -o /dev/null -X POST \
  http://localhost:5000/v2/demo/app/blobs/uploads | awk '/^[Ll]ocation:/{print $2}' | tr -d '\r')
echo "$LOC"   # /v2/demo/app/blobs/uploads/<uuid>

# 上传前 5 字节（模拟中断）
curl -s -X PATCH -H 'Content-Type: application/octet-stream' \
  --data-binary @- "http://localhost:5000$LOC" <<< '' >/dev/null  # 仅占位
head -c 5 layer.bin | curl -s -D - -o /dev/null -X PATCH \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @- "http://localhost:5000$LOC" | grep -i range
# Range: 0-4

# 断点续传：先查询进度，再带 Content-Range 续传剩余部分
curl -s -D - -o /dev/null "http://localhost:5000$LOC" | grep -i range
tail -c +6 layer.bin | curl -s -X PATCH \
  -H 'Content-Type: application/octet-stream' -H 'Content-Range: 5-18' \
  --data-binary @- "http://localhost:5000$LOC" -o /dev/null

# 完成上传并校验摘要（服务按内容重算 sha256，不符则 400 并取消会话）
curl -s -D - -o /dev/null -X PUT "http://localhost:5000$LOC?digest=$DIGEST"
# 201 Created, Docker-Content-Digest: sha256:...
```

小文件也可整体上传：

```bash
curl -s -X POST --data-binary @layer.bin \
  "http://localhost:5000/v2/demo/app/blobs/uploads?digest=$DIGEST"   # 201，重复推送会去重秒回
```

### 2. 发布 manifest（标签 CAS）

```bash
# 先推 config 与 layer 两个 blob（略，同上），然后组装 manifest
cat > manifest.json <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config":   {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "$CONFIG_DIGEST", "size": $CONFIG_SIZE},
  "layers":   [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "$DIGEST", "size": 19}]
}
EOF

# 首次打 tag：无需期望摘要
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 201

# 移动 tag 必须携带期望摘要（CAS），否则 409 TAG_CONFLICT
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest-v2.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 409

# 正确姿势：If-Match 携带 tag 当前指向的 digest
CUR=$(curl -sI http://localhost:5000/v2/demo/app/manifests/v1 | awk '/^[Dd]ocker-[Cc]ontent-[Dd]igest:/{print $2}' | tr -d '\r')
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  -H "If-Match: $CUR" \
  --data-binary @manifest-v2.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 201，tag 完成移动
```

发布时服务端会校验 manifest 引用的 config/layers（以及 index 的子
manifest、referrers 的 subject）必须已存在于本仓库，否则返回
`400 MANIFEST_BLOB_UNKNOWN`，从源头杜绝悬空引用。

### 3. 拉取

```bash
curl -s http://localhost:5000/v2/demo/app/manifests/v1            # 按 tag 拉 manifest
curl -s http://localhost:5000/v2/demo/app/manifests/$MANIFEST_DIGEST
curl -s http://localhost:5000/v2/demo/app/blobs/$DIGEST -o layer.bin
curl -s http://localhost:5000/v2/demo/app/tags/list
```

### 4. 垃圾回收（dry-run 复核计划 → 原子认领 → 凭 plan_id 执行）

GC 采用**两阶段复核**，修复了「dry-run 后标签移动产生新垃圾、执行时被一并
删除」以及「两个进程同时执行同一计划、各自读取 pending 并重复删除」两类
竞态：

1. dry-run 把候选清单**快照持久化**成计划（`plan_id`，状态 `pending`），
   执行时只处理计划内、且此刻仍符合条件的对象；复核窗口内新出现的候选一律
   留给下次审阅。
2. 执行（`dry_run:false`）**先在 SQLite 中原子认领**：单条写事务把计划
   从 `pending` 更新为 `executing` 并立即提交（`UPDATE ... WHERE
   state='pending' AND expires_at>now`）。多个进程挂载同一数据目录并发请求
   同一 `plan_id` 时，只有一个进程认领成功；其余进程收到
   `409 GC_PLAN_IN_PROGRESS`，**不复核、不删除任何候选**。
3. 认领成功的进程再重算可达性与租约、只删计划内仍符合条件的对象，
   **全部删除完成后**才把计划标记为 `executed`。

> 计划保存在 SQLite 中，**进程重启后依然有效**；生成后 **1 小时**内可执行；
> 一份计划**只能执行一次**。认领成功后若执行进程异常或崩溃，计划停留在
> **`executing`（不可重放的执行中状态）**——已删除的部分不会重复执行，
> 对同一 `plan_id` 的重试同样返回 `409 GC_PLAN_IN_PROGRESS`（detail 中
> `stale:true` 表示认领方疑似已崩溃），剩余候选请重新 dry-run 审阅后用新
> 计划回收。

#### 第一步：dry-run 生成计划（不删任何数据）

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": true, "grace_seconds": 0}' \
  http://localhost:5000/gc
```

实际响应（`grace_seconds: 0` 仅为演示立刻可回收；生产可用默认 3600）：

```json
{
  "dry_run": true,
  "executed": false,
  "plan_id": "7d4ff3878fc848959fef26bdf39e3cce",
  "grace_seconds": 0,
  "generated_at": "2026-10-03T02:24:31.973878+00:00",
  "expires_at": "2026-10-03T03:24:31.973878+00:00",
  "summary": {
    "blobs_to_delete": 2,
    "manifests_to_delete": 1,
    "uploads_to_delete": 1,
    "bytes_to_reclaim": 30,
    "manifests_reachable": 2,
    "blobs_reachable": 3,
    "blobs_kept_by_grace": 0,
    "uploads_kept_by_lease": 1
  },
  "delete": {
    "blobs": [
      {"digest": "sha256:492fa4fc2ff764ecd108c3c4fbabd4ff6561a17d8feacfcf100b9f3da05a349c",
       "size": 17, "repos": ["demo/app"],
       "reason": "unreferenced and older than grace period"},
      {"digest": "sha256:9e41582b7b5eced4b02b2f9112ce85ceef30b92cf2b3fb4c92f10061b3400c0b",
       "size": 13, "repos": ["demo/app"],
       "reason": "unreferenced and older than grace period"}
    ],
    "manifests": [
      {"repo": "demo/app",
       "digest": "sha256:28931bb7077f5cb2d2083be81dcb42725febe13ff28a40ae7bc7c461056900e6",
       "media_type": "application/vnd.oci.image.manifest.v1+json",
       "reason": "untagged"}
    ],
    "uploads": [
      {"id": "3601015371a64bc7a702fab2d965a8f1", "repo": "demo/app", "size": 0,
       "expired_at": "2026-10-03T02:23:37.960170+00:00", "reason": "lease expired"}
    ]
  },
  "keep": {
    "blobs_grace_protected": [],
    "uploads_leased": [
      {"id": "95473834eb4d4fe5ad4a28c3cf79ac2e", "repo": "demo/app", "size": 0,
       "expires_at": "2026-10-03T03:24:31.957696+00:00"}
    ]
  }
}
```

复核要点：`delete.blobs / delete.manifests / delete.uploads` 即本次计划的候选；
`keep` 是宽限期/租约保护下保留的对象。记下 `plan_id`。

#### 第二步：凭 plan_id 执行

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": false, "plan_id": "7d4ff3878fc848959fef26bdf39e3cce"}' \
  http://localhost:5000/gc
```

服务端先原子认领该计划（认领成功后再复核、删除）。实际响应——`deleted` 是
真正删除的项，`skipped` 是计划内、但执行时已重新可达/续租/消失而跳过的项：

```json
{
  "dry_run": false,
  "executed": true,
  "plan_id": "7d4ff3878fc848959fef26bdf39e3cce",
  "grace_seconds": 0,
  "generated_at": "2026-10-03T02:24:31.973878+00:00",
  "executed_at": "2026-10-03T02:25:01.028073+00:00",
  "summary": {
    "blobs_deleted": 2,
    "manifests_deleted": 1,
    "uploads_deleted": 1,
    "blobs_skipped": 0,
    "manifests_skipped": 0,
    "uploads_skipped": 0,
    "bytes_reclaimed": 30
  },
  "deleted": {
    "blobs": [
      {"digest": "sha256:492fa4fc2ff764ecd108c3c4fbabd4ff6561a17d8feacfcf100b9f3da05a349c",
       "size": 17, "repos": ["demo/app"], "reason": "still unreferenced at execution"},
      {"digest": "sha256:9e41582b7b5eced4b02b2f9112ce85ceef30b92cf2b3fb4c92f10061b3400c0b",
       "size": 13, "repos": ["demo/app"], "reason": "still unreferenced at execution"}
    ],
    "manifests": [
      {"repo": "demo/app",
       "digest": "sha256:28931bb7077f5cb2d2083be81dcb42725febe13ff28a40ae7bc7c461056900e6",
       "media_type": "application/vnd.oci.image.manifest.v1+json",
       "reason": "still untagged at execution"}
    ],
    "uploads": [
      {"id": "3601015371a64bc7a702fab2d965a8f1", "repo": "demo/app", "size": 0,
       "reason": "lease still expired at execution"}
    ]
  },
  "skipped": {"blobs": [], "manifests": [], "uploads": []}
}
```

复核窗口内若把一个候选重新打上标签，执行时它会进入 `skipped` 而不是被删，例如：

```json
"skipped": {
  "blobs": [
    {"digest": "sha256:....", "reason": "reachable again: referenced by a tagged manifest"}
  ],
  "manifests": [
    {"repo": "demo/app", "digest": "sha256:....",
     "reason": "reachable again: a tag (or a tagged parent) points to it"}
  ],
  "uploads": [
    {"id": "....", "repo": "demo/app", "reason": "lease renewed or still active"}
  ]
}
```

#### 拒绝执行的情况（均不删数据）

| 情况 | 状态码 | `errors[0].code` |
| --- | --- | --- |
| 未带 `plan_id` / 空白 | 400 | `GC_PLAN_REQUIRED` |
| `plan_id` 未知 | 404 | `GC_PLAN_UNKNOWN` |
| 计划生成已超过 1 小时 | 410 | `GC_PLAN_EXPIRED` |
| 计划已经执行过（旧计划重试） | 409 | `GC_PLAN_ALREADY_EXECUTED` |
| 计划已被另一进程认领、正在执行；或认领后执行方崩溃留下不可重放的 `executing` 计划 | 409 | `GC_PLAN_IN_PROGRESS` |

实测请求与响应：

```bash
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false}' http://localhost:5000/gc
# HTTP 400
{"errors":[{"code":"GC_PLAN_REQUIRED",
  "message":"execution (dry_run=false) requires the plan_id of a prior dry-run report; run a dry-run first and re-submit the returned plan_id",
  "detail":{"hint":"POST /gc with {\"dry_run\": true} to obtain a plan_id"}}]}

$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "does-not-exist"}' http://localhost:5000/gc
# HTTP 404
{"errors":[{"code":"GC_PLAN_UNKNOWN",
  "message":"unknown GC plan_id: 'does-not-exist'; run a new dry-run",
  "detail":{"plan_id":"does-not-exist"}}]}

$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "32507fa25c9747df9bb8b53c7b6693b6"}' http://localhost:5000/gc
# HTTP 410（计划超过生成后 1 小时）
{"errors":[{"code":"GC_PLAN_EXPIRED",
  "message":"GC plan '32507fa25c9747df9bb8b53c7b6693b6' expired at 2026-10-03T02:25:42.964437+00:00 (plans are valid for one hour after generation); run a new dry-run",
  "detail":{"plan_id":"32507fa25c9747df9bb8b53c7b6693b6",
            "expires_at":"2026-10-03T02:25:42.964437+00:00"}}]}

$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "7d4ff3878fc848959fef26bdf39e3cce"}' http://localhost:5000/gc
# HTTP 409（旧计划重试：计划已经执行过）
{"errors":[{"code":"GC_PLAN_ALREADY_EXECUTED",
  "message":"GC plan '7d4ff3878fc848959fef26bdf39e3cce' was already executed at 2026-10-03T02:25:01.028073+00:00; repeated execution deletes nothing, run a new dry-run for a new plan",
  "detail":{"plan_id":"7d4ff3878fc848959fef26bdf39e3cce",
            "executed_at":"2026-10-03T02:25:01.028073+00:00"}}]}
```

并发竞争失败（另一进程认领成功、正在执行）时的实际 409 响应——本请求没有
复核也没有触碰任何候选：

```bash
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "071462689fce42529d3b4464801ad1a5"}' \
    http://localhost:5000/gc
# HTTP 409
{"errors":[{"code":"GC_PLAN_IN_PROGRESS",
  "message":"GC plan '071462689fce42529d3b4464801ad1a5' is already being executed by another process (claimed at 2026-10-03T02:26:05.298377+00:00 by registry-2:1042:9f31a2bc); concurrent execution of the same plan is rejected and no candidates were touched by this request, retry later or run a new dry-run",
  "detail":{"plan_id":"071462689fce42529d3b4464801ad1a5",
            "claimed_at":"2026-10-03T02:26:05.298377+00:00",
            "claimer":"registry-2:1042:9f31a2bc",
            "stale":false}}]}
```

若认领成功的进程随后崩溃、计划长时间停留在 `executing`（超过 5 分钟），重试
得到同样的 409，但 `detail.stale` 为 `true`、消息提示认领方已崩溃：

```json
{"errors":[{"code":"GC_PLAN_IN_PROGRESS",
  "message":"GC plan '...' was claimed at ... by dead-host:1:deadbeef but never finished (the executing process crashed or was killed); it cannot be replayed and no candidates were touched by this request, run a new dry-run for a new plan",
  "detail":{"plan_id":"...", "claimed_at":"...", "claimer":"dead-host:1:deadbeef",
            "stale":true}}]}
```

#### 跨进程并发实测：同一 plan_id 只有一个进程删除

启动两个独立进程、挂载**同一个数据目录**（相当于两个容器共用同一卷），对
同一个 `plan_id` 同时发起 20 个执行请求（每个进程 10 个）：

```bash
REGISTRY_DATA_DIR=/data uvicorn app.main:app --port 8000 &   # 进程 A
REGISTRY_DATA_DIR=/data uvicorn app.main:app --port 8001 &   # 进程 B
for i in $(seq 1 10); do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "ea2789edc5934436b3fb00e792d443e7"}' :8000/gc &
  curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "ea2789edc5934436b3fb00e792d443e7"}' :8001/gc &
done; wait
```

实际结果：**恰好 1 个 `200`，其余 19 个全部 `409`**——执行窗口内竞争失败的
是 `GC_PLAN_IN_PROGRESS`，赢家完成后到达的是 `GC_PLAN_ALREADY_EXECUTED`；
计划内的候选只被删除一次：

```text
      1 200
     19 409

1 200-ok
9 GC_PLAN_ALREADY_EXECUTED
10 GC_PLAN_IN_PROGRESS
```

认领后执行进程崩溃的语义同理：计划保持 `executing`，不会有第二个进程重放它；
尚未删除的候选依然完好，重新 dry-run 后可以用**新计划**审阅、回收。

#### 复核窗口内新产生的候选不会被误删

在上面 dry-run 与执行之间，若发生标签移动或悬空推送而产生**新的**不可达对象
（即使它已老过宽限期），执行时它**不在计划内，因而保留**；它只会出现在**下一次**
dry-run 的候选中，等待新一轮人工复核：

```bash
# 执行后：计划内旧垃圾已删，dry-run 之后才出现的新 blob 仍在
$ curl -s -o /dev/null -w '%{http_code}\n' \
    http://localhost:5000/v2/demo/app/blobs/sha256:c97adf18...
200

# 下一次 dry-run 才把它列为候选
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": true, "grace_seconds": 0}' http://localhost:5000/gc
# {"plan_id": "23338789...", "delete": {"blobs": [{"digest": "sha256:c97adf18..."}]}, ...}
```

GC 语义：

- **两阶段复核**：dry-run 快照候选为持久化计划（`gc_plans` 等表），执行必须凭
  `plan_id`；计划缺失/未知/超过 1 小时/已执行一律拒绝，不删任何数据；计划跨
  重启有效，且只能执行一次。
- **跨进程原子认领**：执行的第一步是在 SQLite 写事务内
  `UPDATE gc_plans SET state='executing' WHERE id=? AND state='pending'
  AND expires_at>?` 并立即提交。WAL + `busy_timeout` 保证跨进程写串行，因此
  两个进程同时执行同一计划时只有一个 `rowcount==1` 的赢家；输家收到
  409 `GC_PLAN_IN_PROGRESS`，不读取候选、不做删除。进程内 RLock 只负责同进程
  内的串行；与并发发布的跨进程互斥见下一条与「跨进程发布/GC 竞态与协调」。
- **跨进程发布/提交互斥（防悬空引用）**：认领成功后的「可达性复核 → 元数据
  删除 → blob 文件删除」全程持有数据目录内的 `registry.lock` 排他 `flock`，
  与 `PUT manifest` 的「引用校验 → manifest 落库 → 标签 CAS 提交」区间、以及
  blob 提交（`POST ?digest=` 单块重传、`PUT .../uploads/<id>?digest=` 分块
  完成、`POST ...?mount=` 跨仓库挂载）的「内容就位 → blobs/repo_blobs 关联
  落库」区间互斥。发布先提交标签则复核时候选重新可达、进入 `skipped`；GC
  先删完则发布的引用校验失败（400 `MANIFEST_BLOB_UNKNOWN`），标签不写入；
  同摘要重传不会与「已删元数据、未删文件」窗口交错——GC 先删完时重传落下
  完整新文件并重建关联（挂载方观察到 blob 已不存在，回退普通上传会话），
  重传先完成则按可达性/宽限期正常复核。锁随进程死亡由内核自动释放。
- **认领即不可重放**：认领提交后若执行方异常或崩溃，计划永久停留在
  `executing`（`claimed_at`/`claimer` 记录认领时刻与 `host:pid` 实例）。重试
  同一 `plan_id` 一律 409，已删部分不会重复执行；剩余候选重新 dry-run 后用
  新计划审阅、回收。全部删除成功提交后状态才变为 `executed`。
- **执行时复核（TOCTOU 防护）**：认领成功后以**计划记录的宽限期**重新
  计算可达图与上传租约——计划内且仍不可达/仍超宽限/租约仍过期的才删除；
  重新可达、续租或已不存在的进入 `skipped`。
- **计划外不碰**：dry-run 之后因标签移动、删除、悬空推送新产生的候选不在计划内，
  本次执行绝不删除，留待下次 dry-run 审阅（上面已实测）。
- **根集合**：所有 tag 指向的 manifest；沿 `config`/`layers`/`manifests[]`/
  `subject` 遍历出全部可达 blob 与 manifest。
- **回收对象**：不可达且超出宽限期的 blob、不可达（无标签）的 manifest、
  租约已过期的上传会话。
- **保护机制**：未过期上传租约不会被清理；宽限期内的“新 blob”不会被清理
  （覆盖“已推送 blob、尚未发布 manifest”的并发窗口）；GC 执行的复核/删除区间与
  manifest 发布的引用校验/标签提交区间由跨进程 `flock`（`registry.lock`）串行，
  同进程内再由 `RLock` 串行，拉取在打开文件句柄后再流式发送，文件随后被删也不
  影响进行中的下载。
- 删除顺序为先清元数据再删文件，崩溃残留由下次启动时的孤儿文件清扫兜底。

## 跨进程发布/GC 竞态与协调

### 问题：进程内锁跨进程无效

GC 与 HTTP 请求在同一进程内由一把 `RLock` 串行；但两个进程挂载同一数据
目录时（同一卷上的两个容器、滚动升级新旧实例并存）进程锁互不可见，于是
可能发生：

1. 进程 A `POST /gc`（`dry_run:false` + `plan_id`）原子认领计划，**复核
   可达性**：blob X 不可达、已过宽限期，是计划内候选；
2. 进程 B `PUT /v2/demo/app/manifests/v1` 发布一个**引用 blob X** 的标签，
   引用存在性校验通过（X 还在）；
3. 进程 A 继续删除 X 的元数据与文件；进程 B 提交标签并返回 `201`。

结果：标签可读（manifest 200），但拉取其引用的 blob 得到 `404`——悬空
引用。

### 协调：一把 flock 串行关键区间

数据目录内新增 `registry.lock`（`app/coord.py`，`fcntl.flock` 咨询锁）：

| 临界区 | 持锁 | 覆盖范围 |
| --- | --- | --- |
| `PUT manifests/<ref>` | 排他 EX | **引用存在性校验 → manifest 落库 → 标签 CAS 提交** |
| `POST .../uploads?digest=` / `PUT .../uploads/<id>?digest=` / `POST ...?mount=` | 排他 EX | **内容就位（os.replace）→ blobs/repo_blobs 关联落库 → 会话置 committed** |
| `POST /gc` 执行（认领成功后） | 排他 EX | **可达性/租约复核 → 删元数据 → 删 blob 文件 → 标记 `executed`** |
| `POST /gc` dry-run | 共享 SH | 候选快照（多个 dry-run 可并行，但看不到发布或提交进行到一半的状态） |

`gc_plans` 的 `pending → executing` 原子认领仍负责「同一计划只能执行一次」；
`flock` 负责「发布/blob 提交与 GC 的判定/提交区间不交错」。两者叠加后：

- **发布先完成**：标签已提交、引用先成为 GC 删除前的既成事实，GC 复核时
  候选重新可达 → 进入 `skipped`，blob/manifest 都不删，标签与引用都可拉取；
- **GC 先完成**：blob 的元数据与文件都已删除，发布随后拿到锁、重新做引用
  校验 → `400 MANIFEST_BLOB_UNKNOWN`，manifest 与标签都不写入；既有标签
  未变，其引用一律可拉取；
- **同摘要重传/挂载 vs GC**：重传先完成则按可达性/宽限期正常复核；GC 先
  删完则重传落下完整新文件并重建关联（标签可发布、blob 可拉取），挂载方
  观察到 blob 已不存在而回退普通上传会话（202）。详见下文
  「同摘要重传 / 跨仓库挂载与 GC 删除窗口的竞态」。

`flock` 由内核对**进程**持有，进程异常退出或被 `SIGKILL` 时锁自动释放，
不会留下僵死锁（区别于需要 TTL/续约的租约方案）。

### 实际请求与响应（两种顺序）

下面的响应在两个共享同一数据目录的真实实例（`:5000` / `:5001`）上实测。

**顺序一：发布先完成 → GC 跳过已可达候选**

```bash
# dry-run：新 blob 此刻不可达，被列为计划内候选
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": true, "grace_seconds": 0}' http://localhost:5000/gc   # plan_id=P1

# 复核窗口内，另一请求先发布引用该 blob 的标签（201）
curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
  -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest.json \
  http://localhost:5000/v2/race/demo/manifests/cap-pub-first
# 201

# 然后才执行 GC：复核发现候选已被标签引用 -> skipped，不删除
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": false, "plan_id": "P1"}' http://localhost:5000/gc
```

```json
{
  "summary": {"blobs_deleted": 0, "manifests_deleted": 0, "uploads_deleted": 0,
              "blobs_skipped": 1, "manifests_skipped": 0, "uploads_skipped": 0,
              "bytes_reclaimed": 0},
  "deleted": {"blobs": [], "manifests": [], "uploads": []},
  "skipped": {
    "blobs": [
      {"digest": "sha256:3743d06b58c3d00e65d41cd2b2b3fd085734090d9666a67afc9b989f8bde153b",
       "reason": "reachable again: referenced by a tagged manifest"}
    ],
    "manifests": [], "uploads": []
  }
}
```

标签与引用随后都可拉取：

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  http://localhost:5000/v2/race/demo/manifests/cap-pub-first   # 200
curl -s -o /dev/null -w '%{http_code}\n' \
  http://localhost:5000/v2/race/demo/blobs/sha256:3743d06b... # 200
```

**顺序二：GC 先删除 → PUT 返回 400，且无可见标签**

```bash
# dry-run 生成计划（plan_id=P2，候选为 blob sha256:b2f4...）
# 先执行 GC：候选仍不可达，元数据与文件都被删除
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": false, "plan_id": "P2"}' http://localhost:5001/gc
# deleted.blobs 含：
# {"digest": "sha256:b2f4078147e44eb852792b0ab466ef2dd4c423bbcfa02a86a8871ee60504b101",
#  "size": 22, "repos": ["race/demo"], "reason": "still unreferenced at execution"}

# 之后（在真实并发里是「被锁挡住、GC 释放后才继续」的那次）发布引用它的标签
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest.json \
  http://localhost:5001/v2/race/demo/manifests/cap-gc-first
```

```http
HTTP/1.1 400 Bad Request
```

```json
{"errors":[{"code":"MANIFEST_BLOB_UNKNOWN",
  "message":"manifest references unknown content in repo 'race/demo': sha256:b2f4078147e44eb852792b0ab466ef2dd4c423bbcfa02a86a8871ee60504b101",
  "detail":{"repo":"race/demo",
            "missing":["sha256:b2f4078147e44eb852792b0ab466ef2dd4c423bbcfa02a86a8871ee60504b101"]}}]}
```

标签没有写入，两个实例上都不可见；既有标签未受影响：

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  http://localhost:5000/v2/race/demo/manifests/cap-gc-first    # 404 MANIFEST_UNKNOWN
curl -s http://localhost:5000/v2/race/demo/tags/list
# {"name":"race/demo","tags":[... 不含 cap-gc-first ...]}
```

### 并发对抗实测（真实 HTTP，双进程）

在两个独立进程上，每轮上传一个候选 blob、dry-run 出计划后，用三个线程
**同时**发起：进程 A 执行 GC、进程 B 执行同一计划、进程 B `PUT` 引用该
blob 的新标签；连续 40 轮，每轮从两个端口核对「有标签必有 blob、无标签
必为 404、同一计划恰好一个 GC 赢家」：

```text
{
  "tag_ok": 36,              # 发布先完成：201，候选进入 GC 的 skipped
  "put_rejected": 4,         # GC 先删完：400 MANIFEST_BLOB_UNKNOWN，无标签
  "gc_200": 40,              # 每轮恰好一个认领赢家执行成功
  "gc_409": 40,              # 另一进程：GC_PLAN_IN_PROGRESS / ALREADY_EXECUTED
  "candidate_deleted": 4
}
OK: 40 rounds, both orderings observed, no dangling tags
```

- 36 轮发布先提交标签：PUT `201`，GC 响应 `skipped.blobs` 中带
  `"reachable again: referenced by a tagged manifest"`，blob 仍可拉取；
- 4 轮 GC 先删完：PUT `400 MANIFEST_BLOB_UNKNOWN`，标签与 manifest 均为
  `404`，两个端口上都没有悬空引用；
- 80 次 GC 执行请求中恰好 40 个 `200`、40 个 `409`（认领互斥 + 不可重放

保留不变）。

### 同摘要重传 / 跨仓库挂载与 GC 删除窗口的竞态

#### 问题：去重路径丢弃新内容

blob 全局按摘要去重，同一个 digest 可能被重复推送（客户端重试、镜像
re-push、跨仓库共享）。GC 删除一个候选 blob 时分两步：**先删 SQLite 里的
`blobs`/`repo_blobs` 记录，再删 blob 文件**。修复前这两步只与「发布
manifest」互斥，而**不与 blob 提交互斥**，于是共享目录的两个进程可能：

1. 进程 A `POST /gc`（认领计划后）删掉 blob X 的 SQLite 记录，**尚未删文件**；
2. 进程 B `POST /v2/demo/app/blobs/uploads?digest=X` 重传同一内容；
   `FileStorage.commit_blob` 发现**旧文件还在**，便按去重逻辑丢弃 B 刚校验
   过的新字节，却为 X **重建了 blobs/repo_blobs 关联**；
3. 进程 A 接着删掉 blob 文件。

结果：数据库里有 X 的关联、磁盘上却没有文件。随后 `PUT manifest` 的引用
存在性校验只查数据库（通过），标签发布返回 `201`，但 `GET blob` 是 `404`
——又是一个悬空引用。

#### 裁决：把 blob 提交纳入同一把排他 flock

单块整体上传（`POST ?digest=`）、分块完成（`PUT ?digest=`，含摘要复核）与
跨仓库挂载（`?mount=&from=`）的「**内容就位（os.replace）→ blobs/repo_blobs
关联落库**」区间，现在与 GC 的「复核 → 删元数据 → 删文件」区间、以及
manifest 发布区间，在同一把跨进程排他 `flock` 上严格串行。于是只有两种
交错，且都安全：

- **重传/挂载先完成**：blob 内容与关联先成为既成事实；若此后标签已发布，
  GC 复核到候选重新可达 → `skipped`，不删；若始终没有标签引用，GC 按
  可达性/宽限期正常回收，不会出现有记录无文件的悬空 blob。
- **GC 先完成**：重传方在锁外等到「元数据与文件都删完」才继续，
  `os.replace` 把**刚按摘要校验过的完整新内容**落为新文件并重建关联
  （新记录受宽限期保护）；随后发布标签 `201`、拉取 blob `200`。跨仓库
  挂载方则观察到 blob 已不存在，`mount_blob` 返回 `false`，路由按 OCI
  规范**回退为普通上传会话（202）**，不会产生指向不存在内容的挂载。

摘要校验（重传内容仍与 `?digest=` 重算比对）、计划认领及范围、宽限期、
上传租约与标签 CAS 行为全部保持不变。

#### 实际请求与响应（真实 HTTP 实测）

以下在 `docker compose up`（或 `uvicorn app.main:app`）的实例上实测。

**A. 普通同摘要重传：两次都是 201（去重，不丢内容）**

```bash
$ printf 'same-digest retransmit demo' > blob.bin
$ DIGEST="sha256:$(sha256sum blob.bin | cut -d' ' -f1)"   # sha256:58f37737...
$ curl -s -i -X POST --data-binary @blob.bin \
    "http://localhost:5000/v2/demo/app/blobs/uploads?digest=$DIGEST"
# HTTP/1.1 201 Created
# location: /v2/demo/app/blobs/sha256:58f37737f1968745aa01e61e21810330a436f101dd2d476f866d6509d41bec62
# docker-content-digest: sha256:58f37737f1968745aa01e61e21810330a436f101dd2d476f866d6509d41bec62

# 同一摘要再传一次（重试 / re-push）：仍然 201，全局仍只有一份内容
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST --data-binary @blob.bin \
    "http://localhost:5000/v2/demo/app/blobs/uploads?digest=$DIGEST"
# 201
```

**B. GC 先删：重传落下完整新文件，标签发布后 blob 可拉取**

先让该 blob 老过宽限期，dry-run + execute 把它回收（`grace_seconds:0`
仅为演示）：

```bash
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": true, "grace_seconds": 0}' http://localhost:5000/gc   # plan_id=d3528c2c...
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "d3528c2c90974068ae45113313f6f9e7"}' http://localhost:5000/gc
# {"summary": {"blobs_deleted": 1, "bytes_reclaimed": 27, ...},
#  "deleted": {"blobs": [{"digest": "sha256:58f37737...", "size": 27,
#                         "repos": ["demo/app"],
#                         "reason": "still unreferenced at execution"}], ...}}

$ curl -s -o /dev/null -w '%{http_code}\n' \
    "http://localhost:5000/v2/demo/app/blobs/$DIGEST"
# 404
{"errors":[{"code":"BLOB_UNKNOWN","message":"blob sha256:58f37737... unknown in repo 'demo/app'", ...}]}
```

此刻（真实并发里是「被 flock 挡在 GC 的删元数据/删文件窗口之外」）重传
同一摘要——修复前这里会因旧文件还在而丢弃新字节；修复后重传方在锁外等
到文件也被删完，`os.replace` 落下完整新内容并重建关联：

```bash
$ curl -s -i -X POST --data-binary @blob.bin \
    "http://localhost:5000/v2/demo/app/blobs/uploads?digest=$DIGEST"
# HTTP/1.1 201 Created
# location: /v2/demo/app/blobs/sha256:58f37737f1968745aa01e61e21810330a436f101dd2d476f866d6509d41bec62
# docker-content-digest: sha256:58f37737f1968745aa01e61e21810330a436f101dd2d476f866d6509d41bec62

$ curl -s -o got.bin -w 'HTTP %{http_code}, %{size_download} bytes\n' \
    "http://localhost:5000/v2/demo/app/blobs/$DIGEST"
# HTTP 200, 27 bytes        （字节与重传内容一致，不再是 404）
```

随后发布引用它的标签：`201`，且标签与 blob **都**可拉取（修复前为
`PUT 201` 而 `GET blob 404`）：

```bash
$ curl -s -o /dev/null -w 'PUT manifest -> %{http_code}\n' -X PUT \
    -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
    --data '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
             "layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar",
                        "digest":"sha256:58f37737...","size":27}]}' \
    http://localhost:5000/v2/demo/team/manifests/reuploaded
# PUT manifest -> 201
# docker-content-digest: sha256:ba508aba09c09215a9e3f0765a82f1c32c3deeedf85ca2ca288e55a58748543b

$ curl -s -o /dev/null -w 'GET manifest -> %{http_code}\n' \
    http://localhost:5000/v2/demo/team/manifests/reuploaded
# GET manifest -> 200
$ curl -s -o /dev/null -w 'GET blob     -> %{http_code}\n' \
    "http://localhost:5000/v2/demo/team/blobs/$DIGEST"
# GET blob     -> 200
```

**C. 跨仓库挂载撞上 GC 已删：回退普通上传会话（202），而不是 201 悬空挂载**

```bash
$ curl -s -i -X POST \
    "http://localhost:5000/v2/demo/team/blobs/uploads?mount=$DIGEST&from=demo/app"
# HTTP/1.1 202 Accepted
# location: /v2/demo/team/blobs/uploads/ddbd4b1a5da14ee09e8e94b680140bf4
# range: 0-0
# docker-upload-uuid: ddbd4b1a5da14ee09e8e94b680140bf4

# 用回退会话正常完成上传，再发布标签，manifest 与 blob 均 200
$ curl -s -o /dev/null -w '%{http_code}\n' -X PUT --data-binary @blob.bin \
    "http://localhost:5000/v2/demo/team/blobs/uploads/ddbd4b1a5da14ee09e8e94b680140bf4?digest=$DIGEST"
# 201
```

blob 仍存在时挂载不受影响，照常 `201`：

```bash
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST \
    "http://localhost:5000/v2/demo/team/blobs/uploads?mount=$DIGEST&from=demo/app"
# 201
```

#### 双进程删除窗口实测（确定性复现）

仓库附带 `tests/live_race_demo.py`：启动一个 `uvicorn` 实例后，脚本以
**共享同一数据目录的第二个进程**身份执行 GC（在「已删元数据、未删文件」
窗口内暂停），对第一个实例的真实 HTTP 端口依次发起单块重传、分块提交、
跨仓库挂载。实测输出：

```text
=== scenario: monolithic retransmit ===
[A] initial push                 -> 201
[B] deleted DB rows, file still present; holding window...
[A] same-digest retransmit      -> 201 (blocks on flock until GC finishes)
[A] GET retransmitted blob        -> 200, 38 bytes, content intact
[A] PUT manifest tag t-...         -> 201 (sha256:ad2bcde2...)
[A] GET manifest / GET blob       -> 200 / 200

=== scenario: chunked finalize ===
[B] deleted DB rows, file still present; holding window...
[A] chunked finalize             -> 201 (blocks on flock until GC finishes)
[A] GET retransmitted blob        -> 200, 189 bytes, content intact
[A] PUT manifest tag t-...         -> 201 (sha256:90c3af78...)
[A] GET manifest / GET blob       -> 200 / 200

=== scenario: cross-repo mount ===
[B] deleted DB rows, file still present; holding window...
[A] cross-repo mount             -> 202 (202 = fell back to a normal upload session)
[A] blob absent after fallback?  -> True
[A] PUT manifest tag t-...         -> 201 (sha256:b8aed0c7...)
[A] GET manifest / GET blob       -> 200 / 200

ALL SCENARIOS OK
```

## 目录结构

```
app/
  main.py       # FastAPI 路由（/v2/*、/gc 两阶段复核与跨进程原子认领）
  registry.py   # 核心逻辑：上传、manifest CAS、GC 可达图与计划原子认领/复核执行；
                #   发布、blob 提交与 GC 的关键区间在进程锁外再持跨进程 flock
  coord.py      # 跨进程协调：fcntl.flock 文件读写锁（registry.lock）
                #   EX=发布/blob 提交/GC 执行，SH=dry-run；进程死亡由内核自动释放
  db.py         # SQLite 元数据（WAL + busy_timeout；gc_plans 支持 pending/executing/executed 三态原子认领）
  storage.py    # 本地文件存储：blobs/sha256/<hex> 内容寻址 + uploads/<uuid>
  digest.py     # digest/tag/repo 校验与 sha256 计算
  errors.py     # OCI 错误模型（含 GC_PLAN_*，认领失败为 GC_PLAN_IN_PROGRESS 409）
  config.py     # 环境变量配置（含 registry.lock 路径）
tests/test_registry.py   # 39 个端到端测试
tests/live_race_demo.py  # 双进程删除窗口真实 HTTP 竞态演示（手动运行，非 pytest 用例）
Dockerfile / docker-compose.yml（默认单实例；--profile multiproc 启动共享卷双实例）
```

## 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：分块上传与续传、摘要校验失败、幂等重试、跨仓库去重与挂载、
manifest 引用校验、digest 不可变性、标签 CAS 冲突、GC dry-run/执行、
宽限期与租约保护、GC 与并发拉取/发布竞争、重启后恢复上传，以及 GC 复核计划：
dry-run 后新增候选不被误删、重新可达/续租跳过、计划缺失/未知/过期(410)/已执行
(409)拒绝、计划跨重启有效且只能执行一次；两个独立进程（各自的连接、锁
与 Registry 实例，fork 子进程）同时执行同一 `plan_id` 时恰好一个成功删除、竞争
方 409 `GC_PLAN_IN_PROGRESS` 且不碰候选；认领后模拟崩溃，计划保留为不可重放的
`executing`、重试 409、剩余候选可重新 dry-run 回收；长期未完成的僵死计划
`stale:true` 提示；旧两态（pending/executed）数据库平滑迁移；**新增两个
跨进程竞态测试（fork 独立进程共享数据目录，真实 SQLite/文件/flock，不经
HTTP mock）：发布先提交标签时 GC 复核在锁后进行、候选重新可达被跳过且标签
引用可拉取；GC 持锁先删完元数据与文件后发布才拿到锁、PUT 返回
`400 MANIFEST_BLOB_UNKNOWN`、标签不可见（404）且既有标签引用仍可拉取**；
**再新增四个同摘要重传/挂载测试：单块重传、分块提交、跨仓库挂载三个 fork
子进程在 GC「已删元数据、未删文件」窗口内发起，均被 flock 挡到删除完成——
重传落下完整新文件并重建关联（标签 201、blob 200），挂载按规范回退普通
上传会话（202）；重传并先发布标签时 GC 复核跳过可达候选。这四个测试在
修复前的代码上确定性失败（重传不被阻塞，产生有记录无文件的悬空 blob）**。
真实 HTTP 的删除窗口复现可手动运行 `python3 tests/live_race_demo.py
<data_dir>`（另起一个 uvicorn 实例，脚本作为共享目录的第二个进程）。

## 设计说明与限制

- **共享数据目录与多进程**：SQLite 以 WAL 打开并设置 15s `busy_timeout`，
  多个进程可以挂载同一个数据目录（例如同一卷上的两个容器、滚动升级时的新旧
  实例并存）。两层协调保证一致性：`gc_plans` 的原子状态认领保证同一计划跨
  进程只会被一个进程执行（竞争方 409，不碰候选）；数据目录内的
  `registry.lock`（`fcntl.flock`）把三类关键区间跨进程严格串行——manifest
  发布的「引用校验→标签提交」、blob 提交（单块重传/分块完成/跨仓库挂载）的
  「内容就位→关联落库」与 GC 的「复核→元数据/文件删除」。发布先提交则 GC
  跳过已可达候选，GC 先删完则发布 400 且无可见标签；同摘要重传先完成则 GC
  按可达性/宽限期正常复核，GC 先删完则重传落下完整新文件并重建关联（挂载
  回退普通上传）；标签可见时其引用必可拉取。
  `flock` 随进程死亡由内核自动释放，无需 TTL/续约，也不存在僵死锁；dry-run
  持共享锁拍摄快照。compose 默认单实例运行，加 `--profile multiproc` 可一键
  启动共享同一卷的两个实例（`:5000`/`:5001`）做并发验证；`flock` 要求各
  实例与数据目录在同一台宿主机上，水平扩展到多节点仍需替换为外部数据库与
  共享存储。
- **命名空间隔离**：blob 物理上全局去重，但 `GET /v2/<repo>/blobs/<digest>`
  要求该 blob 已在 `<repo>` 命名空间内关联，跨仓库不可直接读。
- **manifest 不可变**：同一 digest 的内容恒定；digest 寻址的 PUT 会校验内容
  与摘要一致；标签只是指向 digest 的可变指针，且移动必须走 CAS。
