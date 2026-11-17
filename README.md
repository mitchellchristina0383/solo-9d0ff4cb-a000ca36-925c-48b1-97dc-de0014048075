# Build Artifact Cache

CI 构建产物缓存服务：FastAPI + SQLite + 本地目录。解决"清理构建产物缓存时，
运行中的构建可能仍在下载旧产物"的问题——下载必须持有**限时占用令牌（租约）**，
回收只触碰**未被占用**的产物，且按**最久未用（LRU）**优先。

**内容寻址去重（dedup）**：已通过 SHA-256 校验的相同字节只在磁盘上保存一份，
多个产物号、多次重传代际各自独立，但共享同一份物理存储（blob）；只有最后一个
引用被回收时才真正删除文件、释放磁盘。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000/docs （Swagger UI，可直接操作全部接口）
```

本地开发：

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload        # http://127.0.0.1:8000/docs
.venv/bin/pytest                               # 运行测试
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `PUT` | `/artifacts/{number}?sha256=<hex>` | 上传/重传产物（≤ 1 MiB，校验 SHA-256，不匹配 400，超限 413）；相同内容去重为同一 blob |
| `GET` | `/artifacts` / `/artifacts/{number}` | 列表 / 元数据（`downloadable` 仅在记录、blob 与文件一致且就绪时为真；含 `shared`、`references`） |
| `POST` | `/artifacts/{number}/tokens` | 领取限时占用令牌（回收中的产物 409 拒发） |
| `GET` | `/artifacts/{number}/content?token=` | 凭令牌下载（无令牌 401，无效/过期/已释放 403，代际不符 409） |
| `POST` | `/tokens/{token}/renew` | 续期一个 TTL（已过期/已释放/产物变更均 409） |
| `POST` | `/tokens/{token}/release` | 完成后释放（幂等） |
| `POST` | `/reclaim` | 输入 `target_bytes`，LRU 优先回收未占用产物，返回**实际** `freed_bytes`；删除共享引用但仍有他者引用时 `file_deleted=false` 且不计字节 |
| `POST` | `/admin/sweep` | 手动触发恢复清扫（中断的回收、零引用 blob、孤儿文件） |
| `GET` | `/stats` / `/health` | 统计（含 `blobs`、`physical_bytes`、`shared_blobs`）/ 健康检查 |

配置（环境变量）：`TOKEN_TTL_SECONDS`（默认 300）、`MAX_UPLOAD_BYTES`（默认
1048576）、`CACHE_DB_PATH`、`CACHE_STORAGE_DIR`、`SWEEP_INTERVAL_SECONDS`（后台
清扫周期，0 关闭）、`ORPHAN_GRACE_SECONDS`（孤儿文件宽限期）。

## 关键设计

- **内容寻址 blob 存储**：物理文件按 SHA-256 存放于
  `blobs/<前两位>/<sha256>`，`blobs.ref_count` 记录被多少个产物行引用。产物行
  只保存 `(number, generation, blob_sha256, ...)`，因此不同产物号或不同代际的
  相同字节只占一份磁盘；产物身份、代际、令牌与 LRU 顺序仍各自独立。
- **损坏文件不能充当共享来源**：复用已有 blob 前先读回并重新计算 SHA-256；
  文件缺失或哈希不符时，用本次已校验的上传体原子重写修复。`downloadable`
  会同时检查文件存在、长度与哈希；每次下载仍逐字节复核，损坏即 409，绝不
  对外声称可下载。
- **租约占用**：令牌绑定 `(number, generation)`，限时有效；回收候选必须
  `NOT EXISTS` 活跃令牌，因此**占用中的产物绝不会被删**。
- **共享引用回收**：删一条未占用引用时，若 blob 仍被其他产物引用，则只删
  产物行、保留物理文件（另一条仍可下载），`freed_bytes` 不增加；仅当删除的
  是最后一个引用、文件真正 unlink 成功时，才把该 blob 大小计入 `freed_bytes`。
  空间不足时如实返回实际能释放的字节数。
- **代际（generation）**：每次上传 generation +1；旧令牌读不到同号新产物
  （409）。同号重传相同内容时引用随产物行平移、不重复计数；重传不同内容会
  释放旧 blob（若已是零引用）。
- **两段式回收与失败恢复**：先条件 `UPDATE ... SET state='reclaiming'`
  （原子复查"无活跃令牌"，此后拒发新令牌、拒绝续期），再删产物行；零引用
  blob 标记 `reclaiming` 后删文件、删行。unlink 失败则保留零引用 blob 行，
  由下次回收 / 后台清扫 / **启动清扫**重试；重传使 blob 重新获得引用时，
  待删除操作会跳过它并恢复 `ready`，**不会误删仍被引用的内容**。
- **失败与重启一致性**：上传先落盘再提交行，崩溃至多留下孤儿文件（宽限后
  清扫）；任何可下载记录都不会指向已删内容——最后引用删除与 blob 释放串行
  进行，零引用 blob 无任何产物行指向它。旧版（按上传独立文件）数据库在启动
  时自动迁移到 blob 布局。
- **并发模型**：单进程单 SQLite 连接 + 全局锁串行化全部操作（WAL、显式
  `BEGIN IMMEDIATE` 保证崩溃原子性）。多副本水平扩展不在本设计范围内。

## 已验证

- 22 项原有功能/并发测试（含真实 uvicorn 下 8 线程交错）全部保持通过，另新增
  15 项去重专项测试：跨产物号/跨代际去重只占一份磁盘、共享引用回收不影响他者、
  `freed_bytes` 仅在真正 unlink 时计数、损坏/缺失文件被修复且不充当共享来源、
  unlink 失败可恢复、重启后完成中断回收、零引用/孤儿 blob 清扫、旧版库自动迁移，
  以及真实 uvicorn 下高重复内容的 8 线程压力（无 5xx、可下载记录均可下载、
  引用计数与磁盘文件一一对应）。
- 真实 HTTP 冒烟（容器同款 `uvicorn app.main:app` 命令）：`/docs`、
  `/openapi.json` 可用；两号同内容仅一个物理文件、`physical_bytes` 为一份大小；
  删一条共享引用后另一条仍可下载且 `freed_bytes=0`，最后引用回收才释放字节；
  进程重启后数据存活，启动清扫清掉中断回收遗留的零引用 blob 而保留仍被引用者。

限制：本沙箱无 Docker 引擎，`docker compose up --build` 未实际执行；Compose
文件本身保持可用，且其使用的命令（`uvicorn app.main:app`）、环境变量与依赖
均已在本地逐一验证。
