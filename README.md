# yikv-server

[English README](README.en.md)

## 1. 项目作用

**yikv-server** 在 [yikv](../yikv) 之上提供 **多表 KV 在线服务**：数据落在 `{db_path}/{表名}/` 的 mmap arena 与索引中，通过 RPC 暴露 **Get / Put / PutBatch / BatchGet**。

- **双协议、同端口**：**brpc `baidu_std`**（`BaiduMasterService`）与 **gRPC `h2:grpc`**（[`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)）；业务载荷均为 **FlatBuffers**（[`proto/yikv_server.fbs`](proto/yikv_server.fbs) → [`gen/yikv_server_generated.h`](gen/yikv_server_generated.h)）。
- **离线建索引**：对外通过 **`pipeline_agent` HTTP API** 编排导入与制品；底层由 **`yikv_import_pipeline`** 写入 `KVIndex`。**调度与业务只调 HTTP**，勿在机器上手跑导入命令。
- **实时增量（可选）**：表目录下 **`table.json`** 配置 Kafka，由 **KafkaSource** 消费 JSON 变更写入同一索引（格式见下文简述）。

更细的契约、排错与内部流程见 **[`db.md`](db.md)**。

---

## 2. 快速安装

**环境**

- **Bazel**（与仓库 [`MODULE.bazel`](MODULE.bazel) / [`.bazelversion`](.bazelversion) 一致）。
- **系统**：**OpenSSL**；编译/运行导入与压测需 **Apache Arrow C++ / Parquet**（如 `libarrow-dev`、`libparquet-dev`）。
- **MySQL 源导入（可选）**：`libmysqlclient-dev`（构建）及运行时的 **`libmysqlclient.so`**。

**构建**

```bash
cd yikv-server
bazel build //:yikv_server //:yikv_import_pipeline //:yikv_server_bench
```

可执行文件在 `bazel-bin/`（或 `bazel run //:yikv_server -- /path/to/config.json`）。

**配置**

复制并编辑 [`config.example.json`](config.example.json) 为 `config.json`。服务与导入工具**共用**其中的 `db_path`、`arena_seg_gb`、`arena_max_gb`、`exclusive_arena_lock`（**不可**在导入 CLI 里改这些，避免与线上一致）。

| 字段 | 说明 |
|------|------|
| `db_path` | 数据根目录；每个子目录名即表名 |
| `listen` | 监听地址，默认 `0.0.0.0:9000` |
| `arena_seg_gb` / `arena_max_gb` | 单段与总 arena 上限 (GiB) |
| `exclusive_arena_lock` | 默认 `true`，导入与服务不要同时写同一库 |
| `kafka.default_brokers` | 可选；表级可在 `table.json` 覆盖 |

---

## 3. 流程介绍

### 3.1 离线索引构建（仅 HTTP API）

**构建机 / 线上机**运行 **[`tools/pipeline_agent/pipeline_agent.py`](tools/pipeline_agent/pipeline_agent.py)**（FastAPI）。**离线构建、推送、部署只对调度侧暴露 HTTP**；agent 在进程内调用 `yikv_import_pipeline` 与 `tools/artifact_sync`。数据源形态（Parquet/CSV/目录/云 URI/MySQL 等）与导入细节见 **[`db.md`](db.md)**；**直接执行 `yikv_import_pipeline` 仅用于排错或开发**。

**依赖（构建机）**

```bash
pip install -r tools/pipeline_agent/requirements.txt
cd yikv-server   # 须已 `bazel build //:yikv_import_pipeline`（§2）
python3 tools/pipeline_agent/pipeline_agent.py   # 默认 0.0.0.0:8787，HOST/PORT/WORK 等见脚本文件头
```

**HTTP 路由摘要**

| 方法 | 路径 | 作用 |
|------|------|------|
| `GET` | `/health` | 探活 |
| `POST` | `/buildIndex` | 仅本机构建（删除 `BUILD_DB/<表>` 后全量导入） |
| `POST` | `/pushIndex` | 仅推送制品 |
| `POST` | `/publishIndex` | 构建 + 推送 |
| `POST` | `/deployIndex` | 拉取、切 `active`、更新 `SERVER_DB` 链接触发 `reload`（通常跑在**线上机**） |
| `POST` | `/pullIndex` | 仅拉取（可选不切 `active`） |
| `POST` | `/switchReloadIndex` | 本地已有 release 时切 `active` 并 `reload` |

**请求示例**：`POST /publishIndex`，`Content-Type: application/json`

```json
{
  "table": "my_table",
  "data_dir": "/data/raw/my_table",
  "schema_json": "/abs/path/schema.json"
}
```

`input` 与 `data_dir` **二选一**；**`schema_json`** 可省略时使用 agent 环境默认值。列名须与 schema 一致（不区分大小写）。

### 3.1.1 离线索引发布与制品

推送与拉取走 **`/pushIndex`**、**`/publishIndex`**、**`/deployIndex`**；存储后端由 **`$WORK/artifact-storage.yaml`**（agent 可自动生成）配置，说明见 **[`tools/artifact_sync/README.md`](tools/artifact_sync/README.md)**（agent 内部会调用该工具）。

### 3.1.2 离线索引端到端流程（构建 → 制品 → 上线）

典型 **构建机 + 线上机**；**调度只发 HTTP**（可用 [`tools/schedule_pipeline.py`](tools/schedule_pipeline.py) / [`tools/pipeline_reload.py`](tools/pipeline_reload.py)）。

| 阶段 | HTTP | 说明 |
|------|------|------|
| 1. 离线导入 | **`POST /buildIndex`** 或 **`/publishIndex`** 的第一步 | 使用与线上一致的 `schema.json`；agent 每次会先删掉本机 **`BUILD_DB/<表名>`**（默认 `$WORK/build_db/<表名>`），再全量导入，避免在原目录上重复 Open 导致数据叠加。 |
| 2. 上传制品 | **`POST /pushIndex`** 或 **`/publishIndex`** 内嵌 | 将表目录打成 **`build_id`** 写入制品库。 |
| 3. 拉取并切版本 | **`POST /deployIndex`**（线上 agent） | 在 **`$WORK/releases/<表名>/<build_id>/`** 落盘，原子更新 **`active`**。 |
| 4. 服务目录 | 含于 **`/deployIndex`** | **`SERVER_DB/<表名>`** → **`releases/<表名>/active`**（须与 **`config.json`** 的 **`db_path`** 一致）。 |
| 5. 热加载 | 含于 **`/deployIndex`** / **`/switchReloadIndex`** | 经 **`admin_unix_socket`** 发送 **`reload <表名>`**（路径与 `db_path` 默认规则见 **[`db.md`](db.md)** §3）。 |

**调度客户端**：[`tools/schedule_pipeline.py`](tools/schedule_pipeline.py)（或 [`tools/pipeline_reload.py`](tools/pipeline_reload.py)）——`POST /publishIndex` + `POST /deployIndex`。环境变量 **`BUILD_AGENT_URL`**、**`ONLINE_AGENT_URL`**、`--table`、`--data-dir` 等见 **`--help`**。

**自检**：部署后 `readlink -f $db_path/<表名>` 应指向当前 **`…/releases/<表名>/<build_id>`**；`reload` 应答为 **`ok`**（见 **[`db.md`](db.md)**）。

### 3.2 启动服务

**唯一参数**为配置文件路径：

```bash
./bazel-bin/yikv_server ./config.json
```

确保 `db_path` 存在、磁盘与 arena 配置足够。服务**启动时**扫描 `db_path` 下的表子目录并打开；运行期中新增的表目录可在落盘后发送 **`reload <表名>`**（`admin_unix_socket`）完成首次加载，已打开表的换盘同样使用该命令（详见 **[`db.md`](db.md)**）。

### 3.3 使用示例

**RPC 调用**

- 服务名：**`yikv.db.YikvDb`**；方法：**Get、Put、PutBatch、BatchGet**。
- **brpc**：`baidu_std`，请求体为 FlatBuffers 序列化后的 `GetRequest` / `PutRequest` 等（见 `yikv_server.fbs`）。
- **gRPC**：`h2:grpc`，`FbRpcRequest.payload` / `FbRpcResponse.payload` 为**同一套** FlatBuffers 字节。

任意语言：可用 **brpc C++** 或 **官方 gRPC + `yikv_grpc.proto` 生成 Stub**，按 fbs 表构造/解析 payload。双栈细节与 PutBatch 语义见 **[`db.md`](db.md)**。

**Kafka 实时写入（可选）**

在 `{db_path}/{表名}/table.json` 配置 topic 等；消息为 JSON，字段 **`_op`**（`INSERT`/`UPSERT`/`DELETE`）、**`_ts`**（毫秒）及业务字段。全局 broker 来自 `config.json` 的 `kafka.default_brokers`。完整约定见 [`src/yikv_server/table_config.h`](src/yikv_server/table_config.h) 与 [`db.md`](db.md)。

---

## 4. Benchmark

**`yikv_server_bench`** 对 **Get** 发压，协议与线上一致（brpc `baidu_std`）。

**必填**

- `--server HOST:PORT`（别名 `--grpc_target`）
- `--index`（或 `--table`）：须与已加载表名一致
- 键源：**`--keys_file`** 或 **`--local_parquet PATH --pk COLUMN`**

**负载**：`--requests N` 与 `--duration_sec T` 二选一；默认 `--requests 50000`。

**常用**

| 参数 | 默认 | 说明 |
|------|------|------|
| `--workers` | 8 | 并发线程 |
| `--warmup` | 32 | 预热次数 |
| `--max_keys` | 200000 | 从 Parquet 采样主键上限 |

```bash
./bazel-bin/yikv_server_bench \
  --server 127.0.0.1:9000 \
  --index my_table \
  --keys_file ./pks.txt \
  --workers 16 \
  --requests 1000000
```

标准输出 **一行 JSON**（`qps`、`latency_ms`、`phases_ms` 等）。

---

## 延伸阅读

- 协议与导入流水线架构：**[`db.md`](db.md)**
- 离线索引 HTTP agent：**[`tools/pipeline_agent/pipeline_agent.py`](tools/pipeline_agent/pipeline_agent.py)**
- gRPC 定义：**[`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)**
