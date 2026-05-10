# yikv-server

[English README](README.en.md)

## 1. 项目作用

**yikv-server** 在 [yikv](../yikv) 之上提供 **多表 KV 在线服务**：数据落在 `{db_path}/{表名}/` 的 mmap arena 与索引中，通过 RPC 暴露 **Get / Put / PutBatch / BatchGet**。

- **双协议、同端口**：**brpc `baidu_std`**（`BaiduMasterService`）与 **gRPC `h2:grpc`**（[`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)）；业务载荷均为 **FlatBuffers**（[`proto/yikv_server.fbs`](proto/yikv_server.fbs) → [`gen/yikv_server_generated.h`](gen/yikv_server_generated.h)）。
- **离线建索引**：**`yikv_import_pipeline`** 将 Parquet/CSV、云存储 URI（oss/s3/cos/obs/gs）、或 **MySQL 兼容**查询结果流式写入 `KVIndex`。
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

### 3.1 索引构建

使用 **`yikv_import_pipeline`**。导入前建议 **停止**已打开同一 `db_path` 的 `yikv_server`（或本进程使用 `--no_arena_lock`，慎用）。

**文件 / 目录（本地或云 URI）**

- 支持 **`.parquet` / `.csv`**；`--input_dir` 递归收集；可多组 `--input` / `--input_list`。
- 云前缀：**`oss://` `s3://` `cos://` `obs://` `gs://`**，环境变量见构建日志与 [`db.md`](db.md)。

```bash
./bazel-bin/yikv_import_pipeline \
  --config ./config.json \
  --index my_table \
  --schema_json ./schema.json \
  --create_if_missing \
  --input_dir /data/parts/ \
  --import_io_workers 4 \
  --import_queue_batches 32
```

**MySQL / StarRocks（MySQL 协议）**：`--mysql_query` 或 `--mysql_query_file`，连接可用 `--mysql_*` 或环境变量 `MYSQL_HOST`、`MYSQL_USER` 等（CLI 优先）。详见 **[`examples/mysql_import/README.md`](examples/mysql_import/README.md)**。

**说明**：结果列名须与 **`schema.json`** 字段名一致（不区分大小写）；CSV / MySQL 路径不支持数组字段。新建表时可用 **`--sql_est_rows`**（如 `COUNT(*)`）辅助 HashMap 容量估计。

### 3.1.1 离线索引发布（可选）

构建完成后若要把表目录同步到 **本机汇总目录** 或 **S3 兼容对象存储**（OSS/OBS/COS 等），可用配置驱动脚本 **`tools/artifact_sync/`**：一份 `artifact-storage.yaml` 切换 `local` / `s3_compatible`，无需改 `yikv_import_pipeline` 二进制。见 **[`tools/artifact_sync/README.md`](tools/artifact_sync/README.md)**。

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
- gRPC 定义：**[`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)**
