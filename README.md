# yikv-server

在本仓库 Bazel 依赖的 **yikv** 库（见同级目录 [`../yikv`](../yikv)）之上提供对外 KV 服务：

- 同一 **`listen`** 上并存 **brpc `baidu_std`（`BaiduMasterService`，载荷为 FlatBuffers）** 与 **标准 gRPC `h2:grpc`**（Protobuf 外壳 + `payload` 内 FlatBuffers）。
- 多表：每个表对应 `{db_path}/{table_name}/` 目录；**Kafka** 等表级配置在表目录的 **`table.json`**；离线导入用 **`yikv_import_pipeline`**（Parquet/CSV、云 URI、MySQL 等，见 [`db.md`](db.md)）。

协议与内部实现细节见 [`db.md`](db.md)、[`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)、[`gen/yikv_server_generated.h`](gen/yikv_server_generated.h)（由 `proto/yikv_server.fbs` 生成）。

---

## 构建

在仓库根目录 `yikv-server` 下：

```bash
bazel build //:yikv_server //:yikv_import_pipeline //:yikv_server_bench
```

产物位于 `bazel-bin/`（或 `bazel run //:yikv_server -- …` 直接运行）。

**系统依赖（概览）**

- **服务**：brpc/protobuf 等由 Bazel 拉取；系统常用 **OpenSSL**。
- **导入 / 压测（Parquet 路径）**：需安装 **Apache Arrow C++ / Parquet** 开发包（如 `libarrow-dev`、`libparquet-dev`），并与 `linkopts` 中的 `-larrow` / `-lparquet` 一致。
- **`yikv_import_pipeline` MySQL 兼容拉数**：需 **MySQL C 客户端**（如 Debian/Ubuntu 安装 `libmysqlclient-dev` 开发与链接、运行时需 `libmysqlclient.so` 所在包通常随带），链接 **动态** `-lmysqlclient`。

---

## `config.json`（`yikv_server` / `yikv_import_pipeline` 共用）

`yikv_server` **唯一参数**为配置文件路径；**`yikv_import_pipeline` 必须使用同一文件中的 `db_path`、`arena_seg_gb`、`arena_max_gb`、`exclusive_arena_lock`**，避免与线上一致性不一致。

与 [`config.example.json`](config.example.json) 对齐的字段说明：

| 字段 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `db_path` | ✓ | — | 数据根目录；其下每个子目录名为表名（若含合法表数据则会被加载） |
| `listen` | — | `0.0.0.0:9000` | 服务监听地址 |
| `arena_seg_gb` | — | `1` | 单段 arena 大小 (GiB)，须为正 |
| `arena_max_gb` | — | `512` | arena 总上限 (GiB)，须 ≥ `arena_seg_gb` |
| `exclusive_arena_lock` | — | `true` | 打开时 mmap 前排他 `flock(arena.lock)`；多进程/工具需谨慎 |
| `kafka.default_brokers` | — | 空 | 全局默认 Kafka broker 列表；表级可在 `table.json` 覆盖 |

顶层 **不再有 `index` 块**；建表、schema、Kafka topic 等见下文章节。

---

## 表目录布局

每个表一个目录：`{db_path}/{table_name}/`。

| 文件 / 目录 | 说明 |
|-------------|------|
| arena 相关 | 由 yikv / 导入工具创建与维护 |
| `schema.json` | 表 Schema（导入创建表时可由工具写入；服务亦可依赖此） |
| `table.json` | 可选；配置该表的 **Kafka**（`topic`、`partition`、`brokers` 可选覆盖全局） |

热加载：服务对 `db_path` 做扫描/inotify，新表目录可被加载（见实现）。

---

## `yikv_server`：启动服务

```bash
./bazel-bin/yikv_server /absolute/or/relative/path/to/config.json
```

- 启动前确保 `config.json` 中 `db_path` 存在且磁盘空间充足。
- 若 `exclusive_arena_lock` 为 `true`，同一 `db_path` 上不应再有第二个写端（如正在跑的导入工具）同时打开。

---

## `yikv_import_pipeline`：并行导入（Phase A）

**`config.json` 与文件模式 CLI** 含 `--config`、`--index`、`--input*`、`--schema_json`、`--create_if_missing`、`--recreate`、`--no_arena_lock`；另支持 **`--import_io_workers`**、**`--import_queue_batches`**。多线程仅做 **IO 与解析**（文件模式下面向多文件 claim；MySQL 模式下仅单连接，多余 worker 空闲）；**`NewDoc` / `BatchPut` 固定单线程**，`AllocatorMode::SingleWriter`。

### 设计思想（导入流水线）

- **统一产出**：一切数据源最终变成 **`arrow::RecordBatch`**（列名与表 `schema.json` 一致），再经同一套 **`arrow_doc_helpers`** 写入 `KVIndex`。这样 Parquet、CSV、SQL 结果集在写入层不重复实现类型映射。
- **边界分层**：**`Source`**（[`src/indexer/source/source.h`](src/indexer/source/source.h)）只负责 `ProduceLoop` → 有界队列 **[`BoundedParsedBatchQueue`](src/indexer/queue/parsed_batch_queue.h)**；**[`IoPublisher`](src/indexer/publisher/io_publisher.cc)** 只负责起多线程调 `Source`；**[`RunKvWriteLoop`](src/indexer/worker/kv_write_worker.h)** 单线程消费队列、校验列、批量 `BatchPut`。
- **按协议扩展，而非「一个 SuperSQL」**：文件走 **[`FileSource`](src/indexer/source/file/file_source.h)**；**MySQL 线协议**（MySQL / MariaDB / **StarRocks FE** 等）走 **[`MysqlWireSource`](src/indexer/source/sql/mysql_wire_source.h)**；Hive / ODPS 等可另做 `Source` 实现，**不假设**所有引擎都兼容 MySQL 协议（详见实现与计划文档）。PG、Tunnel、Paimon 落盘等走各自实现或继续 **文件导入**。
- **容量提示**：新建索引时 **`bucket_bits`** 由估计行数推导。文件模式用 Parquet 元数据 / CSV 行数；MySQL 模式用 **`--sql_est_rows`**（可为 `COUNT(*)` 结果），未给时按 `0` 行处理为较小的默认 bucket 配置。

架构与其它阶段说明仍见 **[`db.md`](db.md) → 通用导入流水线**。

```bash
bazel build //:yikv_import_pipeline
./bazel-bin/yikv_import_pipeline \
  --config ./config.json \
  --index my_table \
  --input_dir /data/parts/ \
  --import_io_workers 4 \
  --import_queue_batches 32
```

### MySQL 兼容源（流式 `SELECT`）

连接参数字段与 **`libmysqlclient`** / `mysql_real_connect` 一致；`SELECT` 的结果列名须与 **`schema.json` 字段名**匹配（比较时**不区分大小写**）。不支持 **数组字段**（与 CSV 规则一致）；需 `CAST` 或展开成标量列。大结果集使用 **`mysql_use_result`** 流式读取，按 **`--mysql_batch_rows`** 切块推送 RecordBatch。

**必填**：`--mysql_host`、`--mysql_user`、`--mysql_database`，以及 **`--mysql_query`** 或 **`--mysql_query_file`**（文件内可为多行 SQL）。**不要**再传文件 `--input*`。

**常用可选项**：`--mysql_password`、`--mysql_port`（默认 3306）、`--mysql_batch_rows`（默认 4096）、`--mysql_init_sql`（每行一条会话 SQL）、`--sql_est_rows`（建索引估计行数）、`--import_io_workers 1`（推荐，单连接）。

**环境变量**（CLI 未传时补齐；**命令行优先**）：`MYSQL_HOST`、`MYSQL_USER`、`MYSQL_PASSWORD`（或已废弃的 `MYSQL_PWD`）、`MYSQL_DATABASE`、`MYSQL_TCP_PORT` 或 `MYSQL_PORT`。也可使用 `YIKV_MYSQL_HOST`、`YIKV_MYSQL_USER`、`YIKV_MYSQL_PASSWORD`、`YIKV_MYSQL_DATABASE`、`YIKV_MYSQL_PORT`。未传 `--mysql_port` 时才读端口环境变量。

**示例**（含 `import.sql`、`demo_table.sql`、分步说明）：[`examples/mysql_import/README.md`](examples/mysql_import/README.md)。

```bash
./bazel-bin/yikv_import_pipeline \
  --config ./config.json \
  --index my_table \
  --schema_json ./schema.json \
  --create_if_missing \
  --mysql_host 127.0.0.1 \
  --mysql_user root \
  --mysql_database demo \
  --mysql_query_file ./import.sql \
  --sql_est_rows 1000000 \
  --import_io_workers 1
```

---

## `yikv_server_bench`：Get 压测（brpc `baidu_std`）

对 `yikv.db.YikvDb` / **Get** 发压，协议与线上一致（`SerializedRequest` + `SampledRequest` meta）。

**必填**

- `--server HOST:PORT`（或兼容别名 `--grpc_target`）
- `--index` / `--table`：表名，须与服务器已加载表名一致
- 键源二选一：`--keys_file PATH` **或** `--local_parquet PATH --pk COLUMN`

**负载模式**

- `--requests N` 与 `--duration_sec T` 二选一；都不给时默认 `--requests 50000`。

**其它**

| 参数 | 说明 |
|------|------|
| `--workers` | 并发线程数，默认 `8` |
| `--warmup` | 预热 RPC 次数，默认 `32` |
| `--max_keys` | 从 Parquet 最多采样的主键行数，默认 `200000` |
| `--max_latency_samples` | 每 worker 参与分位的延迟样本上限，`0` 表示不截断 |

示例：

```bash
./bazel-bin/yikv_server_bench \
  --server 127.0.0.1:9000 \
  --index dsp_test3 \
  --keys_file ./pks.txt \
  --workers 16 \
  --requests 1000000

./bazel-bin/yikv_server_bench \
  --server 127.0.0.1:9000 \
  --index dsp_test3 \
  --local_parquet /data/part.snappy.parquet \
  --pk key \
  --workers 16 \
  --requests 100000
```

标准输出为一行 JSON，含 `qps`、`latency_ms`、`phases_ms` 等。

---

## Kafka：`table.json` 与消息格式

表级 Kafka 见 [`src/yikv_server/table_config.h`](src/yikv_server/table_config.h) 中的 **`table.json`** 结构；全局默认 broker 为 **`config.json` → `kafka.default_brokers`**。

每条 Kafka 消息为 JSON 对象（单条）或 JSON 数组（批量）。

### 单条消息

```json
{ "_op": "INSERT", "_ts": 1746784320000, "id": "abc", "score": 42, "tags": ["x","y"] }
{ "_op": "UPSERT", "_ts": 1746784321000, "id": "abc", "score": 99 }
{ "_op": "DELETE", "_ts": 1746784322000, "id": "abc" }
```

### 批量消息（一条 Kafka 消息包含多个操作）

```json
[
  { "_op": "INSERT", "_ts": 1746784320000, "id": "1", "name": "Alice" },
  { "_op": "DELETE", "_ts": 1746784321000, "id": "2" }
]
```

### 字段说明

| 字段 | 必填 | 说明 |
|------|------|------|
| `_op` | ✓ | `INSERT` / `UPSERT` / `DELETE`（大小写不敏感） |
| `_ts` | ✓ | Unix 毫秒时间戳（日志等用途） |
| 其它字段 | — | 与 schema 字段名对应；未知字段可忽略 |

- **INSERT**：`Put`（PK 已存在时行为依赖实现，调用方宜保证幂等或先查）。
- **UPSERT**：`Upsert`。
- **DELETE**：`Delete`，仅需 PK。

offset 文件等运行细节仍以 [`db.md`](db.md) 与源码为准。

---

## 附录：产品与架构备忘

- RPC 框架：**brpc**；业务载荷：**FlatBuffers**（见 `proto/yikv_server.fbs`）。
- 数据源：**KafkaSource**（实时）；离线 bulk：**`yikv_import_pipeline`**。
- 更多接口说明、双栈细节、排错：**[`db.md`](db.md)**。
