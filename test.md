# yikv / yikv-server 测试方案

覆盖三块：**yikv 核心索引**、**yikv-server 在线 RPC 正确性**、以及 **数据流（Kafka / import / offset）**。目标：**PR 上尽量只靠单测与进程内 mock 发现问题**；在线接口与全链路在合并后或发布前补强。

## 分层总览

| 层级 | 内容 | 外部依赖 | 典型运行时机 |
|------|------|----------|----------------|
| L0 | **在线接口**：FlatBuffers → `HandleGet` / `HandlePut` / `HandlePutBatch` / `HandleBatchGet`（与 brpc/gRPC 同路径） | 无（`//tests:handlers_rpc_test`） | 每次 PR |
| L1 | 合并逻辑 + offset 文件契约 | 无 | 每次 PR |
| L2 | `RunKafkaImportCatchup` + rdkafka 进程内 mock | 系统 `librdkafka`（mock API） | PR 或每日构建 |
| L3 | OSS / 全链路 import + 真实 Kafka（可选） | MinIO/真实 OSS、真实 broker | 发布前 / 专项 |

实现与源码：[tests/](tests/)，`BUILD` 见 [tests/BUILD.bazel](tests/BUILD.bazel)。

## L0：在线接口正确性（yikv-server）

在线路径指：**brpc / gRPC 入口最终调用的同一套处理函数**（FlatBuffers 字节流），见 [src/yikv_server/db/handlers.h](src/yikv_server/db/handlers.h)。

| 入口 | 说明 |
|------|------|
| `HandleGet` | 单 key 查询，校验 `GetResponse`、字段与 `Doc` 一致 |
| `HandlePut` | 单条写入 / upsert 语义与错误码（表不存在、字段类型等） |
| `HandlePutBatch` | 批量写入、部分失败时响应与索引状态 |
| `HandleBatchGet` | 多 key、缺失 key 行为 |

**与 Kafka 路径的差异**：ingest 走 JSON + `stream::ApplyStreamJsonObject`；RPC 走 FlatBuffers `Row` + `ApplyRowToDoc`。两套都落同一 `KVIndex`，但**编解码与错误处理不同**，必须用在线用例单独覆盖，不能只用 `json_stream_ingest_test` 替代。

### 已实现：`//tests:handlers_rpc_test`

进程内：`DB::Init` 临时目录 → `CreateKVIndex` → `TableRegistry::ScanAndLoad` → 构建 FlatBuffers 请求并直接调用 `Handle*`。**不启监听端口**。

覆盖要点（与线上一致处理函数，区别于 Kafka JSON merge）：

| 用例 | 说明 |
|------|------|
| `PutThenGetRoundTrip` | Put → Get，`found`、标量与 string 字段 |
| `GetMissingReturnsNotFound` | 未命中 `found=false` |
| `UnknownTableReturnsErrorString` | 未知表名，`err` 含 `unknown table` |
| `UpsertOverwritesWholeDocument` | 第二次 Put 仅含部分列时 **整行替换**，`name` 被清空（RPC 非 JSON patch） |
| `PutBatchInsertsTwoRows` | `HandlePutBatch` 两行可读 |
| `BatchGetHitAndMiss` | 命中行有数据，缺失 key 对应空 row |
| `PutWrongValueTypeFails` / `PutUnknownFieldIdFails` | `ApplyRowToDoc` 校验 |
| `Int64ArrayRoundTrip` | `ARR_I64` 数组读写 |

共享库 [`db_handlers_lib`](BUILD.bazel) 与 [`yikv_server_lib`](BUILD.bazel) 共用同一份 [`handlers.cc`](src/yikv_server/db/handlers.cc)，避免 RPC 二进制与单测脱节。

### 其他手段（可选）

1. **本地黑盒**  
   `bazel run //:yikv_server -- config.json`，用客户端对 **Get/Put** 打请求做 golden 或抽样。适合上线前，CI 成本较高。

2. **已有二进制（区分用途）**  
   - [`tools/reload_smoke_test.cc`](tools/reload_smoke_test.cc) → `bazel run //:reload_smoke`：**ReloadTable / symlink**，不覆盖 RPC 编解码。  
   - [`src/bench_main.cc`](src/bench_main.cc) → `yikv_server_bench`：**压测**，不验证业务正确性。

### 运行

```bash
bazel test //tests:handlers_rpc_test
bazel test //tests:all_tests              # 含 L0 + L1
bazel test //tests:all_tests_with_mock   # 再加 Kafka mock（L2）
```

## L1：无外部依赖（必跑）

### `//tests:json_stream_ingest_test`

- 测 `ApplyStreamJsonObject` / `MergeJsonIntoDoc` 等：**JSON 未出现的字段不覆盖**、**数组 append**、`_op` / `_ts`、INSERT 重复 PK、DELETE、未知字段忽略、`null` 不覆盖等。
- 使用内存 `FtAllocator` + `KVIndex`，**不连 Kafka**。

### `//tests:kafka_offset_test`

- `kafka.offset` 单行数字读写、`LoadOffset` 语义（committed `N` → 下次从 `N+1` 读）。
- catch-up **时间戳公式**：`(offline_watermark_sec - rewind_minutes * 60) * 1000`，负值截为 0。
- `kafka_meta.json` 字段结构与读写。

### 运行

```bash
cd yikv-server
bazel test //tests:all_tests
```

`all_tests` 仅包含上述两个 target，耗时通常 &lt; 1s。

## L2：Kafka 进程内 mock（推荐在 CI 中跑）

### `//tests:kafka_source_test`

- 使用 librdkafka **`test.mock.num.brokers`** 启动**嵌入式 mock 集群**（无需独立 Kafka 进程）。
- 覆盖：`RunKafkaImportCatchup` 从 produce → consume → `ApplyStreamJsonObject` → 写 `kafka.offset` / `kafka_meta.json` 的完整路径。
- 若运行环境的 librdkafka 过旧、无 mock，相关用例会 **GTEST_SKIP**，不会当失败。

Bazel 上该 target 带 tag：`kafka-mock`。

### 运行

```bash
bazel test //tests:kafka_source_test
# 或一并跑 L1+L2：
bazel test //tests:all_tests_with_mock
```

### 生产侧注意

- catch-up 消费循环依赖 **`enable.partition.eof=true`** 才能在读到分区末尾后正常结束；否则可能长时间空轮询。见 `kafka_import_catchup.cc`。

## L3：OSS / 全链路（规划与落地方式）

当前仓库 **未** 将 L3 编进默认 Bazel target，建议按业务需要单独加 `cc_test` 或脚本：

| 方向 | 做法 |
|------|------|
| 对象存储 | **MinIO**（S3 兼容）或测试桶 + 只读凭证；用环境变量注入 endpoint/credential，`yikv_import_pipeline` 走现有云路径 |
| 真实 Kafka | Testcontainers / Docker Compose 起 broker；或对预发 topic 跑一次性 catch-up 校验 |
| 断言 | 行数、关键字段抽样、`kafka.offset` 与线上 consumer 是否连续 |

L3 建议在 CI 用单独 job + secret，**不阻塞**普通 PR。

## CI 建议

1. **必过**：`bazel test //tests:all_tests`（**L0** `handlers_rpc_test` + **L1**）
2. **合并后或 nightly**：`bazel test //tests:all_tests_with_mock`（需链接系统 `librdkafka` 且支持 mock）
3. **发布前**：L0 黑盒或预发环境对 **Get/Put/BatchGet/PutBatch** 抽样对比全量（或与离线导入结果对账）
4. **标签**：L3 可用 `tags = ["integration"]`，默认 `bazel test //tests:... --test_tag_filters=-integration` 排除

## 相关源码索引

| 模块 | 路径 |
|------|------|
| JSON 合并与 ingest | [src/yikv_server/stream/json_stream_ingest.cc](src/yikv_server/stream/json_stream_ingest.cc) |
| 在线 Kafka 消费 | [src/yikv_server/kafka/kafka_source.cc](src/yikv_server/kafka/kafka_source.cc) |
| 离线 catch-up | [src/yikv_server/kafka/kafka_import_catchup.cc](src/yikv_server/kafka/kafka_import_catchup.cc) |
| 表目录 `kafka.offset` | [src/yikv_server/table_registry.cc](src/yikv_server/table_registry.cc)（`table_config_dir / "kafka.offset"`） |
| RPC 处理层（在线接口） | [src/yikv_server/db/handlers.cc](src/yikv_server/db/handlers.cc)（链入 [`db_handlers_lib`](BUILD.bazel)） |
| brpc / gRPC 转发 | [src/yikv_server/rpc/db_brpc_service.cc](src/yikv_server/rpc/db_brpc_service.cc)、[db_grpc_service.cc](src/yikv_server/rpc/db_grpc_service.cc) |

## yikv 核心库测试

**yikv** 子模块：`../yikv/tests/`，验证 **allocator、HashMap、KVIndex、Doc、恢复** 等，不涉及网络协议：

```bash
cd ../yikv && bazel test //tests:all_tests
```

与 **yikv-server** 的衔接关系：

| 层次 | 验证重点 |
|------|----------|
| yikv `//tests:all_tests` | 索引与 Doc 语义正确 |
| yikv-server **L0** | `handlers_rpc_test`：`Handle*` + FlatBuffers 与线上协议一致 |
| yikv-server L1/L2 | Kafka JSON ingest 与 offset 契约 |

完整质量需要 **三层都跑**；仅跑 yikv 单测**不能**保证在线 Put/Get 与 JSON ingest 无回归。
