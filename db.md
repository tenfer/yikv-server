# yikv-server（yidiandb）

基于 [yikv](../yikv) 的 KV 存储，对外提供 **brpc（baidu_std）+ FlatBuffers** 读写服务；RPC 正文使用 **`SerializedRequest` / `SerializedResponse` 承载 FlatBuffers 根表**（`BaiduMasterService` 路径，无 gRPC）。提供 **C++** `yikv_import_parquet` 从 Parquet 批量写入索引（直连 `KVIndex::Put`）。Python 工具仅保留 **Parquet → schema.json**（`tools/oss_import_parquet.py --emit_schema_json`）；gRPC 时代脚本已移除。

## 依赖

**C++ 服务（Bazel）**

- 本仓库 `MODULE.bazel` 拉取 **Apache brpc 1.16.0**、与 yikv 对齐的 **protobuf / leveldb** 等；系统 **OpenSSL**（`-lssl -lcrypto`）。
- **系统库**：`libflatbuffers-dev`（与 `deps/include` 头一致即可）、Parquet 压测需 **Apache Arrow C++ / Parquet**（`libarrow-dev` `libparquet-dev`）。

**Python（仅 schema 推断）**

```bash
pip install -r requirements.txt
```

FlatBuffers 生成：

```bash
flatc --python -o gen_py proto/yikv_server.fbs
flatc --cpp -o gen proto/yikv_server.fbs
```

`flatc --cpp` 会写出 `gen/yikv_server_generated.h`（与 C++ `#include "yikv_server_generated.h"` 一致）。

## 原生 C++ Parquet 导入（高性能）

与 `yidiandb_server` 相同的 `--db` / `--index`，调用 **`KVIndex::NewDoc` / `Put` / `Publish`**。导入前须停止已打开该索引的服务器。

```bash
bazel build //:yikv_import_parquet
./bazel-bin/yikv_import_parquet \
  --db /data/yidiandb_data \
  --index dsp_test \
  --input /data/part1.parquet
```

递归导入某目录下全部 **`.parquet`**（路径排序后按文件顺序导入），例如 OSS 同步目录：

```bash
./bazel-bin/yikv_import_parquet \
  --db /data/yidiandb_data \
  --index dsp_test \
  --input_dir /data/oss/all
```

可与多个 `--input` 混用；`--input_list` 与 `--input_dir` 也可同时使用。

**容量**：主键 HashMap 单索引约可支撑 **数千万级** 唯一主键（随 yikv 版本而变）。用旧版 yikv 建的索引目录（HashMap v1）在本库升级后 **无法直接打开**，需删掉该 `--index` 对应目录后 `--create_if_missing` 重建并全量重导。

```bash
bazel run //:yidiandb_server -- \
  --db /data/yidiandb_data \
  --index main \
  --listen 0.0.0.0:8000 \
  --create_if_missing \
  --schema_json ./schema.json
```

| 参数 | 含义 |
|------|------|
| `--db` | yikv 数据根目录 |
| `--index` | 索引名 |
| `--listen` | `host:port`（默认 `0.0.0.0:8000`） |
| `--port` | 仅指定监听端口（1–65535）；覆盖 `--listen` 中的端口；若 `listen` 不含 `:` 则视为 host 并追加 `:port` |
| `--create_if_missing` | 创建索引（需 `--schema_json`） |
| `--schema_json` | yikv Schema JSON |
| `--recreate` | 删除已有索引目录后重建（慎用） |
| `--arena_seg_gb` / `--arena_max_gb` | arena 段与上限（GiB） |

**RPC 契约**

- **协议**：`baidu_std`
- **Service（meta）**：`yikv.db.YikvDb`
- **Methods**：`Get` / `Put` / `PutBatch` / `BatchGet`
- **请求/响应体**：FlatBuffers 根表分别为 `GetRequest`↔`GetResponse` 等；放在 **`SerializedRequest.serialized_data` / `SerializedResponse.serialized_data`**（裸 `Finish` 字节，无额外 protobuf 嵌套）。

表定义见 [`proto/yikv_server.fbs`](proto/yikv_server.fbs)。

**PutBatch**：整批原子语义——任一行校验失败则整批返回 `ok=false` 且不 `Publish`；全部成功后一次 `Publish()`。空批或缺失 `rows` 返回错误。

## 客户端压测（C++）

```bash
bazel run //:yidiandb_bench -- \
  --server 127.0.0.1:8000 \
  --keys_file ./pks.txt \
  --workers 8 \
  --requests 50000
```

支持 `--local_parquet` + `--pk`；兼容旧参数名 **`--grpc_target`**（等同于 `--server`）。输出 JSON 中含 `qps`、延迟分位、`index_get`（服务端 `GetResponse.index_get_ns` 汇总）。

## OSS Parquet

仅 **`--emit_schema_json`**：从 OSS 或本地读首个 Parquet，写出 `schema.json`。批量 Put 请用本地 Parquet + `yikv_import_parquet`，或自研 brpc 客户端。

```bash
export OSS_ENDPOINT=... OSS_ACCESS_KEY_ID=... OSS_ACCESS_KEY_SECRET=...
python3 tools/oss_import_parquet.py \
  --oss_uri 'oss://bucket/prefix/' \
  --pk <主键列名> \
  --emit_schema_json ./schema.json
```

## 布局

- [`proto/yikv_server.fbs`](proto/yikv_server.fbs) — FlatBuffers 契约
- [`proto/yikv_db_wire.proto`](proto/yikv_db_wire.proto) — brpc meta 与 method 名文档（`proto_library` //:yikv_db_wire_proto）
- [`src/yikv_server/main.cc`](src/yikv_server/main.cc) — 进程入口、`brpc::Server`
- [`src/yikv_server/rpc/db_brpc_service.cc`](src/yikv_server/rpc/db_brpc_service.cc) — `BaiduMasterService` 分发
- [`src/yikv_server/db/handlers.cc`](src/yikv_server/db/handlers.cc) — 索引与 FlatBuffers 编解码
- [`gen/yikv_server_generated.h`](gen/yikv_server_generated.h) — `flatc` 自 `proto/yikv_server.fbs` 生成（C++ 唯一入口）
- [`src/bench_main.cc`](src/bench_main.cc) — brpc Get 压测
- [`src/import_parquet_main.cc`](src/import_parquet_main.cc) — `//:yikv_import_parquet`
