# yikv-server

[中文版 README](README.md)

## 1. What it does

**yikv-server** is a **multi-table KV serving layer** on top of [yikv](../yikv): data lives under `{db_path}/{table_name}/` in mmap arenas and indexes, exposed over RPC as **Get / Put / PutBatch / BatchGet**.

- **Two protocols, one port**: **brpc `baidu_std`** (`BaiduMasterService`) and **gRPC `h2:grpc`** ([`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)); payloads are **FlatBuffers** ([`proto/yikv_server.fbs`](proto/yikv_server.fbs) → [`gen/yikv_server_generated.h`](gen/yikv_server_generated.h)).
- **Offline index build**: **`yikv_import_pipeline`** streams Parquet/CSV, cloud object URIs (oss/s3/cos/obs/gs), or **MySQL-compatible** query results into `KVIndex`.
- **Real-time ingest (optional)**: per-table **`table.json`** configures Kafka; **KafkaSource** applies JSON change events to the same index (format summarized below; details in [`db.md`](db.md)).

For RPC details, troubleshooting, and internals, see **[`db.md`](db.md)**.

---

## 2. Quick setup

**Requirements**

- **Bazel** (match [`MODULE.bazel`](MODULE.bazel) / [`.bazelversion`](.bazelversion)).
- **OS**: **OpenSSL**; **Apache Arrow C++ / Parquet** dev packages to build/run import and bench (e.g. `libarrow-dev`, `libparquet-dev`).
- **MySQL wire import (optional)**: `libmysqlclient-dev` at build time and **`libmysqlclient.so`** at runtime.

**Build**

```bash
cd yikv-server
bazel build //:yikv_server //:yikv_import_pipeline //:yikv_server_bench
```

Binaries are under `bazel-bin/` (or `bazel run //:yikv_server -- /path/to/config.json`).

**Configuration**

Copy [`config.example.json`](config.example.json) to `config.json` and edit. The server and import tool **share** `db_path`, `arena_seg_gb`, `arena_max_gb`, and `exclusive_arena_lock` (**do not** override these via import CLI; keep prod consistent).

| Key | Description |
|-----|-------------|
| `db_path` | Data root; each subdirectory name is a table name |
| `listen` | Bind address, default `0.0.0.0:9000` |
| `arena_seg_gb` / `arena_max_gb` | Per-segment and max arena size (GiB) |
| `exclusive_arena_lock` | Default `true`; avoid concurrent writers on the same DB |
| `kafka.default_brokers` | Optional global brokers; per-table override in `table.json` |

---

## 3. End-to-end flow

### 3.1 Build the index

Use **`yikv_import_pipeline`**. Before import, **stop** `yikv_server` if it holds the same `db_path`, or pass `--no_arena_lock` for this run only (use with care).

**Files / directories (local or cloud URIs)**

- **`.parquet` / `.csv`**; `--input_dir` walks recursively; multiple `--input` / `--input_list` allowed.
- Cloud schemes: **`oss://` `s3://` `cos://` `obs://` `gs://`**; environment variables are documented in [`db.md`](db.md).

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

**MySQL / StarRocks (MySQL protocol)**: `--mysql_query` or `--mysql_query_file`; connection via `--mysql_*` or env vars `MYSQL_HOST`, `MYSQL_USER`, etc. (CLI wins). See **[`examples/mysql_import/README.md`](examples/mysql_import/README.md)**.

**Notes**: result column names must match **`schema.json`** field names (case-insensitive). CSV / MySQL paths do not support array fields. For new tables, **`--sql_est_rows`** (e.g. from `COUNT(*)`) helps size the primary-key HashMap.

### 3.1.1 Publish built indexes (optional)

To sync a table directory to a **local artifact root** or **S3-compatible object storage** after `yikv_import_pipeline`, use **`tools/artifact_sync/`**: one `artifact-storage.yaml` switches `local` vs `s3_compatible` without changing the import binary. See **[`tools/artifact_sync/README.md`](tools/artifact_sync/README.md)**.

### 3.1.2 End-to-end offline index pipeline (build → artifacts → online)

Usually split between a **build host** and an **online host** (they may be the same machine). Schedulers call HTTP only; paths come from per-host env vars (see the header of [`tools/pipeline_agent/pipeline_agent.py`](tools/pipeline_agent/pipeline_agent.py)).

| Step | What happens | Notes |
|------|----------------|------|
| 1. Offline import | `yikv_import_pipeline` or **`POST /buildIndex` / `/publishIndex`** | Same **`schema.json`** as production. The **pipeline agent** removes **`BUILD_DB/<table>`** (default **`$WORK/build_db/<table>`**) before each import, then runs **`--create_if_missing`** for a full rebuild—avoid reopening an existing tree and duplicating rows. |
| 2. Upload artifact | **`POST /pushIndex`** or push inside publish | Writes a timestamp **`build_id`** tree into the artifact store (local root or S3-compatible), same model as **`tools/artifact_sync/`**. |
| 3. Pull & switch | **`POST /deployIndex`** or `artifact_sync pull … --switch-active` | Materializes **`$WORK/releases/<table>/<build_id>/`** and atomically points **`active`** at that build. |
| 4. Server layout | `link` (inside deploy) | **`SERVER_DB/<table>`** → **`releases/<table>/active`** (**`SERVER_DB`** defaults to **`$WORK/server_db`** and must match **`db_path`** in **`config.json`**). |
| 5. Hot reload | **`reload <table>`** | Sent on **`admin_unix_socket`** (defaults to **`parent(db_path)/admin.sock`**, which lines up with **`$WORK/admin.sock`** when `db_path` is **`…/server_db`**). **`ReloadTable`** remaps mmap without restarting **`yikv_server`**. |

**Thin scheduler**: run [`tools/schedule_pipeline.py`](tools/schedule_pipeline.py) (or [`tools/pipeline_reload.py`](tools/pipeline_reload.py))—**`/publishIndex`** on the build agent, then **`/deployIndex`** on the online agent. See **`--help`** for **`BUILD_AGENT_URL`**, **`ONLINE_AGENT_URL`**, **`--table`**, **`--data-dir`**, etc.

**Sanity checks**: after deploy, `readlink -f $db_path/<table>` should resolve to **`…/releases/<table>/<build_id>`**; `printf 'reload <table>\n' | nc -U <admin.sock>` should print **`ok`**. RPC / admin details: **[`db.md`](db.md)** §3, **[`tools/artifact_sync/README.md`](tools/artifact_sync/README.md)**.

### 3.2 Run the server

Single argument: path to config file.

```bash
./bazel-bin/yikv_server ./config.json
```

Ensure `db_path` exists and disk / arena settings are sufficient. The server scans table subdirectories **at startup**; new directories added later can be first-opened with **`reload <table>`** on `admin_unix_socket`, and already-open tables use the same command after «active» changes (see **[`db.md`](db.md)**).

### 3.3 How to use

**RPC**

- Service: **`yikv.db.YikvDb`**; methods: **Get, Put, PutBatch, BatchGet**.
- **brpc**: `baidu_std`, body is serialized FlatBuffers `GetRequest` / `PutRequest` / … (see `yikv_server.fbs`).
- **gRPC**: `h2:grpc`, `FbRpcRequest.payload` / `FbRpcResponse.payload` carry the **same** FlatBuffers bytes.

From any language: **brpc C++** or **official gRPC** with stubs from **`yikv_grpc.proto`**, building/parsing payloads per the `.fbs` tables. Dual-stack and PutBatch semantics: **[`db.md`](db.md)**.

**Kafka (optional)**

Configure topic etc. in `{db_path}/{table}/table.json`. Messages are JSON with **`_op`** (`INSERT` / `UPSERT` / `DELETE`), **`_ts`** (epoch ms), and business fields. Global brokers: `config.json` → `kafka.default_brokers`. Full rules: [`src/yikv_server/table_config.h`](src/yikv_server/table_config.h) and [`db.md`](db.md).

---

## 4. Benchmark

**`yikv_server_bench`** load-tests **Get** using the same brpc **`baidu_std`** path as production.

**Required**

- `--server HOST:PORT` (alias `--grpc_target`)
- `--index` (or `--table`): must match a loaded table
- Key source: **`--keys_file`** or **`--local_parquet PATH --pk COLUMN`**

**Load**: `--requests N` **or** `--duration_sec T`; default `--requests 50000`.

**Common flags**

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | 8 | Concurrent threads |
| `--warmup` | 32 | Warmup RPC count |
| `--max_keys` | 200000 | Max PK samples from Parquet |

```bash
./bazel-bin/yikv_server_bench \
  --server 127.0.0.1:9000 \
  --index my_table \
  --keys_file ./pks.txt \
  --workers 16 \
  --requests 1000000
```

Stdout is **one line of JSON** (`qps`, `latency_ms`, `phases_ms`, etc.).

---

## Further reading

- Protocols and import pipeline: **[`db.md`](db.md)**
- gRPC definition: **[`proto/yikv_grpc.proto`](proto/yikv_grpc.proto)**
