# MySQL / StarRocks（MySQL 协议）导入示例

与仓库内 [`schema.json`](../../schema.json) 字段名对齐：`key`, `duf_inner_pkg`, `duf_inner_all`, `duf_outer_pkg`, `duf_all_all`（均为**标量**；导入不支持数组列）。

## 1. 在 MySQL 里准备测试表（可选）

```bash
mysql -h 127.0.0.1 -u root -p < demo_table.sql
```

`demo_table.sql` 会建表 `yikv_demo.dsp_rows` 并插入两行样例；可按需改库名/表名。

## 2. 编辑查询

[`import.sql`](import.sql) 中的 **SELECT 列名或别名**必须与 yikv 表 `schema.json` 里 `fields[].name` 一致（**不区分大小写**）。若线上列名不同，请在 SQL 里写 `AS key` 等别名。

## 3. 运行导入

在目录 **`yikv-server/examples/mysql_import`** 下执行（或把 `import.sql` / `init_session.sql` / `schema.json` 改成你的绝对路径）。

在 **`yikv-server`** 根目录先构建：

```bash
cd /path/to/yikv-server
bazel build //:yikv_import_pipeline
```

再进入示例目录导入（`--config` 指向你的 `config.json`，`db_path` / `arena_*` 与线上一致）：

```bash
cd examples/mysql_import

../../bazel-bin/yikv_import_pipeline \
  --config /path/to/config.json \
  --index dsp_test \
  --schema_json ../../schema.json \
  --create_if_missing \
  --mysql_host 127.0.0.1 \
  --mysql_port 3306 \
  --mysql_user root \
  --mysql_password 'your_password' \
  --mysql_database yikv_demo \
  --mysql_query_file import.sql \
  --mysql_init_sql init_session.sql \
  --sql_est_rows 100000 \
  --import_io_workers 1
```

说明：

- 不要用文件 `--input*`，MySQL 模式单独一条链路。
- **连接信息**可从环境变量读取（见下方）；**命令行参数优先**。
- **`--import_io_workers 1`**：单连接，推荐。
- **`--sql_est_rows`**：新建索引时估行数用；可用 `SELECT COUNT(*) FROM ...` 的结果填入。
- **`--mysql_init_sql`**：每行一条语句，例如 `SET NAMES utf8mb4`（见 [`init_session.sql`](init_session.sql)）。
- 也可用 **`--mysql_query 'SELECT ...'`** 代替 **`--mysql_query_file`**（复杂 SQL 建议用文件）。

### 环境变量（可选）

| 变量 | 作用 |
|------|------|
| `MYSQL_HOST` / `YIKV_MYSQL_HOST` | 主机 |
| `MYSQL_USER` / `YIKV_MYSQL_USER` | 用户 |
| `MYSQL_PASSWORD` / `YIKV_MYSQL_PASSWORD` | 密码 |
| `MYSQL_PWD` | 若上两者未设密码时的遗留客户端变量 |
| `MYSQL_DATABASE` / `YIKV_MYSQL_DATABASE` | 库名 |
| `MYSQL_TCP_PORT` / `MYSQL_PORT` / `YIKV_MYSQL_PORT` | 端口（仅当**未**指定 `--mysql_port` 时生效） |

示例：

```bash
export MYSQL_HOST=127.0.0.1 MYSQL_USER=root MYSQL_DATABASE=yikv_demo MYSQL_PASSWORD=secret
../../bazel-bin/yikv_import_pipeline \
  --config /path/to/config.json \
  --index dsp_test \
  --schema_json ../../schema.json \
  --create_if_missing \
  --mysql_query_file import.sql \
  --import_io_workers 1
```

## 4. 依赖

- 编译：`libmysqlclient-dev`（或等价包）与头文件路径。
- 运行：系统能找到 **`libmysqlclient.so`**（及 OpenSSL 等）。

更多设计说明见 [`README.md`](../../README.md) 中 **MySQL 兼容源** 小节。
