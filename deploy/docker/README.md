# Container images (Ubuntu 22.04)

- **Multi-stage** [`Dockerfile`](Dockerfile):
  - **`builder-base`**: Bazel + 编译依赖，**不含**源码与 `bazel build`，适合挂宿主机代码当开发环境。
  - **`builder`**: 在固定快照上执行 `bazel build -c opt //:yikv_server`（CI /  reproducible）。
  - **`runtime`**（默认）：只含二进制与运行所需 `.so`。

- **Build context** must be the **repository root** that contains both `yikv/` and `yikv-server/` (see `yikv-server/MODULE.bazel` `local_path_override`).
- Vendored `third_party/*_stub` trees are minimal; the **builder** image matches a typical Ubuntu dev host where the compiler resolves full headers from `/usr/include`.

```bash
# From repo root (e.g. fansichi/)
docker build -f yikv-server/deploy/docker/Dockerfile -t yikv-server:latest .

docker build -f yikv-server/deploy/docker/Dockerfile --target builder -t yikv-server:build .
```

## 开发环境（推荐 `builder-base`）

不要用带整仓 `COPY` + `bazel build` 的 **`builder`** 当日常开发镜像：每次打镜像都会全量编译，且代码会冻在镜像里。

应打 **`builder-base`**，把本机 **`yikv`**、**`yikv-server`** 挂到容器里 **`/src/yikv`**、**`/src/yikv-server`**（与 `MODULE.bazel` 里 `../yikv` 一致）：

```bash
# 仓库根目录（同时含 yikv/ 与 yikv-server/）
docker build -f yikv-server/deploy/docker/Dockerfile --target builder-base -t yikv-server:dev .

docker run --rm -it \
  -v "$(pwd)/yikv:/src/yikv" \
  -v "$(pwd)/yikv-server:/src/yikv-server" \
  -w /src/yikv-server \
  yikv-server:dev bash
```

容器内例如：

```bash
bazel build //:yikv_server
bazel test //tests:all_tests
bazel run //:yikv_server -- /path/in/container/config.json   # 需自备 config 或再挂卷
```

可选：在 `docker run` 上加 `-v yikv-bazel-cache:/root/.cache/bazel` 之类以复用 Bazel 缓存（路径按你本机习惯调整）。

## 运行正式镜像（runtime）

```bash
docker run --rm -p 9000:9000 \
  -v "$PWD/config.json:/etc/yikv/config.json:ro" \
  -v yikv-data:/data \
  yikv-server:latest
```

Use the same `db_path` in `config.json` as the volume mount (e.g. `/data/db`).
