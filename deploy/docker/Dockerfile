# syntax=docker/dockerfile:1
# Build from repository root (parent of yikv-server with sibling yikv/):
#   docker build -f yikv-server/deploy/docker/Dockerfile -t yikv-server:latest .
#
# CI / reproducible compile (includes COPY + bazel build):
#   docker build -f yikv-server/deploy/docker/Dockerfile --target builder -t yikv-server:build .
#
# Dev toolchain only (fast; mount host yikv + yikv-server under /src):
#   docker build -f yikv-server/deploy/docker/Dockerfile --target builder-base -t yikv-server:dev .

FROM ubuntu:22.04 AS builder-base
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libflatbuffers-dev \
    libssl-dev \
    librdkafka-dev \
    nlohmann-json3-dev \
    pkg-config \
    unzip \
    zip \
    && rm -rf /var/lib/apt/lists/*

ARG BAZEL_VERSION=7.4.1
RUN curl -fsSL "https://github.com/bazelbuild/bazel/releases/download/${BAZEL_VERSION}/bazel-${BAZEL_VERSION}-linux-x86_64" \
    -o /usr/local/bin/bazel && chmod +x /usr/local/bin/bazel

WORKDIR /src


FROM builder-base AS builder
COPY yikv /src/yikv
COPY yikv-server /src/yikv-server

WORKDIR /src/yikv-server
# third_party stubs are minimal; compilers pick full headers from these packages (same as typical dev machines).
RUN bazel build -c opt //:yikv_server


FROM ubuntu:22.04 AS runtime
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgcc-s1 \
    liblz4-1 \
    librdkafka1 \
    libsasl2-2 \
    libssl3 \
    libstdc++6 \
    zlib1g \
    libzstd1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/yikv-server/bazel-bin/yikv_server /usr/local/bin/yikv_server

ENTRYPOINT ["/usr/local/bin/yikv_server"]
CMD ["/etc/yikv/config.json"]
