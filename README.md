# yikv-server

yikv 以库形式发布；**yikv-server** 在 yikv 之上提供 **brpc** 网络层与 KV 读写 RPC（载荷为 **FlatBuffers**）。

## 已实现 RPC（brpc `baidu_std`）

- **Service meta**：`yikv.db.YikvDb`（与 `src/yikv_server/rpc/rpc_constants.h` 一致）
- **Methods**：`Get`、`Put`、`PutBatch`、`BatchGet`
- **载荷**：`SerializedRequest` / `SerializedResponse` 的 `serialized_data()` 中为 FlatBuffers 根表（见 `proto/yikv_server.fbs`）。
- **与旧 gRPC 不兼容**：无 `/yidiandb.Yidiandb/*` 路径；客户端需改用 brpc + 上述 meta。

详见 [`db.md`](db.md)。构建：`bazel build //:yidiandb_server //:yidiandb_bench`。

---

# 技术限制
  rpc框架使用brpc
  协议使用flatbuffer

# 架构
1. yikv-server 接口
  1）每个接口都加上接口总耗时和Index.Get耗时情况
  1）数据类型增加string数组
  2）增加PutBatch接口，提升写入性能
2. 数据源 Source
    + 实时数据源：增加KafkaSource（可扩展其他Source,暂不实现）, 批量写入index并立即生效，注意yikv是单写多读架构。
    + 离线数据源: 不提供bulk load的能力，提供一个工具yikv-index-builder（生产环境下 和yikv-server不是一台机器）, 这个工具支持从本地文件(CSV，parquet等格式，优先实现)/云端（先不实现）读取原始数据生成索引。
      索引文件如何分发本系统先不提供解决方案（一般做法：目录挂载到yikv-server的机器 或者上传到云存储，然后yikv-server拉到本地）
    
      yikv-server 本地索引需保留两个版本，保证新索引版本更新失败或者后面发现有问题能快速回滚。数据切换过程不能停在线服务。

  
    