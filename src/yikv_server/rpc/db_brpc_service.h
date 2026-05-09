#pragma once

#include <brpc/baidu_master_service.h>
#include <mutex>

#include "src/index/kv_index.h"

namespace yikv::schema {
class Schema;
}

namespace yikv_server::rpc {

// brpc adapter: BaiduMasterService + SerializedRequest/Response bodies = FlatBuffers.
class DbBrpcService final : public brpc::BaiduMasterService {
public:
    explicit DbBrpcService(yikv::index::KVIndex* idx);

    void ProcessRpcRequest(brpc::Controller* cntl,
                           const brpc::SerializedRequest* request,
                           brpc::SerializedResponse* response,
                           ::google::protobuf::Closure* done) override;

private:
    yikv::index::KVIndex*         idx_;
    const yikv::schema::Schema*   schema_;
    std::mutex                    write_mu_;
};

}  // namespace yikv_server::rpc
