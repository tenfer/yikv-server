#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

#include <flatbuffers/flatbuffers.h>

#include "yikv_server_generated.h"

namespace yikv::index {
class Doc;
class KVIndex;
}  // namespace yikv::index
namespace yikv::schema {
class Schema;
struct FieldDef;
}  // namespace yikv::schema

namespace yikv_server::db {

struct HandlerStatus {
    bool        ok{true};
    std::string err;
    static HandlerStatus Ok() {
        return {};
    }
    static HandlerStatus Fail(std::string e) {
        HandlerStatus s;
        s.ok = false;
        s.err = std::move(e);
        return s;
    }
};

HandlerStatus ApplyRowToDoc(yikv::index::Doc* doc, const yidiandb::Row* row,
                            const yikv::schema::Schema* schema);

std::string      ExtractPkString(const yikv::index::Doc& doc, const yikv::schema::Schema* schema);
flatbuffers::Offset<yidiandb::Row> BuildRow(flatbuffers::FlatBufferBuilder&    fbb,
                                            const yikv::index::Doc&            doc,
                                            const yikv::schema::Schema*        schema);

void HandleGet(yikv::index::KVIndex* idx, const yikv::schema::Schema* schema, const void* req,
               size_t req_len, std::string* out_resp);

void HandlePut(yikv::index::KVIndex* idx, const yikv::schema::Schema* schema, std::mutex* write_mu,
               const void* req, size_t req_len, std::string* out_resp);

void HandlePutBatch(yikv::index::KVIndex* idx, const yikv::schema::Schema* schema,
                    std::mutex* write_mu, const void* req, size_t req_len, std::string* out_resp);

void HandleBatchGet(yikv::index::KVIndex* idx, const yikv::schema::Schema* schema, const void* req,
                    size_t req_len, std::string* out_resp);

}  // namespace yikv_server::db
