#include "db/handlers.h"

#include <chrono>

#include "src/index/doc.h"
#include "src/index/kv_index.h"
#include "src/schema/schema.h"

#include <cstring>

#include <string_view>
#include <vector>

namespace yikv_server::db {

using yikv::index::Doc;
using yikv::index::KVIndex;
using yikv::schema::DataType;
using yikv::schema::FieldDef;
using yikv::schema::Schema;

static std::string FieldTypeMismatch(const FieldDef& def, yidiandb::ValueType vt) {
    return "field_id " + std::to_string(def.field_id) + " type mismatch for schema "
           + std::string(yikv::schema::DataTypeName(def.type));
}

static bool FieldValueMatchesSchema(const FieldDef& def, yidiandb::ValueType vt) {
    if (def.is_array) {
        switch (def.type) {
            case DataType::Int32:
                return vt == yidiandb::ValueType_ARR_I32;
            case DataType::Int64:
                return vt == yidiandb::ValueType_ARR_I64;
            case DataType::Float32:
                return vt == yidiandb::ValueType_ARR_F32;
            case DataType::Float64:
                return vt == yidiandb::ValueType_ARR_F64;
            case DataType::String:
                return vt == yidiandb::ValueType_ARR_STRING;
            default:
                return false;
        }
    }
    switch (def.type) {
        case DataType::Bool:
            return vt == yidiandb::ValueType_BOOL;
        case DataType::Int32:
            return vt == yidiandb::ValueType_I32;
        case DataType::Int64:
            return vt == yidiandb::ValueType_I64;
        case DataType::Float32:
            return vt == yidiandb::ValueType_F32;
        case DataType::Float64:
            return vt == yidiandb::ValueType_F64;
        case DataType::String:
            return vt == yidiandb::ValueType_STRING;
        case DataType::Bytes:
            return vt == yidiandb::ValueType_BYTES;
    }
    return false;
}

HandlerStatus ApplyRowToDoc(Doc* doc, const yidiandb::Row* row, const Schema* schema) {
    if (!row || !row->fields()) return HandlerStatus::Ok();
    for (auto fv : *row->fields()) {
        if (!fv) continue;
        const FieldDef* def = schema->FindFieldById(static_cast<uint16_t>(fv->field_id()));
        if (!def) {
            return HandlerStatus::Fail("unknown field_id " + std::to_string(fv->field_id()));
        }
        if (!FieldValueMatchesSchema(*def, fv->vtype())) {
            return HandlerStatus::Fail(FieldTypeMismatch(*def, fv->vtype()));
        }
        const uint32_t fid = def->field_id;
        switch (fv->vtype()) {
            case yidiandb::ValueType_BOOL:
                doc->put_int32(fid, fv->i32() ? 1 : 0);
                break;
            case yidiandb::ValueType_I32:
                doc->put_int32(fid, fv->i32());
                break;
            case yidiandb::ValueType_I64:
                doc->put_int64(fid, fv->i64());
                break;
            case yidiandb::ValueType_F32:
                doc->put_float(fid, fv->f32());
                break;
            case yidiandb::ValueType_F64:
                doc->put_double(fid, fv->f64());
                break;
            case yidiandb::ValueType_STRING:
                if (fv->s())
                    doc->put_string(fid, fv->s()->str());
                else
                    doc->put_string(fid, "");
                break;
            case yidiandb::ValueType_BYTES:
                if (fv->raw())
                    doc->put_string(
                        fid, std::string_view(reinterpret_cast<const char*>(fv->raw()->Data()),
                                              fv->raw()->size()));
                else
                    doc->put_string(fid, "");
                break;
            case yidiandb::ValueType_ARR_I32:
                if (fv->ai32())
                    doc->array_put_int32(fid, fv->ai32()->data(),
                                         static_cast<uint32_t>(fv->ai32()->size()));
                break;
            case yidiandb::ValueType_ARR_I64:
                if (fv->ai64())
                    doc->array_put_int64(fid, fv->ai64()->data(),
                                         static_cast<uint32_t>(fv->ai64()->size()));
                break;
            case yidiandb::ValueType_ARR_F32:
                if (fv->af32())
                    doc->array_put_float(fid, fv->af32()->data(),
                                         static_cast<uint32_t>(fv->af32()->size()));
                break;
            case yidiandb::ValueType_ARR_F64:
                if (fv->af64())
                    doc->array_put_double(fid, fv->af64()->data(),
                                          static_cast<uint32_t>(fv->af64()->size()));
                break;
            case yidiandb::ValueType_ARR_STRING: {
                if (!fv->as()) break;
                const auto* vec = fv->as();
                std::vector<std::string_view> parts;
                parts.reserve(vec->size());
                for (flatbuffers::uoffset_t i = 0; i < vec->size(); ++i) {
                    const auto* st = vec->Get(i);
                    parts.push_back(st ? std::string_view(st->c_str(), st->size()) : std::string_view{});
                }
                doc->array_put_string(fid, parts.data(), static_cast<uint32_t>(parts.size()));
                break;
            }
            default:
                return HandlerStatus::Fail("unsupported ValueType for field_id "
                                           + std::to_string(fv->field_id()));
        }
    }
    return HandlerStatus::Ok();
}

std::string ExtractPkString(const Doc& doc, const Schema* schema) {
    const FieldDef* pk = schema->FindField(schema->pk());
    if (!pk) return {};
    const uint32_t fid = pk->field_id;
    switch (pk->type) {
        case DataType::Bool:
            return std::to_string(doc.get_int32(fid));
        case DataType::Int32:
            return std::to_string(doc.get_int32(fid));
        case DataType::Int64:
            return std::to_string(doc.get_int64(fid));
        case DataType::String:
            return std::string(doc.get_string(fid));
        default:
            return {};
    }
}

static flatbuffers::Offset<yidiandb::FieldValue> BuildFieldValue(flatbuffers::FlatBufferBuilder& fbb,
                                                                 const Doc&                     doc,
                                                                 const FieldDef&                def) {
    const uint32_t fid = def.field_id;
    if (def.is_array) {
        switch (def.type) {
            case DataType::Int32: {
                auto v = doc.array_view_int32(fid);
                auto vec = v.second ? fbb.CreateVector(v.first, v.second) : 0;
                return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                                  yidiandb::ValueType_ARR_I32,
                                                  0, 0, 0, 0, 0, 0, vec, 0, 0, 0, 0);
            }
            case DataType::Int64: {
                auto v = doc.array_view_int64(fid);
                auto vec = v.second ? fbb.CreateVector(v.first, v.second) : 0;
                return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                                  yidiandb::ValueType_ARR_I64,
                                                  0, 0, 0, 0, 0, 0, 0, vec, 0, 0, 0);
            }
            case DataType::Float32: {
                auto v = doc.array_view_float(fid);
                auto vec = v.second ? fbb.CreateVector(v.first, v.second) : 0;
                return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                                  yidiandb::ValueType_ARR_F32,
                                                  0, 0, 0, 0, 0, 0, 0, 0, vec, 0, 0);
            }
            case DataType::Float64: {
                auto v = doc.array_view_double(fid);
                auto vec = v.second ? fbb.CreateVector(v.first, v.second) : 0;
                return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                                  yidiandb::ValueType_ARR_F64,
                                                  0, 0, 0, 0, 0, 0, 0, 0, 0, vec, 0);
            }
            case DataType::String: {
                const uint32_t n = doc.array_size(fid);
                std::vector<flatbuffers::Offset<flatbuffers::String>> strs;
                strs.reserve(n);
                for (uint32_t i = 0; i < n; ++i) {
                    auto sv = doc.array_get_string(fid, i);
                    strs.push_back(fbb.CreateString(sv.data(), sv.size()));
                }
                auto vec = n ? fbb.CreateVector(strs) : 0;
                return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                                  yidiandb::ValueType_ARR_STRING,
                                                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, vec);
            }
            default:
                return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                                  yidiandb::ValueType_NONE,
                                                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }
    }
    switch (def.type) {
        case DataType::Bool:
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_BOOL,
                                              doc.get_int32(fid) ? 1 : 0,
                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        case DataType::Int32:
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_I32,
                                              doc.get_int32(fid), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        case DataType::Int64:
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_I64, 0,
                                              doc.get_int64(fid), 0, 0, 0, 0, 0, 0, 0, 0, 0);
        case DataType::Float32:
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_F32, 0, 0,
                                              doc.get_float(fid), 0, 0, 0, 0, 0, 0, 0, 0);
        case DataType::Float64:
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_F64, 0, 0, 0,
                                              doc.get_double(fid), 0, 0, 0, 0, 0, 0, 0);
        case DataType::String: {
            auto sv = doc.get_string(fid);
            auto s  = fbb.CreateString(sv.data(), sv.size());
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_STRING,
                                              0, 0, 0, 0, s, 0, 0, 0, 0, 0, 0);
        }
        case DataType::Bytes: {
            auto sv = doc.get_string(fid);
            auto b  = fbb.CreateVector(reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
            return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid),
                                              yidiandb::ValueType_BYTES,
                                              0, 0, 0, 0, 0, b, 0, 0, 0, 0, 0);
        }
    }
    return yidiandb::CreateFieldValue(fbb, static_cast<uint16_t>(fid), yidiandb::ValueType_NONE,
                                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
}

flatbuffers::Offset<yidiandb::Row> BuildRow(flatbuffers::FlatBufferBuilder& fbb, const Doc& doc,
                                            const Schema* schema) {
    std::vector<flatbuffers::Offset<yidiandb::FieldValue>> offs;
    for (const auto& fp : schema->fields()) {
        const FieldDef* def = fp.get();
        offs.push_back(BuildFieldValue(fbb, doc, *def));
    }
    return yidiandb::CreateRow(fbb, fbb.CreateVector(offs));
}

void HandleGet(KVIndex* idx, const Schema* schema, const void* req, size_t req_len,
               std::string* out_resp) {
    const yidiandb::GetRequest* preq =
        flatbuffers::GetRoot<yidiandb::GetRequest>(reinterpret_cast<const uint8_t*>(req));
    flatbuffers::FlatBufferBuilder        fbb(1024);
    if (!preq || !preq->pk()) {
        auto er = fbb.CreateString("missing pk");
        auto nr = yidiandb::CreateGetResponse(fbb, false, er, 0, 0);
        fbb.Finish(nr);
        out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
        return;
    }
    std::string                      pk = preq->pk()->str();
    Doc                              out;
    uint64_t                         index_get_ns = 0;
    bool                             hit          = false;
    {
        auto t0 = std::chrono::steady_clock::now();
        hit     = idx->Get(pk, &out);
        index_get_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
                .count());
    }
    if (!hit) {
        auto er = fbb.CreateString("");
        auto nr = yidiandb::CreateGetResponse(fbb, false, er, 0, index_get_ns);
        fbb.Finish(nr);
        out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
        return;
    }
    auto row = BuildRow(fbb, out, schema);
    auto er  = fbb.CreateString("");
    auto nr  = yidiandb::CreateGetResponse(fbb, true, er, row, index_get_ns);
    fbb.Finish(nr);
    out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
}

void HandlePut(KVIndex* idx, const Schema* schema, std::mutex* write_mu, const void* req,
               size_t req_len, std::string* out_resp) {
    const yidiandb::PutRequest* preq =
        flatbuffers::GetRoot<yidiandb::PutRequest>(reinterpret_cast<const uint8_t*>(req));
    flatbuffers::FlatBufferBuilder fbb(256);
    if (!preq || !preq->row()) {
        auto es = fbb.CreateString("missing row");
        auto pr = yidiandb::CreatePutResponse(fbb, false, es);
        fbb.Finish(pr);
        out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
        return;
    }
    std::string err;
    {
        std::lock_guard<std::mutex> lk(*write_mu);
        Doc doc = idx->NewDoc();
        HandlerStatus st = ApplyRowToDoc(&doc, preq->row(), schema);
        if (!st.ok) {
            auto es = fbb.CreateString(st.err);
            auto pr = yidiandb::CreatePutResponse(fbb, false, es);
            fbb.Finish(pr);
            out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
            return;
        }
        std::string pk = ExtractPkString(doc, schema);
        if (pk.empty()) {
            auto es = fbb.CreateString("cannot derive pk (check pk field type in schema)");
            auto pr = yidiandb::CreatePutResponse(fbb, false, es);
            fbb.Finish(pr);
            out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
            return;
        }
        idx->Upsert(&doc);
    }
    auto es = fbb.CreateString("");
    auto pr = yidiandb::CreatePutResponse(fbb, true, es);
    fbb.Finish(pr);
    out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
}

void HandlePutBatch(KVIndex* idx, const Schema* schema, std::mutex* write_mu, const void* req,
                    size_t req_len, std::string* out_resp) {
    const yidiandb::PutBatchRequest* preq =
        flatbuffers::GetRoot<yidiandb::PutBatchRequest>(reinterpret_cast<const uint8_t*>(req));
    flatbuffers::FlatBufferBuilder fbb(256);
    if (!preq || !preq->rows()) {
        auto es = fbb.CreateString("missing rows");
        auto pr = yidiandb::CreatePutBatchResponse(fbb, false, es);
        fbb.Finish(pr);
        out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
        return;
    }
    const auto* rows = preq->rows();
    if (rows->size() == 0) {
        auto es = fbb.CreateString("empty batch");
        auto pr = yidiandb::CreatePutBatchResponse(fbb, false, es);
        fbb.Finish(pr);
        out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
        return;
    }
    {
        std::lock_guard<std::mutex> lk(*write_mu);
        std::vector<Doc> staged;
        staged.reserve(rows->size());
        for (flatbuffers::uoffset_t i = 0; i < rows->size(); ++i) {
            const yidiandb::Row* row = rows->Get(i);
            if (!row) {
                auto es = fbb.CreateString("null row in batch");
                auto pr = yidiandb::CreatePutBatchResponse(fbb, false, es);
                fbb.Finish(pr);
                out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
                return;
            }
            Doc doc = idx->NewDoc();
            HandlerStatus st = ApplyRowToDoc(&doc, row, schema);
            if (!st.ok) {
                auto es = fbb.CreateString(st.err);
                auto pr = yidiandb::CreatePutBatchResponse(fbb, false, es);
                fbb.Finish(pr);
                out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
                return;
            }
            std::string pk = ExtractPkString(doc, schema);
            if (pk.empty()) {
                auto es = fbb.CreateString("cannot derive pk (check pk field type in schema)");
                auto pr = yidiandb::CreatePutBatchResponse(fbb, false, es);
                fbb.Finish(pr);
                out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
                return;
            }
            staged.push_back(doc);
        }
        std::vector<Doc*> ptrs;
        ptrs.reserve(staged.size());
        for (auto& doc : staged) {
            ptrs.push_back(&doc);
        }
        idx->BatchUpsert(ptrs);
    }
    auto es = fbb.CreateString("");
    auto pr = yidiandb::CreatePutBatchResponse(fbb, true, es);
    fbb.Finish(pr);
    out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
}

void HandleBatchGet(KVIndex* idx, const Schema* schema, const void* req, size_t req_len,
                    std::string* out_resp) {
    const yidiandb::BatchGetRequest* preq =
        flatbuffers::GetRoot<yidiandb::BatchGetRequest>(reinterpret_cast<const uint8_t*>(req));
    flatbuffers::FlatBufferBuilder        fbb(4096);
    flatbuffers::Offset<flatbuffers::String> es = fbb.CreateString("");
    if (!preq || !preq->pks()) {
        auto resp = yidiandb::CreateBatchGetResponse(fbb, 0, es);
        fbb.Finish(resp);
        out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
        return;
    }
    std::vector<flatbuffers::Offset<yidiandb::Row>> row_offs;
    const auto* pks = preq->pks();
    for (flatbuffers::uoffset_t i = 0; i < pks->size(); ++i) {
        const auto* pk_s = pks->Get(i);
        if (!pk_s) {
            row_offs.push_back(yidiandb::CreateRow(fbb, 0));
            continue;
        }
        Doc out;
        if (!idx->Get(pk_s->str(), &out)) {
            row_offs.push_back(yidiandb::CreateRow(fbb, 0));
        } else {
            row_offs.push_back(BuildRow(fbb, out, schema));
        }
    }
    auto resp = yidiandb::CreateBatchGetResponse(fbb, fbb.CreateVector(row_offs), es);
    fbb.Finish(resp);
    out_resp->assign(reinterpret_cast<const char*>(fbb.GetBufferPointer()), fbb.GetSize());
}

}  // namespace yikv_server::db
