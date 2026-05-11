#include "stream/json_stream_ingest.h"

#include <cctype>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "src/index/doc.h"
#include "src/index/kv_index.h"
#include "src/schema/schema.h"

namespace yikv_server::stream {

using yikv::index::Doc;
using yikv::index::KVIndex;
using yikv::schema::DataType;
using yikv::schema::FieldDef;
using yikv::schema::Schema;

void CopyDocFromSchema(const Schema* schema, const Doc& src, Doc* dst) {
    if (!schema || !dst) return;
    for (const auto& fp : schema->fields()) {
        const FieldDef& def = *fp;
        const uint32_t  fid = def.field_id;
        if (def.is_array) {
            switch (def.type) {
                case DataType::Int32: {
                    auto [p, n] = src.array_view_int32(fid);
                    if (n > 0) dst->array_put_int32(fid, p, n);
                    break;
                }
                case DataType::Int64: {
                    auto [p, n] = src.array_view_int64(fid);
                    if (n > 0) dst->array_put_int64(fid, p, n);
                    break;
                }
                case DataType::Float32: {
                    auto [p, n] = src.array_view_float(fid);
                    if (n > 0) dst->array_put_float(fid, p, n);
                    break;
                }
                case DataType::Float64: {
                    auto [p, n] = src.array_view_double(fid);
                    if (n > 0) dst->array_put_double(fid, p, n);
                    break;
                }
                case DataType::String:
                case DataType::Bytes: {
                    const uint32_t n = src.array_size(fid);
                    if (n == 0) break;
                    std::vector<std::string>      buf;
                    std::vector<std::string_view> views;
                    buf.reserve(n);
                    views.reserve(n);
                    for (uint32_t i = 0; i < n; ++i) {
                        buf.emplace_back(src.array_get_string(fid, i));
                        views.push_back(buf.back());
                    }
                    dst->array_put_string(fid, views.data(), n);
                    break;
                }
                default:
                    break;
            }
        } else {
            switch (def.type) {
                case DataType::Bool:
                case DataType::Int32:
                    dst->put_int32(fid, src.get_int32(fid));
                    break;
                case DataType::Int64:
                    dst->put_int64(fid, src.get_int64(fid));
                    break;
                case DataType::Float32:
                    dst->put_float(fid, src.get_float(fid));
                    break;
                case DataType::Float64:
                    dst->put_double(fid, src.get_double(fid));
                    break;
                case DataType::String:
                case DataType::Bytes:
                    dst->put_string(fid, src.get_string(fid));
                    break;
            }
        }
    }
}

static bool MergeOneField(const FieldDef* def, Doc* doc, const nlohmann::json& val,
                          const LogFn& log_err) {
    if (!def || !doc) return false;
    if (val.is_null()) return true;

    const uint32_t fid = def->field_id;

    if (def->is_array) {
        if (!val.is_array()) {
            if (log_err) log_err("field expects JSON array");
            return false;
        }
        switch (def->type) {
            case DataType::Int32:
                for (const auto& e : val) doc->array_append_int32(fid, e.get<int32_t>());
                break;
            case DataType::Int64:
                for (const auto& e : val) doc->array_append_int64(fid, e.get<int64_t>());
                break;
            case DataType::Float32:
                for (const auto& e : val) doc->array_append_float(fid, e.get<float>());
                break;
            case DataType::Float64:
                for (const auto& e : val) doc->array_append_double(fid, e.get<double>());
                break;
            case DataType::String:
            case DataType::Bytes:
                for (const auto& e : val)
                    doc->array_append_string(fid, e.get<std::string>());
                break;
            default:
                if (log_err) log_err("unsupported array element type");
                return false;
        }
        return true;
    }

    switch (def->type) {
        case DataType::Bool:
            doc->put_int32(fid, val.get<bool>() ? 1 : 0);
            break;
        case DataType::Int32:
            doc->put_int32(fid, val.get<int32_t>());
            break;
        case DataType::Int64:
            doc->put_int64(fid, val.get<int64_t>());
            break;
        case DataType::Float32:
            doc->put_float(fid, val.get<float>());
            break;
        case DataType::Float64:
            doc->put_double(fid, val.get<double>());
            break;
        case DataType::String:
        case DataType::Bytes:
            doc->put_string(fid, val.get<std::string>());
            break;
    }
    return true;
}

bool MergeJsonIntoDoc(const Schema* schema, Doc* doc, const nlohmann::json& obj,
                      const LogFn& log_err) {
    for (const auto& [key, val] : obj.items()) {
        if (key == "_op" || key == "_ts") continue;
        const FieldDef* def = schema->FindField(key);
        if (!def) continue;
        if (!MergeOneField(def, doc, val, log_err)) return false;
    }
    return true;
}

bool ExtractPkString(const Schema* schema, const nlohmann::json& obj, std::string* pk_out,
                     const LogFn& log_err) {
    const FieldDef* pk_def = schema->FindField(schema->pk());
    if (!pk_def) {
        if (log_err) log_err("schema has no pk field");
        return false;
    }
    if (!obj.contains(pk_def->name)) {
        if (log_err) log_err("message missing pk field");
        return false;
    }
    const auto& pv = obj[pk_def->name];
    if (pv.is_null()) {
        if (log_err) log_err("pk is null");
        return false;
    }
    switch (pk_def->type) {
        case DataType::Int32:
            *pk_out = std::to_string(pv.get<int32_t>());
            return true;
        case DataType::Int64:
            *pk_out = std::to_string(pv.get<int64_t>());
            return true;
        case DataType::String:
        case DataType::Bytes:
            *pk_out = pv.get<std::string>();
            return true;
        default:
            if (log_err) log_err("unsupported pk type");
            return false;
    }
}

static bool ApplyInsert(KVIndex* idx, const Schema* schema, const nlohmann::json& obj,
                        const LogFn& log_err) {
    std::string pk;
    if (!ExtractPkString(schema, obj, &pk, log_err)) return false;
    Doc existing;
    if (idx->Get(pk, &existing)) {
        if (log_err) log_err("INSERT for existing pk (use UPSERT)");
        return false;
    }
    Doc doc = idx->NewDoc();
    if (!MergeJsonIntoDoc(schema, &doc, obj, log_err)) return false;
    idx->Put(&doc);
    return true;
}

static bool ApplyUpsertMerge(KVIndex* idx, const Schema* schema, const nlohmann::json& obj,
                             const LogFn& log_err) {
    std::string pk;
    if (!ExtractPkString(schema, obj, &pk, log_err)) return false;

    Doc doc = idx->NewDoc();
    Doc old;
    if (idx->Get(pk, &old)) CopyDocFromSchema(schema, old, &doc);
    if (!MergeJsonIntoDoc(schema, &doc, obj, log_err)) return false;
    idx->Upsert(&doc);
    return true;
}

static bool ApplyDelete(KVIndex* idx, const Schema* schema, const nlohmann::json& obj,
                        const LogFn& log_err) {
    const FieldDef* pk_def = schema->FindField(schema->pk());
    if (!pk_def) {
        if (log_err) log_err("schema has no pk field");
        return false;
    }
    if (!obj.contains(pk_def->name)) {
        if (log_err) log_err("DELETE message missing pk field");
        return false;
    }
    std::string pk;
    if (!ExtractPkString(schema, obj, &pk, log_err)) return false;
    idx->Delete(pk);
    return true;
}

bool ApplyStreamJsonObject(KVIndex* idx, const Schema* schema, const nlohmann::json& obj,
                           const LogFn& log_err) {
    std::string op;
    if (obj.contains("_op") && obj["_op"].is_string()) {
        op = obj["_op"].get<std::string>();
        for (auto& ch : op)
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }

    if (op == "delete") return ApplyDelete(idx, schema, obj, log_err);
    if (op == "insert") return ApplyInsert(idx, schema, obj, log_err);
    if (op == "upsert" || op.empty()) return ApplyUpsertMerge(idx, schema, obj, log_err);

    if (log_err) log_err("unknown _op");
    return false;
}

}  // namespace yikv_server::stream
