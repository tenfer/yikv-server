#include "kafka/kafka_source.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <librdkafka/rdkafka.h>
#include <nlohmann/json.hpp>

#include "src/index/doc.h"
#include "src/schema/schema.h"

namespace yikv_server::kafka {

using yikv::index::Doc;
using yikv::index::KVIndex;
using yikv::schema::DataType;
using yikv::schema::FieldDef;
using yikv::schema::Schema;

// ─── helpers ──────────────────────────────────────────────────────────────────

static std::string WallTs() {
    auto now  = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    return buf;
}

#define LOG_ERR(msg) std::cerr << "[" << WallTs() << "][kafka] ERROR: " << msg << "\n"
#define LOG_INF(msg) std::cerr << "[" << WallTs() << "][kafka] " << msg << "\n"

// ─── KafkaSource ─────────────────────────────────────────────────────────────

KafkaSource::KafkaSource(KVIndex* idx, const Schema* schema, Config cfg)
    : idx_(idx), schema_(schema), cfg_(std::move(cfg)) {}

KafkaSource::~KafkaSource() {
    Stop();
}

void KafkaSource::Start() {
    thread_ = std::thread(&KafkaSource::ConsumeLoop, this);
}

void KafkaSource::Stop() {
    stop_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) thread_.join();
}

// ─── Offset file ─────────────────────────────────────────────────────────────

int64_t KafkaSource::LoadOffset() const {
    std::ifstream f(cfg_.offset_file);
    if (!f) return RD_KAFKA_OFFSET_BEGINNING;
    int64_t off = RD_KAFKA_OFFSET_BEGINNING;
    f >> off;
    // Resume from the next message after the last committed one.
    return (off >= 0) ? off + 1 : RD_KAFKA_OFFSET_BEGINNING;
}

void KafkaSource::SaveOffset(int64_t offset) const {
    std::ofstream f(cfg_.offset_file, std::ios::trunc);
    if (!f) {
        LOG_ERR("cannot write offset file: " << cfg_.offset_file);
        return;
    }
    f << offset << "\n";
}

// ─── Consume loop ─────────────────────────────────────────────────────────────

void KafkaSource::ConsumeLoop() {
    char errstr[512];

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", cfg_.brokers.c_str(),
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        LOG_ERR("conf bootstrap.servers: " << errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }
    // Disable auto-commit; we manage offsets manually via the offset file.
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", nullptr, 0);
    rd_kafka_conf_set(conf, "auto.offset.reset",  "earliest", nullptr, 0);

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        LOG_ERR("rd_kafka_new: " << errstr);
        return;
    }
    // conf is now owned by rk.

    int64_t start_offset = LoadOffset();

    rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, cfg_.topic.c_str(), nullptr);
    if (!rkt) {
        LOG_ERR("rd_kafka_topic_new failed for topic: " << cfg_.topic);
        rd_kafka_destroy(rk);
        return;
    }

    if (rd_kafka_consume_start(rkt, cfg_.partition, start_offset) == -1) {
        LOG_ERR("rd_kafka_consume_start: " << rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        return;
    }

    LOG_INF("consuming topic=" << cfg_.topic << " partition=" << cfg_.partition
            << " start_offset=" << start_offset);

    while (!stop_.load(std::memory_order_relaxed)) {
        rd_kafka_message_t* msg = rd_kafka_consume(rkt, cfg_.partition, /*timeout_ms=*/200);
        if (!msg) continue;

        if (msg->err) {
            if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                rd_kafka_message_destroy(msg);
                continue;
            }
            LOG_ERR("consume error: " << rd_kafka_message_errstr(msg));
            rd_kafka_message_destroy(msg);
            continue;
        }

        std::string_view payload(static_cast<const char*>(msg->payload),
                                 static_cast<size_t>(msg->len));
        const int64_t offset = msg->offset;
        rd_kafka_message_destroy(msg);

        // Parse and apply.
        try {
            auto j = nlohmann::json::parse(payload);
            if (j.is_array()) {
                for (const auto& item : j) {
                    if (!item.is_object()) {
                        LOG_ERR("batch element is not a JSON object, skipping");
                        continue;
                    }
                    ApplySingleOp(item);
                }
            } else if (j.is_object()) {
                ApplySingleOp(j);
            } else {
                LOG_ERR("message is neither object nor array, offset=" << offset);
            }
        } catch (const nlohmann::json::exception& e) {
            LOG_ERR("JSON parse failed at offset=" << offset << ": " << e.what());
        }

        SaveOffset(offset);
    }

    rd_kafka_consume_stop(rkt, cfg_.partition);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

    LOG_INF("consumer stopped for topic=" << cfg_.topic);
}

// ─── Op dispatch ─────────────────────────────────────────────────────────────

bool KafkaSource::ApplySingleOp(const nlohmann::json& obj) {
    if (!obj.contains("_op") || !obj["_op"].is_string()) {
        LOG_ERR("message missing _op field");
        return false;
    }
    if (!obj.contains("_ts")) {
        LOG_ERR("message missing _ts field");
        return false;
    }

    const std::string op = obj["_op"].get<std::string>();
    const int64_t     ts = obj["_ts"].get<int64_t>();

    // Log event time for observability.
    (void)ts;

    if (op == "INSERT" || op == "insert") return ApplyInsert(obj);
    if (op == "UPSERT" || op == "upsert") return ApplyUpsert(obj);
    if (op == "DELETE" || op == "delete") return ApplyDelete(obj);

    LOG_ERR("unknown _op: " << op);
    return false;
}

// ─── INSERT ──────────────────────────────────────────────────────────────────

bool KafkaSource::ApplyInsert(const nlohmann::json& obj) {
    Doc doc = idx_->NewDoc();
    if (!FillDoc(&doc, obj)) return false;
    idx_->Put(&doc);
    return true;
}

// ─── UPSERT ──────────────────────────────────────────────────────────────────

bool KafkaSource::ApplyUpsert(const nlohmann::json& obj) {
    Doc doc = idx_->NewDoc();
    if (!FillDoc(&doc, obj)) return false;
    idx_->Upsert(&doc);
    return true;
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

bool KafkaSource::ApplyDelete(const nlohmann::json& obj) {
    const FieldDef* pk_def = schema_->FindField(schema_->pk());
    if (!pk_def) {
        LOG_ERR("schema has no pk field: " << schema_->pk());
        return false;
    }
    if (!obj.contains(pk_def->name)) {
        LOG_ERR("DELETE message missing pk field: " << pk_def->name);
        return false;
    }

    std::string pk_str;
    const auto& pv = obj[pk_def->name];
    switch (pk_def->type) {
        case DataType::Int32:
            pk_str = std::to_string(pv.get<int32_t>());
            break;
        case DataType::Int64:
            pk_str = std::to_string(pv.get<int64_t>());
            break;
        case DataType::String:
            pk_str = pv.get<std::string>();
            break;
        default:
            LOG_ERR("unsupported pk type for DELETE");
            return false;
    }

    idx_->Delete(pk_str);
    return true;
}

// ─── FillDoc ─────────────────────────────────────────────────────────────────

bool KafkaSource::FillDoc(Doc* doc, const nlohmann::json& obj) {
    for (const auto& [key, val] : obj.items()) {
        // Skip metadata fields.
        if (key == "_op" || key == "_ts") continue;

        const FieldDef* def = schema_->FindField(key);
        if (!def) continue;  // unknown field — silently skip

        const uint16_t fid = def->field_id;

        if (def->is_array) {
            if (!val.is_array()) {
                LOG_ERR("field " << key << " expects array");
                return false;
            }
            switch (def->type) {
                case DataType::Int32: {
                    std::vector<int32_t> v;
                    v.reserve(val.size());
                    for (const auto& e : val) v.push_back(e.get<int32_t>());
                    doc->array_put_int32(fid, v.data(), static_cast<uint32_t>(v.size()));
                    break;
                }
                case DataType::Int64: {
                    std::vector<int64_t> v;
                    v.reserve(val.size());
                    for (const auto& e : val) v.push_back(e.get<int64_t>());
                    doc->array_put_int64(fid, v.data(), static_cast<uint32_t>(v.size()));
                    break;
                }
                case DataType::Float32: {
                    std::vector<float> v;
                    v.reserve(val.size());
                    for (const auto& e : val) v.push_back(e.get<float>());
                    doc->array_put_float(fid, v.data(), static_cast<uint32_t>(v.size()));
                    break;
                }
                case DataType::Float64: {
                    std::vector<double> v;
                    v.reserve(val.size());
                    for (const auto& e : val) v.push_back(e.get<double>());
                    doc->array_put_double(fid, v.data(), static_cast<uint32_t>(v.size()));
                    break;
                }
                case DataType::String: {
                    std::vector<std::string>      sv;
                    std::vector<std::string_view> views;
                    sv.reserve(val.size());
                    views.reserve(val.size());
                    for (const auto& e : val) {
                        sv.push_back(e.get<std::string>());
                        views.push_back(sv.back());
                    }
                    doc->array_put_string(fid, views.data(),
                                          static_cast<uint32_t>(views.size()));
                    break;
                }
                default:
                    LOG_ERR("unsupported array element type for field " << key);
                    return false;
            }
        } else {
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
        }
    }
    return true;
}

}  // namespace yikv_server::kafka
