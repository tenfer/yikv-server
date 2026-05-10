#pragma once

// KafkaSource — single-partition Kafka consumer embedded in yikv_server.
//
// Consumes JSON messages from one Kafka topic/partition and applies
// INSERT / UPSERT / DELETE operations to a KVIndex.
//
// Standard JSON message format (single op):
//   { "_op": "INSERT", "_ts": <ms>, "field": value, ... }
//   { "_op": "UPSERT", "_ts": <ms>, "field": value, ... }
//   { "_op": "DELETE", "_ts": <ms>, "<pk_field>": pk_value }
//
// Batch (JSON array in one Kafka message):
//   [ { "_op": "INSERT", ... }, { "_op": "DELETE", ... } ]
//
// Notes:
//  - _op   : required. Case-insensitive.
//  - _ts   : required. Unix millisecond event timestamp. Logged; not stored
//             unless the schema has a field literally named "_ts".
//  - Field names match schema field names (not field_id).
//  - Unknown field names are silently ignored.
//  - For DELETE, only the PK field is required.
//  - Single-threaded consumer (CoW single-writer requirement).
//  - Offset is persisted to a local file after every committed message.

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "src/index/kv_index.h"
#include "src/schema/schema.h"

namespace yikv_server::kafka {

class KafkaSource {
public:
    struct Config {
        std::string brokers;
        std::string topic;
        int32_t     partition   = 0;
        // Full path to the offset persistence file.
        // Conventionally: {db_path}/{index}_{topic}_{partition}.offset
        std::string offset_file;
    };

    KafkaSource(yikv::index::KVIndex*        idx,
                const yikv::schema::Schema*   schema,
                Config                        cfg);
    ~KafkaSource();

    // Start the background consumer thread. Must be called at most once.
    void Start();

    // Signal the consumer to stop and wait for the thread to exit.
    void Stop();

private:
    void ConsumeLoop();

    // Parse and apply one JSON object (a single op).
    // Returns true on success; logs and returns false on error.
    bool ApplySingleOp(const nlohmann::json& obj);

    // Apply helpers.
    bool ApplyInsert(const nlohmann::json& obj);
    bool ApplyUpsert(const nlohmann::json& obj);
    bool ApplyDelete(const nlohmann::json& obj);

    // Fill doc fields from JSON object (shared by INSERT and UPSERT).
    bool FillDoc(yikv::index::Doc* doc, const nlohmann::json& obj);

    // Offset persistence.
    int64_t LoadOffset() const;
    void    SaveOffset(int64_t offset) const;

    yikv::index::KVIndex*       idx_;
    const yikv::schema::Schema* schema_;
    Config                      cfg_;

    std::thread       thread_;
    std::atomic<bool> stop_{false};
};

}  // namespace yikv_server::kafka
