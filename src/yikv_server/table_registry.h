#pragma once

// TableRegistry — manages all loaded KV tables for yikv_server.
//
// Responsibilities:
//  1. Startup scan: load every sub-directory of db_path that contains a
//     valid arena (opened via DB::OpenIndex).
//  2. Hot-add:  inotify watch on db_path; when a new sub-directory appears
//     (offline build just finished copying), auto-load it.
//  3. RPC routing: Lookup(table_name) returns the KVIndex + write_mu for
//     the requested table in O(1).
//
// Thread-safety:
//  - Lookup() is lock-free for the reader path (shared_mutex read lock).
//  - LoadTable() (called from the inotify thread) takes an exclusive lock.
//  - Each table's write_mu serialises KVIndex mutations (single-writer rule).

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "kafka/kafka_source.h"

#include "src/index/kv_index.h"
#include "src/schema/schema.h"

namespace yikv_server {

class TableRegistry {
public:
    struct TableSlot {
        yikv::index::KVIndex*       kv     = nullptr;
        const yikv::schema::Schema* schema = nullptr;
        std::mutex                  write_mu;
        std::unique_ptr<kafka::KafkaSource> kafka_src;

        // Non-copyable, non-movable (mutex + unique_ptr).
        TableSlot() = default;
        TableSlot(const TableSlot&) = delete;
        TableSlot& operator=(const TableSlot&) = delete;
    };

    // db_path        : root directory containing per-table sub-directories.
    // default_brokers: global Kafka broker list (may be overridden per table).
    explicit TableRegistry(std::filesystem::path db_path,
                           std::string default_brokers = {});
    ~TableRegistry();

    // Scan db_path, open all valid tables, start KafkaSources.
    // Call once after DB::Init().
    void ScanAndLoad();

    // Start background inotify thread to auto-load newly created table dirs.
    void StartWatcher();
    void StopWatcher();

    // Returns nullptr if the table is not loaded.
    TableSlot* Lookup(const std::string& table_name);

    // For iterating all loaded tables (used by main to register services).
    // Caller must not hold any lock while calling this.
    template <typename Fn>
    void ForEach(Fn&& fn) {
        std::shared_lock lk(mu_);
        for (auto& [name, slot] : tables_) fn(name, *slot);
    }

    size_t TableCount() const {
        std::shared_lock lk(mu_);
        return tables_.size();
    }

private:
    // Try to open and register one table directory.
    // Returns true on success; logs and returns false on error.
    bool LoadTable(const std::filesystem::path& table_dir);

    void WatchLoop();

    std::filesystem::path db_path_;
    std::string           default_brokers_;

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<TableSlot>> tables_;

    std::thread       watcher_thread_;
    std::atomic<bool> stop_watcher_{false};
    int               inotify_fd_  = -1;
};

}  // namespace yikv_server
