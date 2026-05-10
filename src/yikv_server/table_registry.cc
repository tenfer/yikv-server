#include "table_registry.h"

#include <chrono>
#include <ctime>
#include <iostream>
#include <stdexcept>

#include <sys/inotify.h>
#include <unistd.h>

#include "table_config.h"

#include "src/db/db.h"

namespace yikv_server {

namespace fs = std::filesystem;

static std::string WallTs() {
    auto      now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    return buf;
}

#define LOG_INF(msg) std::cerr << "[" << WallTs() << "][registry] " << msg << "\n"
#define LOG_ERR(msg) std::cerr << "[" << WallTs() << "][registry] ERROR: " << msg << "\n"

// ─── ctor / dtor ─────────────────────────────────────────────────────────────

TableRegistry::TableRegistry(fs::path db_path, std::string default_brokers)
    : db_path_(std::move(db_path)), default_brokers_(std::move(default_brokers)) {}

TableRegistry::~TableRegistry() {
    StopWatcher();
    // Stop all KafkaSources before the KVIndexes are invalidated.
    std::unique_lock lk(mu_);
    for (auto& [name, slot] : tables_) {
        if (slot->kafka_src) slot->kafka_src->Stop();
    }
}

// ─── ScanAndLoad ─────────────────────────────────────────────────────────────

void TableRegistry::ScanAndLoad() {
    if (!fs::is_directory(db_path_)) {
        LOG_ERR("db_path is not a directory: " << db_path_);
        return;
    }
    for (const auto& entry : fs::directory_iterator(db_path_)) {
        if (entry.is_directory()) {
            LoadTable(entry.path());
        }
    }
    LOG_INF("startup scan complete: " << tables_.size() << " table(s) loaded");
}

// ─── LoadTable ───────────────────────────────────────────────────────────────

bool TableRegistry::LoadTable(const fs::path& table_dir) {
    const std::string name = table_dir.filename().string();
    if (name.empty() || name[0] == '.') return false;  // skip hidden dirs

    {
        std::shared_lock lk(mu_);
        if (tables_.count(name)) return true;  // already loaded
    }

    // Open via DB. DB::OpenIndex recovers schema from the arena.
    try {
        yikv::db::DB::Instance().OpenIndex(name);
    } catch (const std::exception& e) {
        LOG_ERR("OpenIndex(" << name << ") failed: " << e.what());
        return false;
    }

    yikv::index::KVIndex*       kv     = yikv::db::DB::Instance().GetKVIndex(name);
    const yikv::schema::Schema* schema = kv->schema();
    if (!kv || !schema) {
        LOG_ERR("GetKVIndex(" << name << ") returned null");
        return false;
    }

    auto slot = std::make_unique<TableSlot>();
    slot->kv     = kv;
    slot->schema = schema;

    // Load per-table config (kafka).
    TableConfig tcfg;
    try {
        tcfg = LoadTableConfig(table_dir);
    } catch (const std::exception& e) {
        LOG_ERR("table.json error for " << name << ": " << e.what());
        // Non-fatal: proceed without kafka.
    }

    if (tcfg.kafka.has_value()) {
        const auto& kc = *tcfg.kafka;
        const std::string& brokers = kc.brokers.empty() ? default_brokers_ : kc.brokers;
        if (brokers.empty()) {
            LOG_ERR("no kafka brokers for table " << name << "; skipping KafkaSource");
        } else {
            std::string offset_file =
                (db_path_ / (name + "_" + kc.topic + "_" +
                             std::to_string(kc.partition) + ".offset")).string();
            slot->kafka_src = std::make_unique<kafka::KafkaSource>(
                kv, schema,
                kafka::KafkaSource::Config{
                    .brokers     = brokers,
                    .topic       = kc.topic,
                    .partition   = kc.partition,
                    .offset_file = std::move(offset_file),
                });
            slot->kafka_src->Start();
            LOG_INF("KafkaSource started for " << name
                    << " topic=" << kc.topic << " partition=" << kc.partition);
        }
    }

    {
        std::unique_lock lk(mu_);
        tables_[name] = std::move(slot);
    }
    LOG_INF("loaded table: " << name);
    return true;
}

// ─── Lookup ──────────────────────────────────────────────────────────────────

TableRegistry::TableSlot* TableRegistry::Lookup(const std::string& table_name) {
    std::shared_lock lk(mu_);
    auto it = tables_.find(table_name);
    return (it != tables_.end()) ? it->second.get() : nullptr;
}

// ─── inotify watcher ─────────────────────────────────────────────────────────

void TableRegistry::StartWatcher() {
    inotify_fd_ = inotify_init1(IN_NONBLOCK);
    if (inotify_fd_ < 0) {
        LOG_ERR("inotify_init1 failed; hot-add disabled");
        return;
    }
    // Watch for new directories created/moved into db_path_.
    if (inotify_add_watch(inotify_fd_, db_path_.c_str(),
                          IN_CREATE | IN_MOVED_TO) < 0) {
        LOG_ERR("inotify_add_watch failed on " << db_path_);
        close(inotify_fd_);
        inotify_fd_ = -1;
        return;
    }
    watcher_thread_ = std::thread(&TableRegistry::WatchLoop, this);
    LOG_INF("inotify watcher started on " << db_path_);
}

void TableRegistry::StopWatcher() {
    stop_watcher_.store(true, std::memory_order_relaxed);
    if (inotify_fd_ >= 0) {
        close(inotify_fd_);
        inotify_fd_ = -1;
    }
    if (watcher_thread_.joinable()) watcher_thread_.join();
}

void TableRegistry::WatchLoop() {
    constexpr size_t kBufSz = sizeof(inotify_event) + NAME_MAX + 1;
    alignas(inotify_event) char buf[kBufSz * 8];

    while (!stop_watcher_.load(std::memory_order_relaxed)) {
        // inotify_fd_ is non-blocking; sleep briefly between polls.
        ssize_t n = read(inotify_fd_, buf, sizeof(buf));
        if (n <= 0) {
            if (stop_watcher_.load(std::memory_order_relaxed)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        for (char* p = buf; p < buf + n; ) {
            auto* ev = reinterpret_cast<inotify_event*>(p);
            p += sizeof(inotify_event) + ev->len;

            if (!(ev->mask & (IN_CREATE | IN_MOVED_TO))) continue;
            if (!(ev->mask & IN_ISDIR))                  continue;
            if (ev->len == 0)                             continue;

            const std::string dir_name(ev->name);
            // Give the build tool a moment to finish writing all files.
            std::this_thread::sleep_for(std::chrono::seconds(2));

            const fs::path new_dir = db_path_ / dir_name;
            if (fs::is_directory(new_dir)) {
                LOG_INF("new table directory detected: " << dir_name);
                LoadTable(new_dir);
            }
        }
    }
}

}  // namespace yikv_server
