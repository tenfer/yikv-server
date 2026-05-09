// yikv-server — brpc (SerializedRequest payload = FlatBuffers) over yikv KV index.

#include "rpc/db_brpc_service.h"

#include <brpc/server.h>

#include "src/db/db.h"
#include "src/schema/schema.h"
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

using yikv::alloc::AllocatorMode;
using yikv::db::DB;
using yikv::db::DBOptions;

struct Flags {
    std::string   db_path;
    std::string   index_name;
    std::string   listen = "0.0.0.0:9000";
    int           port = -1;  // if >= 0, replaces port in --listen (or appends to host-only)
    std::string   schema_json_path;
    bool          create_if_missing = false;
    bool          recreate          = false;
    std::uint64_t arena_seg_gb      = 1;
    std::uint64_t arena_max_gb      = 512;
};

bool ParseFlags(int argc, char** argv, Flags* f) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
            f->db_path = argv[++i];
        } else if (std::strcmp(argv[i], "--index") == 0 && i + 1 < argc) {
            f->index_name = argv[++i];
        } else if (std::strcmp(argv[i], "--listen") == 0 && i + 1 < argc) {
            f->listen = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            char*            end = nullptr;
            unsigned long    v   = std::strtoul(argv[++i], &end, 10);
            if (end == argv[i] || *end != '\0' || v == 0 || v > 65535) {
                std::cerr << "--port must be 1-65535\n";
                return false;
            }
            f->port = static_cast<int>(v);
        } else if (std::strcmp(argv[i], "--schema_json") == 0 && i + 1 < argc) {
            f->schema_json_path = argv[++i];
        } else if (std::strcmp(argv[i], "--create_if_missing") == 0) {
            f->create_if_missing = true;
        } else if (std::strcmp(argv[i], "--recreate") == 0) {
            f->recreate = true;
        } else if (std::strcmp(argv[i], "--arena_seg_gb") == 0 && i + 1 < argc) {
            f->arena_seg_gb = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--arena_max_gb") == 0 && i + 1 < argc) {
            f->arena_max_gb = std::strtoull(argv[++i], nullptr, 10);
        } else {
            std::cerr << "Unknown arg: " << argv[i] << "\n";
            return false;
        }
    }
    if (f->db_path.empty() || f->index_name.empty()) {
        std::cerr << "Required: --db PATH --index NAME\n";
        return false;
    }
    if (f->create_if_missing && f->schema_json_path.empty()) {
        std::cerr << "--create_if_missing requires --schema_json FILE\n";
        return false;
    }
    if (f->arena_seg_gb == 0 || f->arena_max_gb == 0) {
        std::cerr << "--arena_seg_gb and --arena_max_gb must be positive\n";
        return false;
    }
    if (f->arena_max_gb < f->arena_seg_gb) {
        std::cerr << "--arena_max_gb must be >= --arena_seg_gb\n";
        return false;
    }
    if (f->port >= 0) {
        const auto colon = f->listen.rfind(':');
        if (colon != std::string::npos) {
            f->listen = f->listen.substr(0, colon + 1) + std::to_string(f->port);
        } else {
            f->listen += ':';
            f->listen += std::to_string(f->port);
        }
    }
    return true;
}

std::string ReadFile(const std::string& path) {
    std::ifstream in(path, std::ios::in | std::ios::binary);
    if (!in) throw std::runtime_error("cannot read file: " + path);
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return data;
}

}  // namespace

int main(int argc, char** argv) {
    Flags fl;
    if (!ParseFlags(argc, argv, &fl)) return 2;

    yikv::db::DBOptions opt;
    opt.db_path                       = fl.db_path;
    opt.alloc_defaults.mode           = AllocatorMode::Concurrent;
    const std::uint64_t seg_b         = fl.arena_seg_gb * 1024ull * 1024ull * 1024ull;
    opt.alloc_defaults.arena_size     = seg_b;
    opt.alloc_defaults.segment_size   = seg_b;
    opt.alloc_defaults.max_arena_size = fl.arena_max_gb * 1024ull * 1024ull * 1024ull;
    yikv::db::DB::Init(std::move(opt));

    namespace fs = std::filesystem;
    fs::path idx_dir = fs::path(fl.db_path) / fl.index_name;
    bool     exists  = fs::is_directory(idx_dir);

    if (fl.recreate && exists) {
        fs::remove_all(idx_dir);
        exists = false;
    }

    try {
        if (!exists && fl.create_if_missing) {
            yikv::schema::Schema sch;
            std::string          err;
            if (!sch.LoadJson(ReadFile(fl.schema_json_path), &err)) {
                std::cerr << "schema: " << err << "\n";
                return 1;
            }
            yikv::db::DB::Instance().CreateKVIndex(fl.index_name, sch);
        } else {
            yikv::db::DB::Instance().OpenIndex(fl.index_name);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }

    yikv::index::KVIndex* kv = yikv::db::DB::Instance().GetKVIndex(fl.index_name);

    yikv_server::rpc::DbBrpcService* brpc_svc = new yikv_server::rpc::DbBrpcService(kv);

    brpc::Server         server;
    brpc::ServerOptions  sopt;
    sopt.baidu_master_service = brpc_svc;

    if (server.Start(fl.listen.c_str(), &sopt) != 0) {
        std::cerr << "Fail to start server on " << fl.listen << "\n";
        return 1;
    }
    std::cerr << "yikv-server (brpc) listening on " << fl.listen << "\n";
    server.RunUntilAskedToQuit();
    return 0;
}
