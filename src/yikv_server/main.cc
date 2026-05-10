// yikv-server: multi-table KV index server.
//
// All per-server parameters come from a single config.json:
//   yikv_server config.json
//
// Per-table config (kafka topic, etc.) lives in {db_path}/{table_name}/table.json
// and is loaded automatically on startup. New tables dropped into db_path are
// detected by inotify and hot-loaded without restarting the server.

#include "rpc/db_brpc_service.h"
#include "rpc/db_grpc_service.h"
#include "server_config.h"
#include "table_registry.h"

#include <brpc/server.h>

#include "src/db/db.h"

#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: yikv_server config.json\n";
        return 2;
    }

    yikv_server::ServerConfig cfg;
    try {
        cfg = yikv_server::LoadServerConfig(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "config error: " << e.what() << "\n";
        return 2;
    }

    // ── Init DB ──────────────────────────────────────────────────────────────
    yikv::db::DBOptions opt;
    opt.db_path                       = cfg.db_path;
    opt.alloc_defaults.mode           = yikv::alloc::AllocatorMode::Concurrent;
    const uint64_t seg_b              = cfg.arena_seg_gb * 1024ull * 1024ull * 1024ull;
    opt.alloc_defaults.arena_size     = seg_b;
    opt.alloc_defaults.segment_size   = seg_b;
    opt.alloc_defaults.max_arena_size = cfg.arena_max_gb * 1024ull * 1024ull * 1024ull;
    opt.exclusive_arena_lock          = cfg.exclusive_arena_lock;
    yikv::db::DB::Init(std::move(opt));

    // ── TableRegistry: scan all tables, start KafkaSources ───────────────────
    yikv_server::TableRegistry reg(cfg.db_path, cfg.kafka_default_brokers);
    reg.ScanAndLoad();

    if (reg.TableCount() == 0) {
        std::cerr << "WARNING: no tables found under " << cfg.db_path << "\n";
    }

    // Hot-add: inotify watch on db_path for new table directories.
    reg.StartWatcher();

    // ── RPC services ──────────────────────────────────────────────────────────
    yikv_server::rpc::YikvDbGrpcService grpc_svc(&reg);
    yikv_server::rpc::DbBrpcService*    brpc_svc = new yikv_server::rpc::DbBrpcService(&reg);

    brpc::Server        server;
    brpc::ServerOptions sopt;
    sopt.baidu_master_service = brpc_svc;

    if (server.AddService(&grpc_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Fail to add yikv.db.YikvDb gRPC service\n";
        return 1;
    }
    if (server.Start(cfg.listen.c_str(), &sopt) != 0) {
        std::cerr << "Fail to start server on " << cfg.listen << "\n";
        return 1;
    }
    std::cerr << "yikv-server listening on " << cfg.listen
              << " (baidu_std + h2:grpc)\n";

    server.RunUntilAskedToQuit();

    reg.StopWatcher();
    return 0;
}
