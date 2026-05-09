// yikv_import_parquet — bulk load Parquet into a KV index via KVIndex::BatchPut per record batch
// (no gRPC / FlatBuffers).
//
// Requires Apache Arrow C++ + Parquet (e.g. Ubuntu: libarrow-dev libparquet-dev).
// Stop yidiandb_server (or any process holding this index) first: arena.lock is exclusive by default.

#include "src/db/db.h"
#include "src/index/doc.h"
#include "src/index/kv_index.h"
#include "src/schema/schema.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;

using yikv::alloc::AllocatorMode;
using yikv::db::DB;
using yikv::db::DBOptions;
using yikv::index::Doc;
using yikv::index::KVIndex;
using yikv::schema::DataType;
using yikv::schema::FieldDef;
using yikv::schema::Schema;

namespace {

// Returns current local time as "YYYY-MM-DD HH:MM:SS".
static std::string WallTimestamp() {
    const auto now  = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf {};
    ::localtime_r(&t, &tm_buf);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    return buf;
}

static bool EndsWithParquetExtension(const fs::path& p) {
    std::string ext = p.extension().string();
    for (char& ch : ext) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return ext == ".parquet";
}

static bool CollectParquetFilesUnderDir(const fs::path& dir, std::vector<std::string>* out, std::string* err) {
    std::error_code ec;
    if (!fs::exists(dir, ec) || ec) {
        *err = "path does not exist: " + dir.string();
        return false;
    }
    if (!fs::is_directory(dir, ec) || ec) {
        *err = "not a directory: " + dir.string();
        return false;
    }
    std::vector<fs::path> found;
    try {
        const auto opt = fs::directory_options::skip_permission_denied;
        for (const auto& ent : fs::recursive_directory_iterator(dir, opt)) {
            std::error_code fst;
            if (!ent.is_regular_file(fst) || fst) continue;
            if (!EndsWithParquetExtension(ent.path())) continue;
            found.push_back(ent.path());
        }
    } catch (const fs::filesystem_error& e) {
        *err = e.what();
        return false;
    }
    if (found.empty()) {
        *err = "no .parquet files under " + dir.string();
        return false;
    }
    std::sort(found.begin(), found.end());
    for (const auto& p : found) {
        out->push_back(p.string());
    }
    return true;
}

std::string ReadFile(const std::string& path) {
    std::ifstream in(path, std::ios::in | std::ios::binary);
    if (!in) throw std::runtime_error("cannot read file: " + path);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

// Read total row count from Parquet metadata (fast: no data pages read).
static uint64_t EstimateTotalRows(const std::vector<std::string>& files) {
    uint64_t total = 0;
    for (const auto& path : files) {
        auto infile_res = arrow::io::ReadableFile::Open(path);
        if (!infile_res.ok()) continue;
        auto reader_res =
            parquet::arrow::OpenFile(*infile_res, arrow::default_memory_pool());
        if (!reader_res.ok()) continue;
        auto* pr = (*reader_res)->parquet_reader();
        if (pr && pr->metadata())
            total += static_cast<uint64_t>(pr->metadata()->num_rows());
    }
    return total;
}

// Compute the smallest power-of-2 bucket count ≥ expected_rows, expressed
// as the bit-width (bits). With kLoadFactor=2, rehash triggers only when
// entries > 2×bucket_count, so bucket_count=rows guarantees zero rehashes.
static uint32_t BucketBitsForRows(uint64_t n_rows) {
    if (n_rows == 0) return 15;  // default
    uint32_t bits    = 1;
    uint64_t buckets = 2;
    while (buckets < n_rows) { buckets <<= 1; ++bits; }
    return (bits > 28) ? 28u : bits;  // cap at 2^28 ≈ 256M buckets
}

void Usage() {
    std::cerr
        << "Usage: yikv_import_parquet \\\n"
        << "         --db PATH --index NAME \\\n"
        << "         (--input FILE)+ | --input_list PATH | --input_dir PATH \\\n"
        << "         [--schema_json PATH] [--create_if_missing] [--recreate] \\\n"
        << "         [--arena_seg_gb N] [--arena_max_gb N] [--no_arena_lock]\n"
        << "\n"
        << "  --input_dir      Recursively import every *.parquet under PATH (sorted by path).\n"
        << "  --no_arena_lock  Skip flock(arena.lock); only for offline tools when no other opener.\n";
}

struct Flags {
    std::string              db_path;
    std::string              index_name;
    std::string              schema_json_path;
    bool                     create_if_missing = false;
    bool                     recreate          = false;
    bool                     no_arena_lock     = false;
    uint64_t                 arena_seg_gb      = 1;
    uint64_t                 arena_max_gb      = 512;
    std::vector<std::string> input_files;
};

bool ParseFlags(int argc, char** argv, Flags* f) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
            f->db_path = argv[++i];
        } else if (std::strcmp(argv[i], "--index") == 0 && i + 1 < argc) {
            f->index_name = argv[++i];
        } else if (std::strcmp(argv[i], "--schema_json") == 0 && i + 1 < argc) {
            f->schema_json_path = argv[++i];
        } else if (std::strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            f->input_files.push_back(argv[++i]);
        } else if (std::strcmp(argv[i], "--input_list") == 0 && i + 1 < argc) {
            std::ifstream in(argv[++i]);
            if (!in) {
                std::cerr << "cannot open --input_list\n";
                return false;
            }
            std::string line;
            while (std::getline(in, line)) {
                if (line.empty()) continue;
                while (!line.empty() && (line.back() == '\r' || line.back() == ' ' || line.back() == '\t'))
                    line.pop_back();
                if (!line.empty()) f->input_files.push_back(line);
            }
        } else if (std::strcmp(argv[i], "--input_dir") == 0 && i + 1 < argc) {
            std::string err;
            if (!CollectParquetFilesUnderDir(fs::path(argv[++i]), &f->input_files, &err)) {
                std::cerr << "--input_dir: " << err << "\n";
                return false;
            }
        } else if (std::strcmp(argv[i], "--create_if_missing") == 0) {
            f->create_if_missing = true;
        } else if (std::strcmp(argv[i], "--recreate") == 0) {
            f->recreate = true;
        } else if (std::strcmp(argv[i], "--no_arena_lock") == 0) {
            f->no_arena_lock = true;
        } else if (std::strcmp(argv[i], "--arena_seg_gb") == 0 && i + 1 < argc) {
            f->arena_seg_gb = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--arena_max_gb") == 0 && i + 1 < argc) {
            f->arena_max_gb = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "-h") == 0 || std::strcmp(argv[i], "--help") == 0) {
            Usage();
            return false;
        } else {
            std::cerr << "Unknown arg: " << argv[i] << "\n";
            return false;
        }
    }
    if (f->db_path.empty() || f->index_name.empty() || f->input_files.empty()) {
        std::cerr << "Required: --db --index and at least one --input or --input_list\n";
        return false;
    }
    if (f->create_if_missing && f->schema_json_path.empty()) {
        std::cerr << "--create_if_missing requires --schema_json\n";
        return false;
    }
    if (f->arena_seg_gb == 0 || f->arena_max_gb == 0) {
        std::cerr << "arena sizes must be positive\n";
        return false;
    }
    if (f->arena_max_gb < f->arena_seg_gb) {
        std::cerr << "--arena_max_gb must be >= --arena_seg_gb\n";
        return false;
    }
    return true;
}

bool ArrowCellIsNull(const arrow::Array& col, int64_t row) { return col.IsNull(row); }

arrow::Status ApplyScalar(Doc* doc, const FieldDef& def, const arrow::Array& col, int64_t row) {
    if (ArrowCellIsNull(col, row)) return arrow::Status::OK();
    const uint32_t fid = def.field_id;
    switch (def.type) {
        case DataType::Bool: {
            if (col.type_id() != arrow::Type::BOOL) {
                return arrow::Status::Invalid("field ", def.name, ": expected bool column");
            }
            const auto& a = static_cast<const arrow::BooleanArray&>(col);
            doc->put_int32(fid, a.Value(row) ? 1 : 0);
            return arrow::Status::OK();
        }
        case DataType::Int32: {
            int32_t v = 0;
            switch (col.type_id()) {
                case arrow::Type::INT8:
                    v = static_cast<const arrow::Int8Array&>(col).Value(row);
                    break;
                case arrow::Type::INT16:
                    v = static_cast<const arrow::Int16Array&>(col).Value(row);
                    break;
                case arrow::Type::INT32:
                    v = static_cast<const arrow::Int32Array&>(col).Value(row);
                    break;
                case arrow::Type::UINT8:
                    v = static_cast<const arrow::UInt8Array&>(col).Value(row);
                    break;
                case arrow::Type::UINT16:
                    v = static_cast<const arrow::UInt16Array&>(col).Value(row);
                    break;
                case arrow::Type::UINT32: {
                    uint32_t u = static_cast<const arrow::UInt32Array&>(col).Value(row);
                    if (u > static_cast<uint32_t>(INT32_MAX))
                        return arrow::Status::Invalid("field ", def.name, ": uint32 too large for int32");
                    v = static_cast<int32_t>(u);
                    break;
                }
                case arrow::Type::INT64: {
                    int64_t x = static_cast<const arrow::Int64Array&>(col).Value(row);
                    if (x > INT32_MAX || x < INT32_MIN)
                        return arrow::Status::Invalid("field ", def.name, ": int64 out of int32 range");
                    v = static_cast<int32_t>(x);
                    break;
                }
                case arrow::Type::UINT64: {
                    uint64_t u = static_cast<const arrow::UInt64Array&>(col).Value(row);
                    if (u > static_cast<uint64_t>(INT32_MAX))
                        return arrow::Status::Invalid("field ", def.name, ": uint64 too large for int32");
                    v = static_cast<int32_t>(u);
                    break;
                }
                default:
                    return arrow::Status::Invalid("field ", def.name, ": unsupported Arrow type for int32");
            }
            doc->put_int32(fid, v);
            return arrow::Status::OK();
        }
        case DataType::Int64: {
            int64_t v = 0;
            switch (col.type_id()) {
                case arrow::Type::INT64:
                    v = static_cast<const arrow::Int64Array&>(col).Value(row);
                    break;
                case arrow::Type::INT32:
                    v = static_cast<const arrow::Int32Array&>(col).Value(row);
                    break;
                case arrow::Type::UINT32:
                    v = static_cast<int64_t>(static_cast<const arrow::UInt32Array&>(col).Value(row));
                    break;
                case arrow::Type::UINT64: {
                    uint64_t u = static_cast<const arrow::UInt64Array&>(col).Value(row);
                    if (u > static_cast<uint64_t>(INT64_MAX))
                        return arrow::Status::Invalid("field ", def.name, ": uint64 too large for int64");
                    v = static_cast<int64_t>(u);
                    break;
                }
                default:
                    return arrow::Status::Invalid("field ", def.name, ": unsupported type for int64");
            }
            doc->put_int64(fid, v);
            return arrow::Status::OK();
        }
        case DataType::Float32: {
            if (col.type_id() != arrow::Type::FLOAT) {
                return arrow::Status::Invalid("field ", def.name, ": expected float");
            }
            const auto& a = static_cast<const arrow::FloatArray&>(col);
            doc->put_float(fid, a.Value(row));
            return arrow::Status::OK();
        }
        case DataType::Float64: {
            if (col.type_id() != arrow::Type::DOUBLE) {
                return arrow::Status::Invalid("field ", def.name, ": expected double");
            }
            const auto& a = static_cast<const arrow::DoubleArray&>(col);
            doc->put_double(fid, a.Value(row));
            return arrow::Status::OK();
        }
        case DataType::String:
        case DataType::Bytes: {
            if (col.type_id() == arrow::Type::STRING) {
                const auto& a = static_cast<const arrow::StringArray&>(col);
                auto v = a.GetView(row);
                doc->put_string(fid, v);
            } else if (col.type_id() == arrow::Type::BINARY) {
                const auto& a = static_cast<const arrow::BinaryArray&>(col);
                auto v = a.GetView(row);
                doc->put_string(fid, std::string_view(v.data(), static_cast<size_t>(v.size())));
            } else {
                return arrow::Status::Invalid("field ", def.name, ": expected string/binary");
            }
            return arrow::Status::OK();
        }
        default:
            return arrow::Status::Invalid("unsupported schema DataType for field ", def.name);
    }
}

template<typename ListArrayType>
arrow::Status ApplyListFixed(Doc* doc, const FieldDef& def, const ListArrayType& list_arr, int64_t row,
                             DataType elem_dt) {
    if (list_arr.IsNull(row)) return arrow::Status::OK();
    const int64_t off = list_arr.value_offset(row);
    const int64_t len = list_arr.value_length(row);
    if (len == 0) {
        switch (elem_dt) {
            case DataType::Int32:    doc->array_put_int32(def.field_id, nullptr, 0); break;
            case DataType::Int64:    doc->array_put_int64(def.field_id, nullptr, 0); break;
            case DataType::Float32:  doc->array_put_float(def.field_id, nullptr, 0); break;
            case DataType::Float64:  doc->array_put_double(def.field_id, nullptr, 0); break;
            case DataType::String:   doc->array_put_string(def.field_id, nullptr, 0); break;
            default: break;
        }
        return arrow::Status::OK();
    }
    auto values = std::static_pointer_cast<arrow::Array>(list_arr.values());
    switch (elem_dt) {
        case DataType::Int32: {
            const auto* a = static_cast<const arrow::Int32Array*>(values.get());
            doc->array_put_int32(def.field_id, a->raw_values() + off, static_cast<uint32_t>(len));
            return arrow::Status::OK();
        }
        case DataType::Int64: {
            const auto* a = static_cast<const arrow::Int64Array*>(values.get());
            doc->array_put_int64(def.field_id, a->raw_values() + off, static_cast<uint32_t>(len));
            return arrow::Status::OK();
        }
        case DataType::Float32: {
            const auto* a = static_cast<const arrow::FloatArray*>(values.get());
            doc->array_put_float(def.field_id, a->raw_values() + off, static_cast<uint32_t>(len));
            return arrow::Status::OK();
        }
        case DataType::Float64: {
            const auto* a = static_cast<const arrow::DoubleArray*>(values.get());
            doc->array_put_double(def.field_id, a->raw_values() + off, static_cast<uint32_t>(len));
            return arrow::Status::OK();
        }
        case DataType::String: {
            if (values->type_id() != arrow::Type::STRING) {
                return arrow::Status::Invalid("field ", def.name, ": list<string> expected STRING values");
            }
            const auto* a = static_cast<const arrow::StringArray*>(values.get());
            std::vector<std::string_view> parts;
            parts.reserve(static_cast<size_t>(len));
            for (int64_t i = 0; i < len; ++i) {
                auto v = a->GetView(off + i);
                parts.emplace_back(v.data(), static_cast<size_t>(v.size()));
            }
            doc->array_put_string(def.field_id, parts.data(), static_cast<uint32_t>(parts.size()));
            return arrow::Status::OK();
        }
        default:
            return arrow::Status::Invalid("unsupported list element type");
    }
}

arrow::Status ApplyField(Doc* doc, const FieldDef& def, const std::shared_ptr<arrow::Array>& col,
                         int64_t row) {
    if (!def.is_array) return ApplyScalar(doc, def, *col, row);

    switch (col->type_id()) {
        case arrow::Type::LIST:
            return ApplyListFixed(doc, def, static_cast<const arrow::ListArray&>(*col), row, def.type);
        case arrow::Type::LARGE_LIST:
            return ApplyListFixed(doc, def, static_cast<const arrow::LargeListArray&>(*col), row,
                                  def.type);
        default:
            return arrow::Status::Invalid("field ", def.name, ": expected list column for is_array");
    }
}

static constexpr uint64_t kProgressLogEveryRows = 10'000ULL;

struct ImportProgress {
    std::chrono::steady_clock::time_point t_start{};
    std::chrono::steady_clock::time_point t_last_log{};
    uint64_t                              rows_at_last_log = 0;
    uint64_t                              next_milestone   = kProgressLogEveryRows;
};

static void LogImportMilestone(uint64_t               total_imported,
                               const std::string&    path,
                               size_t                 file_index_1based,
                               size_t                 num_files,
                               ImportProgress*        prog) {
    const auto   now          = std::chrono::steady_clock::now();
    const double interval_sec = std::chrono::duration<double>(now - prog->t_last_log).count();
    const uint64_t d_rows     = total_imported - prog->rows_at_last_log;
    const double interval_tps = interval_sec > 0 ? static_cast<double>(d_rows) / interval_sec : 0.0;
    const double overall_sec  = std::chrono::duration<double>(now - prog->t_start).count();
    const double overall_tps  = overall_sec > 0 ? static_cast<double>(total_imported) / overall_sec : 0.0;
    const auto   iv_tps_ll    = static_cast<long long>(interval_tps + 0.5);
    const auto   ov_tps_ll    = static_cast<long long>(overall_tps + 0.5);

    std::cerr << WallTimestamp()
              << " import_progress total_rows=" << total_imported << " file_index=" << file_index_1based
              << "/" << num_files << " parquet_file=" << fs::path(path).filename().string()
              << " parquet_path=" << path << " interval_tps=" << iv_tps_ll << " overall_tps=" << ov_tps_ll
              << "\n";

    prog->t_last_log       = now;
    prog->rows_at_last_log = total_imported;
}

static void MaybeLogImportProgress(uint64_t total_imported, const std::string& path,
                                   size_t file_index_1based, size_t num_files, ImportProgress* prog) {
    if (total_imported < prog->next_milestone) return;
    LogImportMilestone(total_imported, path, file_index_1based, num_files, prog);
    prog->next_milestone = (total_imported / kProgressLogEveryRows + 1) * kProgressLogEveryRows;
}

arrow::Status ImportFile(KVIndex* idx, const Schema& sch, const std::string& path,
                         size_t file_index_1based, size_t num_files, uint64_t* rows_imported,
                         uint64_t* skipped_pk_null, ImportProgress* prog) {
    ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));
    ARROW_ASSIGN_OR_RAISE(auto rb_reader, reader->GetRecordBatchReader());

    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto batch, rb_reader->Next());
        if (!batch) break;
        for (const auto& fp : sch.fields()) {
            const FieldDef* def = fp.get();
            if (!batch->GetColumnByName(def->name)) {
                return arrow::Status::Invalid("parquet missing column: ", def->name);
            }
        }
        const int64_t n = batch->num_rows();
        const FieldDef* pk_def = sch.FindField(sch.pk());
        if (!pk_def) return arrow::Status::Invalid("schema has no pk");

        std::vector<Doc>    batch_docs;
        std::vector<Doc*>   batch_ptrs;
        batch_docs.reserve(static_cast<size_t>(n));
        batch_ptrs.reserve(static_cast<size_t>(n));

        for (int64_t r = 0; r < n; ++r) {
            auto pk_col = batch->GetColumnByName(pk_def->name);
            if (!pk_col || ArrowCellIsNull(*pk_col, r)) {
                ++(*skipped_pk_null);
                continue;
            }

            Doc doc = idx->NewDoc();
            for (const auto& fp : sch.fields()) {
                const FieldDef* def = fp.get();
                auto              col = batch->GetColumnByName(def->name);
                ARROW_RETURN_NOT_OK(ApplyField(&doc, *def, col, r));
            }

            std::string pk_str;
            switch (pk_def->type) {
                case DataType::Bool:
                    pk_str = std::to_string(doc.get_int32(pk_def->field_id));
                    break;
                case DataType::Int32:
                    pk_str = std::to_string(doc.get_int32(pk_def->field_id));
                    break;
                case DataType::Int64:
                    pk_str = std::to_string(doc.get_int64(pk_def->field_id));
                    break;
                case DataType::String:
                case DataType::Bytes: {
                    auto sv = doc.get_string(pk_def->field_id);
                    pk_str.assign(sv.data(), sv.size());
                    break;
                }
                default:
                    return arrow::Status::Invalid("unsupported pk type for import");
            }
            if (pk_str.empty()) {
                ++(*skipped_pk_null);
                continue;
            }

            batch_docs.push_back(std::move(doc));
            batch_ptrs.push_back(&batch_docs.back());
        }

        if (!batch_ptrs.empty()) {
            idx->BatchPut(batch_ptrs);
            *rows_imported += batch_ptrs.size();
            MaybeLogImportProgress(*rows_imported, path, file_index_1based, num_files, prog);
        }
    }
    return arrow::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
    Flags fl;
    if (!ParseFlags(argc, argv, &fl)) return 2;

    DBOptions opt;
    opt.db_path                           = fl.db_path;
    opt.exclusive_arena_lock            = !fl.no_arena_lock;
    opt.alloc_defaults.mode             = AllocatorMode::SingleWriter;
    opt.alloc_defaults.reclaim_delay_ns = 0;
    const uint64_t seg_b                = fl.arena_seg_gb * 1024ULL * 1024ULL * 1024ULL;
    opt.alloc_defaults.arena_size       = seg_b;
    opt.alloc_defaults.segment_size       = seg_b;
    opt.alloc_defaults.max_arena_size     = fl.arena_max_gb * 1024ULL * 1024ULL * 1024ULL;

    fs::path idx_dir = fs::path(fl.db_path) / fl.index_name;
    bool     exists  = fs::is_directory(idx_dir);

    if (fl.recreate && exists) {
        fs::remove_all(idx_dir);
        exists = false;
    }

    // Pre-size the HashMap to avoid rehashes during import (Plan B).
    // Reading Parquet metadata is fast (no data pages accessed).
    const uint64_t est_rows   = EstimateTotalRows(fl.input_files);
    const uint32_t bucket_bits = BucketBitsForRows(est_rows);
    std::cerr << WallTimestamp()
              << " import_init estimated_rows=" << est_rows
              << " bucket_bits=" << bucket_bits
              << " (bucket_count=" << (uint64_t{1} << bucket_bits) << ")\n";

    try {
        yikv::db::DB::Init(std::move(opt));
        if (!exists && fl.create_if_missing) {
            Schema sch;
            std::string err;
            if (!sch.LoadJson(ReadFile(fl.schema_json_path), &err)) {
                std::cerr << "schema: " << err << "\n";
                return 1;
            }
            DB::Instance().CreateKVIndex(fl.index_name, sch, bucket_bits);
        } else {
            DB::Instance().OpenIndex(fl.index_name);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }

    KVIndex*      idx = DB::Instance().GetKVIndex(fl.index_name);
    // Enable in-place bulk-insert mode (Plan E): no CoW per put, no retire_.
    // Safe because the import tool runs with the server stopped (no readers).
    idx->EnableBulkMode();
    const Schema* sch = idx->schema();
    const auto    t0  = std::chrono::steady_clock::now();
    uint64_t      total_rows        = 0;
    uint64_t      skipped_pk_null   = 0;
    uint64_t      files_ok          = 0;
    ImportProgress prog{};
    prog.t_start     = t0;
    prog.t_last_log  = t0;
    const size_t n_files = fl.input_files.size();

    for (size_t fi = 0; fi < n_files; ++fi) {
        const std::string& path = fl.input_files[fi];
        arrow::Status        st =
            ImportFile(idx, *sch, path, fi + 1, n_files, &total_rows, &skipped_pk_null, &prog);
        if (!st.ok()) {
            std::cerr << WallTimestamp() << " import " << path << ": " << st.ToString() << "\n";
            return 1;
        }
        ++files_ok;
    }

    const auto t1    = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration<double>(t1 - t0).count();

    std::cerr << WallTimestamp()
              << " {\"files\":" << files_ok << ",\"rows\":" << total_rows
              << ",\"skipped_pk_null\":" << skipped_pk_null << ",\"wall_sec\":" << sec
              << ",\"rows_per_sec\":" << (sec > 0 ? static_cast<double>(total_rows) / sec : 0.0)
              << "}\n";

    DB::Instance().CloseAll();
    return 0;
}
