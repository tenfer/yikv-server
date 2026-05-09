#!/usr/bin/env python3
"""Infer yikv schema from Parquet (OSS or local). Network Put was removed with gRPC; use C++ importer or brpc client later."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from oss2 import Auth, Bucket


def parse_oss_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("oss://"):
        raise ValueError("expected oss://bucket/prefix")
    rest = uri[6:]
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        raise ValueError("missing bucket")
    return bucket, prefix


def arrow_type_to_yikv(t: pa.DataType) -> Tuple[str, bool]:
    """Return (yikv data_type name, is_array)."""
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        vt = t.value_type
        if pa.types.is_int32(vt):
            return "int32", True
        if pa.types.is_int64(vt):
            return "int64", True
        if pa.types.is_float32(vt):
            return "float32", True
        if pa.types.is_float64(vt):
            return "float64", True
        raise TypeError(f"unsupported list value type: {vt}")
    if pa.types.is_boolean(t):
        return "bool", False
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t) or pa.types.is_uint8(t) or pa.types.is_uint16(t):
        return "int32", False
    if pa.types.is_int64(t) or pa.types.is_uint32(t):
        return "int64", False
    if pa.types.is_float32(t):
        return "float32", False
    if pa.types.is_float64(t):
        return "float64", False
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", False
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "bytes", False
    return "string", False


def build_schema_json(table: pa.Table, table_name: str, pk: str) -> dict:
    names = table.column_names
    if pk not in names:
        raise ValueError(f"pk column {pk!r} not in parquet columns {names}")
    fields = []
    for i, name in enumerate(names):
        col = table.column(i)
        field = table.schema.field(i)
        dt, is_arr = arrow_type_to_yikv(col.type)
        fields.append(
            {
                "name": name,
                "data_type": dt,
                "is_array": is_arr,
                "nullable": field.nullable,
                "is_pk": name == pk,
                "is_index": False,
                "field_id": i + 1,
            }
        )
    return {"table_name": table_name, "pk": pk, "fields": fields}


def list_parquet_keys(bucket: Bucket, prefix: str) -> List[str]:
    keys: List[str] = []
    marker = ""
    while True:
        r = bucket.list_objects(prefix=prefix, marker=marker, max_keys=500)
        for obj in r.object_list or []:
            if obj.key.endswith(".parquet"):
                keys.append(obj.key)
        if not r.is_truncated:
            break
        marker = r.next_marker
    keys.sort()
    return keys


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--oss_uri", help="oss://bucket/prefix (parquet objects)")
    ap.add_argument("--local_parquet", help="Local Parquet file (instead of OSS)")
    ap.add_argument("--pk", required=True, help="PK column; must match server schema")
    ap.add_argument("--table_name", default="imported")
    ap.add_argument("--emit_schema_json", help="Write inferred yikv schema JSON; use first file only")
    args = ap.parse_args()

    if bool(args.oss_uri) == bool(args.local_parquet):
        print("Exactly one of --oss_uri or --local_parquet is required", file=sys.stderr)
        sys.exit(2)

    if not args.emit_schema_json:
        print(
            "Import-with-Put over the network was removed with gRPC. "
            "Use --emit_schema_json to generate schema, then C++ yidiandb_import_parquet or a brpc client.",
            file=sys.stderr,
        )
        sys.exit(2)

    t0 = time.perf_counter()
    stats: Dict[str, Any] = {"bytes_downloaded": 0, "objects": []}

    def table_from_bytes(raw: bytes) -> pa.Table:
        stats["bytes_downloaded"] = stats.get("bytes_downloaded", 0) + len(raw)
        return pq.read_table(pa.BufferReader(raw))

    if args.local_parquet:
        raw = Path(args.local_parquet).read_bytes()
        table = table_from_bytes(raw)
        sch = build_schema_json(table, args.table_name, args.pk)
        Path(args.emit_schema_json).write_text(json.dumps(sch, indent=2), encoding="utf-8")
        print(json.dumps({"schema_written": args.emit_schema_json}, indent=2))
        return

    endpoint = os.environ.get("OSS_ENDPOINT", "").strip()
    key_id = os.environ.get("OSS_ACCESS_KEY_ID", "").strip()
    secret = os.environ.get("OSS_ACCESS_KEY_SECRET", "").strip()
    if not endpoint or not key_id or not secret:
        print("Set OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET", file=sys.stderr)
        sys.exit(2)
    bucket_name, prefix = parse_oss_uri(args.oss_uri)
    bucket = Bucket(Auth(key_id, secret), endpoint, bucket_name)
    keys = list_parquet_keys(bucket, prefix)
    stats["objects"] = keys
    if not keys:
        print("no .parquet under prefix", file=sys.stderr)
        sys.exit(1)
    raw = bucket.get_object(keys[0]).read()
    table = table_from_bytes(raw)
    sch = build_schema_json(table, args.table_name, args.pk)
    Path(args.emit_schema_json).write_text(json.dumps(sch, indent=2), encoding="utf-8")
    wall_ms = (time.perf_counter() - t0) * 1000.0
    print(json.dumps({"schema_written": args.emit_schema_json, "sample_key": keys[0], "wall_ms": wall_ms}, indent=2))


if __name__ == "__main__":
    main()
