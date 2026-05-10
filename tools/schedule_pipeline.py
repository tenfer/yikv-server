#!/usr/bin/env python3
"""Thin scheduler: HTTP only — POST /publishIndex on the build agent, then /deployIndex on the online agent.

No WORK / artifact paths here; configure each host via pipeline_agent environment.

Environment:
  BUILD_AGENT_URL   default http://127.0.0.1:8787
  ONLINE_AGENT_URL  default same as BUILD_AGENT_URL
  TABLE             default dsp_test5 (overridable by --table)

Examples:
  ./tools/schedule_pipeline.py --data-dir /data/raw/my_table
  ./tools/schedule_pipeline.py --input /data/raw/file.parquet --table t1
  BUILD_AGENT_URL=http://build:8787 ONLINE_AGENT_URL=http://online:8789 \\
    ./tools/schedule_pipeline.py --data-dir /data/raw --table t1

  ./tools/schedule_pipeline.py --publish-only --data-dir /path
  ./tools/schedule_pipeline.py --deploy-only --table t1
  ./tools/schedule_pipeline.py --deploy-only --table t1 --deploy-build-id 20260101120000
  ./tools/schedule_pipeline.py --reload-only --table t1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any


def _post_json(base_url: str, path: str, body: dict[str, Any], *, timeout: float = 86400.0) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            out = resp.read().decode("utf-8")
            if not out.strip():
                return {}
            return json.loads(out)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(raw)
        except json.JSONDecodeError:
            detail = raw
        raise SystemExit(f"HTTP {e.code} {url}: {detail}") from None


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    default_build = os.environ.get("BUILD_AGENT_URL", "http://127.0.0.1:8787").strip()
    default_online = os.environ.get("ONLINE_AGENT_URL", default_build).strip()

    ap = argparse.ArgumentParser(description="publishIndex → deployIndex (pipeline_agent)")
    ap.add_argument("--build-url", default=default_build, help="Build host pipeline_agent base URL")
    ap.add_argument("--online-url", default=default_online, help="Online host pipeline_agent base URL")
    ap.add_argument("--table", default=os.environ.get("TABLE", "dsp_test5"))
    ap.add_argument("--data-dir", type=str, default=None, help="Raw data directory (on build host)")
    ap.add_argument("--input", type=str, default=None, help="Single data file (on build host)")
    ap.add_argument("--schema-json", type=str, default=None, help="Forwarded to publishIndex")
    ap.add_argument("--recreate", action="store_true")
    ap.add_argument(
        "--deploy-build-id",
        type=str,
        default=None,
        help="build_id for deployIndex (default: from publish response when running both steps)",
    )
    ap.add_argument("--max-local-versions", type=int, default=2)
    ap.add_argument("--force-refresh", action="store_true", help="Forward to deployIndex")
    ap.add_argument("--publish-only", action="store_true")
    ap.add_argument("--deploy-only", action="store_true")
    ap.add_argument(
        "--reload-only",
        action="store_true",
        help="Only POST /switchReloadIndex on online agent (no pull; release layout must exist)",
    )
    args = ap.parse_args(argv)

    if args.reload_only:
        if args.publish_only or args.deploy_only:
            ap.error("--reload-only conflicts with --publish-only / --deploy-only")
        body: dict[str, Any] = {"table": args.table}
        if args.deploy_build_id:
            body["build_id"] = args.deploy_build_id
        r = _post_json(args.online_url, "/switchReloadIndex", body)
        print(json.dumps(r, indent=2))
        return

    has_in = args.input is not None and args.input.strip() != ""
    has_dir = args.data_dir is not None and args.data_dir.strip() != ""
    if has_in == has_dir and not args.deploy_only:
        ap.error("provide exactly one of --data-dir or --input (unless --deploy-only)")

    if args.publish_only and args.deploy_only:
        ap.error("use only one of --publish-only and --deploy-only, or neither for full run")

    build_id_for_deploy: str | None = args.deploy_build_id

    if not args.deploy_only:
        pub: dict[str, Any] = {
            "table": args.table,
            "recreate": args.recreate,
        }
        if args.schema_json:
            pub["schema_json"] = args.schema_json
        if has_dir:
            pub["data_dir"] = args.data_dir
        else:
            pub["input"] = args.input
        pr = _post_json(args.build_url, "/publishIndex", pub)
        print(json.dumps(pr, indent=2))
        if args.publish_only:
            return
        if build_id_for_deploy is None and isinstance(pr.get("build_id"), str):
            build_id_for_deploy = pr["build_id"]

    dep: dict[str, Any] = {
        "table": args.table,
        "force_refresh": args.force_refresh,
        "max_local_versions": args.max_local_versions,
    }
    if build_id_for_deploy:
        dep["build_id"] = build_id_for_deploy

    dr = _post_json(args.online_url, "/deployIndex", dep)
    print(json.dumps(dr, indent=2))


if __name__ == "__main__":
    main()
