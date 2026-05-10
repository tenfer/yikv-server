#!/usr/bin/env python3
"""HTTP agent for build machine + online machine.

Composite routes (for schedulers): POST /publishIndex (build + push), POST /deployIndex (pull +
switch-active + link server + reload). Granular: /buildIndex, /pushIndex, /pullIndex, /switchReloadIndex.

Deploy a dedicated process on the build host and one on the online host; paths (WORK, BUILD_DB, …) are
set via environment on each machine. Schedulers should call HTTP only — see tools/schedule_pipeline.py.

No authentication (use private network / VPC).

Environment (defaults align with tools/pipeline_reload.sh):
  YIKV_ROOT          repo root (default: parent of tools/)
  WORK               default /data/yikvdb
  BUILD_DB           default $WORK/build_db
  SERVER_DB          default $WORK/server_db
  ARTIFACT_STORE     default $WORK/artifact_store (when auto-creating artifact-storage.yaml)
  ARTIFACT_KEY_PREFIX  default yikv-index
  ARTIFACT_YAML      default $WORK/artifact-storage.yaml (created with provider: local if missing)
  ADMIN_SOCKET       default $WORK/admin.sock
  SERVER_CONFIG      default $WORK/config.server.json
  YIKV_IMPORT_BIN    default $YIKV_ROOT/bazel-bin/yikv_import_pipeline
  YIKV_SERVER_BIN    default $YIKV_ROOT/bazel-bin/yikv_server
  SCHEMA_JSON        default $YIKV_ROOT/schema.json (buildIndex fallback)
  IMPORT_LISTEN      default 127.0.0.1:59999 (import pipeline config listen)
  SERVER_LISTEN      default 0.0.0.0:9000 (written into SERVER_CONFIG)
  AUTO_START_SERVER  default 1 — deployIndex / switchReloadIndex may start yikv_server if admin socket missing
  HOST               default 0.0.0.0
  PORT               default 8787
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

# --- paths & env ---

_SCRIPT_DIR = Path(__file__).resolve().parent
_YIKV_ROOT_ENV = os.environ.get("YIKV_ROOT")
YIKV_ROOT = Path(_YIKV_ROOT_ENV if _YIKV_ROOT_ENV else _SCRIPT_DIR.parent.parent).resolve()

WORK = Path(os.environ.get("WORK", "/data/yikvdb")).resolve()
BUILD_DB = Path(os.environ.get("BUILD_DB", str(WORK / "build_db"))).resolve()
SERVER_DB = Path(os.environ.get("SERVER_DB", str(WORK / "server_db"))).resolve()
ARTIFACT_STORE = Path(os.environ.get("ARTIFACT_STORE", str(WORK / "artifact_store"))).resolve()
ARTIFACT_KEY_PREFIX = os.environ.get("ARTIFACT_KEY_PREFIX", "yikv-index")
ARTIFACT_YAML = Path(os.environ.get("ARTIFACT_YAML", str(WORK / "artifact-storage.yaml"))).resolve()
ADMIN_SOCKET = Path(os.environ.get("ADMIN_SOCKET", str(WORK / "admin.sock"))).resolve()
SERVER_CONFIG = Path(os.environ.get("SERVER_CONFIG", str(WORK / "config.server.json"))).resolve()
IMPORT_BIN = Path(os.environ.get("YIKV_IMPORT_BIN", str(YIKV_ROOT / "bazel-bin/yikv_import_pipeline")))
YIKV_SERVER_BIN = Path(os.environ.get("YIKV_SERVER_BIN", str(YIKV_ROOT / "bazel-bin/yikv_server")))
SCHEMA_JSON_DEFAULT = Path(os.environ.get("SCHEMA_JSON", str(YIKV_ROOT / "schema.json"))).resolve()
IMPORT_LISTEN = os.environ.get("IMPORT_LISTEN", "127.0.0.1:59999")
SERVER_LISTEN = os.environ.get("SERVER_LISTEN", "0.0.0.0:9000")
AUTO_START_SERVER = os.environ.get("AUTO_START_SERVER", "1") == "1"

ARTIFACT_PY = YIKV_ROOT / "tools" / "artifact_sync" / "artifact_sync.py"

_import_lock = threading.Lock()
app = FastAPI(title="yikv pipeline agent", version="1.0.0")


def _release_root(table: str) -> Path:
    return (WORK / "releases" / table).resolve()


def ensure_artifact_yaml_local() -> None:
    """If ARTIFACT_YAML is missing, write a minimal provider: local config (same idea as pipeline_reload.sh)."""
    if ARTIFACT_YAML.is_file():
        return
    WORK.mkdir(parents=True, exist_ok=True)
    ARTIFACT_STORE.mkdir(parents=True, exist_ok=True)
    text = (
        "provider: local\n"
        "env: dev\n"
        f"key_prefix: {ARTIFACT_KEY_PREFIX}\n"
        "local:\n"
        f"  root: {ARTIFACT_STORE}\n"
    )
    ARTIFACT_YAML.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_YAML.write_text(text, encoding="utf-8")


def _write_import_config(table: str) -> Path:
    agent_dir = WORK / ".pipeline_agent"
    agent_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = agent_dir / f"import_{table}.json"
    body: dict[str, Any] = {
        "db_path": str(BUILD_DB),
        "listen": IMPORT_LISTEN,
        "arena_seg_gb": 1,
        "arena_max_gb": 4,
        "exclusive_arena_lock": False,
        "admin_unix_socket": str(ADMIN_SOCKET),
    }

    cfg_path.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
    return cfg_path


def _run_artifact_sync(args: list[str], *, need_config: bool) -> subprocess.CompletedProcess[str]:
    cmd = [os.environ.get("PYTHON", "python3"), str(ARTIFACT_PY)]
    if need_config:
        cmd.extend(["-c", str(ARTIFACT_YAML)])
    cmd.extend(args)
    return subprocess.run(
        cmd,
        cwd=str(YIKV_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


def _ensure_executable(bin_path: Path, hint: str) -> None:
    if not bin_path.is_file():
        raise HTTPException(status_code=500, detail=f"missing binary {bin_path} ({hint})")
    if not os.access(bin_path, os.X_OK):
        raise HTTPException(status_code=500, detail=f"not executable: {bin_path} ({hint})")


def ensure_yikv_server_for_reload() -> None:
    if ADMIN_SOCKET.is_socket():
        return
    if not AUTO_START_SERVER:
        raise HTTPException(
            status_code=503,
            detail=f"admin socket not available: {ADMIN_SOCKET} (set AUTO_START_SERVER=1 to auto-start)",
        )
    if not SERVER_CONFIG.is_file():
        raise HTTPException(status_code=503, detail=f"missing server config {SERVER_CONFIG}; run switchReloadIndex or create it")
    _ensure_executable(YIKV_SERVER_BIN, "bazel build //:yikv_server")
    WORK.mkdir(parents=True, exist_ok=True)
    log = WORK / "yikv_server.log"
    pidfile = WORK / "yikv_server.pid"
    with open(log, "ab", buffering=0) as lf:
        p = subprocess.Popen(
            [str(YIKV_SERVER_BIN), str(SERVER_CONFIG)],
            cwd=str(YIKV_ROOT),
            stdout=lf,
            stderr=subprocess.STDOUT,
        )
    pidfile.write_text(str(p.pid), encoding="utf-8")
    for _ in range(150):
        if ADMIN_SOCKET.is_socket():
            return
        time.sleep(0.2)
    raise HTTPException(status_code=504, detail=f"yikv_server did not open {ADMIN_SOCKET} in 30s (log {log})")


def send_reload(table: str) -> str:
    ensure_yikv_server_for_reload()
    if not ADMIN_SOCKET.is_socket():
        raise HTTPException(status_code=503, detail=f"admin socket missing: {ADMIN_SOCKET}")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(str(ADMIN_SOCKET))
        sock.sendall(f"reload {table}\n".encode())
        data = sock.recv(4096)
    finally:
        sock.close()
    return data.decode(errors="replace")


def link_server_table(table: str) -> None:
    release_root = _release_root(table)
    SERVER_DB.mkdir(parents=True, exist_ok=True)
    tlink = SERVER_DB / table
    if tlink.exists() or tlink.is_symlink():
        tlink.unlink()
    tlink.symlink_to(release_root / "active", target_is_directory=True)
    body: dict[str, Any] = {
        "db_path": str(SERVER_DB),
        "listen": SERVER_LISTEN,
        "arena_seg_gb": 1,
        "arena_max_gb": 512,
        "exclusive_arena_lock": True,
        "admin_unix_socket": str(ADMIN_SOCKET),
    }
    SERVER_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    SERVER_CONFIG.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")


# --- request models ---


class BuildIndexBody(BaseModel):
    table: str = Field(..., min_length=1)
    input: str | None = Field(None, description="Single .parquet/.csv file on this host")
    data_dir: str | None = Field(None, description="Directory scanned recursively for data files (--input_dir)")
    schema_json: str | None = Field(None, description="Defaults to SCHEMA_JSON env / repo schema.json")
    create_if_missing: bool = False
    recreate: bool = False

    @model_validator(mode="after")
    def exactly_one_input_source(self) -> BuildIndexBody:
        has_in = self.input is not None and str(self.input).strip() != ""
        has_dir = self.data_dir is not None and str(self.data_dir).strip() != ""
        if has_in == has_dir:
            raise ValueError("Exactly one of input or data_dir must be set")
        return self


class PublishIndexBody(BaseModel):
    """Build + push; use on build host."""

    table: str = Field(..., min_length=1)
    input: str | None = None
    data_dir: str | None = None
    schema_json: str | None = None
    recreate: bool = False
    build_id: str | None = Field(None, description="Optional explicit build_id for push; else auto")

    @model_validator(mode="after")
    def exactly_one_input_source(self) -> PublishIndexBody:
        has_in = self.input is not None and str(self.input).strip() != ""
        has_dir = self.data_dir is not None and str(self.data_dir).strip() != ""
        if has_in == has_dir:
            raise ValueError("Exactly one of input or data_dir must be set")
        return self


class DeployIndexBody(BaseModel):
    """Pull (switch active) + symlink server table + reload; use on online host."""

    table: str = Field(..., min_length=1)
    build_id: str | None = None
    force_refresh: bool = False
    max_local_versions: int = Field(2, ge=0)


class PushIndexBody(BaseModel):
    table: str = Field(..., min_length=1)
    build_id: str | None = Field(None, description="omit for auto timestamp id")


class PullIndexBody(BaseModel):
    table: str = Field(..., min_length=1)
    build_id: str | None = None
    force_refresh: bool = False
    max_local_versions: int = Field(2, ge=0)
    switch_active: bool = False


class SwitchReloadBody(BaseModel):
    table: str = Field(..., min_length=1)
    build_id: str | None = Field(None, description="Version dir under releases/<table>/; omit = lexicographic max local")


def _build_index_impl(body: BuildIndexBody) -> dict[str, Any]:
    schema = Path(body.schema_json or SCHEMA_JSON_DEFAULT).resolve()
    if not schema.is_file():
        raise HTTPException(status_code=400, detail=f"schema_json not found: {schema}")

    _ensure_executable(IMPORT_BIN, "bazel build //:yikv_import_pipeline")
    WORK.mkdir(parents=True, exist_ok=True)
    BUILD_DB.mkdir(parents=True, exist_ok=True)

    index_dir = BUILD_DB / body.table
    cfg_path = _write_import_config(body.table)
    cmd: list[str] = [
        str(IMPORT_BIN),
        "--config",
        str(cfg_path),
        "--index",
        body.table,
        "--schema_json",
        str(schema),
    ]
    extra: list[str] = []

    if body.data_dir is not None:
        dpath = Path(body.data_dir).resolve()
        if not dpath.is_dir():
            raise HTTPException(status_code=400, detail=f"data_dir not a directory: {dpath}")
        cmd.extend(["--input_dir", str(dpath)])
        if body.recreate:
            extra.append("--recreate")
        elif not index_dir.is_dir():
            extra.append("--create_if_missing")
    else:
        inp = Path(body.input or "").resolve()
        if not inp.is_file():
            raise HTTPException(status_code=400, detail=f"input not found: {inp}")
        cmd.extend(["--input", str(inp)])
        if body.recreate:
            extra.append("--recreate")
        if body.create_if_missing:
            extra.append("--create_if_missing")

    cmd.extend(extra)
    r = subprocess.run(cmd, cwd=str(YIKV_ROOT), capture_output=True, text=True, check=False)
    if r.returncode != 0:
        msg = (r.stderr or "") + (r.stdout or "")
        raise HTTPException(status_code=500, detail=msg.strip() or "yikv_import_pipeline failed")

    return {
        "ok": True,
        "table": body.table,
        "db_dir": str(BUILD_DB / body.table),
        "log": (r.stderr + r.stdout).strip() or None,
    }


def _push_index_impl(body: PushIndexBody) -> dict[str, Any]:
    ensure_artifact_yaml_local()
    src = (BUILD_DB / body.table).resolve()
    if not src.is_dir():
        raise HTTPException(status_code=400, detail=f"index directory missing (run buildIndex first): {src}")
    args = ["push", "--table", body.table, "--source", str(src), "--emit-build-id"]
    if body.build_id:
        args.extend(["--build-id", body.build_id])
    r = _run_artifact_sync(args, need_config=True)
    if r.returncode != 0:
        raise HTTPException(status_code=500, detail=(r.stderr + r.stdout).strip() or "push failed")
    build_id = (r.stdout or "").strip().splitlines()[-1].strip() if r.stdout else ""
    if not build_id:
        raise HTTPException(status_code=500, detail="push produced no build_id on stdout")
    return {"ok": True, "build_id": build_id}


def _pull_stdout_build_id(proc: subprocess.CompletedProcess[str]) -> str:
    build_id = ""
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if line:
            build_id = line
    return build_id


# --- routes ---


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/publishIndex")
def publish_index(body: PublishIndexBody) -> dict[str, Any]:
    bb = BuildIndexBody(
        table=body.table,
        input=body.input,
        data_dir=body.data_dir,
        schema_json=body.schema_json,
        recreate=body.recreate,
        create_if_missing=False,
    )
    with _import_lock:
        build_out = _build_index_impl(bb)
        push_out = _push_index_impl(PushIndexBody(table=body.table, build_id=body.build_id))
    return {
        "ok": True,
        "build_id": push_out["build_id"],
        "table": body.table,
        "db_dir": build_out.get("db_dir"),
        "build_log": build_out.get("log"),
    }


@app.post("/deployIndex")
def deploy_index(body: DeployIndexBody) -> dict[str, Any]:
    ensure_artifact_yaml_local()
    dest = _release_root(body.table)
    dest.mkdir(parents=True, exist_ok=True)
    args: list[str] = [
        "pull",
        "--table",
        body.table,
        "--dest",
        str(dest),
        "--max-local-versions",
        str(body.max_local_versions),
        "--switch-active",
    ]
    if body.build_id:
        args.extend(["--build-id", body.build_id])
    if body.force_refresh:
        args.append("--force-refresh")
    r = _run_artifact_sync(args, need_config=True)
    if r.returncode != 0:
        raise HTTPException(status_code=500, detail=(r.stderr + r.stdout).strip() or "pull failed")
    build_id = _pull_stdout_build_id(r)
    if not build_id:
        raise HTTPException(status_code=500, detail="pull produced no build_id")

    link_server_table(body.table)
    reload_reply = send_reload(body.table)
    return {
        "ok": True,
        "build_id": build_id,
        "dest": str(dest),
        "server_config": str(SERVER_CONFIG),
        "reload_reply": reload_reply.strip() or None,
        "pull_stderr": r.stderr.strip() or None,
    }


@app.post("/buildIndex")
def build_index(body: BuildIndexBody) -> dict[str, Any]:
    with _import_lock:
        return _build_index_impl(body)


@app.post("/pushIndex")
def push_index(body: PushIndexBody) -> dict[str, Any]:
    return _push_index_impl(body)


@app.post("/pullIndex")
def pull_index(body: PullIndexBody) -> dict[str, Any]:
    ensure_artifact_yaml_local()
    dest = _release_root(body.table)
    dest.mkdir(parents=True, exist_ok=True)
    args: list[str] = [
        "pull",
        "--table",
        body.table,
        "--dest",
        str(dest),
        "--max-local-versions",
        str(body.max_local_versions),
    ]
    if body.build_id:
        args.extend(["--build-id", body.build_id])
    if body.force_refresh:
        args.append("--force-refresh")
    if body.switch_active:
        args.append("--switch-active")
    r = _run_artifact_sync(args, need_config=True)
    if r.returncode != 0:
        raise HTTPException(status_code=500, detail=(r.stderr + r.stdout).strip() or "pull failed")
    build_id = _pull_stdout_build_id(r)
    if not build_id:
        raise HTTPException(status_code=500, detail="pull produced no build_id")
    return {
        "ok": True,
        "build_id": build_id,
        "dest": str(dest),
        "stderr": r.stderr.strip() or None,
    }


@app.post("/switchReloadIndex")
def switch_reload_index(body: SwitchReloadBody) -> dict[str, Any]:
    dest = _release_root(body.table)
    if not dest.is_dir():
        raise HTTPException(status_code=400, detail=f"release dest missing (run pullIndex first): {dest}")
    args: list[str] = ["switch", "--dest", str(dest)]
    if body.build_id:
        args.extend(["--build-id", body.build_id])
    r = _run_artifact_sync(args, need_config=False)
    if r.returncode != 0:
        raise HTTPException(status_code=500, detail=(r.stderr + r.stdout).strip() or "switch failed")
    build_id_lines = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
    build_id = build_id_lines[-1] if build_id_lines else ""

    link_server_table(body.table)
    reload_reply = send_reload(body.table)
    return {
        "ok": True,
        "build_id": build_id or None,
        "server_config": str(SERVER_CONFIG),
        "reload_reply": reload_reply.strip() or None,
    }


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8787"))
    import uvicorn

    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
