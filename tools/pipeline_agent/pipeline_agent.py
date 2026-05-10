#!/usr/bin/env python3
"""HTTP agent for build machine + online machine.

Composite routes (for schedulers): POST /publishIndex (build + push), POST /deployIndex (pull +
switch-active + link server + reload). Granular: /buildIndex, /pushIndex, /pullIndex, /switchReloadIndex.

Deploy a dedicated process on the build host and one on the online host; paths (WORK, BUILD_DB, …) are
set via environment on each machine. Schedulers should call HTTP only — see tools/schedule_pipeline.py.

No authentication (use private network / VPC).

  PIPELINE_CONFIG   optional JSON path: merged on top of tools/pipeline_agent/pipeline.defaults.json (if that file exists)

Environment (override JSON — same names as export from pipeline_config.py --emit-shell):
  YIKV_ROOT          repo root (default: parent of tools/)
  WORK               default /data/yikvdb
  BUILD_DB           default $WORK/build_db
  SERVER_DB          default $WORK/server_db
  ARTIFACT_STORE     default $WORK/artifact_store (when auto-creating artifact-storage.yaml)
  ARTIFACT_KEY_PREFIX  default yikv-index
  ARTIFACT_ENV       default dev (artifact-storage.yaml)
  ARTIFACT_YAML      default $WORK/artifact-storage.yaml (created with provider: local if missing)
  ADMIN_SOCKET       default $WORK/admin.sock (should match parent(db_path)/admin.sock when db_path is $WORK/server_db)
  SERVER_CONFIG      default $WORK/config.server.json — must exist (manual); see config.example.json
  YIKV_IMPORT_BIN    default $YIKV_ROOT/bazel-bin/yikv_import_pipeline
  YIKV_SERVER_BIN    default $YIKV_ROOT/bazel-bin/yikv_server
  SCHEMA_JSON        default $YIKV_ROOT/schema.json (buildIndex fallback)
  IMPORT_LISTEN      default 127.0.0.1:59999 (import pipeline config listen)
  SERVER_LISTEN      not written by agent; set listen in SERVER_CONFIG to match your RPC bind address
  AUTO_START_SERVER  default 1 — deployIndex / switchReloadIndex may start yikv_server if admin socket missing
                        (stale socket files left after a crash are removed after a failed connect probe)
  PIPELINE_ARENA_MAX_GB  import + server JSON (default 4; must match offline build or OpenIndex fails)
  PIPELINE_ARENA_SEG_GB  segment size in GB (default 1)
  IMPORT_IO_WORKERS     yikv_import_pipeline parallel readers (default 1; lower RAM)
  IMPORT_QUEUE_BATCHES  max RecordBatches in flight (default 8; lower RAM)
  HOST               default 0.0.0.0
  PORT               default 8787
  ADMIN_SOCKET_WAIT_SEC  max seconds to wait for yikv_server admin socket after auto-start (default 90)
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from pipeline_config import load_pipeline_settings

_S = load_pipeline_settings()

YIKV_ROOT = _S.yikv_root
WORK = _S.work
BUILD_DB = _S.build_db
SERVER_DB = _S.server_db
ARTIFACT_STORE = _S.artifact_store
ARTIFACT_KEY_PREFIX = _S.artifact_key_prefix
ARTIFACT_ENV = _S.artifact_env
ARTIFACT_YAML = _S.artifact_yaml
ADMIN_SOCKET = _S.admin_socket
SERVER_CONFIG = _S.server_config
IMPORT_BIN = _S.import_bin
YIKV_SERVER_BIN = _S.server_bin
SCHEMA_JSON_DEFAULT = _S.schema_json
IMPORT_LISTEN = _S.import_listen
SERVER_LISTEN = _S.server_listen
AUTO_START_SERVER = _S.auto_start_server
PIPELINE_ARENA_SEG_GB = _S.arena_seg_gb
PIPELINE_ARENA_MAX_GB = _S.arena_max_gb
IMPORT_IO_WORKERS = _S.import_io_workers
IMPORT_QUEUE_BATCHES = _S.import_queue_batches

ARTIFACT_PY = YIKV_ROOT / "tools" / "artifact_sync" / "artifact_sync.py"


_import_lock = threading.Lock()
app = FastAPI(title="yikv pipeline agent", version="1.0.0")


def _release_root(table: str) -> Path:
    return (WORK / "releases" / table).resolve()


def ensure_artifact_yaml_local() -> None:
    """If ARTIFACT_YAML is missing, write a minimal provider: local config (same layout as artifact-storage.example.yaml)."""
    if ARTIFACT_YAML.is_file():
        return
    WORK.mkdir(parents=True, exist_ok=True)
    ARTIFACT_STORE.mkdir(parents=True, exist_ok=True)
    text = (
        "provider: local\n"
        f"env: {ARTIFACT_ENV}\n"
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
        "arena_seg_gb": PIPELINE_ARENA_SEG_GB,
        "arena_max_gb": PIPELINE_ARENA_MAX_GB,
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


def _admin_wait_max_sec() -> float:
    raw = os.environ.get("ADMIN_SOCKET_WAIT_SEC", "").strip()
    if raw:
        return max(5.0, float(raw))
    return 90.0


def _tail_file(path: Path, *, max_lines: int = 120, max_chars: int = 32000) -> str:
    if not path.is_file():
        return "(no log file yet)"
    try:
        data = path.read_bytes()
    except OSError as exc:
        return f"(cannot read log: {exc})"
    if len(data) > max_chars:
        data = data[-max_chars:]
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    return "\n".join(lines)


def _default_admin_unix_for_db_path(db_path_str: str) -> Path:
    """Match yikv_server LoadServerConfig when admin_unix_socket is omitted: parent(db_path)/admin.sock."""
    dbp = Path(str(db_path_str).strip()).expanduser()
    return (dbp.parent / "admin.sock").resolve()


def _require_server_config_admin_socket() -> None:
    """Ensure effective admin path matches ADMIN_SOCKET (implicit default or explicit JSON)."""
    try:
        cfg = json.loads(SERVER_CONFIG.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail=f"missing server config {SERVER_CONFIG}") from None
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=503, detail=f"invalid JSON in {SERVER_CONFIG}: {exc}") from exc
    db_path = cfg.get("db_path")
    if not db_path or not str(db_path).strip():
        raise HTTPException(status_code=503, detail=f"{SERVER_CONFIG} missing db_path")
    raw = cfg.get("admin_unix_socket")
    if raw is None or not str(raw).strip():
        effective = _default_admin_unix_for_db_path(str(db_path))
    else:
        effective = Path(str(raw).strip()).expanduser().resolve()
    want = ADMIN_SOCKET.expanduser().resolve()
    if effective != want:
        raise HTTPException(
            status_code=503,
            detail=(
                f"admin_unix_socket effective path {effective} != agent ADMIN_SOCKET {want}. "
                f"Set admin_unix_socket in {SERVER_CONFIG} or align WORK/SERVER_DB/ADMIN_SOCKET "
                f"(default rule: parent(db_path)/admin.sock)."
            ),
        )


def _admin_unix_reachable(*, timeout_sec: float = 2.0) -> bool:
    """True if yikv_server is accepting connections on ADMIN_SOCKET (not just a leftover inode)."""
    if not ADMIN_SOCKET.is_socket():
        return False
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.settimeout(timeout_sec)
            s.connect(str(ADMIN_SOCKET))
        finally:
            s.close()
        return True
    except OSError:
        return False


def _wait_admin_unix_ready(
    *,
    proc: subprocess.Popen[Any] | None,
    max_wait_sec: float,
    interval_sec: float = 0.2,
) -> None:
    """Poll until the admin socket accepts connections (not only Path.is_socket())."""
    log = WORK / "yikv_server.log"
    deadline = time.monotonic() + max_wait_sec
    while time.monotonic() < deadline:
        if proc is not None:
            rc = proc.poll()
            if rc is not None:
                tail = _tail_file(log)
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"yikv_server exited before admin socket was ready (exit code {rc}). "
                        f"Binary {YIKV_SERVER_BIN}. Config {SERVER_CONFIG}.\n"
                        f"--- tail of {log} ---\n{tail}"
                    ),
                )
        if _admin_unix_reachable(timeout_sec=0.5):
            return
        time.sleep(interval_sec)
    tail = _tail_file(log)
    raise HTTPException(
        status_code=504,
        detail=(
            f"yikv_server did not accept connections on {ADMIN_SOCKET} within {max_wait_sec:.0f}s "
            f"(ADMIN_SOCKET_WAIT_SEC). "
            f"Confirm {SERVER_CONFIG} has admin_unix_socket matching this path and that "
            f"{YIKV_SERVER_BIN} is rebuilt (admin listens before ScanAndLoad).\n"
            f"--- tail of {log} ---\n{tail}"
        ),
    )


def ensure_yikv_server_for_reload() -> None:
    if _admin_unix_reachable():
        return
    if ADMIN_SOCKET.is_socket():
        try:
            ADMIN_SOCKET.unlink()
        except OSError:
            pass
    if not AUTO_START_SERVER:
        raise HTTPException(
            status_code=503,
            detail=f"admin socket not available: {ADMIN_SOCKET} (set AUTO_START_SERVER=1 to auto-start)",
        )
    if not SERVER_CONFIG.is_file():
        raise HTTPException(status_code=503, detail=f"missing server config {SERVER_CONFIG}; create it manually (see config.example.json)")
    _require_server_config_admin_socket()
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
    _wait_admin_unix_ready(proc=p, max_wait_sec=_admin_wait_max_sec(), interval_sec=0.2)


def send_reload(table: str) -> str:
    ensure_yikv_server_for_reload()

    def _exchange() -> str:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(30.0)
            sock.connect(str(ADMIN_SOCKET))
            sock.sendall(f"reload {table}\n".encode())
            data = sock.recv(4096)
        finally:
            sock.close()
        return data.decode(errors="replace")

    try:
        reply = _exchange()
    except ConnectionRefusedError:
        if ADMIN_SOCKET.is_socket():
            try:
                ADMIN_SOCKET.unlink()
            except OSError:
                pass
        ensure_yikv_server_for_reload()
        reply = _exchange()
    rstrip = reply.strip()
    if not rstrip.startswith("ok"):
        raise HTTPException(
            status_code=500,
            detail=(
                f"admin reload {table!r} on {ADMIN_SOCKET} did not succeed: {rstrip!r}. "
                f"Confirm the yikv_server process uses the same admin_unix_socket as this agent "
                f"(see SERVER_CONFIG / config parent(db_path)/admin.sock)."
            ),
        )
    return reply


def link_server_table(table: str) -> None:
    """Symlink SERVER_DB/<table> -> releases/<table>/active. Does not write SERVER_CONFIG (operator creates it)."""
    if not SERVER_CONFIG.is_file():
        ex = YIKV_ROOT / "config.example.json"
        raise HTTPException(
            status_code=503,
            detail=(
                f"SERVER_CONFIG missing: {SERVER_CONFIG}. Create it manually (e.g. copy {ex}); "
                f"db_path must be {SERVER_DB.resolve()} for this pipeline layout."
            ),
        )
    try:
        cfg = json.loads(SERVER_CONFIG.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=503, detail=f"invalid JSON in {SERVER_CONFIG}: {exc}") from exc
    raw_db = cfg.get("db_path")
    if not raw_db or not str(raw_db).strip():
        raise HTTPException(status_code=503, detail=f"{SERVER_CONFIG} must set db_path to {SERVER_DB.resolve()}")
    cfg_db = Path(str(raw_db).strip()).expanduser().resolve()
    if cfg_db != SERVER_DB.resolve():
        raise HTTPException(
            status_code=503,
            detail=(
                f"{SERVER_CONFIG} db_path is {cfg_db}, expected {SERVER_DB.resolve()} "
                f"(SERVER_DB env / pipeline layout)"
            ),
        )

    release_root = _release_root(table)
    SERVER_DB.mkdir(parents=True, exist_ok=True)
    tlink = SERVER_DB / table
    if tlink.exists() or tlink.is_symlink():
        tlink.unlink()
    tlink.symlink_to(release_root / "active", target_is_directory=True)


# --- request models ---


class BuildIndexBody(BaseModel):
    table: str = Field(..., min_length=1)
    input: str | None = Field(None, description="Single .parquet/.csv file on this host")
    data_dir: str | None = Field(None, description="Directory scanned recursively for data files (--input_dir)")
    schema_json: str | None = Field(None, description="Defaults to SCHEMA_JSON env / repo schema.json")
    create_if_missing: bool = Field(
        False,
        description="Ignored; yikv_import_pipeline always runs with --create_if_missing after removing BUILD_DB/<table>.",
    )
    recreate: bool = Field(
        False,
        description="Ignored for import; every build always deletes BUILD_DB/<table> on this host then creates a new index directory.",
    )

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
    recreate: bool = Field(
        False,
        description="Ignored for import; every publish build deletes BUILD_DB/<table> on this host then creates a new index directory.",
    )
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


def _remove_local_build_index(path: Path) -> None:
    """Remove BUILD_DB/<table> so the next import always creates a fresh on-disk index."""
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        shutil.rmtree(path)


def _build_index_impl(body: BuildIndexBody) -> dict[str, Any]:
    schema = Path(body.schema_json or SCHEMA_JSON_DEFAULT).resolve()
    if not schema.is_file():
        raise HTTPException(status_code=400, detail=f"schema_json not found: {schema}")

    _ensure_executable(IMPORT_BIN, "bazel build //:yikv_import_pipeline")
    WORK.mkdir(parents=True, exist_ok=True)
    BUILD_DB.mkdir(parents=True, exist_ok=True)

    index_dir = BUILD_DB / body.table
    _remove_local_build_index(index_dir)

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
    extra: list[str] = ["--create_if_missing"]

    if body.data_dir is not None:
        dpath = Path(body.data_dir).resolve()
        if not dpath.is_dir():
            raise HTTPException(status_code=400, detail=f"data_dir not a directory: {dpath}")
        cmd.extend(["--input_dir", str(dpath)])
    else:
        inp = Path(body.input or "").resolve()
        if not inp.is_file():
            raise HTTPException(status_code=400, detail=f"input not found: {inp}")
        cmd.extend(["--input", str(inp)])

    cmd.extend(extra)
    cmd.extend(
        [
            "--import_io_workers",
            str(IMPORT_IO_WORKERS),
            "--import_queue_batches",
            str(IMPORT_QUEUE_BATCHES),
        ]
    )
    r = subprocess.run(cmd, cwd=str(YIKV_ROOT), capture_output=True, text=True, check=False)
    if r.returncode != 0:
        msg = (r.stderr or "") + (r.stdout or "")
        tail = msg.strip() or "yikv_import_pipeline failed"
        cmd_line = " ".join(cmd)
        raise HTTPException(
            status_code=500,
            detail=f"import cmd (argv): {cmd_line}\n---\n{tail}",
        )

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
