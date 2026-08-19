# /// script
# requires-python = ">=3.12"
# dependencies = ["kubernetes==31.0.0"]
# ///
"""Execute source-owned QuickCache migrations without blocking node heartbeats."""

from __future__ import annotations

import json
import logging
import os
import shlex
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path

from kubernetes import client, config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("quickcache-migration-agent")
STOP_EVENT = threading.Event()


def _enter_host_namespaces() -> None:
    host_root_fd = os.open("/proc/1/root", os.O_RDONLY | os.O_DIRECTORY)
    namespace_fds = [
        os.open("/proc/1/ns/mnt", os.O_RDONLY),
        os.open("/proc/1/ns/net", os.O_RDONLY),
    ]
    try:
        for namespace_fd in namespace_fds:
            os.setns(namespace_fd)
        os.fchdir(host_root_fd)
        os.chroot(".")
        os.chdir("/")
    finally:
        os.close(host_root_fd)
        for namespace_fd in namespace_fds:
            os.close(namespace_fd)


def _write_json_atomic(host_path: str, value: dict) -> None:
    path = Path("/host", host_path.lstrip("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_state(core: client.CoreV1Api) -> tuple[dict, dict, dict]:
    configmap = core.read_namespaced_config_map(
        os.environ["STATE_CONFIGMAP_NAME"],
        os.environ["POD_NAMESPACE"],
    )
    data = configmap.data or {}
    return (
        json.loads(data.get("peers.json", "{}")),
        json.loads(data.get("rebalance.json", "{}")),
        json.loads(data.get("pending_shard_map.json", "{}")),
    )


def _patch_status(core: client.CoreV1Api, value: dict) -> None:
    core.patch_node(
        os.environ["NODE_NAME"],
        {
            "metadata": {
                "annotations": {
                    os.environ["MIGRATION_STATUS_ANNOTATION"]: json.dumps(
                        value, sort_keys=True, separators=(",", ":")
                    )
                }
            }
        },
    )


def _marker_path(generation: int, plan_id: str, phase: str) -> str:
    config_root = os.environ["HOST_CONFIG_ROOT"].rstrip("/")
    return f"{config_root}/migrations/{generation}-{plan_id}-{phase}.json"


def _plan_identity(plan: dict) -> tuple[int, str]:
    generation = int(plan["generation"])
    if generation <= 0:
        raise ValueError("migration generation must be positive")
    plan_id = str(uuid.UUID(str(plan["planId"])))
    if plan_id != plan["planId"]:
        raise ValueError("migration planId must be a canonical UUID")
    return generation, plan_id


def _load_marker(generation: int, plan_id: str, phase: str) -> dict | None:
    path = Path("/host", _marker_path(generation, plan_id, phase).lstrip("/"))
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _run_migration(plan: dict, peers: dict, phase: str) -> dict:
    generation, plan_id = _plan_identity(plan)
    marker = _load_marker(generation, plan_id, phase)
    if marker is not None:
        return marker

    config_root = os.environ["HOST_CONFIG_ROOT"].rstrip("/")
    plan_path = f"{config_root}/migration-plan.json"
    peers_path = f"{config_root}/migration-peers.json"
    _write_json_atomic(plan_path, plan)
    _write_json_atomic(peers_path, peers)
    runtime_root = os.environ["HOST_RUNTIME_ROOT"].rstrip("/")
    command = [
        "setpriv",
        "--groups",
        os.environ["SHARED_GID"],
        "python3",
        f"{runtime_root}/migrate_shards.py",
        "--plan",
        plan_path,
        "--peers",
        peers_path,
        "--source-uid",
        os.environ["NODE_UID"],
        "--local-cache-root",
        os.environ["LOCAL_CACHE_PATH"],
        "--cache-dir-name",
        os.environ["CACHE_DIR_NAME"],
        "--phase",
        phase,
        "--directory-mode",
        "2770" if os.environ["ACCESS_MODE"] == "sharedGroup" else "0777",
    ]
    LOG.info("starting migration: %s", shlex.join(command))
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=int(os.environ.get("MIGRATION_TIMEOUT", "86400")),
        preexec_fn=_enter_host_namespaces,
    )
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    _write_json_atomic(_marker_path(generation, plan_id, phase), result)
    return result


def reconcile(core: client.CoreV1Api) -> None:
    peers, plan, _pending_map = _read_state(core)
    phase_name = plan.get("phase")
    phase = (
        "estimate"
        if phase_name == "awaitingApproval"
        else "copy"
        if phase_name == "migrating"
        else "cleanup"
        if phase_name == "cleanup"
        else None
    )
    if phase is None:
        return
    generation, plan_id = _plan_identity(plan)
    node_uid = os.environ["NODE_UID"]
    source_moves = [move for move in plan.get("moves", []) if move.get("source") == node_uid]
    if not source_moves:
        return

    _patch_status(
        core,
        {
            "generation": generation,
            "planId": plan_id,
            "phase": phase,
            "status": "running",
        },
    )
    try:
        result = _run_migration(plan, peers, phase)
    except Exception as exc:
        _patch_status(
            core,
            {
                "generation": generation,
                "planId": plan_id,
                "phase": phase,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}"[:512],
            },
        )
        raise
    _patch_status(
        core,
        {
            "generation": generation,
            "planId": plan_id,
            "phase": phase,
            "status": "succeeded",
            "shards": result.get("shards", 0),
            "files": result.get(
                "files_scanned", result.get("files_copied", 0)
            ),
            "bytes": result.get(
                "bytes_scanned", result.get("bytes_copied", 0)
            ),
        },
    )


def _stop(*_args) -> None:
    STOP_EVENT.set()


def main() -> None:
    config.load_incluster_config()
    core = client.CoreV1Api()
    node = core.read_node(os.environ["NODE_NAME"])
    os.environ["NODE_UID"] = str(node.metadata.uid)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    interval = int(os.environ.get("RECONCILE_INTERVAL", "30"))
    while not STOP_EVENT.is_set():
        try:
            reconcile(core)
        except Exception:
            LOG.exception("migration reconciliation failed")
        STOP_EVENT.wait(interval)


if __name__ == "__main__":
    main()
