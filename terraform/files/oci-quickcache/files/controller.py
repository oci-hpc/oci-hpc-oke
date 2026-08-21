# /// script
# requires-python = ">=3.12"
# dependencies = ["kubernetes==31.0.0"]
# ///
"""QuickCache cluster membership and staged shard-map controller."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from rebalance import (
    advance_staged_rebalance,
    moved_shards as _moved_shards,
    new_plan as _new_plan,
    plan_members as _plan_members,
)
from sharding import rebalance_shards
from state_store import (
    backup_shard_map as _backup_shard_map,
    empty_state as _empty_state,
    load_state as _load_state,
    map_digest as _map_digest,
    write_state as _write_state,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("quickcache-controller")
HEALTH_FILE = Path("/tmp/health/alive")
READY_FILE = Path("/tmp/health/ready")


def _node_is_ready(node: client.V1Node) -> bool:
    return any(
        condition.type == "Ready" and condition.status == "True"
        for condition in (node.status.conditions or [])
    )


def _internal_ip(node: client.V1Node) -> str | None:
    for address in node.status.addresses or []:
        if address.type == "InternalIP":
            return address.address
    return None


def _healthy_peers(
    core: client.CoreV1Api,
    enabled_label: str,
    heartbeat_annotation: str,
    heartbeat_timeout: int,
    mount_root: str,
) -> dict[str, dict[str, str]]:
    now = int(time.time())
    # The heartbeat proves that the node agent has prepared the local export.
    # Do not require its workload-ready label here: that label is only set
    # after the agent has consumed the controller's first shard map.
    nodes = core.list_node(label_selector=f"{enabled_label}=true").items
    peers: dict[str, dict[str, str]] = {}
    for node in nodes:
        heartbeat = (node.metadata.annotations or {}).get(heartbeat_annotation, "0")
        try:
            heartbeat_is_fresh = abs(now - int(heartbeat)) <= heartbeat_timeout
        except ValueError:
            heartbeat_is_fresh = False
        internal_ip = _internal_ip(node)
        if not (_node_is_ready(node) and heartbeat_is_fresh and internal_ip):
            continue
        uid = str(node.metadata.uid)
        peers[uid] = {
            "nodeName": node.metadata.name,
            "internalIP": internal_ip,
            "mountPath": f"{mount_root.rstrip('/')}/{uid}",
        }
    return dict(sorted(peers.items()))


def _clear_stale_ready_labels(
    core: client.CoreV1Api,
    ready_label: str,
    healthy_uids: set[str],
) -> None:
    """Prevent workloads from targeting nodes without a healthy agent."""
    nodes = core.list_node(label_selector=f"{ready_label}=true").items
    for node in nodes:
        labels = node.metadata.labels or {}
        if (
            str(node.metadata.uid) not in healthy_uids
            and labels.get(ready_label) == "true"
        ):
            try:
                core.patch_node(
                    node.metadata.name,
                    {"metadata": {"labels": {ready_label: "false"}}},
                )
            except ApiException as exc:
                LOG.warning(
                    "could not clear stale QuickCache readiness from node %s: %s",
                    node.metadata.name,
                    exc,
                )


def _migration_complete(
    core: client.CoreV1Api,
    enabled_label: str,
    status_annotation: str,
    plan: dict,
    phase: str,
) -> bool:
    expected = _plan_members(plan, "source")
    if not expected:
        return True
    statuses = _migration_statuses(core, enabled_label, status_annotation)
    return all(
        _status_matches(statuses.get(uid, {}), plan, phase)
        for uid in expected
    )


def _migration_statuses(
    core: client.CoreV1Api,
    enabled_label: str,
    status_annotation: str,
) -> dict[str, dict]:
    statuses: dict[str, dict] = {}
    for node in core.list_node(label_selector=f"{enabled_label}=true").items:
        raw = (node.metadata.annotations or {}).get(status_annotation, "{}")
        try:
            status = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(status, dict):
            statuses[str(node.metadata.uid)] = status
    return statuses


def _status_matches(status: dict, plan: dict, phase: str) -> bool:
    try:
        generation_matches = int(status.get("generation", -1)) == int(
            plan["generation"]
        )
    except (KeyError, TypeError, ValueError):
        return False
    return (
        generation_matches
        and status.get("planId") == plan.get("planId")
        and status.get("phase") == phase
        and status.get("status") == "succeeded"
    )


def _migration_estimate(
    core: client.CoreV1Api,
    enabled_label: str,
    status_annotation: str,
    plan: dict,
) -> dict | None:
    expected = _plan_members(plan, "source")
    expected_shards = {
        uid: sum(
            1
            for move in plan.get("moves", [])
            if isinstance(move, dict) and str(move.get("source")) == uid
        )
        for uid in expected
    }
    statuses = _migration_statuses(core, enabled_label, status_annotation)

    per_source = {}
    for uid in sorted(expected):
        status = statuses.get(uid, {})
        try:
            files = int(status.get("files", 0))
            size = int(status.get("bytes", 0))
            shards = int(status.get("shards", 0))
        except (TypeError, ValueError):
            return None
        if (
            not _status_matches(status, plan, "estimate")
            or min(files, size, shards) < 0
            or shards != expected_shards[uid]
        ):
            return None
        per_source[uid] = {"shards": shards, "files": files, "bytes": size}
    return {
        "status": "complete",
        "sourceNodes": len(per_source),
        "shards": sum(value["shards"] for value in per_source.values()),
        "files": sum(value["files"] for value in per_source.values()),
        "bytes": sum(value["bytes"] for value in per_source.values()),
        "perSource": per_source,
    }


def _staged_rebalance(
    core: client.CoreV1Api,
    state: dict,
    peers: dict,
    desired: dict,
    mode: str,
    enabled_label: str,
    approval_annotation: str,
    status_annotation: str,
    annotations: dict[str, str],
    fallback_grace: int,
    now: int,
) -> None:
    advance_staged_rebalance(
        state=state,
        peers=peers,
        desired=desired,
        mode=mode,
        approval_annotation=approval_annotation,
        annotations=annotations,
        fallback_grace=fallback_grace,
        now=now,
        migration_complete=lambda plan, phase: _migration_complete(
            core,
            enabled_label,
            status_annotation,
            plan,
            phase,
        ),
        migration_estimate=lambda plan: _migration_estimate(
            core,
            enabled_label,
            status_annotation,
            plan,
        ),
    )


def reconcile(core: client.CoreV1Api) -> None:
    namespace = os.environ["POD_NAMESPACE"]
    state_name = os.environ.get("STATE_CONFIGMAP_NAME", "oci-quickcache-state")
    enabled_label = os.environ["QUICKCACHE_LABEL"]
    ready_label = os.environ["QUICKCACHE_READY_LABEL"]
    client_enabled = os.environ.get("CLIENT_ENABLED", "false").lower() == "true"
    client_label = os.environ.get("QUICKCACHE_CLIENT_LABEL", "")
    client_ready_label = os.environ.get("QUICKCACHE_CLIENT_READY_LABEL", "")
    heartbeat_annotation = os.environ["HEARTBEAT_ANNOTATION"]
    heartbeat_timeout = int(os.environ.get("HEARTBEAT_TIMEOUT", "120"))
    mount_root = os.environ.get("HOST_MOUNT_ROOT", "/var/lib/ociqc/mounts")
    virtual_shards = int(os.environ.get("VIRTUAL_SHARDS", "1024"))
    mode = os.environ.get("REBALANCE_MODE", "automatic")
    if mode not in {"automatic", "manual", "immediate"}:
        raise ValueError("REBALANCE_MODE must be automatic, manual, or immediate")
    fallback_grace = int(os.environ.get("FALLBACK_GRACE_SECONDS", "3600"))
    approval_annotation = os.environ["REBALANCE_APPROVAL_ANNOTATION"]
    status_annotation = os.environ["MIGRATION_STATUS_ANNOTATION"]
    backup_retention = int(os.environ.get("MAP_BACKUP_RETENTION", "20"))
    if backup_retention <= 0:
        raise ValueError("MAP_BACKUP_RETENTION must be positive")

    peers = _healthy_peers(
        core,
        enabled_label,
        heartbeat_annotation,
        heartbeat_timeout,
        mount_root,
    )
    _clear_stale_ready_labels(core, ready_label, set(peers))
    if client_enabled:
        healthy_clients = _healthy_peers(
            core,
            client_label,
            heartbeat_annotation,
            heartbeat_timeout,
            mount_root,
        )
        _clear_stale_ready_labels(core, client_ready_label, set(healthy_clients))

    state, resource_version, annotations = _load_state(core, namespace, state_name)
    original_active = dict(state["active"])
    before = json.dumps(state, sort_keys=True)
    state["peers"] = peers
    desired = rebalance_shards(state["active"], list(peers), virtual_shards)
    if not state["active"]:
        state["active"] = desired
    elif mode == "immediate":
        if desired != state["active"]:
            state["generation"] += 1
        state["active"] = desired
        state["pending"] = {}
        state["previous"] = {}
        state["rebalance"] = {}
    else:
        _staged_rebalance(
            core,
            state,
            peers,
            desired,
            mode,
            enabled_label,
            approval_annotation,
            status_annotation,
            annotations,
            fallback_grace,
            int(time.time()),
        )

    if before == json.dumps(state, sort_keys=True):
        return
    if original_active != state["active"]:
        _backup_shard_map(
            core,
            namespace,
            state_name,
            original_active,
            state["active"],
            state["generation"],
            backup_retention,
            int(time.time()),
        )
    _write_state(
        core,
        namespace,
        state_name,
        state,
        resource_version,
        annotations,
    )
    LOG.info(
        "published QuickCache state: peers=%d active_shards=%d phase=%s generation=%d",
        len(peers),
        len(state["active"]),
        state["rebalance"].get("phase", "stable"),
        state["generation"],
    )


def main() -> None:
    config.load_incluster_config()
    core = client.CoreV1Api()
    interval = int(os.environ.get("RECONCILE_INTERVAL", "30"))
    READY_FILE.unlink(missing_ok=True)
    HEALTH_FILE.touch()
    while True:
        try:
            reconcile(core)
            HEALTH_FILE.touch()
            READY_FILE.touch()
        except Exception:
            READY_FILE.unlink(missing_ok=True)
            LOG.exception("reconciliation failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
