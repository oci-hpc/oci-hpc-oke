# /// script
# requires-python = ">=3.12"
# dependencies = ["kubernetes==31.0.0"]
# ///
"""QuickCache cluster membership and staged shard-map controller."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from sharding import rebalance_shards


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("quickcache-controller")
HEALTH_FILE = Path("/tmp/health/alive")
READY_FILE = Path("/tmp/health/ready")


def _empty_state() -> dict:
    return {
        "peers": {},
        "active": {},
        "pending": {},
        "previous": {},
        "rebalance": {},
        "generation": 0,
    }


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


def _json_object(data: dict, key: str) -> dict:
    try:
        value = json.loads(data.get(key, "{}"))
    except (TypeError, json.JSONDecodeError):
        LOG.warning("QuickCache state field %s is invalid; ignoring it", key)
        return {}
    if not isinstance(value, dict):
        LOG.warning("QuickCache state field %s is not an object; ignoring it", key)
        return {}
    return value


def _load_state(
    core: client.CoreV1Api, namespace: str, name: str
) -> tuple[dict, str | None, dict[str, str]]:
    try:
        configmap = core.read_namespaced_config_map(name, namespace)
    except ApiException as exc:
        if exc.status == 404:
            return _empty_state(), None, {}
        raise
    data = configmap.data or {}
    try:
        generation = max(0, int(data.get("generation", "0")))
    except (TypeError, ValueError):
        LOG.warning("QuickCache generation is invalid; resetting it")
        generation = 0
    state = {
        "peers": _json_object(data, "peers.json"),
        "active": _json_object(data, "shard_map.json"),
        "pending": _json_object(data, "pending_shard_map.json"),
        "previous": _json_object(data, "previous_shard_map.json"),
        "rebalance": _json_object(data, "rebalance.json"),
        "generation": generation,
    }
    return (
        state,
        configmap.metadata.resource_version,
        dict(getattr(configmap.metadata, "annotations", None) or {}),
    )


def _write_state(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
    state: dict,
    resource_version: str | None,
    annotations: dict[str, str],
) -> None:
    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels={"app.kubernetes.io/name": "oci-quickcache"},
            annotations=annotations,
            resource_version=resource_version,
        ),
        data={
            "peers.json": json.dumps(state["peers"], indent=2, sort_keys=True),
            "shard_map.json": json.dumps(state["active"], indent=2, sort_keys=True),
            "pending_shard_map.json": json.dumps(
                state["pending"], indent=2, sort_keys=True
            ),
            "previous_shard_map.json": json.dumps(
                state["previous"], indent=2, sort_keys=True
            ),
            "rebalance.json": json.dumps(
                state["rebalance"], indent=2, sort_keys=True
            ),
            "generation": str(state["generation"]),
            "updated_at": str(int(time.time())),
        },
    )
    if resource_version:
        core.replace_namespaced_config_map(name, namespace, body)
    else:
        core.create_namespaced_config_map(namespace, body)


def _map_digest(shard_map: dict) -> str:
    serialized = json.dumps(shard_map, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _backup_shard_map(
    core: client.CoreV1Api,
    namespace: str,
    state_name: str,
    old_map: dict,
    new_map: dict,
    generation: int,
    retention: int,
    now: int,
) -> None:
    """Persist the complete pre-cutover map in a bounded ConfigMap history."""
    if not old_map or old_map == new_map:
        return
    old_digest = _map_digest(old_map)
    new_digest = _map_digest(new_map)
    # The deterministic transition name makes retries idempotent if writing
    # the live state later loses a resourceVersion race.
    name = (
        f"{state_name[:160]}-map-g{generation:08d}-"
        f"{old_digest[:12]}-{new_digest[:12]}"
    )
    owner_id = hashlib.sha256(state_name.encode("utf-8")).hexdigest()[:16]
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime(now))
    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels={
                "app.kubernetes.io/name": "oci-quickcache",
                "app.kubernetes.io/component": "shard-map-backup",
                "oci-hpc-oke.oracle.com/state-backup-owner": owner_id,
            },
            annotations={
                "oci-hpc-oke.oracle.com/quickcache-backup-created-at": str(now),
                "oci-hpc-oke.oracle.com/quickcache-old-map-sha256": old_digest,
                "oci-hpc-oke.oracle.com/quickcache-new-map-sha256": new_digest,
            },
        ),
        data={
            f"shard_map.{timestamp}.bak": json.dumps(
                old_map, indent=2, sort_keys=True
            ),
            "generation": str(generation),
            "created_at": str(now),
        },
    )
    try:
        core.create_namespaced_config_map(namespace, body)
        LOG.info("created full shard-map backup %s", name)
    except ApiException as exc:
        if exc.status != 409:
            raise
        existing = core.read_namespaced_config_map(name, namespace)
        backup_values = [
            value
            for key, value in (existing.data or {}).items()
            if key.endswith(".bak")
        ]
        try:
            existing_map = json.loads(backup_values[0])
        except (IndexError, TypeError, json.JSONDecodeError) as invalid:
            raise RuntimeError(
                f"existing shard-map backup {name} is invalid"
            ) from invalid
        if (
            len(backup_values) != 1
            or not isinstance(existing_map, dict)
            or _map_digest(existing_map) != old_digest
        ):
            raise RuntimeError(
                f"existing shard-map backup {name} does not match cutover"
            )

    # Backup creation protects cutover; retention is best effort so an API
    # deletion problem cannot keep a healthy cache permanently unbalanced.
    try:
        backups = core.list_namespaced_config_map(
            namespace,
            label_selector=(
                "app.kubernetes.io/name=oci-quickcache,"
                "app.kubernetes.io/component=shard-map-backup,"
                f"oci-hpc-oke.oracle.com/state-backup-owner={owner_id}"
            ),
        ).items
        backups.sort(
            key=lambda item: (
                int(
                    (item.metadata.annotations or {}).get(
                        "oci-hpc-oke.oracle.com/quickcache-backup-created-at", "0"
                    )
                ),
                item.metadata.name,
            )
        )
        for backup in backups[:-retention]:
            core.delete_namespaced_config_map(backup.metadata.name, namespace)
    except (ApiException, TypeError, ValueError):
        LOG.warning("could not enforce shard-map backup retention", exc_info=True)


def _moved_shards(old_map: dict, new_map: dict) -> list[dict[str, str]]:
    return [
        {"shard": str(shard), "source": str(source), "target": str(new_map[shard])}
        for shard, source in sorted(old_map.items(), key=lambda item: int(item[0]))
        if shard in new_map and source != new_map[shard]
    ]


def _new_plan(
    generation: int,
    mode: str,
    active: dict,
    pending: dict,
    now: int,
) -> dict:
    plan_id = str(uuid.uuid4())
    return {
        "generation": generation,
        "planId": plan_id,
        "approvalToken": f"{generation}:{plan_id}",
        "mode": mode,
        "phase": "awaitingApproval" if mode == "manual" else "migrating",
        "createdAt": now,
        "moves": _moved_shards(active, pending),
    }


def _plan_members(plan: dict, member: str) -> set[str]:
    return {
        str(move.get(member))
        for move in plan.get("moves", [])
        if isinstance(move, dict) and move.get(member)
    }


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
    statuses: dict[str, dict] = {}
    for node in core.list_node(label_selector=f"{enabled_label}=true").items:
        raw = (node.metadata.annotations or {}).get(status_annotation, "{}")
        try:
            status = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(status, dict):
            statuses[str(node.metadata.uid)] = status
    generation = int(plan["generation"])
    plan_id = plan.get("planId")
    for uid in expected:
        status = statuses.get(uid, {})
        try:
            status_generation = int(status.get("generation", -1))
        except (TypeError, ValueError):
            return False
        if not (
            status_generation == generation
            and status.get("planId") == plan_id
            and status.get("phase") == phase
            and status.get("status") == "succeeded"
        ):
            return False
    return True


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
    statuses: dict[str, dict] = {}
    for node in core.list_node(label_selector=f"{enabled_label}=true").items:
        raw = (node.metadata.annotations or {}).get(status_annotation, "{}")
        try:
            status = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(status, dict):
            statuses[str(node.metadata.uid)] = status

    per_source = {}
    for uid in sorted(expected):
        status = statuses.get(uid, {})
        try:
            matching = (
                int(status.get("generation", -1)) == int(plan["generation"])
                and status.get("planId") == plan.get("planId")
                and status.get("phase") == "estimate"
                and status.get("status") == "succeeded"
            )
            files = int(status.get("files", 0))
            size = int(status.get("bytes", 0))
            shards = int(status.get("shards", 0))
        except (TypeError, ValueError):
            return None
        if (
            not matching
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
    active = state["active"]
    plan = state["rebalance"]

    # Losing an active owner is failover, not a scale-out rebalance. There is
    # no healthy source from which to stage data, so availability wins.
    if active and not set(active.values()).issubset(peers):
        LOG.warning("an active cache owner disappeared; applying immediate failover")
        state["generation"] += 1
        state["active"] = desired
        state["pending"] = {}
        state["previous"] = {}
        state["rebalance"] = {}
        return

    if plan:
        phase = plan.get("phase")
        if phase in {"awaitingApproval", "migrating"} and not _plan_members(
            plan, "target"
        ).issubset(peers):
            LOG.warning("a pending cache owner disappeared; cancelling staged rebalance")
            state["pending"] = {}
            state["rebalance"] = {}
            return

        generation = str(plan.get("generation", ""))
        approved = annotations.get(approval_annotation) == plan.get("approvalToken")
        estimate = None
        if phase == "awaitingApproval":
            estimate = _migration_estimate(
                core, enabled_label, status_annotation, plan
            )
            if estimate is not None and plan.get("estimate") != estimate:
                plan["estimate"] = estimate
        if phase == "awaitingApproval" and (
            mode == "automatic" or (approved and estimate is not None)
        ):
            plan["phase"] = "migrating"
            plan["approvedAt"] = now
            phase = "migrating"
            LOG.info("started QuickCache rebalance generation %s", generation)

        if phase == "migrating" and _migration_complete(
            core, enabled_label, status_annotation, plan, "copy"
        ):
            # Cut over only after every source reports a successful copy.
            state["previous"] = active
            state["active"] = state["pending"]
            state["pending"] = {}
            plan["phase"] = "fallback"
            plan["activatedAt"] = now
            plan["cleanupAfter"] = now + fallback_grace
            LOG.info("activated warm QuickCache rebalance generation %s", generation)
            return

        if phase == "fallback":
            if not _plan_members(plan, "source").issubset(peers):
                LOG.warning("a previous owner disappeared; ending fallback early")
                state["previous"] = {}
                state["rebalance"] = {}
            elif now >= int(plan.get("cleanupAfter", now + fallback_grace)):
                plan["phase"] = "cleanup"
                plan["cleanupStartedAt"] = now
                LOG.info("started cleanup for QuickCache rebalance %s", generation)
            return

        if phase == "cleanup" and _migration_complete(
            core, enabled_label, status_annotation, plan, "cleanup"
        ):
            state["previous"] = {}
            state["rebalance"] = {}
            LOG.info("completed QuickCache rebalance generation %s", generation)
        return

    if desired == active:
        return
    state["generation"] += 1
    state["pending"] = desired
    state["previous"] = {}
    state["rebalance"] = _new_plan(
        state["generation"], mode, active, desired, now
    )
    # An empty move set only occurs for initialization-like map expansion and
    # has no warm data to preserve.
    if not state["rebalance"]["moves"]:
        state["active"] = desired
        state["pending"] = {}
        state["rebalance"] = {}


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
