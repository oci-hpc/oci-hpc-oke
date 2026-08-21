"""Kubernetes persistence for QuickCache live state and map backups."""

from __future__ import annotations

import hashlib
import json
import logging
import time

from kubernetes import client
from kubernetes.client.rest import ApiException


LOG = logging.getLogger("quickcache-state-store")


def empty_state() -> dict:
    return {
        "peers": {},
        "active": {},
        "pending": {},
        "previous": {},
        "rebalance": {},
        "generation": 0,
    }


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


def load_state(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
) -> tuple[dict, str | None, dict[str, str]]:
    """Load and defensively decode the live QuickCache ConfigMap."""
    try:
        configmap = core.read_namespaced_config_map(name, namespace)
    except ApiException as exc:
        if exc.status == 404:
            return empty_state(), None, {}
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


def write_state(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
    state: dict,
    resource_version: str | None,
    annotations: dict[str, str],
) -> None:
    """Create or replace the complete live state with optimistic concurrency."""
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


def map_digest(shard_map: dict) -> str:
    serialized = json.dumps(shard_map, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def backup_shard_map(
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
    old_digest = map_digest(old_map)
    new_digest = map_digest(new_map)
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
            or map_digest(existing_map) != old_digest
        ):
            raise RuntimeError(
                f"existing shard-map backup {name} does not match cutover"
            )

    # Cutover requires the backup. Retention is best effort so a failed delete
    # cannot leave a healthy cache permanently unbalanced.
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
