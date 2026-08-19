# /// script
# requires-python = ">=3.12"
# dependencies = ["kubernetes==31.0.0"]
# ///
"""Block Helm completion until the QuickCache cluster is genuinely ready."""

from __future__ import annotations

import json
import os
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException


def _node_ready(node) -> bool:
    return any(
        condition.type == "Ready" and condition.status == "True"
        for condition in (node.status.conditions or [])
    )


def _evaluate(
    nodes,
    deployment,
    daemonset,
    peers: dict,
    shard_map: dict,
    ready_label: str,
    virtual_shards: int,
    client_nodes=None,
    client_daemonset=None,
    client_ready_label: str | None = None,
) -> tuple[bool, str]:
    if not isinstance(peers, dict) or not isinstance(shard_map, dict):
        return False, "controller state is not a JSON object"
    if not nodes:
        return False, "no QuickCache nodes are registered"

    node_uids = {str(node.metadata.uid) for node in nodes}
    not_ready = [
        node.metadata.name
        for node in nodes
        if not _node_ready(node)
        or (node.metadata.labels or {}).get(ready_label) != "true"
    ]
    if not_ready:
        return False, f"nodes not cache-ready: {','.join(sorted(not_ready))}"

    desired_deployment = deployment.spec.replicas or 1
    if (deployment.status.available_replicas or 0) < desired_deployment:
        return False, "QuickCache controller is not available"

    desired_agents = daemonset.status.desired_number_scheduled or 0
    ready_agents = daemonset.status.number_ready or 0
    updated_agents = daemonset.status.updated_number_scheduled or 0
    if (
        desired_agents != len(nodes)
        or ready_agents != desired_agents
        or updated_agents != desired_agents
    ):
        return False, (
            "node-agent rollout incomplete: "
            f"nodes={len(nodes)} desired={desired_agents} ready={ready_agents} updated={updated_agents}"
        )

    client_nodes = client_nodes or []
    if client_daemonset is not None:
        unready_clients = [
            node.metadata.name
            for node in client_nodes
            if not _node_ready(node)
            or (node.metadata.labels or {}).get(client_ready_label) != "true"
        ]
        if unready_clients:
            return False, (
                "client nodes not cache-ready: "
                f"{','.join(sorted(unready_clients))}"
            )
        desired_clients = client_daemonset.status.desired_number_scheduled or 0
        ready_clients = client_daemonset.status.number_ready or 0
        updated_clients = client_daemonset.status.updated_number_scheduled or 0
        if (
            desired_clients != len(client_nodes)
            or ready_clients != desired_clients
            or updated_clients != desired_clients
        ):
            return False, (
                "client-agent rollout incomplete: "
                f"nodes={len(client_nodes)} desired={desired_clients} "
                f"ready={ready_clients} updated={updated_clients}"
            )

    if set(peers) != node_uids:
        return (
            False,
            f"peer inventory has {len(peers)} entries for {len(node_uids)} nodes",
        )
    malformed_peers = [
        uid
        for uid, peer in peers.items()
        if not isinstance(peer, dict)
        or not peer.get("internalIP")
        or not isinstance(peer.get("mountPath"), str)
        or not os.path.isabs(peer["mountPath"])
    ]
    if malformed_peers:
        return False, "peer inventory contains incomplete endpoint or mount data"
    if len(shard_map) != virtual_shards:
        return False, f"shard map has {len(shard_map)}/{virtual_shards} entries"
    expected_shards = {str(shard) for shard in range(virtual_shards)}
    if set(shard_map) != expected_shards:
        return False, "shard map keys are incomplete or outside the configured range"
    unknown_assignments = set(shard_map.values()) - set(peers)
    if unknown_assignments:
        return False, "shard map refers to nodes outside the peer inventory"
    return True, (
        f"nodes={len(nodes)} clients={len(client_nodes)} "
        f"peers={len(peers)} shards={len(shard_map)}"
    )


def main() -> None:
    config.load_incluster_config()
    core = client.CoreV1Api()
    apps = client.AppsV1Api()
    namespace = os.environ["POD_NAMESPACE"]
    enabled_label = os.environ["QUICKCACHE_LABEL"]
    ready_label = os.environ["QUICKCACHE_READY_LABEL"]
    state_name = os.environ["STATE_CONFIGMAP_NAME"]
    controller_name = os.environ["CONTROLLER_NAME"]
    node_agent_name = os.environ["NODE_AGENT_NAME"]
    client_enabled = os.environ.get("CLIENT_ENABLED", "false").lower() == "true"
    client_label = os.environ.get("QUICKCACHE_CLIENT_LABEL", "")
    client_ready_label = os.environ.get("QUICKCACHE_CLIENT_READY_LABEL", "")
    client_agent_name = os.environ.get("CLIENT_AGENT_NAME", "")
    virtual_shards = int(os.environ["VIRTUAL_SHARDS"])
    deadline = time.monotonic() + int(os.environ.get("TIMEOUT_SECONDS", "540"))
    last_reason = "waiting for initial state"

    while time.monotonic() < deadline:
        try:
            nodes = core.list_node(label_selector=f"{enabled_label}=true").items
            deployment = apps.read_namespaced_deployment(controller_name, namespace)
            daemonset = apps.read_namespaced_daemon_set(node_agent_name, namespace)
            client_nodes = []
            client_daemonset = None
            if client_enabled:
                client_nodes = core.list_node(
                    label_selector=f"{client_label}=true"
                ).items
                client_daemonset = apps.read_namespaced_daemon_set(
                    client_agent_name, namespace
                )
            state = core.read_namespaced_config_map(state_name, namespace)
            data = state.data or {}
            peers = json.loads(data.get("peers.json", "{}"))
            shard_map = json.loads(data.get("shard_map.json", "{}"))
            ready, reason = _evaluate(
                nodes,
                deployment,
                daemonset,
                peers,
                shard_map,
                ready_label,
                virtual_shards,
                client_nodes,
                client_daemonset,
                client_ready_label,
            )
            if ready:
                print(f"QuickCache readiness verified: {reason}", flush=True)
                return
            if reason != last_reason:
                print(f"Waiting for QuickCache: {reason}", flush=True)
                last_reason = reason
        except (ApiException, json.JSONDecodeError, TypeError, ValueError) as exc:
            reason = f"state unavailable: {exc}"
            if reason != last_reason:
                print(f"Waiting for QuickCache: {reason}", flush=True)
                last_reason = reason
        time.sleep(5)

    raise TimeoutError(f"QuickCache did not become ready: {last_reason}")


if __name__ == "__main__":
    main()
