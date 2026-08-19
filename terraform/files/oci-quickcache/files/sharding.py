"""Stable virtual-shard assignment for OCI QuickCache."""

from __future__ import annotations

import os


def rebalance_shards(
    old_map: dict[str, str] | None,
    hosts: list[str],
    virtual_shards: int,
) -> dict[str, str]:
    """Balance fixed shards while preserving as many existing assignments as possible."""
    if virtual_shards <= 0:
        raise ValueError("virtual_shards must be positive")

    new_hosts = sorted(set(hosts))
    if not new_hosts:
        return {}

    base, remainder = divmod(virtual_shards, len(new_hosts))
    targets = {
        host: base + (1 if index < remainder else 0)
        for index, host in enumerate(new_hosts)
    }
    counts = {host: 0 for host in new_hosts}
    result: dict[str, str] = {}
    unassigned: list[str] = []
    old_map = old_map or {}

    for shard in range(virtual_shards):
        shard_id = str(shard)
        old_host = old_map.get(shard_id)
        if old_host in targets and counts[old_host] < targets[old_host]:
            result[shard_id] = old_host
            counts[old_host] += 1
        else:
            unassigned.append(shard_id)

    host_index = 0
    for shard_id in unassigned:
        while counts[new_hosts[host_index]] >= targets[new_hosts[host_index]]:
            host_index += 1
        host = new_hosts[host_index]
        result[shard_id] = host
        counts[host] += 1

    return result


def resolve_shard_mounts(
    shard_map: dict[str, str],
    peers: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Resolve controller-owned node assignments to workload mount paths."""
    resolved: dict[str, str] = {}
    for shard, node_uid in shard_map.items():
        peer = peers.get(node_uid)
        mount_path = peer.get("mountPath") if peer else None
        if not isinstance(mount_path, str) or not os.path.isabs(mount_path):
            raise ValueError(
                f"shard {shard!r} refers to peer {node_uid!r} without an absolute mount path"
            )
        resolved[str(shard)] = mount_path
    return resolved
