"""Shared host and Kubernetes helpers for QuickCache agents."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def enter_host_namespaces() -> None:
    """Enter the host mount and network namespaces and chroot to the host."""
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


def write_json_atomic(host_path: str, value: dict[str, Any]) -> None:
    """Replace a host JSON document without exposing a partial write."""
    path = Path("/host", host_path.lstrip("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, indent=2, sort_keys=True)
    try:
        if path.read_text(encoding="utf-8") == serialized:
            return
    except OSError:
        pass
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(serialized, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_configmap_json_objects(
    core,
    name: str,
    namespace: str,
    *keys: str,
) -> tuple[dict, ...]:
    """Read selected JSON-object entries from one Kubernetes ConfigMap."""
    configmap = core.read_namespaced_config_map(name, namespace)
    data = configmap.data or {}
    values = []
    for key in keys:
        value = json.loads(data.get(key, "{}"))
        if not isinstance(value, dict):
            raise ValueError(f"ConfigMap field {key!r} must contain a JSON object")
        values.append(value)
    return tuple(values)


def patch_node_json_annotation(
    core,
    node_name: str,
    annotation: str,
    value: dict[str, Any],
) -> None:
    """Publish one compact JSON node annotation."""
    core.patch_node(
        node_name,
        {
            "metadata": {
                "annotations": {
                    annotation: json.dumps(
                        value,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                }
            }
        },
    )
