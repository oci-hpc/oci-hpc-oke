"""Filesystem-safe object placement helpers for OCI QuickCache."""

from __future__ import annotations

import hashlib
import os


def resource_hash(
    bucket: str,
    key: str,
    version_id: str | None = None,
    endpoint_scope: str | None = None,
) -> str:
    # OCI bucket names are unique within an Object Storage namespace, not
    # globally. Include the endpoint so equal bucket/key pairs in different
    # namespaces or S3 services cannot share a cache entry.
    resource = f"{endpoint_scope or 'default-endpoint'}\0s3://{bucket}/{key}"
    if version_id is not None:
        resource = f"{resource}\0versionId={version_id}"
    return hashlib.sha256(resource.encode("utf-8")).hexdigest()


def shard_for_resource(
    bucket: str,
    key: str,
    shard_count: int,
    version_id: str | None = None,
    endpoint_scope: str | None = None,
) -> tuple[int, str]:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    digest = resource_hash(bucket, key, version_id, endpoint_scope)
    return int(digest, 16) % shard_count, digest


def cache_path(
    mount_path: str,
    cache_dir_name: str,
    shard: int,
    region: str,
    bucket: str,
    digest: str,
) -> str:
    """Return a bounded path without embedding the untrusted S3 object key."""
    safe_region = _safe_component(region or "unknown-region")
    safe_bucket = _safe_component(bucket)
    return os.path.join(
        mount_path,
        cache_dir_name,
        f"{shard:04d}",
        safe_region,
        safe_bucket,
        digest[:2],
        digest[2:],
    )


def _safe_component(value: str) -> str:
    encoded = "".join(
        char if char.isalnum() or char in ".-_" else "_" for char in value
    )
    encoded = encoded.strip(".")
    return encoded[:128] or "unknown"
