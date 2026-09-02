"""Filesystem-safe object placement helpers for OCI QuickCache."""

from __future__ import annotations

import hashlib
import os
from urllib.parse import quote


HASHED_LAYOUT = "hashed"
FRIENDLY_LAYOUT = "friendly"
FRIENDLY_LAYOUT_VERSION = "v2"
SUPPORTED_LAYOUTS = {HASHED_LAYOUT, FRIENDLY_LAYOUT}

# Linux and NFS commonly impose NAME_MAX=255 and PATH_MAX=4096. Keep generous
# headroom for temporary/metadata suffixes and for longer peer-mount prefixes.
MAX_COMPONENT_LENGTH = 180
MAX_FILENAME_LENGTH = 220
MAX_CACHE_PATH_LENGTH = 3500
RESOURCE_SUFFIX_LENGTH = 24


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
    *,
    key: str | None = None,
    layout: str = HASHED_LAYOUT,
) -> str:
    """Return a safe cache path for the selected on-disk layout.

    ``hashed`` is the original OKE layout and remains available for rollback
    and warm-cache compatibility. ``friendly`` preserves the S3 key hierarchy
    using reversible percent encoding and appends a resource-hash suffix.
    """
    if layout not in SUPPORTED_LAYOUTS:
        raise ValueError(f"unsupported cache path layout: {layout!r}")
    if layout == FRIENDLY_LAYOUT:
        if key is None:
            raise ValueError("key is required for the friendly cache path layout")
        return _friendly_cache_path(
            mount_path,
            cache_dir_name,
            shard,
            region,
            bucket,
            key,
            digest,
        )

    return legacy_cache_path(
        mount_path, cache_dir_name, shard, region, bucket, digest
    )


def legacy_cache_path(
    mount_path: str,
    cache_dir_name: str,
    shard: int,
    region: str,
    bucket: str,
    digest: str,
) -> str:
    """Return the original OKE hash-only cache path."""
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


def cache_path_candidates(
    mount_path: str,
    cache_dir_name: str,
    shard: int,
    region: str,
    bucket: str,
    key: str,
    digest: str,
    preferred_layout: str,
) -> tuple[str, ...]:
    """Return preferred and compatibility paths without duplicates."""
    if preferred_layout not in SUPPORTED_LAYOUTS:
        raise ValueError(f"unsupported cache path layout: {preferred_layout!r}")
    alternate = (
        HASHED_LAYOUT if preferred_layout == FRIENDLY_LAYOUT else FRIENDLY_LAYOUT
    )
    paths = [
        cache_path(
            mount_path,
            cache_dir_name,
            shard,
            region,
            bucket,
            digest,
            key=key,
            layout=layout,
        )
        for layout in (preferred_layout, alternate)
    ]
    return tuple(dict.fromkeys(paths))


def _friendly_cache_path(
    mount_path: str,
    cache_dir_name: str,
    shard: int,
    region: str,
    bucket: str,
    key: str,
    digest: str,
) -> str:
    safe_region = _bounded_component(region or "unknown-region")
    safe_bucket = _bounded_component(bucket)
    key_parts = key.split("/")
    encoded_parts = [_bounded_component(value) for value in key_parts[:-1]]
    filename = _friendly_filename(key_parts[-1], digest)
    root = os.path.join(
        mount_path,
        cache_dir_name,
        f"{shard:04d}",
        FRIENDLY_LAYOUT_VERSION,
        safe_region,
        safe_bucket,
    )
    path = os.path.join(root, *encoded_parts, filename)
    if len(os.fsencode(path)) <= MAX_CACHE_PATH_LENGTH:
        return path

    # Exceptionally deep or long keys use a bounded, recognizable prefix under
    # a dedicated subtree. The full resource hash remains collision-resistant.
    summary = _bounded_component(key, MAX_FILENAME_LENGTH - 32)
    filename = f"{summary}.__qc_{digest[:RESOURCE_SUFFIX_LENGTH]}"
    return os.path.join(root, "__long_keys__", digest[:2], filename)


def _friendly_filename(value: str, digest: str) -> str:
    suffix = f".__qc_{digest[:RESOURCE_SUFFIX_LENGTH]}"
    encoded = _encoded_component(value)
    prefix_length = MAX_FILENAME_LENGTH - len(suffix)
    if len(encoded) > prefix_length:
        encoded = _safe_prefix(encoded, prefix_length)
    return f"{encoded}{suffix}"


def _bounded_component(value: str, limit: int = MAX_COMPONENT_LENGTH) -> str:
    encoded = _encoded_component(value)
    if len(encoded) <= limit:
        return encoded
    suffix = f".__part_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:16]}"
    return f"{_safe_prefix(encoded, limit - len(suffix))}{suffix}"


def _encoded_component(value: str) -> str:
    """Encode one object-key component without allowing path traversal."""
    if value == "":
        # ``!`` is percent-encoded for a literal key, so this marker is unique.
        return "!empty"
    if value == ".":
        return "%2E"
    if value == "..":
        return "%2E%2E"
    return quote(value, safe="-_~")


def _safe_prefix(encoded: str, length: int) -> str:
    """Avoid ending a truncated percent-encoded component inside ``%XX``."""
    prefix = encoded[: max(1, length)]
    percent = prefix.rfind("%")
    if percent >= 0 and len(prefix) - percent < 3:
        prefix = prefix[:percent]
    return prefix or "encoded"


def _safe_component(value: str) -> str:
    encoded = "".join(
        char if char.isalnum() or char in ".-_" else "_" for char in value
    )
    encoded = encoded.strip(".")
    return encoded[:128] or "unknown"
