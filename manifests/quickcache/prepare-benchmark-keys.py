#!/usr/bin/env python3
"""Choose benchmark object keys that cover every QuickCache shard owner."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from urllib.parse import urlparse


def _endpoint_scope(endpoint_url: str) -> str:
    parsed = urlparse(endpoint_url)
    if not parsed.netloc:
        raise ValueError("endpoint URL must include a scheme and host")
    return (
        f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
        f"{parsed.path.rstrip('/')}"
    )


def _owner(bucket: str, key: str, endpoint_scope: str, shard_map: dict) -> str:
    resource = f"{endpoint_scope}\0s3://{bucket}/{key}"
    digest = hashlib.sha256(resource.encode("utf-8")).hexdigest()
    shard = str(int(digest, 16) % len(shard_map))
    return str(shard_map[shard])


def select_keys(
    shard_map: dict,
    endpoint_url: str,
    bucket: str,
    prefix: str,
    count: int,
) -> list[str]:
    if count <= 0:
        raise ValueError("count must be positive")
    if not shard_map:
        raise ValueError("shard map must not be empty")
    expected_shards = {str(index) for index in range(len(shard_map))}
    if set(shard_map) != expected_shards:
        raise ValueError("shard map keys must be contiguous from zero")
    owners = sorted({str(owner) for owner in shard_map.values()})
    if count < len(owners):
        raise ValueError("count must be at least the number of cache owners")

    endpoint_scope = _endpoint_scope(endpoint_url)
    selected = []
    candidate = 0
    while len(selected) < count:
        desired_owner = owners[len(selected) % len(owners)]
        key = f"{prefix.rstrip('/')}/object-{candidate:08d}.bin"
        candidate += 1
        if _owner(bucket, key, endpoint_scope, shard_map) == desired_owner:
            selected.append(key)
        if candidate > count * len(shard_map) * 100:
            raise RuntimeError("could not find keys covering every cache owner")
    return selected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-map", required=True)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="quickcache-throughput")
    parser.add_argument("--count", required=True, type=int)
    args = parser.parse_args()
    try:
        shard_map = json.loads(Path(args.shard_map).read_text(encoding="utf-8"))
        keys = select_keys(
            shard_map,
            args.endpoint_url,
            args.bucket,
            args.prefix,
            args.count,
        )
    except (
        OSError,
        json.JSONDecodeError,
        KeyError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:
        parser.error(str(exc))
    print(json.dumps(keys))
    print(
        f"selected {len(keys)} keys across {len(set(shard_map.values()))} owners",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
