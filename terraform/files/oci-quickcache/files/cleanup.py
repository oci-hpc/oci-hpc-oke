#!/usr/bin/env python3
"""Remove least-recently-accessed QuickCache objects under disk pressure."""

from __future__ import annotations

import logging
import os
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path


LOG_FORMAT = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(logging.INFO)
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setFormatter(LOG_FORMAT)
ROOT_LOGGER.addHandler(STREAM_HANDLER)
if log_dir := os.environ.get("OCI_QC_LOG_DIR"):
    try:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(Path(log_dir, "cleanup.log"))
        file_handler.setFormatter(LOG_FORMAT)
        ROOT_LOGGER.addHandler(file_handler)
    except OSError:
        pass
LOG = logging.getLogger("quickcache-cleanup")


def parse_positive_integer(value: str, name: str) -> int:
    """Parse integer environment values, including scientific notation."""
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a positive integer, got {value!r}") from exc

    if not number.is_finite() or number != number.to_integral_value() or number <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return int(number)


def cleanup_cache(cache_dir: Path, high: float, target: float, max_files: int) -> int:
    if not cache_dir.exists():
        return 0

    usage = os.statvfs(cache_dir)
    full_ratio = 1 - (usage.f_bavail / usage.f_blocks) if usage.f_blocks else 0
    used_inodes = usage.f_files - usage.f_favail
    if full_ratio < high and used_inodes < max_files:
        return 0

    total_disk_bytes = usage.f_frsize * usage.f_blocks
    used_disk_bytes = usage.f_frsize * (usage.f_blocks - usage.f_bavail)
    bytes_to_free = max(0, used_disk_bytes - int(total_disk_bytes * target))
    files_to_remove = max(0, used_inodes - int(max_files * target))
    objects: list[tuple[float, int, Path]] = []

    for root, _, files in os.walk(cache_dir):
        for filename in files:
            if filename.endswith((".tmp", ".lock", ".meta.json", ".access")):
                continue
            path = Path(root, filename)
            try:
                stat = path.stat()
                access_path = Path(f"{path}.access")
                access_time = (
                    access_path.stat().st_mtime
                    if access_path.exists()
                    else stat.st_atime
                )
            except OSError:
                continue
            objects.append((access_time, stat.st_size, path))

    removed = 0
    freed_bytes = 0
    freed_inodes = 0
    objects.sort(key=lambda item: item[0])
    for _, size, path in objects:
        if freed_bytes >= bytes_to_free and freed_inodes >= files_to_remove:
            break
        try:
            path.unlink()
        except OSError:
            continue
        freed_inodes += 1
        for sidecar in (Path(f"{path}.meta.json"), Path(f"{path}.access")):
            try:
                if sidecar.exists():
                    sidecar.unlink()
                    freed_inodes += 1
            except OSError:
                # The cached object is already gone. Count it and leave an
                # orphaned sidecar for a later cleanup pass.
                pass
        freed_bytes += size
        removed += 1

    cutoff = time.time() - 3600
    for path in cache_dir.rglob("*.tmp"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            pass

    for metadata in cache_dir.rglob("*.meta.json"):
        try:
            object_path = Path(str(metadata).removesuffix(".meta.json"))
            if not object_path.exists():
                metadata.unlink()
        except OSError:
            pass

    for access_path in cache_dir.rglob("*.access"):
        try:
            object_path = Path(str(access_path).removesuffix(".access"))
            if not object_path.exists():
                access_path.unlink()
        except OSError:
            pass

    for root, dirs, _ in os.walk(cache_dir, topdown=False):
        for directory in dirs:
            try:
                Path(root, directory).rmdir()
            except OSError:
                pass
    return removed


def main() -> None:
    local_dir = Path(os.environ.get("OCI_QC_CACHE_DIR_LOCAL", "/cache"))
    cache_name = os.environ.get("OCI_QC_CACHE_DIR_NAME", "OCI_QC_Cache")
    interval = int(os.environ.get("OCI_QC_CLEAN_INTERVAL", "1800"))
    high = float(os.environ.get("OCI_QC_CLEAN_MAX", "0.90"))
    target = float(os.environ.get("OCI_QC_CLEAN_TARGET", "0.70"))
    max_files = parse_positive_integer(
        os.environ.get("OCI_QC_MAX_CACHE_FILES", "500000000"),
        "OCI_QC_MAX_CACHE_FILES",
    )

    while True:
        try:
            removed = cleanup_cache(local_dir / cache_name, high, target, max_files)
            LOG.info("cleanup completed: removed=%d", removed)
        except Exception:
            LOG.exception("cleanup failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
