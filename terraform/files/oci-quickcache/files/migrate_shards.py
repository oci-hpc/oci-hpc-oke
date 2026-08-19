#!/usr/bin/env python3
"""Copy or retire QuickCache shard directories for a staged rebalance."""

from __future__ import annotations

import argparse
import errno
import json
import os
import re
import shutil
import tempfile
from pathlib import Path


UID_RE = re.compile(r"^[a-f0-9-]{16,64}$")
SKIPPED_SUFFIXES = (".tmp", ".lock")


def _safe_root(path: str, base: str, name: str) -> Path:
    value = Path(path)
    normalized = Path(os.path.normpath(value))
    if not normalized.is_absolute() or normalized == Path(base):
        raise ValueError(f"{name} must be an absolute child of {base}")
    if os.path.commonpath((str(normalized), base)) != base:
        raise ValueError(f"{name} must remain below {base}")
    return normalized


def _safe_component(value: str, name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError(f"{name} must be one safe path component")
    return value


def _iter_files(root: Path):
    for current, directories, filenames in os.walk(root, followlinks=False):
        current_path = Path(current)
        directories[:] = [
            name
            for name in directories
            if not (current_path / name).is_symlink()
        ]
        for filename in filenames:
            source = current_path / filename
            if source.is_symlink() or filename.endswith(SKIPPED_SUFFIXES):
                continue
            if source.is_file():
                yield source


def _ensure_directory(path: Path, directory_mode: int) -> None:
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    if current.is_symlink() or not current.is_dir():
        raise ValueError(f"unsafe directory in migration path: {current}")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=directory_mode)
            os.chmod(directory, directory_mode)
        except FileExistsError:
            # A concurrent cache writer may have created it. Validate below,
            # but do not chmod a directory owned by a different workload UID.
            pass
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError(f"unsafe directory in migration path: {directory}")


def _safe_destination(root: Path, relative: Path, directory_mode: int) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"unsafe relative migration path: {relative}")
    destination = root / relative
    root_real = root.resolve()
    _ensure_directory(destination.parent, directory_mode)
    parent_real = destination.parent.resolve()
    if os.path.commonpath((str(parent_real), str(root_real))) != str(root_real):
        raise ValueError(f"destination path escaped shard root: {relative}")
    if destination.is_symlink():
        raise ValueError(f"destination is a symbolic link: {destination}")
    return destination


def _copy_missing_file(source: Path, destination: Path) -> tuple[int, bool]:
    if destination.exists():
        if not destination.is_file() or destination.is_symlink():
            raise ValueError(f"unsafe existing destination: {destination}")
        return 0, False

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.migration-",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as target, source.open("rb") as stream:
            shutil.copyfileobj(stream, target, length=8 * 1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        shutil.copystat(source, temporary, follow_symlinks=False)
        try:
            # Linking a complete temporary file publishes without replacing a
            # cache entry that may have been populated concurrently.
            os.link(temporary, destination)
            copied = True
        except FileExistsError:
            copied = False
        except OSError as exc:
            if exc.errno not in {errno.EOPNOTSUPP, errno.ENOTSUP}:
                raise
            # NFS servers should support hard links, but retain atomic replace
            # as a compatibility fallback only while the destination is absent.
            if destination.exists():
                copied = False
            else:
                os.replace(temporary, destination)
                temporary = None
                copied = True
        return source.stat().st_size if copied else 0, copied
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _copy_shard(
    source: Path, destination: Path, directory_mode: int
) -> dict[str, int]:
    result = {"files_copied": 0, "files_existing": 0, "bytes_copied": 0}
    if not source.exists():
        return result
    if source.is_symlink() or not source.is_dir():
        raise ValueError(f"unsafe source shard directory: {source}")
    _ensure_directory(destination, directory_mode)
    if destination.is_symlink() or not destination.is_dir():
        raise ValueError(f"unsafe destination shard directory: {destination}")

    for source_file in _iter_files(source):
        relative = source_file.relative_to(source)
        destination_file = _safe_destination(
            destination, relative, directory_mode
        )
        copied_bytes, copied = _copy_missing_file(source_file, destination_file)
        if copied:
            result["files_copied"] += 1
            result["bytes_copied"] += copied_bytes
        else:
            result["files_existing"] += 1
    return result


def _coverage_complete(
    source: Path, destination: Path, directory_mode: int
) -> bool:
    for source_file in _iter_files(source):
        destination_file = _safe_destination(
            destination,
            source_file.relative_to(source),
            directory_mode,
        )
        if not destination_file.is_file() or destination_file.is_symlink():
            return False
    return True


def _has_incomplete_writes(source: Path) -> bool:
    return any(
        filename.endswith(SKIPPED_SUFFIXES)
        for _, _, filenames in os.walk(source, followlinks=False)
        for filename in filenames
    )


def migrate(
    plan: dict,
    peers: dict,
    source_uid: str,
    local_cache_root: str,
    cache_dir_name: str,
    phase: str,
    directory_mode: int,
) -> dict:
    if phase not in {"estimate", "copy", "cleanup"}:
        raise ValueError("phase must be estimate, copy, or cleanup")
    if not UID_RE.fullmatch(source_uid):
        raise ValueError("source UID is unsafe")
    if directory_mode not in {0o2770, 0o777}:
        raise ValueError("directory mode must be 2770 or 0777")
    local_root = _safe_root(local_cache_root, "/mnt/nvme", "local cache root")
    cache_name = _safe_component(cache_dir_name, "cache directory name")
    moves = [move for move in plan.get("moves", []) if move.get("source") == source_uid]
    summary = {
        "generation": int(plan["generation"]),
        "phase": phase,
        "source": source_uid,
        "shards": 0,
        "files_copied": 0,
        "files_existing": 0,
        "bytes_copied": 0,
        "shards_removed": 0,
        "files_scanned": 0,
        "bytes_scanned": 0,
    }

    for move in moves:
        shard = str(move.get("shard", ""))
        target_uid = str(move.get("target", ""))
        if not shard.isdigit() or not UID_RE.fullmatch(target_uid):
            raise ValueError(f"invalid migration move: {move!r}")
        shard_name = f"{int(shard):04d}"
        source = local_root / cache_name / shard_name
        summary["shards"] += 1
        if phase == "estimate":
            if source.exists():
                if source.is_symlink() or not source.is_dir():
                    raise ValueError(f"unsafe source shard directory: {source}")
                for source_file in _iter_files(source):
                    summary["files_scanned"] += 1
                    summary["bytes_scanned"] += source_file.stat().st_size
            continue

        peer = peers.get(target_uid)
        target_mount = peer.get("mountPath") if isinstance(peer, dict) else None
        if not isinstance(target_mount, str):
            raise ValueError(f"target peer is unavailable: {target_uid}")
        target_root = _safe_root(
            target_mount,
            "/var/lib/ociqc/mounts",
            "target mount",
        )
        destination = target_root / cache_name / shard_name
        copied = _copy_shard(source, destination, directory_mode)
        for key in ("files_copied", "files_existing", "bytes_copied"):
            summary[key] += copied[key]

        if phase == "cleanup" and source.exists():
            if _has_incomplete_writes(source):
                raise RuntimeError(f"source shard still has incomplete writes: {source}")
            if not _coverage_complete(source, destination, directory_mode):
                raise RuntimeError(f"destination coverage is incomplete: {destination}")
            shutil.rmtree(source)
            summary["shards_removed"] += 1

    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    parser.add_argument("--peers", required=True)
    parser.add_argument("--source-uid", required=True)
    parser.add_argument("--local-cache-root", required=True)
    parser.add_argument("--cache-dir-name", required=True)
    parser.add_argument(
        "--phase", choices=("estimate", "copy", "cleanup"), required=True
    )
    parser.add_argument("--directory-mode", choices=("2770", "0777"), required=True)
    args = parser.parse_args()
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    peers = json.loads(Path(args.peers).read_text(encoding="utf-8"))
    result = migrate(
        plan,
        peers,
        args.source_uid,
        args.local_cache_root,
        args.cache_dir_name,
        args.phase,
        int(args.directory_mode, 8),
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
