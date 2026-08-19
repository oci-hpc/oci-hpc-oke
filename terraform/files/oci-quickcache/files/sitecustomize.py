"""Transparent read-through cache for Botocore S3 GetObject calls."""

from __future__ import annotations

import csv
import datetime as dt
import fcntl
import json
import os
import random
import re
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import botocore.client

from cache_paths import cache_path, shard_for_resource


CONFIG_PATH = Path(os.environ.get("OCI_QC_ENV_PATH", "/etc/ociqc/env.json"))
try:
    CONFIG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    CONFIG = {}

SHARD_MAP_FILE = Path(
    os.environ.get(
        "OCI_QC_SHARD_MAP_FILE",
        CONFIG.get("OCI_QC_SHARD_MAP_FILE", "/etc/ociqc/shard_map.json"),
    )
)
PREVIOUS_SHARD_MAP_FILE = Path(
    os.environ.get(
        "OCI_QC_PREVIOUS_SHARD_MAP_FILE",
        CONFIG.get(
            "OCI_QC_PREVIOUS_SHARD_MAP_FILE",
            "/etc/ociqc/previous_shard_map.json",
        ),
    )
)
CACHE_DIR_NAME = os.environ.get(
    "OCI_QC_CACHE_DIR_NAME", CONFIG.get("OCI_QC_CACHE_DIR_NAME", "OCI_QC_Cache")
)
MAX_CACHE_AGE = int(
    os.environ.get("OCI_QC_MAX_CACHE_AGE", CONFIG.get("OCI_QC_MAX_CACHE_AGE", 36000000))
)
MAP_RELOAD_INTERVAL = int(
    os.environ.get(
        "OCI_QC_MAP_RELOAD_INTERVAL", CONFIG.get("OCI_QC_MAP_RELOAD_INTERVAL", 60)
    )
)
CACHE_RANGE_GETS = str(
    os.environ.get(
        "OCI_QC_CACHE_RANGE_GETS", CONFIG.get("OCI_QC_CACHE_RANGE_GETS", True)
    )
).lower() not in ("0", "false", "no")
LOG_DIR = Path(
    os.environ.get(
        "OCI_QC_LOG_DIR", CONFIG.get("OCI_QC_LOG_DIR", "/tmp/ociqc")
    )
)
LOG_FILE = LOG_DIR / "cache_log.csv"
ERROR_FILE = LOG_DIR / "cache_err.csv"
try:
    CACHE_DIRECTORY_MODE = int(
        str(CONFIG.get("OCI_QC_CACHE_DIRECTORY_MODE", "0777")), 8
    )
    LOG_FILE_MODE = int(str(CONFIG.get("OCI_QC_LOG_FILE_MODE", "0666")), 8)
except ValueError:
    CACHE_DIRECTORY_MODE = 0o777
    LOG_FILE_MODE = 0o666
ACCESS_TIME_UPDATE_INTERVAL = int(os.environ.get("OCI_QC_ATIME_UPDATE_INTERVAL", "60"))

_SHARD_MAP: dict[str, str] = {}
_SHARD_MAP_MTIME = 0.0
_PREVIOUS_SHARD_MAP: dict[str, str] = {}
_PREVIOUS_SHARD_MAP_MTIME = 0.0
_LAST_MAP_CHECK = 0.0
_MAP_LOCK = threading.Lock()
_ORIGINAL_CALL = botocore.client.BaseClient._make_api_call
_CACHEABLE_GET_OPTIONS = {"Bucket", "Key", "Range", "VersionId"}
_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def _log(
    path: Path, resource: str, reason: str, digest: str, shard: int, size: int = 0
) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, LOG_FILE_MODE)
        try:
            os.chmod(path, LOG_FILE_MODE)
        except OSError:
            pass
        with os.fdopen(descriptor, "a", newline="", encoding="utf-8") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            new_file = stream.tell() == 0
            writer = csv.writer(stream)
            if new_file:
                writer.writerow(
                    ["timestamp", "resource", "reason", "hash", "shard", "size_bytes"]
                )
            writer.writerow(
                [
                    dt.datetime.now(dt.timezone.utc).isoformat(),
                    resource,
                    reason,
                    digest,
                    shard,
                    size,
                ]
            )
            stream.flush()
            fcntl.flock(stream, fcntl.LOCK_UN)
    except OSError:
        pass


def _refresh_maps(force: bool = False) -> tuple[dict[str, str], dict[str, str]]:
    global _SHARD_MAP, _SHARD_MAP_MTIME, _PREVIOUS_SHARD_MAP
    global _PREVIOUS_SHARD_MAP_MTIME, _LAST_MAP_CHECK
    now = time.time()
    if not force and now - _LAST_MAP_CHECK < MAP_RELOAD_INTERVAL:
        return _SHARD_MAP, _PREVIOUS_SHARD_MAP
    with _MAP_LOCK:
        _LAST_MAP_CHECK = now
        for path, map_name, mtime_name in (
            (SHARD_MAP_FILE, "_SHARD_MAP", "_SHARD_MAP_MTIME"),
            (
                PREVIOUS_SHARD_MAP_FILE,
                "_PREVIOUS_SHARD_MAP",
                "_PREVIOUS_SHARD_MAP_MTIME",
            ),
        ):
            try:
                mtime = path.stat().st_mtime
                if force or mtime != globals()[mtime_name]:
                    value = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(value, dict):
                        globals()[map_name] = value
                        globals()[mtime_name] = mtime
            except (OSError, json.JSONDecodeError):
                pass
    return _SHARD_MAP, _PREVIOUS_SHARD_MAP


def _refresh_map(force: bool = False) -> dict[str, str]:
    """Backward-compatible active-map accessor used by existing callers/tests."""
    return _refresh_maps(force)[0]


def _region(client) -> str:
    endpoints = [
        getattr(getattr(client, "_endpoint", None), "host", None),
        getattr(client.meta, "endpoint_url", None),
    ]
    for endpoint in filter(None, endpoints):
        host = (urlparse(endpoint).hostname or "").split(".")
        if "objectstorage" in host:
            index = host.index("objectstorage")
            if index + 1 < len(host):
                return host[index + 1]
        if host and host[0] == "s3" and len(host) > 1:
            return host[1]
    return getattr(client.meta, "region_name", None) or "unknown-region"


def _endpoint_scope(client) -> str:
    endpoint = (
        getattr(client.meta, "endpoint_url", None)
        or getattr(getattr(client, "_endpoint", None), "host", None)
        or "unknown-endpoint"
    )
    parsed = urlparse(endpoint)
    if parsed.netloc:
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path.rstrip('/')}"
    return endpoint.rstrip("/").lower()


def _object_location(
    client,
    bucket: str,
    key: str,
    version_id: str | None,
) -> tuple[Path | None, str, int]:
    shard_map = _refresh_map()
    if not shard_map:
        return None, "", 0
    shard, digest = shard_for_resource(
        bucket,
        key,
        len(shard_map),
        version_id,
        _endpoint_scope(client),
    )
    mount_path = shard_map.get(str(shard))
    if not mount_path:
        return None, digest, shard
    try:
        if os.stat(mount_path).st_dev == os.stat("/").st_dev:
            return None, digest, shard
    except OSError:
        return None, digest, shard
    return (
        Path(
            cache_path(
                mount_path, CACHE_DIR_NAME, shard, _region(client), bucket, digest
            )
        ),
        digest,
        shard,
    )


def _previous_object_location(
    client,
    bucket: str,
    key: str,
    version_id: str | None,
    active_path: Path | None,
) -> Path | None:
    shard_map, previous_map = _refresh_maps()
    if not shard_map or not previous_map:
        return None
    shard, digest = shard_for_resource(
        bucket,
        key,
        len(shard_map),
        version_id,
        _endpoint_scope(client),
    )
    mount_path = previous_map.get(str(shard))
    if not mount_path:
        return None
    previous_path = Path(
        cache_path(
            mount_path, CACHE_DIR_NAME, shard, _region(client), bucket, digest
        )
    )
    if active_path == previous_path:
        return None
    try:
        if os.stat(mount_path).st_dev == os.stat("/").st_dev:
            return None
    except OSError:
        return None
    return previous_path


def _touch_access_time(path: Path) -> None:
    """Record LRU time explicitly because OKE NVMe is mounted with noatime.

    Cache readers can run under different UIDs, so changing the object's atime
    is not reliably permitted. Publishing a replaceable sidecar in the shared
    cache directory works across those UIDs without changing the object's
    mtime, which is reserved for freshness checks.
    """
    access_path = Path(f"{path}.access")
    temporary: Path | None = None
    try:
        stat_result = access_path.stat()
        now_ns = time.time_ns()
        if (
            now_ns - stat_result.st_mtime_ns
            < ACCESS_TIME_UPDATE_INTERVAL * 1_000_000_000
        ):
            return
    except OSError:
        pass

    try:
        temporary = Path(f"{access_path}.{random.getrandbits(64):016x}.tmp")
        temporary.touch()
        os.replace(temporary, access_path)
    except OSError:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _ensure_cache_parent(path: Path) -> None:
    """Create cache directories using the configured shared access policy."""
    directories: list[Path] = []
    current = path.parent
    while current.name != CACHE_DIR_NAME:
        if current == current.parent:
            raise OSError(f"cache path is not below {CACHE_DIR_NAME!r}: {path}")
        directories.append(current)
        current = current.parent

    for directory in reversed(directories):
        directory.mkdir(mode=CACHE_DIRECTORY_MODE, exist_ok=True)
        # mkdir honors the workload umask. Correct it so root-squashed NFS
        # clients with the configured access identity can populate the cache.
        directory.chmod(CACHE_DIRECTORY_MODE)


def _iter_body_lines(body, chunk_size: int = 1024, keepends: bool = False):
    pending = b""
    for chunk in body.iter_chunks(chunk_size):
        lines = (pending + chunk).splitlines(True)
        # A trailing CR may be the first half of CRLF split across chunks, so
        # retain it until the following chunk establishes the line ending.
        if lines and (
            not lines[-1].endswith((b"\n", b"\r"))
            or (lines[-1].endswith(b"\r") and not lines[-1].endswith(b"\r\n"))
        ):
            pending = lines.pop()
        else:
            pending = b""
        for line in lines:
            yield line if keepends else line.rstrip(b"\r\n")
    if pending:
        yield pending if keepends else pending.rstrip(b"\r\n")


def _parse_range(value: str | None, size: int) -> tuple[int, int]:
    if not value:
        return 0, max(0, size - 1)
    match = _RANGE_RE.fullmatch(value)
    if not match or size <= 0:
        raise ValueError("invalid byte range")
    start_value, end_value = match.groups()
    if not start_value and not end_value:
        raise ValueError("invalid byte range")
    if not start_value:
        suffix_length = int(end_value)
        if suffix_length <= 0:
            raise ValueError("invalid byte range")
        return max(0, size - suffix_length), size - 1
    start = int(start_value)
    if start >= size:
        raise ValueError("unsatisfiable byte range")
    end = int(end_value) if end_value else size - 1
    if end < start:
        raise ValueError("invalid byte range")
    return start, min(end, size - 1)


class CachedBody:
    def __init__(self, stream, remaining: int | None = None):
        self._stream = stream
        self._remaining = remaining
        self._amount_read = 0
        self.from_cache = True

    def read(self, amount=None):
        if amount is not None and amount < 0:
            amount = None
        if amount == 0:
            return self._stream.read(0)
        if self._remaining is not None:
            if self._remaining <= 0:
                return b""
            amount = self._remaining if amount is None else min(amount, self._remaining)
        data = self._stream.read(amount)
        if self._remaining is not None:
            if not data and self._remaining > 0:
                raise OSError(
                    f"incomplete cached body: missing {self._remaining} bytes"
                )
            self._remaining -= len(data)
        self._amount_read += len(data)
        return data

    def iter_chunks(self, chunk_size=1024):
        while chunk := self.read(chunk_size):
            yield chunk

    def iter_lines(self, chunk_size=1024, keepends=False):
        return _iter_body_lines(self, chunk_size, keepends)

    def __iter__(self):
        return self.iter_chunks(1024)

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def readable(self):
        return True

    def tell(self):
        # Match Botocore StreamingBody: report bytes consumed from this body,
        # not the absolute offset of a ranged file handle.
        return self._amount_read

    def set_socket_timeout(self, _timeout):
        # Cached files do not have a network socket, but callers commonly use
        # this StreamingBody method before reading.
        return None

    def close(self):
        return self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __getattr__(self, name):
        return getattr(self._stream, name)


class TeeBody:
    def __init__(
        self,
        source,
        temporary: Path,
        final: Path,
        metadata: dict,
        resource: str,
        digest: str,
        shard: int,
        expected_length: int | None,
    ):
        self._source = source
        self._temporary = temporary
        self._final = final
        self._metadata = metadata
        self._resource = resource
        self._digest = digest
        self._shard = shard
        self._target = temporary.open("wb")
        self._complete = False
        self._cache_enabled = True
        self._written = 0
        self._expected_length = expected_length
        self.from_cache = False

    def read(self, amount=None):
        if amount is not None and amount < 0:
            amount = None
        if amount == 0:
            return self._source.read(0)
        data = self._source.read(amount)
        if not self._cache_enabled:
            return data
        try:
            if data:
                written = self._target.write(data)
                if written != len(data):
                    raise OSError(f"short cache write: {written}/{len(data)} bytes")
                self._written += written
                if (
                    self._expected_length is not None
                    and self._written >= self._expected_length
                ):
                    self._finish()
            elif not self._complete:
                self._finish()
        except OSError as exc:
            self._abort_cache(exc)
        return data

    def iter_chunks(self, chunk_size=1024):
        while chunk := self.read(chunk_size):
            yield chunk

    def iter_lines(self, chunk_size=1024, keepends=False):
        return _iter_body_lines(self, chunk_size, keepends)

    def __iter__(self):
        return self.iter_chunks(1024)

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def readable(self):
        return True

    def _finish(self):
        if not self._cache_enabled or self._complete:
            return
        if self._expected_length is not None and self._written != self._expected_length:
            self._abort_cache(
                OSError(
                    "incomplete cache body: "
                    f"{self._written}/{self._expected_length} bytes"
                )
            )
            return
        try:
            self._target.flush()
            os.fsync(self._target.fileno())
            self._target.close()
            os.replace(self._temporary, self._final)
            _write_metadata(self._final, self._metadata)
        except OSError as exc:
            self._abort_cache(exc)
            return
        self._complete = True
        self._cache_enabled = False
        _log(LOG_FILE, self._resource, "MISS", self._digest, self._shard, self._written)

    def _abort_cache(self, exc: OSError | None = None):
        if not self._cache_enabled:
            return
        self._cache_enabled = False
        try:
            self._target.close()
        except OSError:
            pass
        try:
            self._temporary.unlink(missing_ok=True)
        except OSError:
            pass
        # If the complete object was already atomically published but its
        # metadata write failed, retain the valid body. Deleting it here could
        # race with another writer that has just published the same cache key.
        if exc is not None:
            _log(
                ERROR_FILE,
                self._resource,
                f"MISS_NO_CACHE:{type(exc).__name__}",
                self._digest,
                self._shard,
            )

    def close(self):
        try:
            self._source.close()
        finally:
            if not self._complete:
                self._abort_cache()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __getattr__(self, name):
        return getattr(self._source, name)


def _metadata_path(path: Path) -> Path:
    return Path(f"{path}.meta.json")


def _serializable_metadata(response: dict) -> dict:
    excluded = {"Body", "ResponseMetadata", "ContentLength", "ContentRange"}
    result = {key: value for key, value in response.items() if key not in excluded}
    return json.loads(json.dumps(result, default=str))


def _write_metadata(path: Path, metadata: dict) -> None:
    target = _metadata_path(path)
    temporary = Path(f"{target}.{random.getrandbits(64):016x}.tmp")
    try:
        temporary.write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _read_metadata(path: Path) -> dict:
    try:
        metadata = json.loads(_metadata_path(path).read_text(encoding="utf-8"))
        for key in ("LastModified", "Expires", "ObjectLockRetainUntil"):
            if isinstance(metadata.get(key), str):
                try:
                    metadata[key] = dt.datetime.fromisoformat(metadata[key])
                except ValueError:
                    pass
        return metadata
    except (OSError, json.JSONDecodeError):
        return {}


def _freshness_mtime(path: Path) -> float:
    """Return cache-validation time without repurposing the LRU marker."""
    object_mtime = path.stat().st_mtime
    try:
        return max(object_mtime, _metadata_path(path).stat().st_mtime)
    except OSError:
        return object_mtime


def _cached_response(
    path: Path,
    requested_range: str | None,
    resource: str,
    digest: str,
    shard: int,
    previous_owner: bool = False,
) -> dict:
    stat_result = path.stat()
    size = stat_result.st_size
    start, end = _parse_range(requested_range, size)
    stream = path.open("rb")
    _touch_access_time(path)
    metadata = _read_metadata(path)
    if requested_range:
        stream.seek(start)
        length = max(0, end - start + 1)
        status = 206
        reason = "HIT_RANGE_PREVIOUS" if previous_owner else "HIT_RANGE"
    else:
        length = size
        status = 200
        reason = "HIT_PREVIOUS" if previous_owner else "HIT"
    response = {
        **metadata,
        "Body": CachedBody(stream, length),
        "ContentLength": length,
        "AcceptRanges": "bytes",
        "ResponseMetadata": {
            "HTTPStatusCode": status,
            "HTTPHeaders": {"content-length": str(length), "accept-ranges": "bytes"},
        },
    }
    if requested_range:
        content_range = f"bytes {start}-{end}/{size}"
        response["ContentRange"] = content_range
        response["ResponseMetadata"]["HTTPHeaders"]["content-range"] = content_range
    _log(LOG_FILE, resource, reason, digest, shard, size)
    return response


def _etag_unchanged(client, kwargs: dict, path: Path) -> bool:
    metadata = _read_metadata(path)
    old_etag = metadata.get("ETag")
    if not old_etag:
        return False
    head_kwargs = {
        key: value
        for key, value in kwargs.items()
        if key
        in {
            "Bucket",
            "Key",
            "VersionId",
            "RequestPayer",
            "ExpectedBucketOwner",
            "ChecksumMode",
        }
    }
    try:
        head = _ORIGINAL_CALL(client, "HeadObject", head_kwargs)
    except Exception:
        return False
    if head.get("ETag") == old_etag:
        # Rewrite metadata atomically to refresh the validation timestamp.
        # This works for readers with different UIDs because the containing
        # cache directory is deliberately shared-writable.
        refreshed_metadata = dict(metadata)
        refreshed_metadata.update(_serializable_metadata(head))
        _write_metadata(path, _serializable_metadata(refreshed_metadata))
        return True
    return False


def _patched_call(client, operation_name: str, kwargs: dict):
    if operation_name != "GetObject" or client.meta.service_model.service_name != "s3":
        return _ORIGINAL_CALL(client, operation_name, kwargs)
    if kwargs.get("Range") and not CACHE_RANGE_GETS:
        return _ORIGINAL_CALL(client, operation_name, kwargs)
    # Conditional, requester-pays, SSE-C, response-override, part-number, and
    # future GetObject options must retain Botocore/S3 authorization semantics.
    if set(kwargs) - _CACHEABLE_GET_OPTIONS:
        return _ORIGINAL_CALL(client, operation_name, kwargs)

    bucket, key = kwargs.get("Bucket"), kwargs.get("Key")
    if not bucket or key is None:
        return _ORIGINAL_CALL(client, operation_name, kwargs)
    version_id = kwargs.get("VersionId")
    resource = f"s3://{bucket}/{key}"
    if version_id is not None:
        resource = f"{resource}?versionId={version_id}"
    path, digest, shard = _object_location(client, bucket, key, version_id)
    previous_path = _previous_object_location(
        client, bucket, key, version_id, path
    )
    if path is None and previous_path is None:
        _log(ERROR_FILE, resource, "MISS_MOUNT_NA", digest, shard)
        return _ORIGINAL_CALL(client, operation_name, kwargs)

    for candidate, previous_owner in ((path, False), (previous_path, True)):
        if candidate is None:
            continue
        try:
            if candidate.exists():
                fresh = time.time() - _freshness_mtime(candidate) <= MAX_CACHE_AGE
                if fresh or _etag_unchanged(client, kwargs, candidate):
                    return _cached_response(
                        candidate,
                        kwargs.get("Range"),
                        resource,
                        digest,
                        shard,
                        previous_owner=previous_owner,
                    )
        except (OSError, ValueError):
            continue

    # A missing active mount can still serve a previous-owner hit, but it
    # cannot safely publish a new cache entry on a network miss.
    if path is None:
        _log(ERROR_FILE, resource, "MISS_MOUNT_NA", digest, shard)
        return _ORIGINAL_CALL(client, operation_name, kwargs)

    network_kwargs = dict(kwargs)
    requested_range = network_kwargs.pop("Range", None)
    response = _ORIGINAL_CALL(client, operation_name, network_kwargs)
    if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
        return response

    temporary: Path | None = None
    try:
        _ensure_cache_parent(path)
        temporary = Path(f"{path}.{random.getrandbits(64):016x}.tmp")
        metadata = _serializable_metadata(response)
        if requested_range:
            try:
                _parse_range(requested_range, int(response.get("ContentLength", 0)))
            except (TypeError, ValueError):
                response["Body"].close()
                return _ORIGINAL_CALL(client, operation_name, kwargs)
            written = 0
            with temporary.open("wb") as target:
                while block := response["Body"].read(1024 * 1024):
                    block_written = target.write(block)
                    if block_written != len(block):
                        raise OSError(
                            f"short cache write: {block_written}/{len(block)} bytes"
                        )
                    written += block_written
            response["Body"].close()
            expected_length = int(response.get("ContentLength", 0))
            if written != expected_length:
                raise OSError(
                    f"incomplete range-prefetch body: {written}/{expected_length} bytes"
                )
            os.replace(temporary, path)
            _write_metadata(path, metadata)
            _log(LOG_FILE, resource, "MISS", digest, shard, path.stat().st_size)
            return _cached_response(path, requested_range, resource, digest, shard)
        expected_length = response.get("ContentLength")
        response["Body"] = TeeBody(
            response["Body"],
            temporary,
            path,
            metadata,
            resource,
            digest,
            shard,
            int(expected_length) if expected_length is not None else None,
        )
        return response
    except OSError as exc:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        _log(ERROR_FILE, resource, f"MISS_NO_CACHE:{type(exc).__name__}", digest, shard)
        if requested_range:
            # The full-object prefetch may already have consumed or closed the
            # first response. Reissue the caller's original ranged request so
            # a cache write failure never returns a broken response body.
            try:
                response["Body"].close()
            except Exception:
                pass
            return _ORIGINAL_CALL(client, operation_name, kwargs)
        return response
    except Exception as exc:
        if not requested_range:
            raise
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        _log(
            ERROR_FILE,
            resource,
            f"MISS_PREFETCH_FAILED:{type(exc).__name__}",
            digest,
            shard,
        )
        try:
            response["Body"].close()
        except Exception:
            pass
        # Range prefetch is an optimization. If consuming the speculative
        # full-object response fails for any reason, preserve application
        # semantics by issuing the original ranged request.
        return _ORIGINAL_CALL(client, operation_name, kwargs)


_refresh_maps(force=True)
botocore.client.BaseClient._make_api_call = _patched_call
