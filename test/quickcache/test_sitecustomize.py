import importlib.util
import io
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


RUNTIME_PATH = (
    Path(__file__).resolve().parents[2] / "terraform/files/oci-quickcache/files"
)
sys.path.insert(0, str(RUNTIME_PATH))

fake_botocore = types.ModuleType("botocore")
fake_client = types.ModuleType("botocore.client")


class FakeBaseClient:
    def _make_api_call(self, operation_name, kwargs):
        raise NotImplementedError


fake_client.BaseClient = FakeBaseClient
fake_botocore.client = fake_client
original_botocore = sys.modules.get("botocore")
original_botocore_client = sys.modules.get("botocore.client")
sys.modules["botocore"] = fake_botocore
sys.modules["botocore.client"] = fake_client

SPEC = importlib.util.spec_from_file_location(
    "quickcache_sitecustomize", RUNTIME_PATH / "sitecustomize.py"
)
sitecustomize = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sitecustomize
SPEC.loader.exec_module(sitecustomize)
if original_botocore is None:
    sys.modules.pop("botocore", None)
else:
    sys.modules["botocore"] = original_botocore
if original_botocore_client is None:
    sys.modules.pop("botocore.client", None)
else:
    sys.modules["botocore.client"] = original_botocore_client


class RangeTests(unittest.TestCase):
    def test_closed_and_open_ranges(self):
        self.assertEqual(sitecustomize._parse_range("bytes=2-5", 10), (2, 5))
        self.assertEqual(sitecustomize._parse_range("bytes=7-", 10), (7, 9))

    def test_suffix_range(self):
        self.assertEqual(sitecustomize._parse_range("bytes=-4", 10), (6, 9))

    def test_invalid_ranges_are_rejected(self):
        for value in ("bytes=-0", "bytes=10-", "bytes=5-2", "not-a-range"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    sitecustomize._parse_range(value, 10)


class BodyCompatibilityTests(unittest.TestCase):
    def test_cached_body_supports_iteration_lines_and_readinto(self):
        body = sitecustomize.CachedBody(io.BytesIO(b"one\ntwo\n"))
        self.assertEqual(list(body.iter_lines(chunk_size=3)), [b"one", b"two"])

        body = sitecustomize.CachedBody(io.BytesIO(b"abcdef"))
        target = bytearray(3)
        self.assertEqual(body.readinto(target), 3)
        self.assertEqual(bytes(target), b"abc")
        self.assertEqual(b"".join(body), b"def")
        self.assertTrue(body.readable())
        self.assertIsNone(body.set_socket_timeout(1))

        ranged_body = sitecustomize.CachedBody(io.BytesIO(b"abcdef"), remaining=3)
        self.assertEqual(ranged_body.tell(), 0)
        self.assertEqual(ranged_body.read(-1), b"abc")
        self.assertEqual(ranged_body.tell(), 3)
        self.assertEqual(ranged_body.read(), b"")

    def test_cached_body_rejects_premature_eof(self):
        body = sitecustomize.CachedBody(io.BytesIO(b"short"), remaining=10)
        self.assertEqual(body.read(), b"short")
        with self.assertRaisesRegex(OSError, "incomplete cached body"):
            body.read()

    def test_zero_length_read_does_not_disable_tee_caching(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temporary = root / "object.tmp"
            final = root / "object"
            body = sitecustomize.TeeBody(
                io.BytesIO(b"abcdef"),
                temporary,
                final,
                {},
                "s3://bucket/object",
                "digest",
                1,
                6,
            )
            self.assertEqual(body.read(0), b"")
            with mock.patch.object(sitecustomize, "_log"):
                self.assertEqual(body.read(), b"abcdef")
            self.assertTrue(final.exists())

    def test_iter_lines_handles_crlf_split_across_chunks(self):
        body = sitecustomize.CachedBody(io.BytesIO(b"one\r\ntwo\r\n"))
        self.assertEqual(
            list(body.iter_lines(chunk_size=4, keepends=True)),
            [b"one\r\n", b"two\r\n"],
        )

    def test_tee_body_populates_cache_and_supports_iteration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temporary = root / "object.tmp"
            final = root / "object"
            body = sitecustomize.TeeBody(
                io.BytesIO(b"abcdef"),
                temporary,
                final,
                {"ETag": "etag"},
                "s3://bucket/object",
                "digest",
                1,
                6,
            )
            with mock.patch.object(sitecustomize, "_log"):
                self.assertEqual(b"".join(body), b"abcdef")
            self.assertEqual(final.read_bytes(), b"abcdef")
            self.assertTrue(Path(f"{final}.meta.json").exists())

    def test_cache_write_failure_does_not_break_application_read(self):
        class FailingTarget:
            def write(self, _data):
                raise OSError("cache unavailable")

            def close(self):
                return None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temporary = root / "object.tmp"
            body = sitecustomize.TeeBody(
                io.BytesIO(b"network-data"),
                temporary,
                root / "object",
                {},
                "s3://bucket/object",
                "digest",
                1,
                len(b"network-data"),
            )
            body._target.close()
            body._target = FailingTarget()
            with mock.patch.object(sitecustomize, "_log"):
                self.assertEqual(body.read(), b"network-data")
            self.assertFalse(temporary.exists())

    def test_short_network_body_is_not_cached(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temporary = root / "object.tmp"
            final = root / "object"
            body = sitecustomize.TeeBody(
                io.BytesIO(b"short"),
                temporary,
                final,
                {},
                "s3://bucket/object",
                "digest",
                1,
                10,
            )
            with mock.patch.object(sitecustomize, "_log"):
                self.assertEqual(body.read(), b"short")
                self.assertEqual(body.read(), b"")
            self.assertFalse(temporary.exists())
            self.assertFalse(final.exists())


class CacheFilesystemTests(unittest.TestCase):
    def test_cache_directories_are_shared_writable(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory) / sitecustomize.CACHE_DIR_NAME
            cache_root.mkdir(mode=0o1777)
            path = cache_root / "0001" / "region" / "bucket" / "ab" / "object"
            sitecustomize._ensure_cache_parent(path)
            for parent in (path.parent, path.parent.parent, path.parent.parent.parent):
                self.assertEqual(parent.stat().st_mode & 0o777, 0o777)

    def test_cache_directories_support_slurm_style_shared_group_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory) / sitecustomize.CACHE_DIR_NAME
            cache_root.mkdir()
            path = cache_root / "0001" / "region" / "bucket" / "ab" / "object"
            with mock.patch.object(sitecustomize, "CACHE_DIRECTORY_MODE", 0o2770):
                sitecustomize._ensure_cache_parent(path)
            # macOS may clear setgid on directories owned by another group;
            # the portable contract here is group-only rwx permissions.
            self.assertEqual(path.parent.stat().st_mode & 0o777, 0o770)

    def test_audit_log_is_append_locked_and_uses_configured_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "cache_log.csv")
            with mock.patch.object(sitecustomize, "LOG_FILE_MODE", 0o660):
                sitecustomize._log(path, "s3://bucket/key", "HIT", "digest", 1, 4)
                sitecustomize._log(path, "s3://bucket/key", "HIT", "digest", 1, 4)
            lines = path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 3)
            self.assertEqual(path.stat().st_mode & 0o777, 0o660)

    def test_cached_response_records_access_without_changing_object_mtime(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "object"
            path.write_bytes(b"data")
            old_atime_ns = 1_000_000_000
            mtime_ns = 2_000_000_000
            os.utime(path, ns=(old_atime_ns, mtime_ns))
            with mock.patch.object(sitecustomize, "_log"):
                response = sitecustomize._cached_response(
                    path, None, "s3://bucket/object", "digest", 1
                )
            response["Body"].close()
            stat_result = path.stat()
            self.assertEqual(stat_result.st_mtime_ns, mtime_ns)
            self.assertTrue(Path(f"{path}.access").exists())

    def test_cold_range_cache_failure_reissues_original_range(self):
        calls = []

        def original_call(_client, _operation, kwargs):
            calls.append(dict(kwargs))
            if "Range" in kwargs:
                return {
                    "Body": io.BytesIO(b"ab"),
                    "ResponseMetadata": {"HTTPStatusCode": 206},
                }
            return {
                "Body": io.BytesIO(b"abcdef"),
                "ContentLength": 6,
                "ResponseMetadata": {"HTTPStatusCode": 200},
            }

        client = SimpleNamespace(
            meta=SimpleNamespace(
                service_model=SimpleNamespace(service_name="s3"),
                endpoint_url="https://namespace.example",
                region_name="test-region",
            )
        )
        request = {"Bucket": "bucket", "Key": "object", "Range": "bytes=0-1"}
        with (
            mock.patch.object(sitecustomize, "_ORIGINAL_CALL", original_call),
            mock.patch.object(
                sitecustomize,
                "_object_location",
                return_value=(
                    Path("/proc") / sitecustomize.CACHE_DIR_NAME / "0001" / "object",
                    "digest",
                    1,
                ),
            ),
            mock.patch.object(sitecustomize, "_log"),
        ):
            response = sitecustomize._patched_call(client, "GetObject", request)

        self.assertEqual(calls, [{"Bucket": "bucket", "Key": "object"}, request])
        self.assertEqual(response["Body"].read(), b"ab")

    def test_etag_revalidation_refreshes_metadata_not_object_mtime(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "object"
            path.write_bytes(b"data")
            sitecustomize._write_metadata(path, {"ETag": '"etag"'})
            old_ns = 2_000_000_000
            os.utime(path, ns=(old_ns, old_ns))
            os.utime(sitecustomize._metadata_path(path), ns=(old_ns, old_ns))

            with mock.patch.object(
                sitecustomize,
                "_ORIGINAL_CALL",
                return_value={"ETag": '"etag"'},
            ):
                unchanged = sitecustomize._etag_unchanged(
                    object(), {"Bucket": "bucket", "Key": "object"}, path
                )

            self.assertTrue(unchanged)
            self.assertEqual(path.stat().st_mtime_ns, old_ns)
            self.assertGreater(
                sitecustomize._metadata_path(path).stat().st_mtime_ns, old_ns
            )
            self.assertGreater(sitecustomize._freshness_mtime(path), old_ns / 1e9)

    def test_previous_owner_serves_hit_during_rebalance_fallback(self):
        client = SimpleNamespace(
            meta=SimpleNamespace(
                service_model=SimpleNamespace(service_name="s3"),
                endpoint_url="https://namespace.example",
                region_name="test-region",
            )
        )
        with tempfile.TemporaryDirectory() as directory:
            previous = Path(directory) / "previous-object"
            previous.write_bytes(b"warm-data")
            reasons = []

            def record_log(_path, _resource, reason, *_args):
                reasons.append(reason)

            with (
                mock.patch.object(
                    sitecustomize,
                    "_ORIGINAL_CALL",
                    side_effect=AssertionError("Object Storage must not be called"),
                ),
                mock.patch.object(
                    sitecustomize,
                    "_object_location",
                    return_value=(Path(directory) / "new-owner", "digest", 7),
                ),
                mock.patch.object(
                    sitecustomize,
                    "_previous_object_location",
                    return_value=previous,
                ),
                mock.patch.object(sitecustomize, "_log", side_effect=record_log),
            ):
                response = sitecustomize._patched_call(
                    client,
                    "GetObject",
                    {"Bucket": "bucket", "Key": "object"},
                )

            self.assertEqual(response["Body"].read(), b"warm-data")
            response["Body"].close()
            self.assertIn("HIT_PREVIOUS", reasons)

    def test_cold_range_prefetch_exception_reissues_original_range(self):
        class FailingBody:
            def read(self, _amount=None):
                raise RuntimeError("network stream failed")

            def close(self):
                return None

        calls = []

        def original_call(_client, _operation, kwargs):
            calls.append(dict(kwargs))
            if "Range" in kwargs:
                return {
                    "Body": io.BytesIO(b"ab"),
                    "ResponseMetadata": {"HTTPStatusCode": 206},
                }
            return {
                "Body": FailingBody(),
                "ContentLength": 6,
                "ResponseMetadata": {"HTTPStatusCode": 200},
            }

        client = SimpleNamespace(
            meta=SimpleNamespace(
                service_model=SimpleNamespace(service_name="s3"),
                endpoint_url="https://namespace.example",
                region_name="test-region",
            )
        )
        request = {"Bucket": "bucket", "Key": "object", "Range": "bytes=0-1"}
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory) / sitecustomize.CACHE_DIR_NAME
            cache_root.mkdir()
            cache_path = cache_root / "0001" / "object"
            with (
                mock.patch.object(sitecustomize, "_ORIGINAL_CALL", original_call),
                mock.patch.object(
                    sitecustomize,
                    "_object_location",
                    return_value=(cache_path, "digest", 1),
                ),
                mock.patch.object(sitecustomize, "_log"),
            ):
                response = sitecustomize._patched_call(client, "GetObject", request)

        self.assertEqual(calls, [{"Bucket": "bucket", "Key": "object"}, request])
        self.assertEqual(response["Body"].read(), b"ab")


if __name__ == "__main__":
    unittest.main()
