import importlib.util
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "terraform/files/oci-quickcache/files/cleanup.py"
)
SPEC = importlib.util.spec_from_file_location("cleanup", MODULE_PATH)
cleanup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cleanup
SPEC.loader.exec_module(cleanup)


class CleanupTests(unittest.TestCase):
    def test_parse_positive_integer_accepts_scientific_notation(self):
        self.assertEqual(
            cleanup.parse_positive_integer("5e+08", "OCI_QC_MAX_CACHE_FILES"),
            500000000,
        )

    def test_parse_positive_integer_rejects_fractional_values(self):
        with self.assertRaisesRegex(ValueError, "must be a positive integer"):
            cleanup.parse_positive_integer("12.5", "OCI_QC_MAX_CACHE_FILES")

    def test_cleanup_removes_oldest_object_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            old = cache / "old"
            new = cache / "new"
            old.write_bytes(b"a" * 70)
            Path(f"{old}.meta.json").write_text("{}", encoding="utf-8")
            new.write_bytes(b"b" * 20)
            old.touch()
            new.touch()
            old_stat = old.stat()
            new_stat = new.stat()
            # Ensure deterministic LRU order.
            old_time = old_stat.st_atime - 100
            Path(old).touch()
            import os

            os.utime(old, (old_time, old_time))
            os.utime(new, (new_stat.st_atime, new_stat.st_mtime))

            usage = mock.Mock(
                f_blocks=100,
                f_bavail=5,
                f_files=1000,
                f_favail=900,
                f_frsize=1,
            )
            with mock.patch.object(cleanup.os, "statvfs", return_value=usage):
                removed = cleanup.cleanup_cache(cache, 0.90, 0.70, 1000)

            self.assertEqual(removed, 1)
            self.assertFalse(old.exists())
            self.assertFalse(Path(f"{old}.meta.json").exists())
            self.assertTrue(new.exists())

    def test_cleanup_accounts_for_non_cache_disk_usage(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            first = cache / "first"
            second = cache / "second"
            first.write_bytes(b"a" * 10)
            second.write_bytes(b"b" * 10)

            usage = mock.Mock(
                f_blocks=100,
                f_bavail=5,
                f_files=1000,
                f_favail=900,
                f_frsize=1,
            )
            with mock.patch.object(cleanup.os, "statvfs", return_value=usage):
                removed = cleanup.cleanup_cache(cache, 0.90, 0.70, 1000)

            # The filesystem needs 25 bytes freed even though the cache only
            # contains 20 bytes, so cleanup must remove everything it can.
            self.assertEqual(removed, 2)

    def test_cleanup_uses_explicit_access_sidecar_for_lru(self):
        import os

        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            recently_accessed = cache / "old-object"
            never_accessed = cache / "new-object"
            recently_accessed.write_bytes(b"a" * 20)
            never_accessed.write_bytes(b"b" * 20)
            old_time = time.time() - 200
            newer_time = time.time() - 100
            os.utime(recently_accessed, (old_time, old_time))
            os.utime(never_accessed, (newer_time, newer_time))
            access_path = Path(f"{recently_accessed}.access")
            access_path.touch()

            usage = mock.Mock(
                f_blocks=100,
                f_bavail=20,
                f_files=1000,
                f_favail=900,
                f_frsize=1,
            )
            with mock.patch.object(cleanup.os, "statvfs", return_value=usage):
                removed = cleanup.cleanup_cache(cache, 0.70, 0.70, 1000)

            self.assertEqual(removed, 1)
            self.assertTrue(recently_accessed.exists())
            self.assertFalse(never_accessed.exists())


if __name__ == "__main__":
    unittest.main()
