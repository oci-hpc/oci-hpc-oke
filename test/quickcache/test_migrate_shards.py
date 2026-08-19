import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


RUNTIME_PATH = (
    Path(__file__).resolve().parents[2] / "terraform/files/oci-quickcache/files"
)
SPEC = importlib.util.spec_from_file_location(
    "quickcache_migrate_shards", RUNTIME_PATH / "migrate_shards.py"
)
migrate_shards = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migrate_shards)


class MigrateShardsTests(unittest.TestCase):
    def test_estimate_reports_files_and_bytes_without_copying(self):
        source_uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        target_uid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        plan = {
            "generation": 4,
            "moves": [{"shard": "1", "source": source_uid, "target": target_uid}],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local_root = root / "local"
            source = local_root / "OCI_QC_Cache" / "0001"
            source.mkdir(parents=True)
            (source / "object").write_bytes(b"warm-data")
            (source / "object.meta.json").write_bytes(b"{}")

            with mock.patch.object(
                migrate_shards,
                "_safe_root",
                side_effect=lambda path, _base, _name: Path(path),
            ):
                estimate = migrate_shards.migrate(
                    plan,
                    {},
                    source_uid,
                    str(local_root),
                    "OCI_QC_Cache",
                    "estimate",
                    0o777,
                )

            self.assertEqual(estimate["shards"], 1)
            self.assertEqual(estimate["files_scanned"], 2)
            self.assertEqual(estimate["bytes_scanned"], 11)
            self.assertFalse((root / "target").exists())

    def test_copy_then_cleanup_preserves_warm_files_before_source_removal(self):
        source_uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        target_uid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        plan = {
            "generation": 4,
            "moves": [{"shard": "1", "source": source_uid, "target": target_uid}],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local_root = root / "local"
            target_mount = root / "target"
            source = local_root / "OCI_QC_Cache" / "0001"
            source.mkdir(parents=True)
            (source / "object").write_bytes(b"warm-data")
            (source / "object.meta.json").write_text("{}", encoding="utf-8")
            peers = {target_uid: {"mountPath": str(target_mount)}}

            with mock.patch.object(
                migrate_shards,
                "_safe_root",
                side_effect=lambda path, _base, _name: Path(path),
            ):
                copied = migrate_shards.migrate(
                    plan,
                    peers,
                    source_uid,
                    str(local_root),
                    "OCI_QC_Cache",
                    "copy",
                    0o777,
                )
                self.assertTrue(source.exists())
                self.assertEqual(copied["files_copied"], 2)
                self.assertEqual(
                    (target_mount / "OCI_QC_Cache/0001/object").read_bytes(),
                    b"warm-data",
                )

                cleaned = migrate_shards.migrate(
                    plan,
                    peers,
                    source_uid,
                    str(local_root),
                    "OCI_QC_Cache",
                    "cleanup",
                    0o777,
                )

            self.assertFalse(source.exists())
            self.assertEqual(cleaned["shards_removed"], 1)

    def test_incomplete_source_write_blocks_cleanup(self):
        source_uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        target_uid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        plan = {
            "generation": 4,
            "moves": [{"shard": "1", "source": source_uid, "target": target_uid}],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local_root = root / "local"
            target_mount = root / "target"
            source = local_root / "OCI_QC_Cache" / "0001"
            source.mkdir(parents=True)
            (source / "object.tmp").write_bytes(b"partial")
            peers = {target_uid: {"mountPath": str(target_mount)}}

            with (
                mock.patch.object(
                    migrate_shards,
                    "_safe_root",
                    side_effect=lambda path, _base, _name: Path(path),
                ),
                self.assertRaisesRegex(RuntimeError, "incomplete writes"),
            ):
                migrate_shards.migrate(
                    plan,
                    peers,
                    source_uid,
                    str(local_root),
                    "OCI_QC_Cache",
                    "cleanup",
                    0o2770,
                )

            self.assertTrue(source.exists())


if __name__ == "__main__":
    unittest.main()
