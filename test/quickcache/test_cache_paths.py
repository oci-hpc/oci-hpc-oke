import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "terraform/files/oci-quickcache/files/cache_paths.py"
)
SPEC = importlib.util.spec_from_file_location("cache_paths", MODULE_PATH)
cache_paths = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cache_paths
SPEC.loader.exec_module(cache_paths)


class CachePathTests(unittest.TestCase):
    def test_object_key_cannot_escape_cache_root(self):
        digest = cache_paths.resource_hash("bucket", "../../etc/passwd")
        path = cache_paths.cache_path(
            "/var/lib/ociqc/mounts/node",
            "OCI_QC_Cache",
            7,
            "us-ashburn-1",
            "bucket",
            digest,
        )
        self.assertTrue(path.startswith("/var/lib/ociqc/mounts/node/OCI_QC_Cache/"))
        self.assertNotIn("..", Path(path).parts)
        self.assertNotIn("passwd", path)

    def test_shard_is_stable(self):
        first = cache_paths.shard_for_resource("bucket", "object", 1024)
        second = cache_paths.shard_for_resource("bucket", "object", 1024)
        self.assertEqual(first, second)
        self.assertGreaterEqual(first[0], 0)
        self.assertLess(first[0], 1024)

    def test_versioned_objects_do_not_collide(self):
        first = cache_paths.resource_hash("bucket", "object", "version-1")
        second = cache_paths.resource_hash("bucket", "object", "version-2")
        latest = cache_paths.resource_hash("bucket", "object")
        self.assertEqual(len({first, second, latest}), 3)

    def test_endpoint_namespaces_do_not_collide(self):
        first = cache_paths.resource_hash(
            "bucket", "object", endpoint_scope="https://namespace-a.example"
        )
        second = cache_paths.resource_hash(
            "bucket", "object", endpoint_scope="https://namespace-b.example"
        )
        self.assertNotEqual(first, second)

    def test_friendly_layout_preserves_recognizable_key_hierarchy(self):
        digest = cache_paths.resource_hash(
            "bucket", "models/llama/checkpoints/model 001.bin"
        )
        path = cache_paths.cache_path(
            "/var/lib/ociqc/mounts/node",
            "OCI_QC_Cache",
            7,
            "us-ashburn-1",
            "bucket",
            digest,
            key="models/llama/checkpoints/model 001.bin",
            layout="friendly",
        )
        self.assertEqual(
            path,
            "/var/lib/ociqc/mounts/node/OCI_QC_Cache/0007/v2/"
            "us-ashburn-1/bucket/models/llama/checkpoints/"
            f"model%20001.bin.__qc_{digest[:24]}",
        )

    def test_friendly_layout_encodes_traversal_and_unsafe_characters(self):
        digest = cache_paths.resource_hash("bucket", "../../a b/#object")
        path = cache_paths.cache_path(
            "/cache",
            "OCI_QC_Cache",
            9,
            "region",
            "bucket",
            digest,
            key="../../a b/#object",
            layout="friendly",
        )
        self.assertNotIn("..", Path(path).parts)
        self.assertIn("%2E%2E", Path(path).parts)
        self.assertIn("a%20b", Path(path).parts)
        self.assertTrue(Path(path).name.startswith("%23object.__qc_"))
        self.assertTrue(path.startswith("/cache/OCI_QC_Cache/0009/v2/"))

    def test_friendly_layout_preserves_empty_and_dot_components_safely(self):
        key = "/models//./checkpoint/"
        digest = cache_paths.resource_hash("bucket", key)
        path = cache_paths.cache_path(
            "/cache",
            "OCI_QC_Cache",
            2,
            "region",
            "bucket",
            digest,
            key=key,
            layout="friendly",
        )
        relative = Path(path).parts
        self.assertEqual(relative.count("!empty"), 2)
        self.assertIn("%2E", relative)
        self.assertTrue(Path(path).name.startswith("!empty.__qc_"))

    def test_friendly_suffix_separates_endpoints_and_versions(self):
        first = cache_paths.resource_hash(
            "bucket", "object", endpoint_scope="https://namespace-a.example"
        )
        second = cache_paths.resource_hash(
            "bucket", "object", endpoint_scope="https://namespace-b.example"
        )
        versioned = cache_paths.resource_hash(
            "bucket",
            "object",
            version_id="version-1",
            endpoint_scope="https://namespace-a.example",
        )
        names = {
            Path(
                cache_paths.cache_path(
                    "/cache",
                    "OCI_QC_Cache",
                    1,
                    "region",
                    "bucket",
                    digest,
                    key="object",
                    layout="friendly",
                )
            ).name
            for digest in (first, second, versioned)
        }
        self.assertEqual(len(names), 3)

    def test_friendly_layout_bounds_long_components_and_deep_keys(self):
        long_key = "/".join(["directory" * 30] * 30 + ["object" * 50])
        digest = cache_paths.resource_hash("bucket", long_key)
        path = cache_paths.cache_path(
            "/var/lib/ociqc/mounts/node",
            "OCI_QC_Cache",
            1,
            "region",
            "bucket",
            digest,
            key=long_key,
            layout="friendly",
        )
        self.assertLessEqual(len(path.encode()), cache_paths.MAX_CACHE_PATH_LENGTH)
        self.assertIn("__long_keys__", Path(path).parts)
        self.assertLessEqual(max(len(part.encode()) for part in Path(path).parts), 255)
        self.assertTrue(Path(path).name.endswith(digest[:24]))

    def test_candidates_prefer_friendly_and_retain_legacy_hash_path(self):
        digest = cache_paths.resource_hash("bucket", "path/object")
        friendly, legacy = cache_paths.cache_path_candidates(
            "/cache",
            "OCI_QC_Cache",
            4,
            "region",
            "bucket",
            "path/object",
            digest,
            "friendly",
        )
        self.assertIn("/v2/region/bucket/path/object.__qc_", friendly)
        self.assertEqual(
            legacy,
            f"/cache/OCI_QC_Cache/0004/region/bucket/{digest[:2]}/{digest[2:]}",
        )


if __name__ == "__main__":
    unittest.main()
