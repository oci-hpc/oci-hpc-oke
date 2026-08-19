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


if __name__ == "__main__":
    unittest.main()
