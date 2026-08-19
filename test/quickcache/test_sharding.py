import importlib.util
import sys
import unittest
from collections import Counter
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "terraform/files/oci-quickcache/files/sharding.py"
)
SPEC = importlib.util.spec_from_file_location("sharding", MODULE_PATH)
sharding = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sharding
SPEC.loader.exec_module(sharding)


class ShardingTests(unittest.TestCase):
    def test_initial_map_is_balanced(self):
        result = sharding.rebalance_shards({}, ["c", "a", "b"], 16)
        counts = Counter(result.values())
        self.assertEqual(len(result), 16)
        self.assertLessEqual(max(counts.values()) - min(counts.values()), 1)

    def test_adding_host_moves_only_required_shards(self):
        old = sharding.rebalance_shards({}, ["a", "b"], 16)
        new = sharding.rebalance_shards(old, ["a", "b", "c"], 16)
        moved = sum(old[shard] != new[shard] for shard in old)
        self.assertEqual(moved, 5)

    def test_removing_host_preserves_surviving_assignments(self):
        old = sharding.rebalance_shards({}, ["a", "b", "c"], 16)
        new = sharding.rebalance_shards(old, ["a", "c"], 16)
        for shard, host in old.items():
            if host in {"a", "c"}:
                self.assertEqual(new[shard], host)

    def test_resolve_shard_mounts_converts_node_ids_to_paths(self):
        result = sharding.resolve_shard_mounts(
            {"0": "node-a", "1": "node-b"},
            {
                "node-a": {"mountPath": "/var/lib/ociqc/mounts/node-a"},
                "node-b": {"mountPath": "/var/lib/ociqc/mounts/node-b"},
            },
        )
        self.assertEqual(
            result,
            {
                "0": "/var/lib/ociqc/mounts/node-a",
                "1": "/var/lib/ociqc/mounts/node-b",
            },
        )

    def test_resolve_shard_mounts_rejects_missing_peer(self):
        with self.assertRaisesRegex(ValueError, "without an absolute mount path"):
            sharding.resolve_shard_mounts({"0": "missing"}, {})


if __name__ == "__main__":
    unittest.main()
