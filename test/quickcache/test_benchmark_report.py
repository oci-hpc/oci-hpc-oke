import importlib.util
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "manifests/quickcache/benchmark-report.py"
)
SPEC = importlib.util.spec_from_file_location("quickcache_benchmark_report", SCRIPT)
benchmark_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark_report)

KEY_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "manifests/quickcache/prepare-benchmark-keys.py"
)
KEY_SPEC = importlib.util.spec_from_file_location(
    "quickcache_prepare_benchmark_keys", KEY_SCRIPT
)
prepare_keys = importlib.util.module_from_spec(KEY_SPEC)
KEY_SPEC.loader.exec_module(prepare_keys)


class BenchmarkReportTests(unittest.TestCase):
    def test_prepared_keys_cover_every_cache_owner(self):
        shard_map = {"0": "owner-a", "1": "owner-b", "2": "owner-c"}

        keys = prepare_keys.select_keys(
            shard_map,
            "https://namespace.compat.objectstorage.region.oraclecloud.com",
            "benchmark",
            "quickcache-throughput",
            6,
        )
        scope = prepare_keys._endpoint_scope(
            "https://namespace.compat.objectstorage.region.oraclecloud.com"
        )
        owners = {
            prepare_keys._owner("benchmark", key, scope, shard_map)
            for key in keys
        }

        self.assertEqual(len(keys), 6)
        self.assertEqual(owners, {"owner-a", "owner-b", "owner-c"})

    def test_concurrent_aggregate_and_baseline_acceptance(self):
        records = [
            {
                "node": "node-a",
                "cache_owner": "owner-a",
                "total_bytes": 1024**3,
                "wall_started_epoch": 100.0,
                "wall_finished_epoch": 102.0,
                "throughput_mib_s": 512.0,
                "average_first_byte_ms": 2.0,
                "all_reads_from_cache": True,
            },
            {
                "node": "node-b",
                "cache_owner": "owner-b",
                "total_bytes": 1024**3,
                "wall_started_epoch": 100.0,
                "wall_finished_epoch": 102.0,
                "throughput_mib_s": 512.0,
                "average_first_byte_ms": 3.0,
                "all_reads_from_cache": True,
            },
        ]

        report = benchmark_report.summarize(
            records,
            expected_nodes=2,
            expected_cache_owners=2,
            minimum_aggregate_gib_s=0.9,
            maximum_p95_first_byte_ms=5.0,
            maximum_start_skew_seconds=1.0,
            baseline_aggregate_gib_s=1.0,
            minimum_parity_ratio=0.95,
        )

        self.assertTrue(report["passed"])
        self.assertEqual(report["aggregate_gib_s"], 1.0)
        self.assertEqual(report["parity_ratio"], 1.0)

    def test_missing_node_fails_acceptance(self):
        record = {
            "node": "node-a",
            "cache_owner": "owner-a",
            "total_bytes": 1024,
            "wall_started_epoch": 100.0,
            "wall_finished_epoch": 101.0,
            "throughput_mib_s": 1.0,
            "average_first_byte_ms": 2.0,
            "all_reads_from_cache": True,
        }

        report = benchmark_report.summarize([record], expected_nodes=2)

        self.assertFalse(report["passed"])
        self.assertFalse(report["checks"]["expected_node_count"])


if __name__ == "__main__":
    unittest.main()
