import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "terraform/files/oci-quickcache/files/healthcheck.py"
)

fake_kubernetes = types.ModuleType("kubernetes")
fake_kubernetes.client = types.ModuleType("kubernetes.client")
fake_kubernetes.config = types.ModuleType("kubernetes.config")
fake_rest = types.ModuleType("kubernetes.client.rest")


class FakeApiException(Exception):
    pass


fake_rest.ApiException = FakeApiException
saved_modules = {
    name: sys.modules.get(name)
    for name in ("kubernetes", "kubernetes.client", "kubernetes.client.rest")
}
sys.modules["kubernetes"] = fake_kubernetes
sys.modules["kubernetes.client"] = fake_kubernetes.client
sys.modules["kubernetes.client.rest"] = fake_rest

SPEC = importlib.util.spec_from_file_location("quickcache_healthcheck", MODULE_PATH)
healthcheck = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = healthcheck
SPEC.loader.exec_module(healthcheck)

for module_name, module in saved_modules.items():
    if module is None:
        sys.modules.pop(module_name, None)
    else:
        sys.modules[module_name] = module


def _node(name: str, uid: str, ready: bool = True, cache_ready: bool = True):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=uid,
            labels={"quickcache-ready": "true" if cache_ready else "false"},
        ),
        status=SimpleNamespace(
            conditions=[
                SimpleNamespace(type="Ready", status="True" if ready else "False")
            ]
        ),
    )


def _deployment(available: int = 1):
    return SimpleNamespace(
        spec=SimpleNamespace(replicas=1),
        status=SimpleNamespace(available_replicas=available),
    )


def _daemonset(desired: int = 2, ready: int = 2, updated: int = 2):
    return SimpleNamespace(
        status=SimpleNamespace(
            desired_number_scheduled=desired,
            number_ready=ready,
            updated_number_scheduled=updated,
        )
    )


class HealthcheckTests(unittest.TestCase):
    def setUp(self):
        self.nodes = [_node("node-a", "uid-a"), _node("node-b", "uid-b")]
        self.peers = {
            "uid-a": {"internalIP": "10.0.0.1", "mountPath": "/mounts/uid-a"},
            "uid-b": {"internalIP": "10.0.0.2", "mountPath": "/mounts/uid-b"},
        }
        self.shard_map = {"0": "uid-a", "1": "uid-b", "2": "uid-a", "3": "uid-b"}

    def evaluate(self, **overrides):
        values = {
            "nodes": self.nodes,
            "deployment": _deployment(),
            "daemonset": _daemonset(),
            "peers": self.peers,
            "shard_map": self.shard_map,
            "ready_label": "quickcache-ready",
            "virtual_shards": 4,
        }
        values.update(overrides)
        return healthcheck._evaluate(**values)

    def test_healthy_data_plane_is_ready(self):
        ready, reason = self.evaluate()
        self.assertTrue(ready)
        self.assertEqual(reason, "nodes=2 clients=0 peers=2 shards=4")

    def test_dedicated_clients_must_be_ready_and_fully_scheduled(self):
        client_nodes = [
            _node("client-a", "client-uid-a"),
            _node("client-b", "client-uid-b"),
        ]
        ready, reason = self.evaluate(
            client_nodes=client_nodes,
            client_daemonset=_daemonset(desired=2, ready=2, updated=2),
            client_ready_label="quickcache-ready",
        )
        self.assertTrue(ready)
        self.assertEqual(reason, "nodes=2 clients=2 peers=2 shards=4")

        client_nodes[1].metadata.labels["quickcache-ready"] = "false"
        ready, reason = self.evaluate(
            client_nodes=client_nodes,
            client_daemonset=_daemonset(desired=2, ready=2, updated=2),
            client_ready_label="quickcache-ready",
        )
        self.assertFalse(ready)
        self.assertIn("client-b", reason)

    def test_unready_node_blocks_install(self):
        ready, reason = self.evaluate(
            nodes=[
                _node("node-a", "uid-a"),
                _node("node-b", "uid-b", cache_ready=False),
            ]
        )
        self.assertFalse(ready)
        self.assertIn("node-b", reason)

    def test_incomplete_daemonset_blocks_install(self):
        ready, reason = self.evaluate(daemonset=_daemonset(ready=1))
        self.assertFalse(ready)
        self.assertIn("rollout incomplete", reason)

    def test_unknown_shard_assignment_blocks_install(self):
        shard_map = dict(self.shard_map)
        shard_map["3"] = "uid-unknown"
        ready, reason = self.evaluate(shard_map=shard_map)
        self.assertFalse(ready)
        self.assertIn("outside the peer inventory", reason)

    def test_wrong_shard_keys_block_install(self):
        shard_map = dict(self.shard_map)
        shard_map["4"] = shard_map.pop("3")
        ready, reason = self.evaluate(shard_map=shard_map)
        self.assertFalse(ready)
        self.assertIn("keys are incomplete", reason)


if __name__ == "__main__":
    unittest.main()
