import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


RUNTIME_PATH = (
    Path(__file__).resolve().parents[2] / "terraform/files/oci-quickcache/files"
)
sys.path.insert(0, str(RUNTIME_PATH))

fake_kubernetes = types.ModuleType("kubernetes")
fake_kubernetes.client = types.ModuleType("kubernetes.client")
fake_kubernetes.config = types.ModuleType("kubernetes.config")
fake_rest = types.ModuleType("kubernetes.client.rest")


class FakeApiException(Exception):
    def __init__(self, status=None):
        super().__init__(f"status={status}")
        self.status = status


fake_rest.ApiException = FakeApiException
saved_modules = {
    name: sys.modules.get(name)
    for name in ("kubernetes", "kubernetes.client", "kubernetes.client.rest")
}
sys.modules["kubernetes"] = fake_kubernetes
sys.modules["kubernetes.client"] = fake_kubernetes.client
sys.modules["kubernetes.client.rest"] = fake_rest

SPEC = importlib.util.spec_from_file_location(
    "quickcache_controller", RUNTIME_PATH / "controller.py"
)
controller = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = controller
SPEC.loader.exec_module(controller)

for module_name, module in saved_modules.items():
    if module is None:
        sys.modules.pop(module_name, None)
    else:
        sys.modules[module_name] = module


def _node(
    name: str,
    uid: str,
    heartbeat: str,
    ready: bool = True,
    cache_ready: bool = False,
):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=uid,
            annotations={"heartbeat": heartbeat},
            labels={"quickcache-ready": "true" if cache_ready else "false"},
        ),
        status=SimpleNamespace(
            conditions=[
                SimpleNamespace(type="Ready", status="True" if ready else "False")
            ],
            addresses=[SimpleNamespace(type="InternalIP", address="10.0.0.1")],
        ),
    )


class ControllerTests(unittest.TestCase):
    def test_healthy_peers_require_current_heartbeat_and_ready_node(self):
        core = SimpleNamespace(
            list_node=lambda **_kwargs: SimpleNamespace(
                items=[
                    _node("healthy", "uid-healthy", "1000"),
                    _node("stale", "uid-stale", "800"),
                    _node("future", "uid-future", "1200"),
                    _node("unready", "uid-unready", "1000", ready=False),
                ]
            )
        )
        with mock.patch.object(controller.time, "time", return_value=1000):
            peers = controller._healthy_peers(
                core,
                "enabled",
                "heartbeat",
                120,
                "/var/lib/ociqc/mounts",
            )

        self.assertEqual(set(peers), {"uid-healthy"})
        self.assertEqual(
            peers["uid-healthy"]["mountPath"],
            "/var/lib/ociqc/mounts/uid-healthy",
        )

    def test_stale_ready_labels_are_cleared(self):
        nodes = [
            _node("healthy", "uid-healthy", "1000", cache_ready=True),
            _node("stale", "uid-stale", "800", cache_ready=True),
            _node("disabled", "uid-disabled", "1000", cache_ready=True),
            _node("already-unready", "uid-unready", "800", cache_ready=False),
        ]
        patches = []
        selectors = []

        def list_nodes(**kwargs):
            selectors.append(kwargs["label_selector"])
            return SimpleNamespace(items=nodes)

        core = SimpleNamespace(
            list_node=list_nodes,
            patch_node=lambda name, body: patches.append((name, body)),
        )

        controller._clear_stale_ready_labels(
            core,
            "quickcache-ready",
            {"uid-healthy"},
        )

        self.assertEqual(selectors, ["quickcache-ready=true"])
        self.assertEqual(
            patches,
            [
                (
                    "stale",
                    {"metadata": {"labels": {"quickcache-ready": "false"}}},
                ),
                (
                    "disabled",
                    {"metadata": {"labels": {"quickcache-ready": "false"}}},
                ),
            ],
        )

    def test_non_object_shard_state_is_rebuilt(self):
        configmap = SimpleNamespace(
            data={"shard_map.json": "[]"},
            metadata=SimpleNamespace(resource_version="42"),
        )
        core = SimpleNamespace(
            read_namespaced_config_map=lambda *_args, **_kwargs: configmap
        )

        state, resource_version, annotations = controller._load_state(
            core, "ns", "state"
        )

        self.assertEqual(state["active"], {})
        self.assertEqual(resource_version, "42")
        self.assertEqual(annotations, {})

    def test_automatic_rebalance_keeps_active_map_until_copy_completes(self):
        state = {
            "peers": {},
            "active": {"0": "a", "1": "a"},
            "pending": {},
            "previous": {},
            "rebalance": {},
            "generation": 0,
        }
        desired = {"0": "a", "1": "b"}
        controller._staged_rebalance(
            SimpleNamespace(),
            state,
            {"a": {}, "b": {}},
            desired,
            "automatic",
            "enabled",
            "approval",
            "migration",
            {},
            3600,
            100,
        )

        self.assertEqual(state["active"], {"0": "a", "1": "a"})
        self.assertEqual(state["pending"], desired)
        self.assertEqual(state["rebalance"]["phase"], "migrating")
        self.assertEqual(
            state["rebalance"]["moves"],
            [{"shard": "1", "source": "a", "target": "b"}],
        )

        with mock.patch.object(controller, "_migration_complete", return_value=True):
            controller._staged_rebalance(
                SimpleNamespace(),
                state,
                {"a": {}, "b": {}},
                desired,
                "automatic",
                "enabled",
                "approval",
                "migration",
                {},
                3600,
                200,
            )

        self.assertEqual(state["active"], desired)
        self.assertEqual(state["previous"], {"0": "a", "1": "a"})
        self.assertEqual(state["rebalance"]["phase"], "fallback")
        self.assertEqual(state["rebalance"]["cleanupAfter"], 3800)

    def test_manual_rebalance_waits_for_matching_generation_approval(self):
        state = {
            "peers": {},
            "active": {"0": "a", "1": "a"},
            "pending": {},
            "previous": {},
            "rebalance": {},
            "generation": 8,
        }
        desired = {"0": "a", "1": "b"}
        args = (
            SimpleNamespace(),
            state,
            {"a": {}, "b": {}},
            desired,
            "manual",
            "enabled",
            "approval",
            "migration",
        )
        controller._staged_rebalance(*args, {}, 3600, 100)
        self.assertEqual(state["rebalance"]["phase"], "awaitingApproval")
        self.assertEqual(state["generation"], 9)

        with mock.patch.object(
            controller, "_migration_estimate", return_value=None
        ):
            controller._staged_rebalance(
                *args,
                {"approval": state["rebalance"]["approvalToken"]},
                3600,
                105,
            )
        self.assertEqual(state["rebalance"]["phase"], "awaitingApproval")

        estimate = {
            "status": "complete",
            "sourceNodes": 1,
            "shards": 1,
            "files": 2,
            "bytes": 9,
            "perSource": {"a": {"shards": 1, "files": 2, "bytes": 9}},
        }
        with mock.patch.object(
            controller, "_migration_estimate", return_value=estimate
        ):
            controller._staged_rebalance(
                *args, {"approval": "wrong"}, 3600, 110
            )
        self.assertEqual(state["rebalance"]["phase"], "awaitingApproval")
        self.assertEqual(state["rebalance"]["estimate"]["bytes"], 9)

        with (
            mock.patch.object(
                controller, "_migration_estimate", return_value=estimate
            ),
            mock.patch.object(
                controller, "_migration_complete", return_value=False
            ),
        ):
            controller._staged_rebalance(
                *args,
                {"approval": state["rebalance"]["approvalToken"]},
                3600,
                120,
            )
        self.assertEqual(state["rebalance"]["phase"], "migrating")

    def test_lost_active_owner_fails_over_without_waiting_for_migration(self):
        state = {
            "peers": {},
            "active": {"0": "a", "1": "lost"},
            "pending": {"0": "a", "1": "b"},
            "previous": {},
            "rebalance": {"generation": 3, "phase": "migrating", "moves": []},
            "generation": 3,
        }
        desired = {"0": "a", "1": "b"}
        controller._staged_rebalance(
            SimpleNamespace(),
            state,
            {"a": {}, "b": {}},
            desired,
            "automatic",
            "enabled",
            "approval",
            "migration",
            {},
            3600,
            100,
        )
        self.assertEqual(state["active"], desired)
        self.assertEqual(state["rebalance"], {})

    def test_migration_estimate_requires_every_source_for_exact_plan(self):
        plan = {
            "generation": 7,
            "planId": "plan-7",
            "moves": [
                {"shard": "1", "source": "a", "target": "c"},
                {"shard": "2", "source": "b", "target": "c"},
            ],
        }

        def node(uid, status):
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    uid=uid,
                    annotations={"migration": json.dumps(status)},
                )
            )

        complete_nodes = [
            node(
                "a",
                {
                    "generation": 7,
                    "planId": "plan-7",
                    "phase": "estimate",
                    "status": "succeeded",
                    "shards": 1,
                    "files": 3,
                    "bytes": 100,
                },
            ),
            node(
                "b",
                {
                    "generation": 7,
                    "planId": "plan-7",
                    "phase": "estimate",
                    "status": "succeeded",
                    "shards": 1,
                    "files": 4,
                    "bytes": 200,
                },
            ),
        ]
        core = SimpleNamespace(
            list_node=lambda **_kwargs: SimpleNamespace(items=complete_nodes)
        )

        estimate = controller._migration_estimate(
            core, "enabled", "migration", plan
        )

        self.assertEqual(estimate["sourceNodes"], 2)
        self.assertEqual(estimate["shards"], 2)
        self.assertEqual(estimate["files"], 7)
        self.assertEqual(estimate["bytes"], 300)

        core.list_node = lambda **_kwargs: SimpleNamespace(items=complete_nodes[:1])
        self.assertIsNone(
            controller._migration_estimate(core, "enabled", "migration", plan)
        )

    def test_full_map_backup_is_timestamped_and_retention_is_bounded(self):
        created = []
        deleted = []
        selectors = []

        def metadata(**kwargs):
            return SimpleNamespace(**kwargs)

        old_backups = [
            SimpleNamespace(
                metadata=SimpleNamespace(
                    name=f"backup-{index}",
                    annotations={
                        "oci-hpc-oke.oracle.com/quickcache-backup-created-at": str(
                            index
                        )
                    },
                )
            )
            for index in (1, 2)
        ]
        core = SimpleNamespace(
            create_namespaced_config_map=lambda _namespace, body: created.append(
                body
            ),
            list_namespaced_config_map=lambda _namespace, **kwargs: (
                selectors.append(kwargs["label_selector"])
                or SimpleNamespace(items=old_backups + created)
            ),
            delete_namespaced_config_map=lambda name, _namespace: deleted.append(
                name
            ),
        )
        with (
            mock.patch.object(controller.client, "V1ObjectMeta", metadata, create=True),
            mock.patch.object(
                controller.client,
                "V1ConfigMap",
                lambda **kwargs: SimpleNamespace(**kwargs),
                create=True,
            ),
        ):
            controller._backup_shard_map(
                core,
                "kube-system",
                "oci-quickcache-state",
                {"0": "old"},
                {"0": "new"},
                4,
                2,
                1_700_000_000,
            )

        self.assertEqual(len(created), 1)
        backup_key = next(key for key in created[0].data if key.endswith(".bak"))
        self.assertEqual(json.loads(created[0].data[backup_key]), {"0": "old"})
        self.assertEqual(deleted, ["backup-1"])
        self.assertIn("state-backup-owner=", selectors[0])


if __name__ == "__main__":
    unittest.main()
