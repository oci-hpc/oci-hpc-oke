# Upstream provenance

The QuickCache runtime in this chart is derived from
`oci-hpc/oci-hpc-clusternetwork-dev`, feature branch `quickcache`, snapshot:

```text
949492c79b40dcb42e0ecb2f5d6989aceb1fec68
```

The OKE port keeps the S3 `GetObject` interception model, fixed virtual shards,
atomic cache writes, cache-age checks, range support, and disk-pressure cleanup.
Cluster membership, peer mounting, and shard-map ownership were rewritten for
Kubernetes. The default `friendly` cache layout preserves the recognizable S3
bucket/key hierarchy using safe per-component encoding and a resource-hash
suffix. The original OKE hash-only layout remains readable and selectable.

The OKE port also adds active peer I/O probes, node-persistent audit files,
optional numeric shared-group permissions, a client-only agent for dedicated
cache-server topologies, and a Kubernetes multi-node benchmark.

## Responsibility map

Keep changes inside the narrowest module that owns the behavior:

| Area | Files | Responsibility |
| --- | --- | --- |
| S3 data path | `sitecustomize.py`, `cache_paths.py` | Intercept `GetObject`, locate objects, and implement atomic read-through caching. This is the part closest to the Slurm runtime. |
| Placement | `sharding.py` | Deterministically assign fixed virtual shards to healthy cache-node UIDs and turn ownership into mount paths. |
| Rebalance policy | `rebalance.py` | Pure, Kubernetes-independent state transitions for automatic/manual warm migration and immediate failover. |
| State persistence | `state_store.py` | Decode and publish the live ConfigMap and retain bounded, full pre-cutover map backups. |
| Kubernetes controller | `controller.py` | Discover healthy nodes, collect migration status, and connect Kubernetes I/O to the placement and rebalance modules. |
| Host reconciliation | `node_agent.py`, `agent_common.py` | Prepare one server/client node, reconcile peer mounts and host runtime/config files, and publish readiness. |
| Host setup | `host_setup.sh`, `host_teardown.sh` | Configure and remove NFS exports/mounts without creating or formatting storage. |
| Warm-data movement | `migration_agent.py`, `migrate_shards.py` | Estimate, copy, verify, and finally remove moved shard directories. |
| Capacity management | `cleanup.py` | Remove least-recently-used cache entries under disk pressure. |
| Deployment checks | `healthcheck.py`, `templates/readiness-job.yaml` | Block Helm success until membership, maps, agents, and peer mounts agree. |
| Packaging | `templates/runtime-configmap.yaml` | Package every file under `files/`; adding a runtime module needs no template edit. |

## Invariants to preserve

These are design constraints, not incidental implementation details:

1. QuickCache never creates, partitions, formats, or rebuilds NVMe storage.
2. Bucket/object-key components are safely encoded and bounded before becoming
   filesystem components. A hash suffix includes endpoint, bucket, key, and
   optional version identity; direct traversal and symlinked directories are
   rejected.
3. A cache body is published with an atomic rename only after the complete
   expected response has been written.
4. The active shard map does not change during a staged copy. Cutover happens
   only after every required source reports success.
5. The previous map remains readable during the fallback grace period.
6. Cleanup removes obsolete source shards only after a final copy verifies
   destination coverage.
7. Losing an active owner fails over immediately; an unavailable source cannot
   be made warm by waiting.
8. A non-initial cutover requires a durable full-map backup. Backup retention
   is bounded so etcd use cannot grow forever.
9. The node ready label means the local runtime and every currently required
   peer mount have passed an I/O probe.
10. A cache failure must fall back to Object Storage and must not break the
    application's normal Botocore response semantics.

## Change workflow

Run the focused checks from the repository root:

```sh
python3 -m unittest discover -s test/quickcache -p 'test_*.py'
helm lint terraform/files/oci-quickcache
helm template quickcache terraform/files/oci-quickcache \
  --namespace kube-system >/tmp/quickcache-rendered.yaml
(cd test && GOCACHE=/tmp/oci-hpc-oke-go-cache \
  go test -count=1 ./... -run 'TestQuickCacheSecurity|TestQuickCache' \
  -timeout 10m)
```

When changing the data path, add or update a test in
`test/quickcache/test_sitecustomize.py`. When changing membership or warm
migration, test the pure transition through `test_controller.py` and retain an
end-to-end scale-out test using `docs/using-quickcache-on-oke.md`.
