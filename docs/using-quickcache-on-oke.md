# Using OCI QuickCache on OKE

OCI QuickCache is an optional, cluster-wide read-through cache for Python
applications that call the S3-compatible `GetObject` API through boto3 or
Botocore. It uses the existing local NVMe filesystem on selected OKE workers
and exports each node's cache to the other cache nodes over NFSv4.

For code ownership, upstream provenance, safety invariants, and focused
maintainer tests, see
[`terraform/files/oci-quickcache/UPSTREAM.md`](../terraform/files/oci-quickcache/UPSTREAM.md).

QuickCache does not cache writes and is not a general POSIX filesystem. The
initial OKE integration is intended for trusted workloads: the cache is shared
and must not be treated as a tenant-isolation boundary.

## Architecture

- The controller watches nodes labeled
  `oci-hpc-oke.oracle.com/quickcache=true` and publishes a peer inventory and
  fixed virtual-shard map in the `oci-quickcache-state` ConfigMap.
- A privileged node-agent DaemonSet verifies `/mnt/nvme`, configures an NFSv4
  export, mounts every healthy peer, copies the runtime to the host, and
  reconciles node additions and removals.
- When dedicated clients are enabled, a separate client-agent DaemonSet mounts
  the cache exports and installs the workload runtime without requiring local
  NVMe or owning shards.
- A cleanup container removes least-recently-accessed local objects when the
  NVMe filesystem reaches the configured watermark.
- In staged rebalance modes, a migration sidecar copies moved shard directories
  over the existing NFS peer mounts before the controller activates the new
  map. The previous map remains readable during a grace period, followed by a
  final copy and removal of only the obsolete source shard directories.
- Each agent actively stats every remote cache directory during reconciliation.
  An unresponsive mount is remounted; a node is not workload-ready while its
  peer probes fail.
- Workload pods mount the host runtime, configuration, and peer tree. Python
  imports `sitecustomize.py` through `PYTHONPATH`, which intercepts Botocore
  `GetObject` calls.

The node agent refuses to use the root filesystem. It never creates,
repartitions, or reformats the NVMe RAID.

## Deploy with Terraform

At least one enabled GPU, RDMA, or GMC pool is required. A CPU pool can be
selected only when it uses a DenseIO shape.

```hcl
install_quickcache = true

# Empty selects all enabled GPU, RDMA, and GMC pools.
quickcache_worker_pools = []
```

For an explicit selection:

```hcl
quickcache_worker_pools = ["oke-rdma"]
```

For dedicated DenseIO cache servers with applications on GPU workers:

```hcl
quickcache_worker_pools        = ["oke-cpu"] # NVMe-backed DenseIO servers
quickcache_client_worker_pools = ["oke-gpu"] # clients; do not own shards
```

Server and client pools must not overlap. In converged mode, leave
`quickcache_client_worker_pools` empty and schedule applications directly on
the server pools.

To restrict writers using a Slurm-style numeric group:

```hcl
quickcache_access_mode = "sharedGroup"
quickcache_shared_gid  = 1500
```

Workload pods must then include `supplementalGroups: [1500]`. The default
`trustedShared` mode preserves support for arbitrary workload UIDs.

Choose the scale-out rebalance policy with:

```hcl
quickcache_rebalance_mode         = "automatic" # automatic, manual, immediate
quickcache_rebalance_grace_period = 3600
quickcache_map_backup_retention   = 20
```

- `automatic` (default) copies all moved shards, activates the new map only
  after every source succeeds, retains a previous-owner read fallback for the
  grace period, performs a final copy, and then removes obsolete source data.
- `manual` creates the same warm migration plan, publishes a read-only
  file/byte estimate, and waits for approval before copying. The active map and
  cache stay unchanged while it waits.
- `immediate` activates the new map without copying. This is the legacy
  cold-rebalance behavior and can make old copies unused.

Node-loss failover is always immediate because an unavailable source cannot be
migrated. This rule applies independently of the selected scale-out mode.
Automatic and manual modes support at most 4,096 virtual shards so their
active, pending, previous, and migration-plan state remains below the
Kubernetes ConfigMap size limit. The default is 1,024.

Keep `nvme_raid_enabled = true`. After `terraform apply`, check the deployment:

```sh
kubectl -n kube-system get deployment quickcache-oci-quickcache-controller
kubectl -n kube-system get daemonset quickcache-oci-quickcache-node-agent
kubectl get nodes -l oci-hpc-oke.oracle.com/quickcache=true
kubectl -n kube-system get configmap oci-quickcache-state
```

In dedicated mode, also check the client agent and its nodes:

```sh
kubectl -n kube-system get daemonset quickcache-oci-quickcache-client-agent
kubectl get nodes -l oci-hpc-oke.oracle.com/quickcache-client=true
```

Existing nodes do not receive new initial node-pool labels automatically. Label
them for an evaluation or cycle the selected pool:

```sh
kubectl label node NODE_NAME oci-hpc-oke.oracle.com/quickcache=true
```

## Deploy the chart directly

For an existing cluster, label the intended nodes and install the chart:

```sh
helm upgrade --install quickcache \
  ./terraform/files/oci-quickcache \
  --namespace kube-system \
  --wait \
  --timeout 15m
```

Every selected node must already have the OKE NVMe array mounted at
`/mnt/nvme`. The node agent installs NFS packages when they are absent, which
requires access to the operating-system package repositories. The worker NSG
must also allow TCP 2049 between selected nodes; Terraform adds that rule when
it installs QuickCache.

## Add QuickCache to a workload

The pod must run on a converged QuickCache node or a dedicated client node and
mount four host directories:

```yaml
spec:
  securityContext:
    # Required in sharedGroup mode; use quickcache_shared_gid.
    supplementalGroups: [1500]
  nodeSelector:
    oci-hpc-oke.oracle.com/quickcache-ready: "true"
  containers:
    - name: workload
      env:
        - name: PYTHONPATH
          value: /opt/ociqc
        - name: OCI_QC_ENV_PATH
          value: /etc/ociqc/env.json
      volumeMounts:
        - name: quickcache-runtime
          mountPath: /opt/ociqc
          readOnly: true
        - name: quickcache-config
          mountPath: /etc/ociqc
          readOnly: true
        - name: quickcache-peers
          mountPath: /var/lib/ociqc/mounts
          mountPropagation: HostToContainer
        - name: quickcache-logs
          mountPath: /var/log/ociqc
  volumes:
    - name: quickcache-runtime
      hostPath:
        path: /var/lib/ociqc/runtime
        type: Directory
    - name: quickcache-config
      hostPath:
        path: /var/lib/ociqc/config
        type: Directory
    - name: quickcache-peers
      hostPath:
        path: /var/lib/ociqc/mounts
        type: Directory
    - name: quickcache-logs
      hostPath:
        path: /var/lib/ociqc/logs
        type: Directory
```

For a dedicated client pool, change the selector to:

```yaml
nodeSelector:
  oci-hpc-oke.oracle.com/quickcache-client-ready: "true"
```

Hit/miss/error CSV files are stored on the workload node at
`/var/lib/ociqc/logs`. Cleanup logs are stored on cache-owner nodes in the same
directory. `shard_map_audit.log` records each map transition seen by a node,
including the number of added, removed, and moved shards. The host installs a
daily, seven-generation logrotate policy. These files survive pod replacement;
use OCI Logging or another node-log collector when logs must also survive node
replacement.

The container must include Botocore. QuickCache uses the application's existing
S3 endpoint and credentials. For OCI Object Storage, configure the Botocore
client with SigV4, path-style addressing, and request/response checksum
behavior set to `when_required`; the supplied smoke Job is a working example.

## Functional test

Create the test Secret from
`manifests/quickcache/quickcache-s3-secret.example.yaml`, then apply the test
Job:

```sh
kubectl apply -f manifests/quickcache/quickcache-s3-secret.yaml
kubectl delete job quickcache-boto3-test --ignore-not-found
kubectl apply -f manifests/quickcache/boto3-getobject-job.yaml
kubectl logs -f job/quickcache-boto3-test
```

Use a small, non-empty object (for example, 1-32 MiB) that has not already been
cached. The Job checks a network miss followed by full, chunked, and ranged
cache hits. Expected output includes:

```text
first_read_from_cache=False
second_read_from_cache=True
chunked_read_from_cache=True
range_read_from_cache=True
content_matches=True
chunked_content_matches=True
range_content_matches=True
```

Inspect controller, agent, and cache state:

```sh
kubectl -n kube-system logs deployment/quickcache-oci-quickcache-controller
kubectl -n kube-system logs daemonset/quickcache-oci-quickcache-node-agent -c node-agent
kubectl -n kube-system get configmap oci-quickcache-state -o jsonpath='{.data.peers\.json}'
kubectl -n kube-system get configmap oci-quickcache-state -o jsonpath='{.data.shard_map\.json}'
kubectl -n kube-system get configmap oci-quickcache-state -o jsonpath='{.data.rebalance\.json}'
```

For `manual` mode, first wait for the distributed, read-only estimate to
finish. The estimate is generated by the migration executable's `estimate`
phase and contains the moved-shard plan plus actual file and byte totals from
every source node:

```sh
kubectl -n kube-system get configmap oci-quickcache-state \
  -o jsonpath='{.data.rebalance\.json}' | jq '{phase, moves, estimate}'
```

Do not approve until `.estimate.status` is `complete`. The controller enforces
this even if an approval annotation is added early. Then read the unique
approval token and approve that exact plan:

```sh
QC_APPROVAL_TOKEN=$(kubectl -n kube-system get configmap oci-quickcache-state \
  -o jsonpath='{.data.rebalance\.json}' | jq -r .approvalToken)
kubectl -n kube-system annotate configmap oci-quickcache-state \
  oci-hpc-oke.oracle.com/quickcache-approve-rebalance="${QC_APPROVAL_TOKEN}" \
  --overwrite
```

Watch the phase move through `awaitingApproval`, `migrating`, `fallback`, and
`cleanup`; an empty `rebalance.json` means stable. Migration-worker status is
reported on each source Node:

```sh
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.oci-hpc-oke\.oracle\.com/quickcache-migration}{"\n"}{end}'
kubectl -n kube-system logs daemonset/quickcache-oci-quickcache-node-agent -c migration-agent --prefix
```

During the fallback phase, cache audit reasons `HIT_PREVIOUS` and
`HIT_RANGE_PREVIOUS` show reads served by the prior owner. Migration failure
does not activate the pending map or remove source data; fix the failure and
the agent retries.

Before every non-initial active-map cutover, the controller saves the complete
old shard map in a timestamped `.bak` key inside a dedicated ConfigMap. Backup
creation is required for cutover and the newest
`quickcache_map_backup_retention` maps are retained (20 by default). Retention
is bounded to avoid unbounded Kubernetes/etcd growth. List and inspect them:

```sh
kubectl -n kube-system get configmaps \
  -l app.kubernetes.io/component=shard-map-backup \
  --sort-by=.metadata.creationTimestamp

QC_BACKUP=$(kubectl -n kube-system get configmaps \
  -l app.kubernetes.io/component=shard-map-backup \
  --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{.items[-1:].metadata.name}')
kubectl -n kube-system get configmap "$QC_BACKUP" -o json |
  jq -r '.data | to_entries[] | select(.key | endswith(".bak")) | .value'
```

In dedicated mode, client-agent logs are available with:

```sh
kubectl -n kube-system logs daemonset/quickcache-oci-quickcache-client-agent -c client-agent
```

Inspect node-local audit files from a debug shell or workload mount:

```sh
ls -lh /var/log/ociqc
tail -n 20 /var/log/ociqc/cache_log.csv
tail -n 20 /var/log/ociqc/cache_err.csv
tail -n 20 /var/log/ociqc/shard_map_audit.log
```

## Multi-node benchmark

The supplied benchmark runs one cache-hit reader on every eligible node. A
single object maps to one shard owner and cannot prove distributed throughput,
so first prepare at least one large object key per benchmark pod, deliberately
spread across all cache owners:

```sh
QC_NODES=$(kubectl get nodes \
  -l oci-hpc-oke.oracle.com/quickcache-ready=true \
  --no-headers | wc -l | tr -d ' ')
kubectl -n kube-system get configmap oci-quickcache-state \
  -o jsonpath='{.data.shard_map\.json}' > /tmp/quickcache-shard-map.json

python3 manifests/quickcache/prepare-benchmark-keys.py \
  --shard-map /tmp/quickcache-shard-map.json \
  --endpoint-url "$S3_ENDPOINT_URL" \
  --bucket "$S3_BUCKET" \
  --prefix "quickcache-throughput-$(date +%s)" \
  --count "$QC_NODES" > /tmp/quickcache-benchmark-keys.json

while IFS= read -r key; do
  oci os object put \
    --namespace-name "$QC_OBJECT_STORAGE_NAMESPACE" \
    --bucket-name "$S3_BUCKET" \
    --name "$key" \
    --file /path/to/large-immutable-test-object.bin \
    --region "$AWS_DEFAULT_REGION" \
    --force
done < <(jq -r '.[]' /tmp/quickcache-benchmark-keys.json)

QC_KEYS_JSON=$(jq -c . /tmp/quickcache-benchmark-keys.json)
kubectl -n default patch secret quickcache-s3 --type merge \
  -p "$(jq -n --arg keys "$QC_KEYS_JSON" \
    '{stringData:{S3_KEYS_JSON:$keys}}')"
```

The runner then schedules exactly one indexed pod per node, sets a shared
future start time, collects per-node JSON, verifies that every cache owner was
exercised, and produces a machine-verifiable cluster report:

```sh
manifests/quickcache/run-multinode-benchmark.sh
```

Use a multi-GiB immutable source object and at least five timed iterations.
The ordinary single `S3_KEY` remains a fallback for one-node tests; a
multi-owner acceptance run requires `S3_KEYS_JSON`. Optional acceptance inputs
are:

```sh
QC_MINIMUM_AGGREGATE_GIB_S=400 \
QC_MAXIMUM_P95_FIRST_BYTE_MS=10 \
QC_BASELINE_AGGREGATE_GIB_S=450 \
QC_MINIMUM_PARITY_RATIO=0.90 \
  manifests/quickcache/run-multinode-benchmark.sh
```

`QC_BASELINE_AGGREGATE_GIB_S` should come from a comparable Slurm run using
the same node shape/count, object, iterations, network, and warm-cache state.
The reporter exits nonzero when a threshold, node-count check, or cache-hit
check fails. For dedicated clients, set
`QC_NODE_SELECTOR=oci-hpc-oke.oracle.com/quickcache-client-ready=true`; the
runner updates the rendered manifest selector.

This benchmark makes large-scale parity testable and produces evidence; it
does not itself prove parity. Run it at the intended customer scale at least
three times and compare median results with the Slurm baseline before claiming
throughput parity. Each run saves its rendered Job, node metadata, QuickCache
state, raw JSONL, and report under a timestamped `/tmp/quickcache-benchmark-*`
directory. Increase `QC_START_DELAY_SECONDS` if large-object warmup or image
startup takes longer than the default 180 seconds.

## Failure and scaling tests

1. Add a labeled NVMe worker and confirm the DaemonSet becomes Ready.
2. Verify the peer count increases and a rebalance appears in the state
   ConfigMap. In manual mode, approve its generation as shown above.
3. Verify `shard_map.json` remains unchanged while phase is `migrating`.
4. Verify phase changes to `fallback` only after every source reports a
   successful copy; run the boto3 test to confirm existing objects remain hits.
5. After the grace period, verify the final copy and cleanup complete and
   `rebalance.json` becomes `{}`.
6. Drain or stop one cache node and wait longer than the heartbeat timeout.
7. Verify its UID disappears and failure recovery updates the map immediately;
   unavailable cache data falls back to Object Storage.

Changing `quickcache_virtual_shards` changes object placement and effectively
invalidates the existing cache. Treat it as immutable after deployment.

Removing the chart disables the QuickCache NFS export and unmounts its peer
mounts on each node. Cached objects under `/mnt/nvme/ociqc/object_store` are
left in place so that an uninstall does not destroy data unexpectedly.

For a full Terraform plan test with the supplied overlay:

```sh
cd test
TFVARS_FILE=./tfvars/base/base.tfvars,./tfvars/quickcache/quickcache.tfvars \
  go test -count=1 ./... -run TestPlanSmoke -timeout 30m
```

## Current boundaries

- Python boto3/Botocore `GetObject` only.
- Cold byte-range reads fetch the complete object into cache.
- A fresh cache hit does not contact Object Storage or reauthorize the caller.
  Use QuickCache only across mutually trusted workloads with equivalent object
  access; cached data is not an authorization boundary.
- Unversioned objects can remain cached until `quickcache_max_cache_age`
  expires. Use immutable/versioned object keys or lower that value for mutable
  datasets.
- NFS uses node Internal IPs and the primary worker network, not RDMA
  interfaces automatically.
- QuickCache shares the `/mnt/nvme` filesystem with OKE runtime data in the
  default node setup. Keep conservative cleanup watermarks and validate pod
  eviction behavior before production use.
