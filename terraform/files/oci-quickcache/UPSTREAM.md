# Upstream provenance

The QuickCache runtime in this chart is derived from
`oci-hpc/oci-hpc-clusternetwork-dev`, feature branch `quickcache`, snapshot:

```text
949492c79b40dcb42e0ecb2f5d6989aceb1fec68
```

The OKE port keeps the S3 `GetObject` interception model, fixed virtual shards,
atomic cache writes, cache-age checks, range support, and disk-pressure cleanup.
Cluster membership, peer mounting, and shard-map ownership were rewritten for
Kubernetes. Raw S3 keys are not used as filesystem paths in this port.

The OKE port also adds active peer I/O probes, node-persistent audit files,
optional numeric shared-group permissions, a client-only agent for dedicated
cache-server topologies, and a Kubernetes multi-node benchmark.
