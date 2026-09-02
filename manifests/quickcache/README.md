# QuickCache test manifests

1. Copy `quickcache-s3-secret.example.yaml` to `quickcache-s3-secret.yaml` and
   enter an S3-compatible endpoint, region, bucket, object key, and credentials.
2. Apply the Secret and `boto3-getobject-job.yaml`.
3. Use a small, non-empty object (for example, 1-32 MiB) whose key has not
   been read through QuickCache before. The smoke test holds multiple copies in
   memory while comparing full, chunked, and ranged reads.
4. Check the Job log for a network-backed first read, cached full/chunked/range
   reads, and matching content.

The Job configures SigV4, path-style addressing, and required-only Botocore
checksum behavior for the OCI Object Storage S3 Compatibility API.

The Secret is an example only. Do not commit real customer credentials.

With the default `friendly` cache path layout, successful misses write safely
encoded, recognizable bucket/object-key paths under each shard's `v2`
directory. The filename ends in `.__qc_<24 hex characters>` to distinguish
endpoint and version identities. Existing hash-only OKE entries remain
readable and are logged as compatibility-layout hits.

## Multi-node benchmark

Use a multi-GiB immutable object that is safe to read repeatedly. A single key
exercises only one shard owner, so use `prepare-benchmark-keys.py` with the
active `shard_map.json`, endpoint, bucket, and eligible node count. Upload the
same test content under every returned key and store the compact JSON array in
the test Secret as `S3_KEYS_JSON`.

The runner sets the Job size to the eligible node count, schedules exactly one
indexed pod per node, synchronizes timed reads, verifies full cache-owner
coverage, collects raw JSONL, and validates cluster-wide wall-clock throughput:

```sh
manifests/quickcache/run-multinode-benchmark.sh
```

Set `QC_MINIMUM_AGGREGATE_GIB_S` and/or
`QC_MAXIMUM_P95_FIRST_BYTE_MS` for absolute checks. Set
`QC_BASELINE_AGGREGATE_GIB_S` and `QC_MINIMUM_PARITY_RATIO` to compare with a
like-for-like Slurm result. The reporter exits nonzero on failure. For
dedicated client nodes, set
`QC_NODE_SELECTOR=oci-hpc-oke.oracle.com/quickcache-client-ready=true`; the
runner updates the rendered manifest selector.

The runner makes performance parity measurable but cannot establish it without
running on the intended cluster. Repeat the same warm-cache test at least three
times on comparable Slurm and OKE clusters. Each run retains the rendered Job,
node metadata, QuickCache state, report, and raw JSONL in a timestamped
artifact directory under `/tmp` by default.
