#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
namespace=${QC_KUBERNETES_NAMESPACE:-default}
quickcache_namespace=${QC_QUICKCACHE_NAMESPACE:-kube-system}
state_configmap=${QC_STATE_CONFIGMAP_NAME:-oci-quickcache-state}
node_selector=${QC_NODE_SELECTOR:-oci-hpc-oke.oracle.com/quickcache-ready=true}
start_delay=${QC_START_DELAY_SECONDS:-180}
run_id=$(date -u +%Y%m%dT%H%M%SZ)
artifact_dir=${QC_ARTIFACT_DIR:-/tmp/quickcache-benchmark-${run_id}}
results_file=${QC_RESULTS_FILE:-${artifact_dir}/results.jsonl}
report_file=${QC_REPORT_FILE:-${artifact_dir}/report.json}
if [[ ! "$node_selector" =~ ^[A-Za-z0-9./_-]+=[A-Za-z0-9._-]+$ ]]; then
  echo "QC_NODE_SELECTOR must be one exact key=value selector" >&2
  exit 1
fi
selector_key=${node_selector%%=*}
selector_value=${node_selector#*=}
mkdir -p "$artifact_dir"

node_count=$(kubectl get nodes -l "$node_selector" --no-headers | wc -l | tr -d ' ')
if [[ "$node_count" -le 0 ]]; then
  echo "No benchmark nodes match $node_selector" >&2
  exit 1
fi
cache_owner_count=${QC_EXPECTED_CACHE_OWNERS:-$(
  kubectl get nodes \
    -l oci-hpc-oke.oracle.com/quickcache-ready=true \
    --no-headers | wc -l | tr -d ' '
)}
if [[ "$cache_owner_count" -le 0 ]]; then
  echo "No QuickCache server nodes are ready" >&2
  exit 1
fi
start_epoch=$(( $(date +%s) + start_delay ))
rendered_manifest=$(mktemp "${TMPDIR:-/tmp}/quickcache-benchmark.XXXXXX")
trap 'rm -f "$rendered_manifest"' EXIT

sed \
  -e "s/completions: 3/completions: ${node_count}/" \
  -e "s/parallelism: 3/parallelism: ${node_count}/" \
  -e "s/value: \"0\"/value: \"${start_epoch}\"/" \
  -e "s#oci-hpc-oke.oracle.com/quickcache-ready: \"true\"#${selector_key}: \"${selector_value}\"#" \
  "$script_dir/multinode-benchmark-job.yaml" > "$rendered_manifest"
cp "$rendered_manifest" "$artifact_dir/job.yaml"
kubectl get nodes -l "$node_selector" -o json > "$artifact_dir/nodes.json"
kubectl -n "$quickcache_namespace" get configmap "$state_configmap" -o json \
  > "$artifact_dir/quickcache-state.json"

kubectl -n "$namespace" delete job quickcache-multinode-benchmark --ignore-not-found
kubectl -n "$namespace" apply -f "$rendered_manifest"
kubectl -n "$namespace" wait --for=condition=complete \
  job/quickcache-multinode-benchmark --timeout="${QC_TIMEOUT:-30m}"
kubectl -n "$namespace" logs \
  -l app=quickcache-multinode-benchmark --prefix --tail=-1 > "$results_file"

report_args=(
  --input "$results_file"
  --expected-nodes "$node_count"
  --expected-cache-owners "$cache_owner_count"
  --maximum-start-skew-seconds "${QC_MAXIMUM_START_SKEW_SECONDS:-5}"
)
if [[ -n "${QC_MINIMUM_AGGREGATE_GIB_S:-}" ]]; then
  report_args+=(--minimum-aggregate-gib-s "$QC_MINIMUM_AGGREGATE_GIB_S")
fi
if [[ -n "${QC_MAXIMUM_P95_FIRST_BYTE_MS:-}" ]]; then
  report_args+=(--maximum-p95-first-byte-ms "$QC_MAXIMUM_P95_FIRST_BYTE_MS")
fi
if [[ -n "${QC_BASELINE_AGGREGATE_GIB_S:-}" ]]; then
  report_args+=(
    --baseline-aggregate-gib-s "$QC_BASELINE_AGGREGATE_GIB_S"
    --minimum-parity-ratio "${QC_MINIMUM_PARITY_RATIO:-0.90}"
  )
fi

python3 "$script_dir/benchmark-report.py" "${report_args[@]}" | tee "$report_file"
echo "Benchmark artifacts: $artifact_dir"
