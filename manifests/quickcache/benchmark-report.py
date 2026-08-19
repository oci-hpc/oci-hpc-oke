#!/usr/bin/env python3
"""Aggregate and validate QuickCache multi-node benchmark JSON records."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path


def _records(lines) -> list[dict]:
    records = []
    for line in lines:
        start = line.find("{")
        if start < 0:
            continue
        try:
            value = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "throughput_mib_s" in value:
            records.append(value)
    return records


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def summarize(
    records: list[dict],
    expected_nodes: int,
    expected_cache_owners: int | None = None,
    minimum_aggregate_gib_s: float | None = None,
    maximum_p95_first_byte_ms: float | None = None,
    maximum_start_skew_seconds: float | None = None,
    baseline_aggregate_gib_s: float | None = None,
    minimum_parity_ratio: float = 0.90,
) -> dict:
    if expected_nodes <= 0:
        raise ValueError("expected_nodes must be positive")
    if not records:
        raise ValueError("no benchmark JSON records found")

    nodes = [str(record["node"]) for record in records]
    cache_owners = [str(record["cache_owner"]) for record in records]
    if len(nodes) != len(set(nodes)):
        raise ValueError("benchmark contains duplicate node results")
    throughputs = [float(record["throughput_mib_s"]) for record in records]
    first_bytes = [float(record["average_first_byte_ms"]) for record in records]
    starts = [float(record["wall_started_epoch"]) for record in records]
    finishes = [float(record["wall_finished_epoch"]) for record in records]
    total_bytes = sum(int(record["total_bytes"]) for record in records)
    numeric_values = throughputs + first_bytes + starts + finishes
    if not all(math.isfinite(value) and value >= 0 for value in numeric_values):
        raise ValueError("benchmark metrics must be finite and non-negative")
    if total_bytes <= 0:
        raise ValueError("benchmark total bytes must be positive")
    if any(finished <= started for started, finished in zip(starts, finishes)):
        raise ValueError("every benchmark finish time must follow its start time")
    window_seconds = max(finishes) - min(starts)
    if window_seconds <= 0:
        raise ValueError("benchmark wall-clock window must be positive")

    # Total bytes divided by the cluster-wide wall-clock window is the measured
    # concurrent aggregate. Summing independent per-pod rates is also reported
    # as a diagnostic, but is not used for the acceptance decision.
    aggregate_mib_s = total_bytes / window_seconds / 1024 / 1024
    aggregate_gib_s = aggregate_mib_s / 1024
    checks = {
        "expected_node_count": len(records) == expected_nodes,
        "unique_node_count": len(set(nodes)) == expected_nodes,
        "all_reads_from_cache": all(
            record.get("all_reads_from_cache") is True for record in records
        ),
    }
    if expected_cache_owners is not None:
        if expected_cache_owners <= 0:
            raise ValueError("expected_cache_owners must be positive")
        checks["expected_cache_owner_coverage"] = (
            len(set(cache_owners)) == expected_cache_owners
        )
    if minimum_aggregate_gib_s is not None:
        if minimum_aggregate_gib_s < 0:
            raise ValueError("minimum_aggregate_gib_s must be non-negative")
        checks["minimum_aggregate_throughput"] = (
            aggregate_gib_s >= minimum_aggregate_gib_s
        )
    if maximum_p95_first_byte_ms is not None:
        if maximum_p95_first_byte_ms < 0:
            raise ValueError("maximum_p95_first_byte_ms must be non-negative")
        checks["maximum_p95_first_byte_latency"] = (
            _percentile(first_bytes, 0.95) <= maximum_p95_first_byte_ms
        )
    start_skew_seconds = max(starts) - min(starts)
    if maximum_start_skew_seconds is not None:
        if maximum_start_skew_seconds < 0:
            raise ValueError("maximum_start_skew_seconds must be non-negative")
        checks["maximum_start_skew"] = (
            start_skew_seconds <= maximum_start_skew_seconds
        )

    parity_ratio = None
    if baseline_aggregate_gib_s is not None:
        if baseline_aggregate_gib_s <= 0:
            raise ValueError("baseline_aggregate_gib_s must be positive")
        if minimum_parity_ratio <= 0:
            raise ValueError("minimum_parity_ratio must be positive")
        parity_ratio = aggregate_gib_s / baseline_aggregate_gib_s
        checks["minimum_baseline_parity"] = parity_ratio >= minimum_parity_ratio

    return {
        "passed": all(checks.values()),
        "checks": checks,
        "expected_nodes": expected_nodes,
        "reported_nodes": len(records),
        "nodes": sorted(nodes),
        "expected_cache_owners": expected_cache_owners,
        "reported_cache_owners": len(set(cache_owners)),
        "cache_owners": sorted(set(cache_owners)),
        "acceptance": {
            "minimum_aggregate_gib_s": minimum_aggregate_gib_s,
            "maximum_p95_first_byte_ms": maximum_p95_first_byte_ms,
            "maximum_start_skew_seconds": maximum_start_skew_seconds,
            "baseline_aggregate_gib_s": baseline_aggregate_gib_s,
            "minimum_parity_ratio": (
                minimum_parity_ratio
                if baseline_aggregate_gib_s is not None
                else None
            ),
        },
        "total_bytes": total_bytes,
        "aggregate_window_seconds": round(window_seconds, 6),
        "start_skew_seconds": round(start_skew_seconds, 6),
        "aggregate_mib_s": round(aggregate_mib_s, 3),
        "aggregate_gib_s": round(aggregate_gib_s, 6),
        "sum_of_node_mib_s_diagnostic": round(sum(throughputs), 3),
        "node_throughput_mib_s": {
            "minimum": round(min(throughputs), 3),
            "median": round(_percentile(throughputs, 0.50), 3),
            "p95": round(_percentile(throughputs, 0.95), 3),
            "maximum": round(max(throughputs), 3),
        },
        "node_average_first_byte_ms": {
            "median": round(_percentile(first_bytes, 0.50), 3),
            "p95": round(_percentile(first_bytes, 0.95), 3),
            "maximum": round(max(first_bytes), 3),
        },
        "baseline_aggregate_gib_s": baseline_aggregate_gib_s,
        "parity_ratio": None if parity_ratio is None else round(parity_ratio, 6),
        "workloads": sorted(
            [
                {
                    "node": str(record["node"]),
                    "pod": str(record.get("pod", "")),
                    "object_key": str(record.get("object_key", "")),
                    "shard": int(record.get("shard", -1)),
                    "cache_owner": str(record["cache_owner"]),
                    "throughput_mib_s": float(record["throughput_mib_s"]),
                    "average_first_byte_ms": float(
                        record["average_first_byte_ms"]
                    ),
                }
                for record in records
            ],
            key=lambda value: value["node"],
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="-", help="JSONL log file or - for stdin")
    parser.add_argument("--expected-nodes", required=True, type=int)
    parser.add_argument("--expected-cache-owners", type=int)
    parser.add_argument("--minimum-aggregate-gib-s", type=float)
    parser.add_argument("--maximum-p95-first-byte-ms", type=float)
    parser.add_argument("--maximum-start-skew-seconds", type=float)
    parser.add_argument("--baseline-aggregate-gib-s", type=float)
    parser.add_argument("--minimum-parity-ratio", type=float, default=0.90)
    args = parser.parse_args()

    if args.input == "-":
        records = _records(sys.stdin)
    else:
        with Path(args.input).open(encoding="utf-8") as stream:
            records = _records(stream)
    try:
        report = summarize(
            records,
            args.expected_nodes,
            args.expected_cache_owners,
            args.minimum_aggregate_gib_s,
            args.maximum_p95_first_byte_ms,
            args.maximum_start_skew_seconds,
            args.baseline_aggregate_gib_s,
            args.minimum_parity_ratio,
        )
    except (KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
