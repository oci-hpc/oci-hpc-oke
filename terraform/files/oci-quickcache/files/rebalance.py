"""Pure state transitions for staged QuickCache shard rebalancing."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable


LOG = logging.getLogger("quickcache-rebalance")
MigrationComplete = Callable[[dict, str], bool]
MigrationEstimate = Callable[[dict], dict | None]


def moved_shards(old_map: dict, new_map: dict) -> list[dict[str, str]]:
    """Return the stable, source-owned migration plan between two maps."""
    return [
        {"shard": str(shard), "source": str(source), "target": str(new_map[shard])}
        for shard, source in sorted(old_map.items(), key=lambda item: int(item[0]))
        if shard in new_map and source != new_map[shard]
    ]


def new_plan(
    generation: int,
    mode: str,
    active: dict,
    pending: dict,
    now: int,
) -> dict:
    """Create an immutable-identity migration plan for one generation."""
    plan_id = str(uuid.uuid4())
    return {
        "generation": generation,
        "planId": plan_id,
        "approvalToken": f"{generation}:{plan_id}",
        "mode": mode,
        "phase": "awaitingApproval" if mode == "manual" else "migrating",
        "createdAt": now,
        "moves": moved_shards(active, pending),
    }


def plan_members(plan: dict, member: str) -> set[str]:
    """Return valid source or target node UIDs referenced by a plan."""
    return {
        str(move.get(member))
        for move in plan.get("moves", [])
        if isinstance(move, dict) and move.get(member)
    }


def advance_staged_rebalance(
    state: dict,
    peers: dict,
    desired: dict,
    mode: str,
    approval_annotation: str,
    annotations: dict[str, str],
    fallback_grace: int,
    now: int,
    migration_complete: MigrationComplete,
    migration_estimate: MigrationEstimate,
) -> None:
    """Advance one staged-rebalance reconciliation without Kubernetes I/O."""
    active = state["active"]
    plan = state["rebalance"]

    # A lost active owner cannot be copied, so availability requires failover.
    if active and not set(active.values()).issubset(peers):
        LOG.warning("an active cache owner disappeared; applying immediate failover")
        state["generation"] += 1
        state["active"] = desired
        state["pending"] = {}
        state["previous"] = {}
        state["rebalance"] = {}
        return

    if plan:
        phase = plan.get("phase")
        if phase in {"awaitingApproval", "migrating"} and not plan_members(
            plan, "target"
        ).issubset(peers):
            LOG.warning("a pending cache owner disappeared; cancelling staged rebalance")
            state["pending"] = {}
            state["rebalance"] = {}
            return

        generation = str(plan.get("generation", ""))
        approved = annotations.get(approval_annotation) == plan.get("approvalToken")
        estimate = None
        if phase == "awaitingApproval":
            estimate = migration_estimate(plan)
            if estimate is not None and plan.get("estimate") != estimate:
                plan["estimate"] = estimate
        if phase == "awaitingApproval" and (
            mode == "automatic" or (approved and estimate is not None)
        ):
            plan["phase"] = "migrating"
            plan["approvedAt"] = now
            phase = "migrating"
            LOG.info("started QuickCache rebalance generation %s", generation)

        if phase == "migrating" and migration_complete(plan, "copy"):
            state["previous"] = active
            state["active"] = state["pending"]
            state["pending"] = {}
            plan["phase"] = "fallback"
            plan["activatedAt"] = now
            plan["cleanupAfter"] = now + fallback_grace
            LOG.info("activated warm QuickCache rebalance generation %s", generation)
            return

        if phase == "fallback":
            if not plan_members(plan, "source").issubset(peers):
                LOG.warning("a previous owner disappeared; ending fallback early")
                state["previous"] = {}
                state["rebalance"] = {}
            elif now >= int(plan.get("cleanupAfter", now + fallback_grace)):
                plan["phase"] = "cleanup"
                plan["cleanupStartedAt"] = now
                LOG.info("started cleanup for QuickCache rebalance %s", generation)
            return

        if phase == "cleanup" and migration_complete(plan, "cleanup"):
            state["previous"] = {}
            state["rebalance"] = {}
            LOG.info("completed QuickCache rebalance generation %s", generation)
        return

    if desired == active:
        return
    state["generation"] += 1
    state["pending"] = desired
    state["previous"] = {}
    state["rebalance"] = new_plan(
        state["generation"], mode, active, desired, now
    )
    if not state["rebalance"]["moves"]:
        state["active"] = desired
        state["pending"] = {}
        state["rebalance"] = {}
