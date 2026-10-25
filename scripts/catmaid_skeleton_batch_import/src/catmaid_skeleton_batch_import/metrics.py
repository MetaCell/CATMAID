"""Small attributable progress and timing records for the importer.

Only monotonic elapsed durations and importer-owned counts are recorded.  WAL
LSN sampling is deliberately absent because cluster-wide WAL cannot be
attributed reliably to one ingestion on an active CATMAID database.
"""

from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any, Mapping

from . import __version__
from .errors import ImmutableStateError, InvalidInputError
from .util import atomic_write_json, load_json


SCHEMA_VERSION = 1
METRICS_FILENAME = "metrics.json"
_METRICS_STATUSES = {
    "pending",
    "in_progress",
    "complete",
    "failed",
    "operator_attention",
}
_PHASE_STATUSES = {"complete", "failed", "retryable", "operator_attention"}


def _count(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise InvalidInputError(f"{label} must be a non-negative integer")
    if isinstance(value, float) and not value.is_integer():
        raise InvalidInputError(f"{label} must be a non-negative integer")
    try:
        result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise InvalidInputError(f"{label} must be a non-negative integer") from exc
    if result < 0:
        raise InvalidInputError(f"{label} must be a non-negative integer")
    return result


def _duration(value: object) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise InvalidInputError("Phase duration must be a finite non-negative number") from exc
    if not math.isfinite(result) or result < 0:
        raise InvalidInputError("Phase duration must be a finite non-negative number")
    return result


def _row_counts(value: Mapping[str, object] | None) -> dict[str, int]:
    result: dict[str, int] = {}
    for name, count in (value or {}).items():
        if not isinstance(name, str) or not name:
            raise InvalidInputError("Row-count names must be non-empty strings")
        result[name] = _count(count, f"row count {name}")
    return dict(sorted(result.items()))


def _phase_identity(
    name: str,
    *,
    batch_index: int | None = None,
    batch_node_count: int | None = None,
    batch_skeleton_count: int | None = None,
) -> dict[str, Any]:
    if not isinstance(name, str) or not name:
        raise InvalidInputError("Phase name must be a non-empty string")
    result: dict[str, Any] = {"name": name}
    if batch_index is not None:
        result["batch_index"] = _count(batch_index, "batch index")
    if batch_node_count is not None:
        result["batch_node_count"] = _count(
            batch_node_count, "batch node count"
        )
    if batch_skeleton_count is not None:
        result["batch_skeleton_count"] = _count(
            batch_skeleton_count, "batch skeleton count"
        )
    return result


def _completed_phase_record(
    metrics: Mapping[str, Any],
    identity: Mapping[str, Any],
    *,
    duration_seconds: float,
    status: str,
    node_count: int | None,
    row_counts: Mapping[str, object] | None,
    error: str | None,
) -> dict[str, Any]:
    if status not in _PHASE_STATUSES:
        raise InvalidInputError(f"Unknown completed phase status: {status}")
    record = {
        "sequence": len(metrics["phases"]) + 1,
        **dict(identity),
        "status": status,
        "duration_seconds": _duration(duration_seconds),
        "node_count": (
            _count(node_count, "phase node count")
            if node_count is not None
            else identity.get("batch_node_count", 0)
        ),
        "row_counts": _row_counts(row_counts),
    }
    if error:
        record["error"] = str(error)
    return record


class MetricsStore:
    """Atomically persist low-overhead phase metrics in a state directory.

    The command-wide :mod:`catmaid_skeleton_batch_import.state` lock provides
    single-writer exclusion.  Atomic replacement still guarantees readers
    never observe a partially written metrics document.
    """

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir.resolve()
        self.path = self.state_dir / METRICS_FILENAME

    def initialize(
        self,
        *,
        planned_batches: int = 0,
        planned_nodes: int = 0,
        planned_skeletons: int = 0,
    ) -> dict[str, Any]:
        expected = {
            "batches": {
                "planned": _count(planned_batches, "planned batches"),
                "completed": 0,
            },
            "nodes": {
                "planned": _count(planned_nodes, "planned nodes"),
                "completed": 0,
            },
            "skeletons": {
                "planned": _count(planned_skeletons, "planned skeletons"),
                "completed": 0,
            },
            "rows": {},
        }
        if self.path.exists():
            metrics = self.load()
            for name in ("batches", "nodes", "skeletons"):
                if metrics["counts"][name]["planned"] != expected[name]["planned"]:
                    raise ImmutableStateError(
                        f"Metrics were initialized with a different planned {name} count"
                    )
            return metrics

        metrics = {
            "schema_version": SCHEMA_VERSION,
            "status": "pending",
            "counts": expected,
            "phases": [],
            "runs": [],
        }
        self.state_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.path, metrics)
        return copy.deepcopy(metrics)

    def load(self) -> dict[str, Any]:
        if not self.path.is_file():
            raise InvalidInputError(f"Metrics have not been initialized: {self.path}")
        value = load_json(self.path)
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
            raise ImmutableStateError("Unsupported or malformed metrics schema")
        return value

    def set_status(self, status: str) -> dict[str, Any]:
        if status not in _METRICS_STATUSES:
            raise InvalidInputError(f"Unknown metrics status: {status}")
        metrics = self.load()
        metrics["status"] = status
        atomic_write_json(self.path, metrics)
        return copy.deepcopy(metrics)

    def record_operational_settings(
        self,
        *,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> dict[str, Any]:
        """Append the effective database timeouts for one ``run`` invocation."""

        metrics = self.load()
        runs = metrics.setdefault("runs", [])
        if not isinstance(runs, list):
            raise ImmutableStateError("Metrics runs history is malformed")
        record = {
            "sequence": len(runs) + 1,
            "importer_version": __version__,
            "statement_timeout_ms": _count(
                statement_timeout_ms, "statement timeout"
            ),
            "lock_timeout_ms": _count(lock_timeout_ms, "lock timeout"),
        }
        runs.append(record)
        atomic_write_json(self.path, metrics)
        return copy.deepcopy(record)

    def update_progress(
        self,
        *,
        completed_batches: int | None = None,
        completed_nodes: int | None = None,
        completed_skeletons: int | None = None,
        row_counts: Mapping[str, object] | None = None,
    ) -> dict[str, Any]:
        metrics = self.load()
        updates = {
            "batches": completed_batches,
            "nodes": completed_nodes,
            "skeletons": completed_skeletons,
        }
        for name, raw in updates.items():
            if raw is None:
                continue
            value = _count(raw, f"completed {name}")
            current = metrics["counts"][name]["completed"]
            planned = metrics["counts"][name]["planned"]
            if value < current:
                raise ImmutableStateError(f"Completed {name} count cannot decrease")
            if value > planned:
                raise ImmutableStateError(
                    f"Completed {name} count cannot exceed the planned count"
                )
            metrics["counts"][name]["completed"] = value

        for name, value in _row_counts(row_counts).items():
            current = metrics["counts"]["rows"].get(name, 0)
            if value < current:
                raise ImmutableStateError(f"Row count {name} cannot decrease")
            metrics["counts"]["rows"][name] = value

        if metrics["status"] == "pending":
            metrics["status"] = "in_progress"
        atomic_write_json(self.path, metrics)
        return copy.deepcopy(metrics)

    def reconcile_progress(
        self,
        *,
        completed_batches: int,
        completed_nodes: int,
        completed_skeletons: int,
    ) -> dict[str, Any]:
        """Replace observational progress with authoritative committed state."""

        metrics = self.load()
        updates = {
            "batches": completed_batches,
            "nodes": completed_nodes,
            "skeletons": completed_skeletons,
        }
        for name, raw in updates.items():
            value = _count(raw, f"completed {name}")
            planned = metrics["counts"][name]["planned"]
            if value > planned:
                raise ImmutableStateError(
                    f"Completed {name} count cannot exceed the planned count"
                )
            metrics["counts"][name]["completed"] = value
        if metrics["status"] == "pending":
            metrics["status"] = "in_progress"
        atomic_write_json(self.path, metrics)
        return copy.deepcopy(metrics)

    def record_phase(
        self,
        name: str,
        *,
        duration_seconds: float,
        status: str = "complete",
        batch_index: int | None = None,
        batch_node_count: int | None = None,
        batch_skeleton_count: int | None = None,
        node_count: int | None = None,
        row_counts: Mapping[str, object] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Append a completed phase with one atomic metrics-file replacement."""

        metrics = self.load()
        identity = _phase_identity(
            name,
            batch_index=batch_index,
            batch_node_count=batch_node_count,
            batch_skeleton_count=batch_skeleton_count,
        )
        record = _completed_phase_record(
            metrics,
            identity,
            duration_seconds=duration_seconds,
            status=status,
            node_count=node_count,
            row_counts=row_counts,
            error=error,
        )
        metrics["phases"].append(record)
        if metrics["status"] == "pending":
            metrics["status"] = "in_progress"
        if status in ("failed", "operator_attention"):
            metrics["status"] = status
        atomic_write_json(self.path, metrics)
        return copy.deepcopy(record)
