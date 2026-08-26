from __future__ import annotations

import json
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.errors import ImmutableStateError, InvalidInputError
from catmaid_skeleton_batch_import.metrics import MetricsStore, SCHEMA_VERSION
from catmaid_skeleton_batch_import.util import canonical_json_bytes


class FakeClock:
    def __init__(self, *values: float):
        self.values = iter(values)

    def __call__(self) -> float:
        return next(self.values)


def test_metrics_initialize_canonically_and_idempotently(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    metrics = store.initialize(
        planned_batches=3, planned_nodes=30, planned_skeletons=6
    )
    assert metrics["schema_version"] == SCHEMA_VERSION
    assert store.path.read_bytes() == canonical_json_bytes(json.loads(store.path.read_bytes()))
    before = store.path.read_bytes()
    assert store.initialize(
        planned_batches=3, planned_nodes=30, planned_skeletons=6
    ) == metrics
    assert store.path.read_bytes() == before

    with pytest.raises(ImmutableStateError, match="planned batches"):
        store.initialize(planned_batches=4, planned_nodes=30, planned_skeletons=6)


def test_progress_counts_are_attributable_and_monotonic(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize(planned_batches=2, planned_nodes=12, planned_skeletons=3)
    metrics = store.update_progress(
        completed_batches=1,
        completed_nodes=5,
        completed_skeletons=1,
        row_counts={"treenode": 5, "class_instance": 2},
    )
    assert metrics["status"] == "in_progress"
    assert metrics["counts"] == {
        "batches": {"planned": 2, "completed": 1},
        "nodes": {"planned": 12, "completed": 5},
        "skeletons": {"planned": 3, "completed": 1},
        "rows": {"class_instance": 2, "treenode": 5},
    }

    with pytest.raises(ImmutableStateError, match="cannot decrease"):
        store.update_progress(completed_nodes=4)
    with pytest.raises(ImmutableStateError, match="planned count"):
        store.update_progress(completed_batches=3)


def test_phase_context_uses_monotonic_duration_and_records_batch_sizes(
    tmp_path: Path,
) -> None:
    store = MetricsStore(tmp_path, clock=FakeClock(100.0, 102.25))
    store.initialize(planned_batches=1, planned_nodes=8, planned_skeletons=2)

    with store.phase(
        "batch.transaction",
        batch_index=1,
        batch_node_count=8,
        batch_skeleton_count=2,
    ) as phase:
        phase.set_counts(
            node_count=8,
            row_counts={"treenode": 8, "treenode_edge": 8},
        )

    record = store.load()["phases"][0]
    assert record == {
        "sequence": 1,
        "name": "batch.transaction",
        "batch_index": 1,
        "batch_node_count": 8,
        "batch_skeleton_count": 2,
        "status": "complete",
        "duration_seconds": 2.25,
        "node_count": 8,
        "row_counts": {"treenode": 8, "treenode_edge": 8},
    }
    assert phase.record == record


def test_failed_phase_is_recorded_and_exception_is_not_swallowed(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path, clock=FakeClock(10.0, 10.5))
    store.initialize()

    with pytest.raises(RuntimeError, match="database unavailable"):
        with store.phase("project.create"):
            raise RuntimeError("database unavailable")

    metrics = store.load()
    assert metrics["active_phase"] is None
    assert metrics["status"] == "failed"
    assert metrics["phases"][0]["status"] == "failed"
    assert metrics["phases"][0]["duration_seconds"] == 0.5
    assert metrics["phases"][0]["error"] == "database unavailable"


def test_active_phase_is_orchestrator_visible(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize()
    active = store.begin_phase("archive.scan", batch_node_count=100)

    assert active == {
        "name": "archive.scan",
        "status": "running",
        "batch_node_count": 100,
    }
    assert store.load()["active_phase"] == active
    with pytest.raises(ImmutableStateError, match="already have an active phase"):
        store.begin_phase("another.phase")


def test_direct_phase_record_and_status_contract(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize()
    record = store.record_phase(
        "archive.hash",
        duration_seconds=0.125,
        node_count=0,
        row_counts={},
    )
    assert record["duration_seconds"] == 0.125
    assert store.set_status("complete")["status"] == "complete"

    with pytest.raises(InvalidInputError, match="duration"):
        store.record_phase("invalid", duration_seconds=float("nan"))


def test_direct_phase_record_does_not_create_an_active_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = MetricsStore(tmp_path)
    store.initialize()

    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("record_phase must not use begin_phase or finish_phase")

    monkeypatch.setattr(store, "begin_phase", unexpected)
    monkeypatch.setattr(store, "finish_phase", unexpected)
    record = store.record_phase("archive.hash", duration_seconds=0.25)

    assert record["status"] == "complete"
    assert store.load()["active_phase"] is None


def test_interrupted_active_phase_is_recovered_durably_and_idempotently(
    tmp_path: Path,
) -> None:
    store = MetricsStore(tmp_path)
    store.initialize(planned_batches=1, planned_nodes=8, planned_skeletons=2)
    store.begin_phase(
        "batch.transaction",
        batch_index=1,
        batch_node_count=8,
        batch_skeleton_count=2,
    )

    recovered = MetricsStore(tmp_path).recover_active_phase()
    assert recovered is not None
    assert recovered["name"] == "batch.transaction"
    assert recovered["status"] == "retryable"
    assert recovered["recovered_after_interruption"] is True
    assert recovered["node_count"] == 8
    metrics = store.load()
    assert metrics["active_phase"] is None
    assert metrics["phases"] == [recovered]
    assert MetricsStore(tmp_path).recover_active_phase() is None
    assert store.load()["phases"] == [recovered]


def test_effective_operational_settings_are_recorded_per_run(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize()

    first = store.record_operational_settings(
        statement_timeout_ms=0,
        lock_timeout_ms=30_000,
    )
    second = store.record_operational_settings(
        statement_timeout_ms=120_000,
        lock_timeout_ms=5_000,
    )

    assert first == {
        "sequence": 1,
        "statement_timeout_ms": 0,
        "lock_timeout_ms": 30_000,
    }
    assert second["sequence"] == 2
    assert store.load()["runs"] == [first, second]


def test_metrics_schema_contains_no_wal_measurement(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize(planned_batches=1, planned_nodes=5)
    store.record_phase(
        "batch.copy",
        duration_seconds=1.0,
        batch_index=1,
        batch_node_count=5,
        row_counts={"treenode": 5},
    )

    assert "wal" not in store.path.read_text().lower()
