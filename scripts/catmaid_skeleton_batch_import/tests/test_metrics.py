from __future__ import annotations

import json
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import import __version__
from catmaid_skeleton_batch_import.errors import ImmutableStateError, InvalidInputError
from catmaid_skeleton_batch_import.metrics import MetricsStore, SCHEMA_VERSION
from catmaid_skeleton_batch_import.util import canonical_json_bytes


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
    assert metrics["status"] == "running"
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


def test_completed_phase_keeps_duration_and_batch_metadata(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize(planned_batches=1, planned_nodes=8, planned_skeletons=2)
    record = store.record_phase(
        "batch.transaction", duration_seconds=2.25, batch_index=1,
        batch_node_count=8, batch_skeleton_count=2, node_count=8,
        row_counts={"treenode": 8, "treenode_edge": 8},
    )
    assert store.load()["phases"] == [record]
    assert record == {
        "sequence": 1, "name": "batch.transaction", "batch_index": 1,
        "batch_node_count": 8, "batch_skeleton_count": 2, "status": "complete",
        "duration_seconds": 2.25, "node_count": 8,
        "row_counts": {"treenode": 8, "treenode_edge": 8},
    }


def test_failed_phase_keeps_failure_details(tmp_path: Path) -> None:
    store = MetricsStore(tmp_path)
    store.initialize()
    record = store.record_phase(
        "project.select", duration_seconds=0.5, status="failed",
        error="database unavailable",
    )
    metrics = store.load()
    assert metrics["status"] == "failed"
    assert metrics["phases"] == [record]
    assert record["status"] == "failed"
    assert record["duration_seconds"] == 0.5
    assert record["error"] == "database unavailable"


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

    from catmaid_skeleton_batch_import import metrics as metrics_module
    write = metrics_module.atomic_write_json
    writes = []

    def observe_write(path, value):
        writes.append(path)
        write(path, value)

    monkeypatch.setattr(metrics_module, "atomic_write_json", observe_write)
    record = store.record_phase("archive.hash", duration_seconds=0.25)
    assert writes == [store.path]
    assert record["status"] == "complete"


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
        "importer_version": __version__,
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


@pytest.mark.parametrize("middle_status", ["complete", "failed"])
def test_buffered_phases_use_one_write_and_keep_sequence_and_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, middle_status: str,
) -> None:
    from catmaid_skeleton_batch_import import metrics as metrics_module

    store = MetricsStore(tmp_path)
    store.initialize()
    store.record_phase("earlier", duration_seconds=0.1)
    write = metrics_module.atomic_write_json
    writes = []

    def observe_write(path, value):
        writes.append(path)
        write(path, value)

    monkeypatch.setattr(metrics_module, "atomic_write_json", observe_write)
    records = store.record_phases([
        {"name": "copy", "duration_seconds": 1.25, "batch_index": 1,
         "batch_node_count": 8, "batch_skeleton_count": 2,
         "row_counts": {"treenode_rows": 8}},
        {"name": "edges", "duration_seconds": 0.5, "status": middle_status,
         "error": "edge failure" if middle_status == "failed" else None},
        {"name": "validation", "duration_seconds": 0.25},
    ])

    assert writes == [store.path]
    saved = store.load()
    assert saved["phases"][1:] == records
    assert [record["sequence"] for record in saved["phases"]] == [1, 2, 3, 4]
    assert [record["name"] for record in records] == ["copy", "edges", "validation"]
    assert records[0]["node_count"] == 8
    assert records[0]["row_counts"] == {"treenode_rows": 8}
    assert saved["status"] == ("failed" if middle_status == "failed" else "running")
    if middle_status == "failed":
        assert records[1]["error"] == "edge failure"


@pytest.mark.parametrize("invalid", [
    {"duration_seconds": float("nan")},
    {"status": "unknown"},
])
def test_invalid_buffered_phase_does_not_persist_partial_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid,
) -> None:
    from catmaid_skeleton_batch_import import metrics as metrics_module

    store = MetricsStore(tmp_path)
    store.initialize()
    before = store.path.read_bytes()

    def unexpected_write(*_args):
        pytest.fail("Invalid buffered metrics must not be written")

    monkeypatch.setattr(metrics_module, "atomic_write_json", unexpected_write)
    with pytest.raises(InvalidInputError):
        store.record_phases([
            {"name": "valid", "duration_seconds": 0.1},
            {"name": "invalid", "duration_seconds": 0.2, **invalid},
        ])
    assert store.path.read_bytes() == before


def test_empty_phase_buffer_does_not_read_or_write_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MetricsStore(tmp_path)

    def unexpected_load():
        pytest.fail("An empty buffer must not read metrics")

    monkeypatch.setattr(store, "load", unexpected_load)
    assert store.record_phases([]) == []
    assert not store.path.exists()
