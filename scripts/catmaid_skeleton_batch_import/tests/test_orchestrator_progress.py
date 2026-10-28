from __future__ import annotations

from pathlib import Path

from catmaid_skeleton_batch_import.metrics import MetricsStore
from catmaid_skeleton_batch_import.orchestrator import (
    _buffer_loader_timing,
    _flush_loader_timings,
    _record_completed_progress,
)


def test_records_confirmed_committed_progress(tmp_path: Path) -> None:
    metrics = MetricsStore(tmp_path)
    metrics.initialize(planned_batches=3, planned_nodes=30, planned_skeletons=6)
    state = {
        "batches": [
            {
                "index": 1,
                "status": "committed",
                "node_count": 8,
                "skeleton_count": 2,
            },
            {
                "index": 2,
                "status": "committed",
                "node_count": 12,
                "skeleton_count": 3,
            },
            {
                "index": 3,
                "status": "prepared",
                "node_count": 10,
                "skeleton_count": 1,
            },
        ]
    }

    reconciled = _record_completed_progress(metrics, state)

    assert reconciled["counts"]["batches"]["completed"] == 2
    assert reconciled["counts"]["nodes"]["completed"] == 20
    assert reconciled["counts"]["skeletons"]["completed"] == 5


def test_loader_timings_are_flushed_together_after_transaction(tmp_path: Path, monkeypatch) -> None:
    from catmaid_skeleton_batch_import import metrics as metrics_module

    metrics = MetricsStore(tmp_path)
    metrics.initialize(planned_batches=1, planned_nodes=8, planned_skeletons=2)
    write = metrics_module.atomic_write_json
    writes = []

    def observe_write(path, value):
        writes.append(path)
        write(path, value)

    monkeypatch.setattr(metrics_module, "atomic_write_json", observe_write)
    records: list[tuple[str, float, dict[str, int]]] = []
    callback = _buffer_loader_timing(records)

    callback(
        "copy",
        1.25,
        {
            "nodes": 8,
            "skeletons": 2,
            "treenode_rows": 8,
            "internal_detail": 99,
        },
    )

    callback("edge_creation", 0.5, {"nodes": 8, "skeletons": 2, "edge_rows": 8})
    callback("summary_refresh", 0.25, {"nodes": 8, "skeletons": 2, "summary_rows": 2})

    assert metrics.load()["phases"] == []
    assert writes == []
    _flush_loader_timings(metrics, 1, records)
    assert writes == [metrics.path]
    phases = metrics.load()["phases"]
    assert [phase["name"] for phase in phases] == [
        "batch_000001_copy", "batch_000001_edge_creation", "batch_000001_summary_refresh",
    ]
    assert [phase["sequence"] for phase in phases] == [1, 2, 3]
    assert [phase["duration_seconds"] for phase in phases] == [1.25, 0.5, 0.25]
    assert phases[1]["row_counts"] == {"edge_rows": 8}
    assert phases[2]["row_counts"] == {"summary_rows": 2}
    phase = phases[0]
    assert phase["name"] == "batch_000001_copy"
    assert phase["duration_seconds"] == 1.25
    assert phase["row_counts"] == {"treenode_rows": 8}
