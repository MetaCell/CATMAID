from __future__ import annotations

from pathlib import Path

from catmaid_skeleton_batch_import.metrics import MetricsStore
from catmaid_skeleton_batch_import.orchestrator import (
    _buffer_loader_timing,
    _flush_loader_timings,
    _reconcile_completed_progress,
)


def test_reconciles_committed_progress_before_skipped_batches(tmp_path: Path) -> None:
    metrics = MetricsStore(tmp_path)
    metrics.initialize(planned_batches=3, planned_nodes=30, planned_skeletons=6)
    metrics.update_progress(
        completed_batches=3,
        completed_nodes=30,
        completed_skeletons=6,
    )
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

    reconciled = _reconcile_completed_progress(metrics, state)

    assert reconciled["counts"]["batches"]["completed"] == 2
    assert reconciled["counts"]["nodes"]["completed"] == 20
    assert reconciled["counts"]["skeletons"]["completed"] == 5


def test_loader_timing_is_buffered_until_after_transaction(tmp_path: Path) -> None:
    metrics = MetricsStore(tmp_path)
    metrics.initialize(planned_batches=1, planned_nodes=8, planned_skeletons=2)
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

    assert metrics.load()["phases"] == []
    _flush_loader_timings(metrics, 1, records)
    phase = metrics.load()["phases"][0]
    assert phase["name"] == "batch_000001_copy"
    assert phase["duration_seconds"] == 1.25
    assert phase["row_counts"] == {"treenode_rows": 8}
