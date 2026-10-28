"""Existing-stack snapshots for database-free importer tests."""

from __future__ import annotations

from pathlib import Path

from catmaid_skeleton_batch_import.util import atomic_write_json


def write_stack_snapshot(
    state_dir: Path, project_id: int = 123, *,
    dimension=(1_000_000_000, 1_000_000_000, 1_000_000_000),
    resolution_nm=(1, 1, 1), translation_nm=(0, 0, 0),
):
    geometry = {
        "schema_version": 1,
        "project_id": project_id,
        "database_target": {"sha256": "test-cluster"},
        "stacks": [{
            "project_stack_id": 8, "stack_id": 9, "orientation": 0,
            "dimension": list(dimension), "resolution_nm": list(resolution_nm),
            "translation_nm": list(translation_nm),
        }],
    }
    atomic_write_json(state_dir / "stack-geometry.json", geometry)
    return geometry
