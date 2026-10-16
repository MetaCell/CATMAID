from __future__ import annotations

from pathlib import Path

from catmaid_skeleton_batch_import.artifacts import (
    artifact_descriptor,
    read_id_artifact,
    write_id_artifact,
)


def test_reserved_id_artifact_is_deterministic_and_verified(tmp_path: Path) -> None:
    first = tmp_path / "first.txt.gz"
    second = tmp_path / "second.txt.gz"
    ids = [10, 15, 19]

    write_id_artifact(first, ids)
    write_id_artifact(second, ids)

    assert first.read_bytes() == second.read_bytes()
    descriptor = artifact_descriptor(
        tmp_path,
        first,
        count=len(ids),
        minimum=min(ids),
        maximum=max(ids),
    )
    assert read_id_artifact(tmp_path, descriptor, expected_count=3) == ids
