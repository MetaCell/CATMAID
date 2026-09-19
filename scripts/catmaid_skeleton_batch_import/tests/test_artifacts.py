from __future__ import annotations

from pathlib import Path

from catmaid_skeleton_batch_import.artifacts import (
    artifact_descriptor,
    manifest_rows,
    read_id_artifact,
    read_manifest,
    write_id_artifact,
    write_manifest,
)
from catmaid_skeleton_batch_import.domain import PlannedBatch, PlannedMember


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


def test_manifest_lists_each_member_annotation_as_a_json_array(tmp_path: Path) -> None:
    member = PlannedMember(
        member_path="191805/0000001.swc",
        node_count=2,
        uncompressed_bytes=20,
        crc32=7,
        display_name="0000001",
        cable_length_nm=2.0,
        batch_index=1,
        annotations=("0000001",),
    )
    batch = PlannedBatch(index=1, members=(member,))
    path = tmp_path / "manifest.csv.gz"

    write_manifest(path, manifest_rows(batch, [100, 101, 102, 103, 104]))

    assert read_manifest(path) == [
        {
            "member_path": "191805/0000001.swc",
            "external_skeleton_id": "",
            "neuron_id": "100",
            "skeleton_id": "101",
            "node_count": "2",
            "name": "0000001",
            "batch_index": "1",
            "annotations": '["0000001"]',
        }
    ]
