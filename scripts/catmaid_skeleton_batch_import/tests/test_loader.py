from __future__ import annotations

import csv
from pathlib import Path

from catmaid_skeleton_batch_import.domain import (
    IngestionRequest,
    PlannedBatch,
    PlannedMember,
    SwcNode,
)
from catmaid_skeleton_batch_import.loader import (
    ProjectContext,
    _treenode_row_matches,
    prepare_copy_rows,
)


def test_copy_rows_scale_coordinates_and_preserve_unknown_radius(tmp_path: Path) -> None:
    request = IngestionRequest(
        schema_version=1,
        archive_path=tmp_path / "input.zip",
        coordinate_unit="um",
        dimension=(10, 10, 10),
        resolution_nm=(1000, 1000, 1000),
    )
    member = PlannedMember(
        member_path="folder/cell.swc",
        node_count=2,
        uncompressed_bytes=20,
        crc32=123,
        display_name="cell",
        cable_length_nm=1000.0,
        batch_index=1,
    )
    batch = PlannedBatch(index=1, members=(member,))
    project = ProjectContext(
        project_id=5,
        stack_id=6,
        project_stack_id=7,
        user_id=1,
        neuron_class_id=8,
        skeleton_class_id=9,
        model_of_relation_id=10,
    )
    nodes = (
        SwcNode(1, 3, 1.0, 2.0, 3.0, -1.0, -1),
        SwcNode(2, 3, 2.0, 2.0, 3.0, 2.0, 1),
    )

    with prepare_copy_rows(
        batch,
        request,
        project,
        concept_ids=[100, 101, 102],
        location_ids=[200, 201],
        load_nodes=lambda _member: nodes,
    ) as prepared:
        prepared.treenode_file.seek(0)
        rows = list(csv.reader(prepared.treenode_file))

    assert rows[0] == [
        "200",
        "5",
        "1000.0",
        "2000.0",
        "3000.0",
        "1",
        "1",
        "101",
        "-1.0",
        r"\N",
    ]
    assert rows[1][8:] == ["2000.0", "200"]


def test_treenode_row_comparison_allows_only_driver_float_rounding() -> None:
    expected = (161, 4, 3033851.0, 4584272.0, 8379523.999999999, 2, 2, 140, 1000.0, 160)
    actual = (161, 4, 3033851.0, 4584272.0, 8379524.0, 2, 2, 140, 1000.0, 160)

    assert _treenode_row_matches(actual, expected)
    assert not _treenode_row_matches(
        (*actual[:4], 8379524.01, *actual[5:]), expected
    )
    assert not _treenode_row_matches(
        (*actual[:9], 999), expected
    )
