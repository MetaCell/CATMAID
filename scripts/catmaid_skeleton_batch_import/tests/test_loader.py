from __future__ import annotations

import csv
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.domain import (
    IngestionRequest,
    PlannedBatch,
    PlannedMember,
    SwcNode,
)
from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.loader import (
    ProjectContext,
    _treenode_row_matches,
    prepare_copy_rows,
)


NODES = (
    SwcNode(1, 3, 1.0, 2.0, 3.0, -1.0, -1),
    SwcNode(2, 3, 2.0, 2.0, 3.0, 2.0, 1),
)


def request(tmp_path: Path) -> IngestionRequest:
    return IngestionRequest(
        schema_version=1,
        archive_path=tmp_path / "input.zip",
        coordinate_unit="um",
        dimension=(10, 10, 10),
        resolution_nm=(1000, 1000, 1000),
    )


def batch(annotations: tuple[str, ...] = ()) -> PlannedBatch:
    member = PlannedMember(
        member_path="folder/cell.swc",
        node_count=2,
        uncompressed_bytes=20,
        crc32=123,
        display_name="cell",
        cable_length_nm=1000.0,
        batch_index=1,
        annotations=annotations,
    )
    return PlannedBatch(index=1, members=(member,))


def project(**annotation_ids: int) -> ProjectContext:
    return ProjectContext(
        project_id=5,
        stack_id=6,
        project_stack_id=7,
        user_id=1,
        neuron_class_id=8,
        skeleton_class_id=9,
        model_of_relation_id=10,
        **annotation_ids,
    )


def test_copy_rows_scale_coordinates_and_preserve_unknown_radius(tmp_path: Path) -> None:
    with prepare_copy_rows(
        batch(),
        request(tmp_path),
        project(),
        concept_ids=[100, 101, 102],
        location_ids=[200, 201],
        load_nodes=lambda _member: NODES,
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


def test_stem_annotation_is_written_verbatim_for_each_member(tmp_path: Path) -> None:
    with prepare_copy_rows(
        batch(annotations=("0000001",)),
        request(tmp_path),
        project(annotation_class_id=11, annotated_with_relation_id=12),
        concept_ids=[100, 101, 102, 103, 104],
        location_ids=[200, 201],
        load_nodes=lambda _member: NODES,
    ) as prepared:
        class_rows = list(csv.reader(prepared.class_file))
        relationship_rows = list(csv.reader(prepared.relationship_file))
        expected = prepared.expected_skeletons[0]

    assert class_rows == [
        ["100", "1", "5", "8", "cell"],
        ["101", "1", "5", "9", "cell"],
        ["103", "1", "5", "11", "0000001"],
    ]
    assert relationship_rows == [
        ["102", "1", "5", "10", "101", "100"],
        ["104", "1", "5", "12", "100", "103"],
    ]
    assert expected.to_validation_dict()["annotations"] == [
        {"name": "0000001", "annotation_id": 103, "link_id": 104}
    ]


def test_copy_rows_reject_a_concept_count_that_omits_annotation_ids(
    tmp_path: Path,
) -> None:
    with pytest.raises(InvalidInputError, match="expected 5 concept IDs, found 3"):
        prepare_copy_rows(
            batch(annotations=("0000001",)),
            request(tmp_path),
            project(annotation_class_id=11, annotated_with_relation_id=12),
            concept_ids=[100, 101, 102],
            location_ids=[200, 201],
            load_nodes=lambda _member: NODES,
        )


def test_project_context_written_before_annotation_support_still_loads() -> None:
    context = ProjectContext.from_dict(
        {
            "schema_version": 1,
            "project_id": 5,
            "stack_id": 6,
            "project_stack_id": 7,
            "user_id": 1,
            "neuron_class_id": 8,
            "skeleton_class_id": 9,
            "model_of_relation_id": 10,
        }
    )

    assert context.annotation_class_id is None
    assert context.annotated_with_relation_id is None
    assert "annotation_class_id" not in context.to_dict()


def test_annotations_are_refused_for_a_project_context_without_annotation_ids(
    tmp_path: Path,
) -> None:
    with pytest.raises(InvalidInputError, match="predates annotation support"):
        prepare_copy_rows(
            batch(annotations=("0000001",)),
            request(tmp_path),
            project(),
            concept_ids=[100, 101, 102, 103, 104],
            location_ids=[200, 201],
            load_nodes=lambda _member: NODES,
        )


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
