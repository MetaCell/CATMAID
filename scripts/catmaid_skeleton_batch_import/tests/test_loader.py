from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.archive import scan_archive
from catmaid_skeleton_batch_import.artifacts import NODE_MAP_FIELDS, manifest_rows, read_manifest, write_manifest
from catmaid_skeleton_batch_import.config import DEFAULTS
from catmaid_skeleton_batch_import.domain import (
    IngestionRequest,
    ImportSettings,
    PlannedBatch,
    PlannedMember,
    SwcNode,
)
from catmaid_skeleton_batch_import.loader import (
    ProjectContext,
    _treenode_row_matches,
    prepare_copy_rows,
)
from catmaid_skeleton_batch_import.planner import plan_archive, read_plan, write_plan
from catmaid_skeleton_batch_import.swc import parse_swc


@pytest.mark.parametrize(("source_name", "import_name"), [
    (r"\N", "_N"), (r"prefix\N", r"prefix\N"), ("cell", "cell"),
])
def test_display_name_sanitization_matches_copy_expectations_and_manifest(
    tmp_path: Path, source_name: str, import_name: str,
) -> None:
    content = b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 1\n"
    archive = tmp_path / "cells.zip"
    member_path = f"folder/{source_name}.swc"
    with zipfile.ZipFile(archive, "w") as target:
        target.writestr(member_path, content)
    request = IngestionRequest(schema_version=1, archive_path=archive,
                               coordinate_unit="nm", target_project_id=5)
    settings = ImportSettings(**DEFAULTS)
    plan = plan_archive(scan_archive(request, settings,
                                    bounds_nm=(((0, 0, 0), (1000, 1000, 1000)),)), settings)
    plan_path = tmp_path / "plan.jsonl.gz"
    write_plan(plan_path, plan)
    batch = read_plan(plan_path).batches[0]
    assert batch.members[0].display_name == source_name
    project = ProjectContext(project_id=5, stack_id=None, project_stack_id=None,
                             user_id=1, neuron_class_id=8, skeleton_class_id=9,
                             model_of_relation_id=10, annotation_class_id=11,
                             annotated_with_relation_id=12, existing_project=True)
    concept_ids = [100, 101, 102, 103, 104]
    nodes = parse_swc(io.BytesIO(content), member_path).nodes
    with prepare_copy_rows(batch, request, project, concept_ids, [200, 201],
                           lambda _member: nodes) as prepared:
        rows = list(csv.reader(prepared.class_file))
        assert [row[4] for row in rows] == [
            import_name, import_name, f"source-fragment:{source_name}",
        ]
        assert prepared.expected_skeletons[0].name == import_name
    manifest = tmp_path / "manifest.csv.gz"
    write_manifest(manifest, manifest_rows(batch, concept_ids))
    record = read_manifest(manifest)[0]
    assert record["name"] == import_name
    assert record["fragment_id"] == source_name
    assert record["member_path"] == member_path


def test_copy_rows_scale_coordinates_and_preserve_unknown_radius(
    tmp_path: Path,
) -> None:
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
        fragment_id="cell",
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
        annotation_class_id=11,
        annotated_with_relation_id=12,
    )
    nodes = (
        SwcNode(1, 3, 1.0, 2.0, 3.0, -1.0, -1),
        SwcNode(2, 3, 2.0, 2.0, 3.0, 2.0, 1),
    )

    with prepare_copy_rows(
        batch,
        request,
        project,
        concept_ids=[100, 101, 102, 103, 104],
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


def test_treenode_row_comparison_uses_postgres_float4_precision() -> None:
    expected = (161, 4, 3033851.0, 4584272.0, 8379523.999999999, 2, 2, 140, 1000.0, 160)
    actual = (161, 4, 3033851.0, 4584272.0, 8379524.0, 2, 2, 140, 1000.0, 160)

    assert _treenode_row_matches(actual, expected)
    assert _treenode_row_matches(
        (*actual[:8], 748.0469, actual[9]),
        (*expected[:8], 748.046875, expected[9]),
    )
    assert not _treenode_row_matches((*actual[:4], 8379525.0, *actual[5:]), expected)
    assert not _treenode_row_matches((*actual[:9], 999), expected)


def test_node_map_uses_exact_copy_assignments_and_full_csv_quoted_member_paths(tmp_path):
    members = tuple(PlannedMember(
        member_path=path, node_count=2, uncompressed_bytes=40, crc32=1,
        display_name=f"cell{i}", fragment_id=f"cell{i}", cable_length_nm=1,
        batch_index=1,
    ) for i, path in enumerate(['folder/细胞,"one".swc', 'other/line\nbreak.swc']))
    batch = PlannedBatch(index=1, members=members)
    request = IngestionRequest(1, tmp_path / "source.zip", "nm", target_project_id=5)
    project = ProjectContext(5, None, None, 1, 8, 9, 10, 11, 12, existing_project=True)
    # Child before parent, with IDs repeated across SWCs. Sequence gaps and
    # integers above JavaScript's exact range must survive the CSV unchanged.
    nodes = (SwcNode(42, 3, 1, 0, 0, -1, 7), SwcNode(7, 1, 0, 0, 0, -1, -1))
    ids = [200, 207, 9007199254740993, 9007199254741001]
    with prepare_copy_rows(batch, request, project, list(range(100, 110)), ids,
                           lambda _member: nodes) as prepared:
        rows = list(csv.DictReader(prepared.node_map_file))
        assert list(rows[0]) == list(NODE_MAP_FIELDS)
        assert rows == [
            {"member_path": members[i // 2].member_path,
             "source_node_id": str([42, 7][i % 2]), "treenode_id": str(node_id)}
            for i, node_id in enumerate(ids)
        ]
        copied = list(csv.reader(prepared.treenode_file))
        assert [int(row[0]) for row in copied] == ids
        assert [row[-1] for row in copied] == [str(ids[1]), r"\N", str(ids[3]), r"\N"]
        prepared.rewind()
        assert prepared.node_map_file.tell() == 0
    assert prepared.node_map_file.closed


def test_expected_cable_uses_catmaid_float4_coordinates(tmp_path: Path) -> None:
    request = IngestionRequest(
        schema_version=1,
        archive_path=tmp_path / "input.zip",
        coordinate_unit="um",
        dimension=(200_000, 10, 10),
        resolution_nm=(1000, 1000, 1000),
    )
    member = PlannedMember(
        member_path="cell.swc",
        node_count=2,
        uncompressed_bytes=20,
        crc32=123,
        display_name="cell",
        fragment_id="cell",
        cable_length_nm=1.0,
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
        annotation_class_id=11,
        annotated_with_relation_id=12,
    )
    nodes = (
        SwcNode(1, 3, 100_000.001, 0.0, 0.0, -1.0, -1),
        SwcNode(2, 3, 100_000.002, 0.0, 0.0, -1.0, 1),
    )

    with prepare_copy_rows(
        batch,
        request,
        project,
        concept_ids=[100, 101, 102, 103, 104],
        location_ids=[200, 201],
        load_nodes=lambda _member: nodes,
    ) as prepared:
        # Both X coordinates round to 100,000,000 in PostgreSQL real columns.
        assert prepared.expected_skeletons[0].cable_length_nm == 0.0
