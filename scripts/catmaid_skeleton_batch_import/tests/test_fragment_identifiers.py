from __future__ import annotations

import ast
import gzip
import json
from dataclasses import replace
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.artifacts import manifest_rows, read_manifest, write_manifest
from catmaid_skeleton_batch_import.errors import ImmutableStateError, InvalidInputError
from catmaid_skeleton_batch_import.fragments import (
    MAX_FRAGMENT_ID_LENGTH, SOURCE_FRAGMENT_ANNOTATION_PREFIX,
    fragment_annotation_name, fragment_id_from_member_path,
)
from catmaid_skeleton_batch_import.planner import build_plan, read_plan, write_plan
from catmaid_skeleton_batch_import.state import StateStore
from test_planner import member


@pytest.mark.parametrize(("filename", "expected"), [
    ("191805/0021814.swc", "0021814"),
    ("1470096546.0.swc", "1470096546.0"),
    ("nested/1054599727.23.SWC", "1054599727.23"),
    ("900719925474099312345678901234567890.swc", "900719925474099312345678901234567890"),
    ("Case {n} {n123} .swc", "Case {n} {n123} "),
    ("nested/a.b.swc.swc", "a.b.swc"),
])
def test_exact_filename_fragment_identifier(filename, expected):
    assert fragment_id_from_member_path(filename) == expected
    assert fragment_annotation_name(expected) == "source-fragment:" + expected


def test_annotation_length_boundary_and_no_silent_truncation():
    assert MAX_FRAGMENT_ID_LENGTH == 239
    assert len(fragment_annotation_name(fragment_id_from_member_path("é" * 239 + ".swc"))) == 255
    with pytest.raises(InvalidInputError, match="239 characters"):
        fragment_id_from_member_path("a" * 240 + ".swc")
    with pytest.raises(InvalidInputError, match="empty fragment identifier"):
        fragment_id_from_member_path("folder/.swc")


def test_duplicate_identifiers_rejected_across_directories_and_batches():
    with pytest.raises(InvalidInputError, match="duplicate fragment identifier"):
        build_plan([member("a/0021814.swc", 2), member("z/0021814.SWC", 2)], 2)
    plan = build_plan([member("ID.swc", 2), member("id.swc", 2), member("1.swc", 2),
                       member("01.swc", 2), member("1.0.swc", 2)], 2)
    assert len(plan.members) == 5


def test_plan_rejects_fragment_identifier_inconsistent_with_filename():
    with pytest.raises(InvalidInputError, match="does not match"):
        build_plan([replace(member("001.swc", 2), fragment_id="1")], 3)


def test_new_plan_and_manifest_preserve_identifier_and_five_id_layout(tmp_path):
    plan = build_plan([member("191805/0021814.swc", 2)], 2)
    path = tmp_path / "plan.jsonl.gz"
    write_plan(path, plan)
    loaded = read_plan(path)
    assert loaded.schema_version == 3
    assert loaded.members[0].fragment_id == "0021814"
    rows = manifest_rows(loaded.batches[0], [10, 11, 12, 13, 14])
    assert rows[0]["fragment_id"] == "0021814"
    assert rows[0]["neuron_id"] == 10 and rows[0]["skeleton_id"] == 11
    manifest = tmp_path / "manifest.csv.gz"
    write_manifest(manifest, rows)
    assert read_manifest(manifest) == [{
        "member_path": "191805/0021814.swc", "fragment_id": "0021814",
        "neuron_id": "10", "skeleton_id": "11", "node_count": "2",
        "name": "0021814", "batch_index": "1",
    }]


@pytest.mark.parametrize("schema_version", [1, 2])
def test_rejects_old_development_plan_before_reading_old_member_layout(tmp_path, schema_version):
    path = tmp_path / "old.jsonl.gz"
    with gzip.open(path, "wt") as target:
        target.write(json.dumps({"record_type": "header", "schema_version": schema_version}) + "\n")
    with pytest.raises(InvalidInputError, match="old development workspaces cannot be resumed"):
        read_plan(path)


@pytest.mark.parametrize("schema_version", [1, 2])
def test_rejects_old_development_state_with_clear_recovery_message(tmp_path, schema_version):
    (tmp_path / "state.json").write_text(json.dumps({"schema_version": schema_version}))
    with pytest.raises(ImmutableStateError, match="start a new state directory"):
        StateStore(tmp_path).load()


def test_importer_and_catmaid_share_literal_annotation_prefix():
    fork = Path(__file__).resolve().parents[3]
    module = fork / "django/applications/catmaid/source_fragments.py"
    assert module.is_file(), "run the fork contract test from a complete CATMAID checkout"
    tree = ast.parse(module.read_text())
    assignments = {
        target.id: ast.literal_eval(node.value)
        for node in tree.body if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id == "SOURCE_FRAGMENT_ANNOTATION_PREFIX"
    }
    assert assignments["SOURCE_FRAGMENT_ANNOTATION_PREFIX"] == SOURCE_FRAGMENT_ANNOTATION_PREFIX


@pytest.mark.parametrize("corruption", [
    "annotation_name", "annotation_class", "annotation_owner", "annotation_project",
    "link_owner", "link_relation", "link_endpoints", "missing_annotation",
    "missing_link", "duplicate_annotation", "duplicate_link",
])
def test_exact_metadata_verification_rejects_corruption(corruption):
    from catmaid_skeleton_batch_import.materialization import validate_batch
    from catmaid_skeleton_batch_import.errors import VerificationError
    from test_catmaid_integration import ScriptedConnection, _valid_batch_responses

    responses = _valid_batch_responses()[2:]
    if corruption.startswith("annotation_"):
        row = list(responses[0][2])
        field = {"annotation_name": 4, "annotation_class": 3,
                 "annotation_owner": 1, "annotation_project": 2}[corruption]
        row[field] = "source-fragment:altered" if field == 4 else 999
        responses[0][2] = tuple(row)
    elif corruption.startswith("link_"):
        row = list(responses[1][1])
        field = {"link_owner": 1, "link_relation": 3, "link_endpoints": 5}[corruption]
        row[field] = 999
        responses[1][1] = tuple(row)
    elif corruption == "missing_annotation":
        responses[5] = []
    elif corruption == "missing_link":
        responses[6] = []
    elif corruption == "duplicate_annotation":
        responses[5].append((99, 7, 1, 33, "source-fragment:a"))
    else:
        responses[6].append((99, 7, 1, 42, 10, 13))
    with pytest.raises(VerificationError):
        validate_batch(
            project_id=1, user_id=7, neuron_class_id=31, skeleton_class_id=32,
            model_of_relation_id=41, annotation_class_id=33,
            annotated_with_relation_id=42,
            skeletons=[dict(neuron_id=10, skeleton_id=11, link_id=12,
                            annotation_id=13, annotation_link_id=14, fragment_id="a",
                            name="a", node_count=2, cable_length_nm=5.0)],
            location_ids=[20, 21], connection=ScriptedConnection(responses),
        )


def test_copy_rows_preserve_literal_identifiers_without_name_template_expansion(tmp_path):
    import csv
    from catmaid_skeleton_batch_import.domain import IngestionRequest, SwcNode
    from catmaid_skeleton_batch_import.loader import ProjectContext, prepare_copy_rows
    request = IngestionRequest(1, tmp_path / "source.zip", "nm", (10, 10, 10), (1, 1, 1))
    plan = build_plan([member("folder/00001.0 {n}.SWC", 2)], 2)
    project = ProjectContext(1, 2, 3, 7, 31, 32, 41, 33, 42)
    nodes = (SwcNode(1, 1, 1, 1, 1, 1, -1), SwcNode(2, 3, 2, 1, 1, 1, 1))
    # This helper checks cable length against the source independently of metadata.
    batch = replace(plan.batches[0], members=(replace(plan.members[0], cable_length_nm=1.0),))
    with prepare_copy_rows(batch, request, project, [10, 11, 12, 13, 14], [20, 21], lambda _: nodes) as prepared:
        classes = list(csv.reader(prepared.class_file))
        links = list(csv.reader(prepared.relationship_file))
        assert classes == [
            ["10", "7", "1", "31", "00001.0 {n}"],
            ["11", "7", "1", "32", "00001.0 {n}"],
            ["13", "7", "1", "33", "source-fragment:00001.0 {n}"],
        ]
        assert links == [["12", "7", "1", "41", "11", "10"],
                         ["14", "7", "1", "42", "10", "13"]]
