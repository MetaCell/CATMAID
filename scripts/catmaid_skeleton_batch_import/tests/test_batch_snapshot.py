import json
from io import BytesIO
from dataclasses import replace
from zipfile import ZipFile
from unittest.mock import patch

import pytest

from helpers import write_stack_snapshot

from catmaid_skeleton_batch_import.errors import ImmutableStateError, InvalidInputError
from catmaid_skeleton_batch_import.orchestrator import plan_ingestion
from catmaid_skeleton_batch_import.planner import read_plan, write_plan
from catmaid_skeleton_batch_import.batch_snapshot import BatchSnapshotReader
from catmaid_skeleton_batch_import.swc import parse_swc


def planned(tmp_path, monkeypatch):
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "4")
    archive = tmp_path / "source.zip"
    text = b"80 1 1.0000000596046448 2 3 1 -1\n3 2 2 2 3 0.10000000000000001 80\n"
    with ZipFile(archive, "w") as output:
        for name in ("2037433456.1.swc", "2037433456.0.swc", "x.swc"):
            output.writestr(name, text)
    request = tmp_path / "request.json"
    request.write_text(json.dumps({'schema_version': 1, 'source': {'archive_path': str(archive), 'coordinate_unit': 'nm'}, 'target': {'project_id': 123}}))
    state = tmp_path / "state"
    write_stack_snapshot(state)
    plan_ingestion(request, state)
    return state, read_plan(state / "plan.jsonl.gz"), text, request


def test_plan_retains_exact_validated_nodes_in_two_bounded_batches(tmp_path, monkeypatch):
    state, plan, text, request = planned(tmp_path, monkeypatch)
    assert plan.schema_version == 4
    assert [b.node_count for b in plan.batches] == [4, 2]
    expected = parse_swc(BytesIO(text), "test", max_bytes=1000, max_nodes=4, scale_nm=1, bounds_nm=(((0, 0, 0), (100, 100, 100)),)).nodes
    with patch("catmaid_skeleton_batch_import.archive.parse_swc", side_effect=AssertionError("SWCs must not be reparsed")):
        for _ in range(2):  # loading and final verification use the same immutable input
            with BatchSnapshotReader(state, plan) as source:
                for member in plan.members:
                    assert source.load(member) == expected
    original = (state / "state.json").read_bytes()
    with pytest.raises(InvalidInputError, match="already contains"):
        plan_ingestion(request, state)
    assert (state / "state.json").read_bytes() == original


def test_snapshot_tampering_fails_before_values_are_used(tmp_path, monkeypatch):
    state, plan, _, _ = planned(tmp_path, monkeypatch)
    path = state / plan.batch_snapshots[0]["path"]
    path.write_bytes(path.read_bytes() + b"tampered")
    with BatchSnapshotReader(state, plan) as source, pytest.raises(ImmutableStateError):
        source.load(plan.members[0])


@pytest.mark.parametrize("damage", ["missing", "path", "count", "hash"])
def test_plan_requires_valid_batch_snapshot_descriptors(tmp_path, monkeypatch, damage):
    state, plan, _, _ = planned(tmp_path, monkeypatch)
    descriptors = [dict(item) for item in plan.batch_snapshots]
    if damage == "missing":
        descriptors = []
    else:
        key, value = {"path": ("path", "unexpected.jsonl.gz"),
                      "count": ("node_count", 999),
                      "hash": ("sha256", "invalid")}[damage]
        descriptors[0][key] = value
    path = state / "invalid.jsonl.gz"
    write_plan(path, replace(plan, batch_snapshots=tuple(descriptors)))
    with pytest.raises(InvalidInputError, match="batch snapshot"):
        read_plan(path)


def test_invalid_archive_does_not_publish_batch_snapshots(tmp_path, monkeypatch):
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "4")
    archive = tmp_path / "source.zip"
    with ZipFile(archive, "w") as output:
        output.writestr("good.swc", "1 1 1 1 1 1 -1\n2 1 2 1 1 1 1\n")
        output.writestr("bad.swc", "1 1 1 1 1 1 99\n")
    request = tmp_path / "request.json"
    request.write_text(json.dumps({'schema_version': 1, 'source': {'archive_path': str(archive), 'coordinate_unit': 'nm'}, 'target': {'project_id': 123}}))
    write_stack_snapshot(tmp_path / "state")
    with pytest.raises(InvalidInputError):
        plan_ingestion(request, tmp_path / "state")
    assert not list((tmp_path / "state").glob("batch-snapshot-*"))
