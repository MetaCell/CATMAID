import json
from io import BytesIO
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile
from unittest.mock import patch

import pytest

from catmaid_skeleton_batch_import.config import load_request
from catmaid_skeleton_batch_import.errors import ImmutableStateError, InvalidInputError
from catmaid_skeleton_batch_import.orchestrator import plan_ingestion
from catmaid_skeleton_batch_import.planner import read_plan, write_plan
from catmaid_skeleton_batch_import.prepared_source import PreparedSourceReader
from catmaid_skeleton_batch_import.swc import parse_swc


def planned(tmp_path, monkeypatch):
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "4")
    archive = tmp_path / "source.zip"
    text = b"80 1 1.0000000596046448 2 3 1 -1\n3 2 2 2 3 0.10000000000000001 80\n"
    with ZipFile(archive, "w") as output:
        for name in ("2037433456.1.swc", "2037433456.0.swc", "x.swc"):
            output.writestr(name, text)
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"schema_version": 1, "source": {"archive_path": str(archive), "coordinate_unit": "nm"},
                                   "stack": {"dimension": [100, 100, 100], "resolution_nm": [1, 1, 1]}}))
    state = tmp_path / "state"
    plan_ingestion(request, state)
    return state, read_plan(state / "plan.jsonl.gz"), text, request


def test_plan_retains_exact_validated_nodes_in_two_bounded_batches(tmp_path, monkeypatch):
    state, plan, text, request = planned(tmp_path, monkeypatch)
    assert plan.schema_version == 4
    assert [b.node_count for b in plan.batches] == [4, 2]
    expected = parse_swc(BytesIO(text), "test", max_bytes=1000, max_nodes=4, scale_nm=1, max_nm=(100, 100, 100)).nodes
    with patch("catmaid_skeleton_batch_import.orchestrator.parse_swc", side_effect=AssertionError("SWCs must not be reparsed")):
        for _ in range(2):  # loading and final verification use the same immutable input
            with PreparedSourceReader(state, plan) as source:
                for member in plan.members:
                    assert source.load(member) == expected
    original = (state / "state.json").read_bytes()
    plan_ingestion(request, state)
    assert (state / "state.json").read_bytes() == original


def test_cache_tampering_fails_before_values_are_used(tmp_path, monkeypatch):
    state, plan, _, _ = planned(tmp_path, monkeypatch)
    path = state / plan.prepared_batches[0]["path"]
    path.write_bytes(path.read_bytes() + b"tampered")
    with PreparedSourceReader(state, plan) as source, pytest.raises(ImmutableStateError):
        source.load(plan.members[0])


def test_legacy_v3_plan_still_round_trips_without_immutable_rewrite(tmp_path, monkeypatch):
    state, plan, _, _ = planned(tmp_path, monkeypatch)
    legacy = replace(plan, schema_version=3, prepared_batches=())
    path = state / "legacy.jsonl.gz"
    digest = write_plan(path, legacy)
    assert read_plan(path) == legacy
    assert write_plan(path, read_plan(path)) == digest


def test_invalid_archive_does_not_publish_prepared_data(tmp_path, monkeypatch):
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "4")
    archive = tmp_path / "source.zip"
    with ZipFile(archive, "w") as output:
        output.writestr("good.swc", "1 1 1 1 1 1 -1\n2 1 2 1 1 1 1\n")
        output.writestr("bad.swc", "1 1 1 1 1 1 99\n")
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"schema_version": 1, "source": {"archive_path": str(archive), "coordinate_unit": "nm"},
                                   "stack": {"dimension": [100, 100, 100], "resolution_nm": [1, 1, 1]}}))
    with pytest.raises(InvalidInputError):
        plan_ingestion(request, tmp_path / "state")
    assert not list((tmp_path / "state").glob("prepared-*"))
