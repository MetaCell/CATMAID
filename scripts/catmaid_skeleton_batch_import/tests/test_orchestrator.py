from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace
import math
from pathlib import Path
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import import loader, materialization, orchestrator, project_checks
from catmaid_skeleton_batch_import.artifacts import (
    artifact_descriptor, verify_artifact, write_id_artifact, write_node_map,
)
from catmaid_skeleton_batch_import.domain import IngestionRequest, PlannedMember, SwcNode
from catmaid_skeleton_batch_import.errors import ImmutableStateError
from catmaid_skeleton_batch_import.loader import ProjectContext, prepare_copy_rows
from catmaid_skeleton_batch_import.planner import build_plan


def _member(path: str, node_count: int) -> PlannedMember:
    return PlannedMember(
        member_path=path, node_count=node_count, uncompressed_bytes=node_count * 10,
        crc32=node_count, display_name=Path(path).stem, fragment_id=Path(path).stem,
        cable_length_nm=float(node_count),
    )


def _verification_inputs(tmp_path, plan, *, existing=False, unit="nm"):
    state = {"batches": []}
    for batch in plan.batches:
        artifacts = {}
        for name, count, start in (
            ("concept_ids", batch.skeleton_count * 5, batch.index * 100),
            ("location_ids", batch.node_count, batch.index * 1000),
        ):
            path = tmp_path / f"{batch.index}-{name}.gz"
            write_id_artifact(path, list(range(start, start + count)))
            artifacts[name] = artifact_descriptor(tmp_path, path, count=count)
        state["batches"].append({"index": batch.index, "artifacts": artifacts})
    project = ProjectContext(1, None if existing else 2, None if existing else 3,
                             7, 31, 32, 41, 33, 42, existing_project=existing)
    request = IngestionRequest(1, tmp_path / "source.zip", unit,
                               target_project_id=1) if existing else IngestionRequest(
        1, tmp_path / "source.zip", unit, (10, 10, 10), (1, 1, 1))
    connection = SimpleNamespace(cursor=lambda: nullcontext(object()))
    def register_artifact(name, path, *, count, compression, batch_index):
        batch = next(item for item in state["batches"] if item["index"] == batch_index)
        batch["artifacts"][name] = artifact_descriptor(tmp_path, tmp_path / path, count=count)
    def verify(name, *, batch_index):
        batch = next(item for item in state["batches"] if item["index"] == batch_index)
        return verify_artifact(tmp_path, batch["artifacts"][name])
    store = SimpleNamespace(state_dir=tmp_path, load=lambda: state,
                            register_artifact=register_artifact, verify_artifact=verify)
    return store, state, project, request, connection


def _save_node_maps(store, plan, request, project, load_nodes):
    for batch in plan.batches:
        with prepare_copy_rows(
            batch, request, project,
            list(range(batch.index * 100, batch.index * 100 + batch.skeleton_count * 5)),
            list(range(batch.index * 1000, batch.index * 1000 + batch.node_count)),
            load_nodes,
        ) as prepared:
            orchestrator._save_batch_node_map(store, prepared)


def _counts(skeletons, nodes):
    return {
        "class_instance_count": skeletons * 3, "neuron_count": skeletons,
        "skeleton_count": skeletons, "relationship_count": skeletons * 2,
        "annotation_count": skeletons, "annotation_link_count": skeletons,
        "model_of_count": skeletons, "treenode_count": nodes, "edge_count": nodes,
        "summary_count": skeletons,
    }


@pytest.mark.parametrize("existing", [False, True])
def test_final_verification_reads_each_swc_once_and_finishes_one_batch_at_a_time(
    tmp_path, monkeypatch, existing,
):
    plan = build_plan([_member("a.swc", 2), _member("b.swc", 3), _member("c.swc", 2)], 3)
    store, state, project, request, connection = _verification_inputs(
        tmp_path, plan, existing=existing)
    events = []
    def load_nodes(member):
        events.append(("load", member.member_path))
        return tuple(SwcNode(i + 1, 3, i * member.node_count / (member.node_count - 1),
                             0, 0, -1, -1 if i == 0 else i)
                     for i in range(member.node_count))
    def validate_batch(**kwargs):
        events.append(("validate", len(kwargs["location_ids"])))
        return _counts(len(kwargs["skeletons"]), len(kwargs["location_ids"]))
    def exact(_cursor, prepared):
        events.append(("exact", prepared.batch.index))
    def aggregate_counts(**kwargs):
        assert not existing
        events.append(("aggregate", kwargs["expected_node_count"]))
        return _counts(plan.member_count, plan.total_nodes)
    monkeypatch.setattr(materialization, "validate_batch", validate_batch)
    monkeypatch.setattr(loader, "verify_exact_treenodes", exact)
    monkeypatch.setattr(project_checks, "verify_project_aggregate_counts", aggregate_counts)

    _save_node_maps(store, plan, request, project, load_nodes)
    events.clear()
    result = orchestrator._verify_database_by_batch(
        store, plan, state, project, connection, load_nodes, request)

    assert result == _counts(3, 7)
    assert events == [
        ("load", "a.swc"), ("validate", 2), ("exact", 1),
        ("load", "b.swc"), ("validate", 3), ("exact", 2),
        ("load", "c.swc"), ("validate", 2), ("exact", 3),
    ] + ([] if existing else [("aggregate", 7)])
    assert all(batch["artifacts"]["node_map"]["count"] == count
               for batch, count in zip(state["batches"], [2, 3, 2]))


def test_final_verification_uses_stored_float4_cable_and_exact_node_checks(tmp_path, monkeypatch):
    nodes = (SwcNode(1, 3, 100_000.001, 0, 0, -1, -1),
             SwcNode(2, 3, 100_000.002, 0, 0, -1, 1))
    source_length_nm = math.dist((nodes[0].x, 0, 0), (nodes[1].x, 0, 0)) * 1000
    plan = build_plan([replace(_member("cell.swc", 2), cable_length_nm=source_length_nm)], 2)
    store, state, project, request, connection = _verification_inputs(
        tmp_path, plan, existing=True, unit="um")
    loads = []
    exact_checks = []
    def load_nodes(member):
        loads.append(member.member_path)
        return nodes
    def validate_batch(**kwargs):
        assert kwargs["skeletons"][0]["cable_length_nm"] == 0.0
        assert list(kwargs["location_ids"]) == [1000, 1001]
        return _counts(1, 2)
    def exact(_cursor, prepared):
        expected = list(loader._iter_expected_treenodes(prepared.treenode_file))
        assert [row[2] for row in expected] == [100_000_000.0, 100_000_000.0]
        exact_checks.append(True)
    monkeypatch.setattr(materialization, "validate_batch", validate_batch)
    monkeypatch.setattr(loader, "verify_exact_treenodes", exact)

    _save_node_maps(store, plan, request, project, load_nodes)
    loads.clear()
    assert orchestrator._verify_database_by_batch(
        store, plan, state, project, connection, load_nodes, request) == _counts(1, 2)
    assert loads == ["cell.swc"]
    assert exact_checks == [True]


@pytest.mark.parametrize("damage", ["missing", "contents", "count", "hash"])
def test_saved_mapping_is_checked_against_reservations_and_artifact_metadata(
    tmp_path, damage,
):
    import io

    plan = build_plan([_member("cell.swc", 2)], 2)
    store, state, project, request, _connection = _verification_inputs(tmp_path, plan)
    nodes = (SwcNode(1, 1, 0, 0, 0, -1, -1), SwcNode(2, 3, 2, 0, 0, -1, 1))
    with prepare_copy_rows(plan.batches[0], request, project, list(range(100, 105)),
                           [1000, 1001], lambda _member: nodes) as prepared:
        orchestrator._save_batch_node_map(store, prepared)
        descriptor = state["batches"][0]["artifacts"]["node_map"]
        path = tmp_path / descriptor["path"]
        if damage == "missing":
            del state["batches"][0]["artifacts"]["node_map"]
        elif damage == "count":
            descriptor["count"] = 1
        elif damage == "hash":
            path.write_bytes(path.read_bytes() + b"changed")
        else:
            write_node_map(path, io.StringIO(
                "member_path,source_node_id,treenode_id\ncell.swc,1,1001\ncell.swc,2,1000\n"))
            state["batches"][0]["artifacts"]["node_map"] = artifact_descriptor(
                tmp_path, path, count=2)
        with pytest.raises(ImmutableStateError):
            orchestrator._verify_batch_node_map(store, prepared)
