from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from catmaid_skeleton_batch_import import materialization, orchestrator, verification
from catmaid_skeleton_batch_import.domain import PlannedMember, SwcNode
from catmaid_skeleton_batch_import.loader import ProjectContext
from catmaid_skeleton_batch_import.planner import build_plan


def _member(path: str, node_count: int) -> PlannedMember:
    return PlannedMember(
        member_path=path,
        node_count=node_count,
        uncompressed_bytes=node_count * 10,
        crc32=node_count,
        display_name=Path(path).stem,
        fragment_id=Path(path).stem,
        cable_length_nm=float(node_count),
    )


def test_final_database_verification_loads_then_validates_one_batch_at_a_time(
    monkeypatch,
) -> None:
    plan = build_plan(
        [_member("a.swc", 2), _member("b.swc", 3), _member("c.swc", 2)],
        max_nodes_per_batch=3,
    )
    project = ProjectContext(
        project_id=1,
        stack_id=2,
        project_stack_id=3,
        user_id=7,
        neuron_class_id=31,
        skeleton_class_id=32,
        model_of_relation_id=41,
        annotation_class_id=33,
        annotated_with_relation_id=42,
    )
    events = []

    def expected_batch_data(_store, batch, _state, _load_nodes, _scale_nm):
        events.append(("load", batch.index, batch.node_count))
        expected = [
            {
                "neuron_id": batch.index * 10,
                "skeleton_id": batch.index * 10 + 1,
                "link_id": batch.index * 10 + 2,
                "annotation_id": batch.index * 10 + 3,
                "annotation_link_id": batch.index * 10 + 4,
                "fragment_id": batch.members[0].fragment_id,
                "name": batch.members[0].display_name,
                "node_count": batch.node_count,
                "cable_length_nm": float(batch.node_count),
            }
        ]
        return expected, list(range(batch.node_count))

    def validate_batch(**kwargs):
        events.append(
            (
                "validate",
                len(kwargs["skeletons"]),
                len(kwargs["location_ids"]),
            )
        )
        skeleton_count = len(kwargs["skeletons"])
        node_count = len(kwargs["location_ids"])
        return {
            "class_instance_count": skeleton_count * 2,
            "neuron_count": skeleton_count,
            "skeleton_count": skeleton_count,
            "relationship_count": skeleton_count,
            "treenode_count": node_count,
            "edge_count": node_count,
            "summary_count": skeleton_count,
        }

    aggregate = {
        "class_instance_count": plan.member_count * 2,
        "neuron_count": plan.member_count,
        "skeleton_count": plan.member_count,
        "relationship_count": plan.member_count,
        "treenode_count": plan.total_nodes,
        "edge_count": plan.total_nodes,
        "summary_count": plan.member_count,
    }

    def aggregate_counts(**kwargs):
        events.append(
            (
                "aggregate",
                kwargs["expected_skeleton_count"],
                kwargs["expected_node_count"],
            )
        )
        return aggregate

    monkeypatch.setattr(orchestrator, "_expected_batch_data", expected_batch_data)
    monkeypatch.setattr(materialization, "validate_batch", validate_batch)
    monkeypatch.setattr(
        verification, "verify_project_aggregate_counts", aggregate_counts
    )

    result = orchestrator._verify_database_by_batch(
        SimpleNamespace(), plan, {}, project, object(), lambda _member: (), 1.0
    )

    assert result == aggregate
    assert events == [
        ("load", 1, 2),
        ("validate", 1, 2),
        ("load", 2, 3),
        ("validate", 1, 3),
        ("load", 3, 2),
        ("validate", 1, 2),
        ("aggregate", 3, 7),
    ]


def test_final_expected_cable_uses_catmaid_float4_coordinates(
    monkeypatch,
    tmp_path,
) -> None:
    batch = build_plan([_member("cell.swc", 2)], max_nodes_per_batch=2).batches[0]
    state = {
        "batches": [
            {
                "index": 1,
                "artifacts": {"concept_ids": {}, "location_ids": {}},
            }
        ]
    }
    monkeypatch.setattr(
        orchestrator,
        "read_id_artifact",
        lambda _state_dir, _descriptor, expected_count: (
            [10, 11, 12, 13, 14] if expected_count == 5 else [20, 21]
        ),
    )
    nodes = (
        SwcNode(1, 3, 100_000.001, 0.0, 0.0, -1.0, -1),
        SwcNode(2, 3, 100_000.002, 0.0, 0.0, -1.0, 1),
    )

    expected, location_ids = orchestrator._expected_batch_data(
        SimpleNamespace(state_dir=tmp_path),
        batch,
        state,
        lambda _member: nodes,
        1000.0,
    )

    assert expected[0]["cable_length_nm"] == 0.0
    assert location_ids == [20, 21]
