from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.domain import PlannedMember
from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.planner import (
    build_plan,
    plan_sha256,
    read_plan,
    serialize_plan,
    write_plan,
)


def member(path: str, nodes: int, external_id: str | None = None) -> PlannedMember:
    return PlannedMember(
        member_path=path,
        node_count=nodes,
        uncompressed_bytes=nodes * 10,
        crc32=nodes,
        display_name=Path(path).stem,
        cable_length_nm=float(nodes),
        external_skeleton_id=external_id,
    )


def test_sorts_exact_paths_and_greedily_builds_hard_batches() -> None:
    plan = build_plan(
        [member("c.swc", 80), member("a.swc", 100), member("b.swc", 120)],
        250,
    )

    assert [item.member_path for item in plan.members] == ["a.swc", "b.swc", "c.swc"]
    assert [batch.node_count for batch in plan.batches] == [220, 80]
    assert [item.batch_index for item in plan.members] == [1, 1, 2]
    assert plan.member_count == 3
    assert plan.batch_count == 2
    assert plan.total_nodes == 300


def test_rejects_oversized_and_duplicate_members() -> None:
    with pytest.raises(InvalidInputError, match="batch limit is 10"):
        build_plan([member("large.swc", 11)], 10)
    with pytest.raises(InvalidInputError, match="duplicate member path"):
        build_plan([member("same.swc", 2), member("same.swc", 2)], 10)
    with pytest.raises(InvalidInputError, match="duplicate external skeleton ID"):
        build_plan(
            [member("a.swc", 2, "same"), member("b.swc", 2, "same")], 10
        )


def test_gzip_jsonl_is_deterministic_and_round_trips(tmp_path: Path) -> None:
    plan = build_plan(
        [member("b.swc", 3, "external-b"), member("a.swc", 2, "external-a")],
        4,
    )
    first = serialize_plan(plan)
    second = serialize_plan(plan)
    assert first == second
    assert plan_sha256(plan) == plan_sha256(plan)

    path = tmp_path / "plan.jsonl.gz"
    digest = write_plan(path, plan)
    assert digest == plan_sha256(plan)
    assert read_plan(path) == plan

    with gzip.open(path, "rt", encoding="utf-8") as source:
        lines = source.readlines()
    assert len(lines) == 3
    assert '"record_type":"header"' in lines[0]
    assert '"member_path":"a.swc"' in lines[1]


def test_read_rejects_tampered_batch_assignment(tmp_path: Path) -> None:
    plan = build_plan([member("a.swc", 2), member("b.swc", 2)], 3)
    payload = gzip.decompress(serialize_plan(plan)).decode("utf-8")
    payload = payload.replace('"batch_index":2', '"batch_index":1')
    path = tmp_path / "tampered.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as target:
        target.write(payload)

    with pytest.raises(InvalidInputError, match="batch assignments"):
        read_plan(path)


def test_read_rejects_header_count_mismatch(tmp_path: Path) -> None:
    plan = build_plan([member("a.swc", 2)], 3)
    payload = gzip.decompress(serialize_plan(plan)).decode("utf-8")
    payload = payload.replace('"member_count":1', '"member_count":2')
    path = tmp_path / "bad-count.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as target:
        target.write(payload)

    with pytest.raises(InvalidInputError, match="header counts"):
        read_plan(path)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('"max_nodes_per_batch":3', '"max_nodes_per_batch":true', "integer"),
        ('"node_count":2', '"node_count":2.5', "integer"),
        ('"member_path":"a.swc"', '"member_path":null', "string"),
    ],
)
def test_read_rejects_noncanonical_field_types(
    tmp_path: Path, old: str, new: str, message: str
) -> None:
    plan = build_plan([member("a.swc", 2)], 3)
    payload = gzip.decompress(serialize_plan(plan)).decode("utf-8").replace(old, new)
    path = tmp_path / "bad-type.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as target:
        target.write(payload)

    with pytest.raises(InvalidInputError, match=message):
        read_plan(path)
