from contextlib import nullcontext
import io
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import import database
from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.stack_bounds import (
    contains_point, geometry_bounds, read_project_stack_geometry,
)
from catmaid_skeleton_batch_import.swc import parse_swc


def stack(**overrides):
    return {"project_stack_id": 1, "stack_id": 2, "orientation": 0,
            "dimension": [10, 20, 30], "resolution_nm": [2, 3, 4],
            "translation_nm": [100, 200, 300], **overrides}


@pytest.mark.parametrize("orientation,maximum", [
    (0, (120, 260, 420)), (1, (120, 320, 360)), (2, (220, 260, 320)),
])
def test_stack_bounds_match_catmaid_orientation_and_translation(orientation, maximum):
    bounds = geometry_bounds({"project_id": 123, "stacks": [stack(orientation=orientation)]})
    assert bounds == (((100, 200, 300), maximum),)
    assert contains_point((100, 200, 300), bounds)
    assert contains_point(maximum, bounds)
    assert not contains_point((99, 200, 300), bounds)
    assert not contains_point(tuple(value + 1 for value in maximum), bounds)


@pytest.mark.parametrize("unit_scale", [1, 1000])
def test_append_parser_checks_the_union_of_actual_stacks_without_accepting_gaps(unit_scale):
    bounds = geometry_bounds({"project_id": 123, "stacks": [
        stack(translation_nm=[0, 0, 0]), stack(translation_nm=[100, 0, 0]),
    ]})
    # Each node may belong to a different associated stack.
    content = (f"1 1 0 0 0 -1 -1\n2 3 {120 / unit_scale} 0 0 -1 1\n").encode()
    assert parse_swc(io.BytesIO(content), "two.swc", scale_nm=unit_scale, bounds_nm=bounds).summary.node_count == 2
    for x in (50, 121):
        content = f"# header\n1 1 {x / unit_scale} 0 0 -1 -1\n".encode()
        with pytest.raises(InvalidInputError, match=r"gap.swc:2: point x=.*outside all associated"):
            parse_swc(io.BytesIO(content), "gap.swc", scale_nm=unit_scale, bounds_nm=bounds)


@pytest.mark.parametrize("overrides", [
    {"orientation": 3}, {"dimension": [0, 2, 3]}, {"resolution_nm": [1, 2, float("inf")]},
    {"translation_nm": [0, 0, float("nan")]}, {"dimension": [1, 2]},
])
def test_invalid_stack_metadata_is_rejected(overrides):
    with pytest.raises(InvalidInputError, match="invalid geometry"):
        geometry_bounds({"project_id": 123, "stacks": [stack(**overrides)]})


def test_target_without_associated_stacks_is_rejected():
    with pytest.raises(InvalidInputError, match="at least one associated stack"):
        geometry_bounds({"project_id": 123, "stacks": []})


def test_lookup_reads_associated_stack_fields_and_binds_database_identity(monkeypatch):
    calls = []
    cursor = SimpleNamespace(
        execute=lambda sql, params: calls.append((sql, params)),
        fetchall=lambda: [(1, 2, 1, 10, 20, 30, 2, 3, 4, 100, 200, 300)],
    )
    connection = SimpleNamespace(cursor=lambda: nullcontext(cursor))
    monkeypatch.setattr(database, "bootstrap_django", lambda: connection)
    monkeypatch.setattr(database, "database_target_fingerprint", lambda actual:
                        {"sha256": "cluster"} if actual is connection else pytest.fail("wrong DB"))
    geometry = read_project_stack_geometry(123)
    assert geometry["database_target"] == {"sha256": "cluster"}
    assert geometry["stacks"] == [stack(orientation=1)]
    assert calls[0][1] == [123]
    assert "WHERE ps.project_id=%s ORDER BY ps.id" in calls[0][0]
    assert "JOIN stack" in calls[0][0]
    assert geometry_bounds(geometry) == (((100, 200, 300), (120, 320, 360)),)
