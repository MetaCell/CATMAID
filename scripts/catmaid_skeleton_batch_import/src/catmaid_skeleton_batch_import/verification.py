"""Read-only final verification for importer-owned CATMAID data."""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .cache import cache_bounds_nm
from .errors import VerificationError
from .materialization import (
    DEFAULT_CABLE_ABS_TOLERANCE_NM,
    DEFAULT_CABLE_REL_TOLERANCE,
    validate_batch,
)
from .project import SYSTEM_PROJECT_PERMISSIONS, build_stack_metadata


def _default_connection(connection: Any | None) -> Any:
    if connection is not None:
        return connection
    from django.db import connection as django_connection

    return django_connection


def _fail(message: str, **details: object) -> None:
    raise VerificationError(message, details=details)


def _vector(value: Any, label: str) -> tuple[float, float, float]:
    if all(hasattr(value, coordinate) for coordinate in ("x", "y", "z")):
        values = (value.x, value.y, value.z)
    elif isinstance(value, str) and value.startswith("(") and value.endswith(")"):
        values = tuple(part.strip() for part in value[1:-1].split(","))
    else:
        try:
            values = tuple(value)
        except TypeError as exc:
            raise VerificationError(f"{label} is not a 3-vector") from exc
    if len(values) != 3:
        _fail(f"{label} is not a 3-vector", actual=value)
    try:
        normalized = tuple(float(item) for item in values)
    except (TypeError, ValueError) as exc:
        raise VerificationError(f"{label} is not numeric") from exc
    return normalized  # type: ignore[return-value]


def _default_permission_snapshot(project_id: int) -> dict[str, Any]:
    from catmaid.models import Project
    from django.contrib.contenttypes.models import ContentType
    from guardian.models import GroupObjectPermission, UserObjectPermission

    project = Project.objects.get(pk=project_id)
    content_type = ContentType.objects.get_for_model(project, for_concrete_model=False)
    lookup = {"content_type": content_type, "object_pk": str(project.pk)}
    user_rows = UserObjectPermission.objects.filter(**lookup).values_list(
        "user_id", "permission__codename"
    )
    group_rows = GroupObjectPermission.objects.filter(**lookup).values_list(
        "group_id", "permission__codename"
    )
    users: dict[int, set[str]] = {}
    groups: dict[int, set[str]] = {}
    for user_id, permission in user_rows:
        users.setdefault(int(user_id), set()).add(str(permission))
    for group_id, permission in group_rows:
        groups.setdefault(int(group_id), set()).add(str(permission))
    return {"users": users, "groups": groups}


def _default_tracing_check(project_id: int) -> bool:
    from catmaid.control.tracing import check_tracing_setup

    return bool(check_tracing_setup(project_id))


def verify_project_stack(
    *,
    project_id: int,
    stack_id: int,
    project_stack_id: int,
    system_user_id: int,
    title: str,
    dimension: Sequence[int],
    resolution_nm: Sequence[int | float],
    cache_cell_size_nm: int = 1_500_000,
    connection: Any | None = None,
    permission_loader: Callable[[int], Mapping[str, Any]] | None = None,
    tracing_check: Callable[[int], bool] | None = None,
) -> dict[str, Any]:
    """Verify exact stack geometry/profile, link, tracing setup, and visibility."""

    expected_metadata = build_stack_metadata(cache_cell_size_nm)
    expected_dimension = tuple(float(value) for value in dimension)
    expected_resolution = tuple(float(value) for value in resolution_nm)
    if len(expected_dimension) != 3 or len(expected_resolution) != 3:
        raise ValueError("dimension and resolution_nm must have three values")

    connection = _default_connection(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT p.id, p.title,
                   s.id, s.title, s.dimension, s.resolution,
                   s.downsample_factors, s.metadata, s.canary_location,
                   ps.id, ps.translation, ps.orientation
            FROM project p
            JOIN project_stack ps ON ps.project_id = p.id
            JOIN stack s ON s.id = ps.stack_id
            WHERE p.id = %s AND s.id = %s AND ps.id = %s
            """,
            (project_id, stack_id, project_stack_id),
        )
        rows = cursor.fetchall()
        cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM project_stack WHERE project_id = %s),
              (SELECT count(*) FROM project_stack WHERE stack_id = %s)
            """,
            (project_id, stack_id),
        )
        link_counts = cursor.fetchone()

    if len(rows) != 1:
        _fail("project, stack, and project-stack link do not resolve exactly once")
    row = rows[0]
    (
        actual_project_id,
        project_title,
        actual_stack_id,
        stack_title,
        actual_dimension,
        actual_resolution,
        downsample_factors,
        metadata,
        canary,
        actual_project_stack_id,
        translation,
        orientation,
    ) = row
    if (
        int(actual_project_id) != project_id
        or int(actual_stack_id) != stack_id
        or int(actual_project_stack_id) != project_stack_id
        or str(project_title) != title
        or str(stack_title) != title
    ):
        _fail("project/stack identity does not match")
    if _vector(actual_dimension, "stack.dimension") != expected_dimension:
        _fail("stack.dimension does not match", actual=actual_dimension)
    if _vector(actual_resolution, "stack.resolution") != expected_resolution:
        _fail("stack.resolution does not match", actual=actual_resolution)
    if downsample_factors is not None:
        _fail("metadata-only stack must not have downsample factors")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise VerificationError("stack metadata is not valid JSON") from exc
    if metadata != expected_metadata:
        _fail("stack metadata does not match the one-profile contract", actual=metadata)
    expected_canary = tuple(float(value // 2) for value in dimension)
    if _vector(canary, "stack.canary_location") != expected_canary:
        _fail("stack canary location does not match")
    if _vector(translation, "project_stack.translation") != (0.0, 0.0, 0.0):
        _fail("project-stack translation must be zero")
    if int(orientation) != 0:
        _fail("project-stack orientation must be XY")
    if link_counts is None or tuple(int(value) for value in link_counts) != (1, 1):
        _fail(
            "importer stack must have exactly one project-stack association",
            actual=link_counts,
        )

    permissions = dict(
        (permission_loader or _default_permission_snapshot)(project_id)
    )
    users = {
        int(user_id): set(user_permissions)
        for user_id, user_permissions in dict(permissions.get("users", {})).items()
    }
    groups = {
        int(group_id): set(group_permissions)
        for group_id, group_permissions in dict(permissions.get("groups", {})).items()
    }
    expected_users = {system_user_id: set(SYSTEM_PROJECT_PERMISSIONS)}
    if users != expected_users or groups:
        _fail(
            "project is not hidden with only the system-user object grants",
            users={key: sorted(value) for key, value in users.items()},
            groups={key: sorted(value) for key, value in groups.items()},
        )
    if not (tracing_check or _default_tracing_check)(project_id):
        _fail("CATMAID tracing setup is incomplete")

    return {
        "project_id": project_id,
        "stack_id": stack_id,
        "project_stack_id": project_stack_id,
        "metadata": expected_metadata,
        "hidden": True,
        "tracing_setup": True,
    }


def verify_project_aggregate_counts(
    *,
    project_id: int,
    neuron_class_id: int,
    skeleton_class_id: int,
    model_of_relation_id: int,
    expected_skeleton_count: int,
    expected_node_count: int,
    connection: Any | None = None,
) -> dict[str, int]:
    """Check whole-project row counts without loading project IDs into memory.

    Callers must still run :func:`validate_batch` for every planned batch.  The
    exact per-batch checks catch missing/replaced IDs and bad topology; these
    aggregate counts then catch any additional project-owned rows.
    """

    for value, label in (
        (project_id, "project_id"),
        (neuron_class_id, "neuron_class_id"),
        (skeleton_class_id, "skeleton_class_id"),
        (model_of_relation_id, "model_of_relation_id"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} must be a positive integer")
    for value, label in (
        (expected_skeleton_count, "expected_skeleton_count"),
        (expected_node_count, "expected_node_count"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{label} must be a non-negative integer")

    connection = _default_connection(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM class_instance
               WHERE project_id = %s AND class_id = %s),
              (SELECT count(*) FROM class_instance
               WHERE project_id = %s AND class_id = %s),
              (SELECT count(*) FROM class_instance_class_instance
               WHERE project_id = %s AND relation_id = %s),
              (SELECT count(*) FROM treenode WHERE project_id = %s),
              (SELECT count(*) FROM treenode_edge WHERE project_id = %s),
              (SELECT count(*) FROM catmaid_skeleton_summary WHERE project_id = %s)
            """,
            (
                project_id,
                neuron_class_id,
                project_id,
                skeleton_class_id,
                project_id,
                model_of_relation_id,
                project_id,
                project_id,
                project_id,
            ),
        )
        row = cursor.fetchone()
    if row is None or len(row) != 6:
        _fail("could not read project-wide database counts")

    actual = {
        "neuron_count": int(row[0]),
        "skeleton_count": int(row[1]),
        "relationship_count": int(row[2]),
        "treenode_count": int(row[3]),
        "edge_count": int(row[4]),
        "summary_count": int(row[5]),
    }
    expected = {
        "neuron_count": expected_skeleton_count,
        "skeleton_count": expected_skeleton_count,
        "relationship_count": expected_skeleton_count,
        "treenode_count": expected_node_count,
        "edge_count": expected_node_count,
        "summary_count": expected_skeleton_count,
    }
    if actual != expected:
        _fail(
            "project-wide database counts do not match the plan",
            expected=expected,
            actual=actual,
        )
    return {
        "class_instance_count": actual["neuron_count"] + actual["skeleton_count"],
        **actual,
    }


def verify_database(
    *,
    project_id: int,
    user_id: int,
    neuron_class_id: int,
    skeleton_class_id: int,
    model_of_relation_id: int,
    skeletons: Sequence[Mapping[str, Any]],
    location_ids: Sequence[int],
    connection: Any | None = None,
    cable_abs_tolerance_nm: float = DEFAULT_CABLE_ABS_TOLERANCE_NM,
    cable_rel_tolerance: float = DEFAULT_CABLE_REL_TOLERANCE,
) -> dict[str, int]:
    """Compatibility wrapper for exact checks over one supplied ID set.

    For large projects, do not collect every ID for this wrapper. Call
    :func:`validate_batch` once per bounded batch followed by
    :func:`verify_project_aggregate_counts` once for the whole project.
    """

    connection = _default_connection(connection)
    batch_counts = validate_batch(
        project_id=project_id,
        user_id=user_id,
        neuron_class_id=neuron_class_id,
        skeleton_class_id=skeleton_class_id,
        model_of_relation_id=model_of_relation_id,
        skeletons=skeletons,
        location_ids=location_ids,
        connection=connection,
        cable_abs_tolerance_nm=cable_abs_tolerance_nm,
        cable_rel_tolerance=cable_rel_tolerance,
    )
    aggregate_counts = verify_project_aggregate_counts(
        project_id=project_id,
        neuron_class_id=neuron_class_id,
        skeleton_class_id=skeleton_class_id,
        model_of_relation_id=model_of_relation_id,
        expected_skeleton_count=len(skeletons),
        expected_node_count=len(location_ids),
        connection=connection,
    )
    if batch_counts != aggregate_counts:
        _fail(
            "exact batch counts and project-wide counts disagree",
            batch=batch_counts,
            project=aggregate_counts,
        )
    return aggregate_counts


def verify_cache(
    *,
    project_id: int,
    dimension: Sequence[int | float],
    resolution_nm: Sequence[int | float],
    expected_node_count: int,
    cache_cell_size_nm: int = 1_500_000,
    lod_levels: int = 7,
    lod_bucket_size: int = 500,
    lod_strategy: str = "quadratic",
    connection: Any | None = None,
) -> dict[str, Any]:
    """Verify the one XY MessagePack grid and its completed cell payloads."""

    if (
        isinstance(expected_node_count, bool)
        or not isinstance(expected_node_count, int)
        or expected_node_count < 0
    ):
        raise ValueError("expected_node_count must be a non-negative integer")
    _, upper = cache_bounds_nm(dimension, resolution_nm)
    connection = _default_connection(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, orientation, cell_width, cell_height, cell_depth,
                   n_lod_levels, lod_strategy, lod_min_bucket_size,
                   n_largest_skeletons_limit, n_last_edited_skeletons_limit,
                   hidden_last_editor_id, allow_empty,
                   has_json_data, has_json_text_data, has_msgpack_data,
                   enabled, ordering
            FROM node_grid_cache
            WHERE project_id = %s
            ORDER BY id
            """,
            (project_id,),
        )
        grids = cursor.fetchall()
        if len(grids) != 1:
            _fail(
                "project must have exactly one node grid cache",
                actual_count=len(grids),
            )
        grid = grids[0]
        grid_id = int(grid[0])
        cursor.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE msgpack_data IS NULL),
                   count(*) FILTER (WHERE json_data IS NOT NULL),
                   count(*) FILTER (WHERE json_text_data IS NOT NULL),
                   min(x_index), max(x_index),
                   min(y_index), max(y_index),
                   min(z_index), max(z_index)
            FROM node_grid_cache_cell
            WHERE grid_id = %s
            """,
            (grid_id,),
        )
        cells = cursor.fetchone()
        cursor.execute(
            "SELECT count(*) FROM dirty_node_grid_cache_cell WHERE grid_id = %s",
            (grid_id,),
        )
        dirty_row = cursor.fetchone()

    (
        _,
        orientation,
        cell_width,
        cell_height,
        cell_depth,
        actual_lod_levels,
        actual_lod_strategy,
        actual_lod_bucket_size,
        largest_limit,
        last_edited_limit,
        hidden_editor,
        allow_empty,
        has_json,
        has_json_text,
        has_msgpack,
        enabled,
        ordering,
    ) = grid
    expected_grid = (
        0,
        cache_cell_size_nm,
        cache_cell_size_nm,
        cache_cell_size_nm,
        lod_levels,
        lod_strategy,
        lod_bucket_size,
    )
    actual_grid = (
        int(orientation),
        int(cell_width),
        int(cell_height),
        int(cell_depth),
        int(actual_lod_levels),
        str(actual_lod_strategy),
        int(actual_lod_bucket_size),
    )
    if actual_grid != expected_grid:
        _fail("node grid definition does not match", expected=expected_grid, actual=actual_grid)
    filters = (largest_limit, last_edited_limit, hidden_editor, ordering)
    if any(value is not None for value in filters):
        _fail("node grid contains unsupported filtering or ordering")
    if bool(allow_empty) or bool(has_json) or bool(has_json_text):
        _fail("node grid must be non-empty-only and MessagePack-only")
    if not bool(has_msgpack) or not bool(enabled):
        _fail("node grid MessagePack payload must be enabled")
    if cells is None:
        _fail("could not count node grid cache cells")
    cell_count = int(cells[0])
    null_msgpack = int(cells[1])
    json_cells = int(cells[2])
    json_text_cells = int(cells[3])
    if expected_node_count > 0 and cell_count == 0:
        _fail("non-empty import produced no cache cells")
    if null_msgpack or json_cells or json_text_cells:
        _fail(
            "cache cells contain missing or unsupported payloads",
            null_msgpack=null_msgpack,
            json_cells=json_cells,
            json_text_cells=json_text_cells,
        )
    dirty_count = int(dirty_row[0]) if dirty_row else -1
    if dirty_count != 0:
        _fail("node grid cache has dirty cells", dirty_count=dirty_count)

    if cell_count:
        extrema = tuple(int(value) for value in cells[4:10])
        max_indices = tuple(math.floor(value / cache_cell_size_nm) for value in upper)
        minimums = extrema[0], extrema[2], extrema[4]
        maximums = extrema[1], extrema[3], extrema[5]
        if any(value < 0 for value in minimums) or any(
            value > maximum for value, maximum in zip(maximums, max_indices)
        ):
            _fail(
                "cache cells fall outside the trusted stack extent",
                extrema=extrema,
                maximum_indices=max_indices,
            )

    return {
        "grid_id": grid_id,
        "grid_count": 1,
        "cell_count": cell_count,
        "dirty_cell_count": dirty_count,
        "msgpack_only": True,
    }


def verify_final(
    *,
    project_expected: Mapping[str, Any],
    database_expected: Mapping[str, Any],
    cache_expected: Mapping[str, Any],
    connection: Any | None = None,
) -> dict[str, Any]:
    """Run all final read-only checks from plain expected-value mappings."""

    connection = _default_connection(connection)
    project_result = verify_project_stack(
        **dict(project_expected), connection=connection
    )
    database_result = verify_database(
        **dict(database_expected), connection=connection
    )
    cache_result = verify_cache(**dict(cache_expected), connection=connection)
    return {
        "project": project_result,
        "database": database_result,
        "cache": cache_result,
    }
