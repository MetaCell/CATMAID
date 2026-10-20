"""Read-only final verification for importer-owned CATMAID data."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .errors import VerificationError
from .fragments import SOURCE_FRAGMENT_ANNOTATION_PREFIX
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
    annotation_class_id: int,
    annotated_with_relation_id: int,
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
        (annotation_class_id, "annotation_class_id"),
        (annotated_with_relation_id, "annotated_with_relation_id"),
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
              (SELECT count(*) FROM catmaid_skeleton_summary WHERE project_id = %s),
              (SELECT count(*) FROM class_instance
               WHERE project_id = %s AND class_id = %s AND name LIKE %s),
              (SELECT count(*) FROM class_instance_class_instance link
               JOIN class_instance annotation ON annotation.id = link.class_instance_b
               WHERE link.project_id = %s AND link.relation_id = %s
                 AND annotation.class_id = %s AND annotation.name LIKE %s)
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
                project_id, annotation_class_id, SOURCE_FRAGMENT_ANNOTATION_PREFIX + "%",
                project_id, annotated_with_relation_id, annotation_class_id,
                SOURCE_FRAGMENT_ANNOTATION_PREFIX + "%",
            ),
        )
        row = cursor.fetchone()
    if row is None or len(row) != 8:
        _fail("could not read project-wide database counts")

    actual = {
        "neuron_count": int(row[0]),
        "skeleton_count": int(row[1]),
        "relationship_count": int(row[2]),
        "treenode_count": int(row[3]),
        "edge_count": int(row[4]),
        "summary_count": int(row[5]),
        "annotation_count": int(row[6]),
        "annotation_link_count": int(row[7]),
    }
    expected = {
        "neuron_count": expected_skeleton_count,
        "skeleton_count": expected_skeleton_count,
        "relationship_count": expected_skeleton_count,
        "treenode_count": expected_node_count,
        "edge_count": expected_node_count,
        "summary_count": expected_skeleton_count,
        "annotation_count": expected_skeleton_count,
        "annotation_link_count": expected_skeleton_count,
    }
    if actual != expected:
        _fail(
            "project-wide database counts do not match the plan",
            expected=expected,
            actual=actual,
        )
    return {
        **actual,
        "class_instance_count": actual["neuron_count"] + actual["skeleton_count"] + actual["annotation_count"],
        "model_of_count": actual["relationship_count"],
        "relationship_count": actual["relationship_count"] + actual["annotation_link_count"],
    }
