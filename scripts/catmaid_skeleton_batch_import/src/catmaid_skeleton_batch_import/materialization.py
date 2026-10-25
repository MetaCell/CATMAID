"""Selective CATMAID materialization and exact in-transaction validation."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from typing import Any, TextIO

from .errors import VerificationError
from .fragments import SOURCE_FRAGMENT_ANNOTATION_PREFIX, fragment_annotation_name


DEFAULT_CABLE_ABS_TOLERANCE_NM = 1e-6
DEFAULT_CABLE_REL_TOLERANCE = 1e-9

_EXPECTED_KEYS = (
    "neuron_id",
    "skeleton_id",
    "link_id",
    "annotation_id",
    "annotation_link_id",
    "fragment_id",
    "name",
    "node_count",
    "cable_length_nm",
)


def _default_connection(connection: Any | None) -> Any:
    if connection is not None:
        return connection
    from django.db import connection as django_connection

    return django_connection


def _fail(message: str, **details: object) -> None:
    raise VerificationError(message, details=details)


def _normalize_expected_skeletons(
    skeletons: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    if not skeletons:
        raise ValueError("at least one expected skeleton is required")
    normalized: list[dict[str, Any]] = []
    concept_ids: list[int] = []
    for index, skeleton in enumerate(skeletons):
        missing = [key for key in _EXPECTED_KEYS if key not in skeleton]
        if missing:
            raise ValueError(
                f"expected skeleton {index} is missing: {', '.join(missing)}"
            )
        values: dict[str, Any] = {}
        for key in (
            "neuron_id", "skeleton_id", "link_id", "annotation_id",
            "annotation_link_id", "node_count",
        ):
            value = skeleton[key]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"expected skeleton {index}.{key} must be positive")
            values[key] = value
        name = skeleton["name"]
        if not isinstance(name, str) or not name:
            raise ValueError(f"expected skeleton {index}.name must be non-empty")
        fragment_id = skeleton.get("fragment_id")
        fragment_annotation_name(fragment_id)
        values["fragment_id"] = fragment_id
        values["name"] = name
        cable_length = float(skeleton["cable_length_nm"])
        if not math.isfinite(cable_length) or cable_length < 0:
            raise ValueError(
                f"expected skeleton {index}.cable_length_nm must be finite and non-negative"
            )
        values["cable_length_nm"] = cable_length
        normalized.append(values)
        concept_ids.extend(
            (values["neuron_id"], values["skeleton_id"], values["link_id"],
             values["annotation_id"], values["annotation_link_id"])
        )
    if len(set(concept_ids)) != len(concept_ids):
        raise ValueError(
            "expected neuron, skeleton, annotation, and relationship IDs must be unique"
        )
    fragment_ids = [item["fragment_id"] for item in normalized]
    if len(set(fragment_ids)) != len(fragment_ids):
        raise ValueError("expected fragment identifiers must be unique")
    return tuple(normalized)


def _normalize_location_ids(
    location_ids: Sequence[int], expected_count: int
) -> tuple[int, ...]:
    normalized = tuple(location_ids)
    if len(normalized) != expected_count:
        raise ValueError(
            f"expected {expected_count} location IDs, received {len(normalized)}"
        )
    if any(
        isinstance(location_id, bool)
        or not isinstance(location_id, int)
        or location_id <= 0
        for location_id in normalized
    ):
        raise ValueError("location IDs must be positive integers")
    if len(set(normalized)) != len(normalized):
        raise ValueError("location IDs must be unique")
    return normalized


def _rows_by_id(
    rows: Sequence[Sequence[Any]], expected_ids: Sequence[int], label: str
) -> dict[int, Sequence[Any]]:
    by_id: dict[int, Sequence[Any]] = {}
    duplicates: list[int] = []
    for row in rows:
        row_id = int(row[0])
        if row_id in by_id:
            duplicates.append(row_id)
        by_id[row_id] = row
    expected = set(expected_ids)
    actual = set(by_id)
    if actual != expected or duplicates:
        _fail(
            f"{label} IDs do not exactly match the reserved IDs",
            missing_ids=sorted(expected - actual),
            unexpected_ids=sorted(actual - expected),
            duplicate_ids=sorted(set(duplicates)),
        )
    return by_id


def _validate_class_instances(
    rows: Sequence[Sequence[Any]],
    skeletons: Sequence[Mapping[str, Any]],
    *,
    project_id: int,
    user_id: int,
    neuron_class_id: int,
    skeleton_class_id: int,
    annotation_class_id: int,
) -> None:
    expected: dict[int, tuple[int, str]] = {}
    for skeleton in skeletons:
        expected[int(skeleton["neuron_id"])] = (
            neuron_class_id,
            str(skeleton["name"]),
        )
        expected[int(skeleton["skeleton_id"])] = (
            skeleton_class_id,
            str(skeleton["name"]),
        )
        expected[int(skeleton["annotation_id"])] = (
            annotation_class_id,
            fragment_annotation_name(skeleton["fragment_id"]),
        )
    by_id = _rows_by_id(rows, tuple(expected), "class_instance")
    for row_id, row in by_id.items():
        _, actual_user, actual_project, actual_class, actual_name = row
        expected_class, expected_name = expected[row_id]
        if (
            int(actual_user) != user_id
            or int(actual_project) != project_id
            or int(actual_class) != expected_class
            or str(actual_name) != expected_name
        ):
            _fail(
                "class_instance ownership or values do not match",
                id=row_id,
                actual={
                    "user_id": actual_user,
                    "project_id": actual_project,
                    "class_id": actual_class,
                    "name": actual_name,
                },
            )


def _validate_relationships(
    rows: Sequence[Sequence[Any]],
    skeletons: Sequence[Mapping[str, Any]],
    *,
    project_id: int,
    user_id: int,
    model_of_relation_id: int,
    annotated_with_relation_id: int,
) -> None:
    expected = {}
    for item in skeletons:
        expected[int(item["link_id"])] = (
            model_of_relation_id, int(item["skeleton_id"]), int(item["neuron_id"]),
        )
        expected[int(item["annotation_link_id"])] = (
            annotated_with_relation_id, int(item["neuron_id"]), int(item["annotation_id"]),
        )
    by_id = _rows_by_id(rows, tuple(expected), "class_instance_class_instance")
    for row_id, row in by_id.items():
        _, actual_user, actual_project, relation_id, class_a, class_b = row
        if (
            int(actual_user) != user_id
            or int(actual_project) != project_id
            or (int(relation_id), int(class_a), int(class_b)) != expected[row_id]
        ):
            _fail("relationship ownership or endpoints do not match", id=row_id)


def _validate_fragment_metadata(
    cursor: Any,
    skeletons: Sequence[Mapping[str, Any]],
    *,
    project_id: int,
    annotation_class_id: int,
    annotated_with_relation_id: int,
) -> None:
    """Reject duplicate names, duplicate links, or additional fragment metadata."""
    annotation_ids = [item["annotation_id"] for item in skeletons]
    neuron_ids = [item["neuron_id"] for item in skeletons]
    names = [fragment_annotation_name(item["fragment_id"]) for item in skeletons]
    cursor.execute(
        """SELECT id, user_id, project_id, class_id, name
           FROM class_instance
           WHERE project_id = %s AND class_id = %s AND name = ANY(%s::text[])
           ORDER BY id""",
        (project_id, annotation_class_id, names),
    )
    annotation_rows = cursor.fetchall()
    _rows_by_id(annotation_rows, annotation_ids, "source fragment annotation")
    cursor.execute(
        """SELECT link.id, link.user_id, link.project_id, link.relation_id,
                  link.class_instance_a, link.class_instance_b
           FROM class_instance_class_instance link
           JOIN class_instance annotation ON annotation.id = link.class_instance_b
           WHERE link.project_id = %s AND link.relation_id = %s
             AND ((link.class_instance_a = ANY(%s::bigint[])
                   AND annotation.name LIKE %s)
                  OR link.class_instance_b = ANY(%s::bigint[]))
           ORDER BY link.id""",
        (project_id, annotated_with_relation_id, neuron_ids,
         SOURCE_FRAGMENT_ANNOTATION_PREFIX + "%", annotation_ids),
    )
    link_rows = cursor.fetchall()
    _rows_by_id(link_rows, [item["annotation_link_id"] for item in skeletons],
                "source fragment annotation link")


def _validate_treenodes(
    rows: Sequence[Sequence[Any]],
    skeletons: Sequence[Mapping[str, Any]],
    location_ids: Sequence[int],
    *,
    project_id: int,
    user_id: int,
) -> dict[int, int | None]:
    by_id = _rows_by_id(rows, location_ids, "treenode")
    expected_counts = {
        int(item["skeleton_id"]): int(item["node_count"]) for item in skeletons
    }
    actual_counts = {skeleton_id: 0 for skeleton_id in expected_counts}
    parents: dict[int, int | None] = {}
    node_skeletons: dict[int, int] = {}

    for row_id, row in by_id.items():
        _, actual_project, actual_user, editor_id, skeleton_id, parent_id = row
        skeleton_id = int(skeleton_id)
        if (
            int(actual_project) != project_id
            or int(actual_user) != user_id
            or int(editor_id) != user_id
            or skeleton_id not in expected_counts
        ):
            _fail("treenode ownership does not match", id=row_id)
        actual_counts[skeleton_id] += 1
        node_skeletons[row_id] = skeleton_id
        parents[row_id] = None if parent_id is None else int(parent_id)

    if actual_counts != expected_counts:
        _fail(
            "treenode counts by skeleton do not match",
            expected=expected_counts,
            actual=actual_counts,
        )

    roots = {skeleton_id: 0 for skeleton_id in expected_counts}
    for node_id, parent_id in parents.items():
        skeleton_id = node_skeletons[node_id]
        if parent_id is None:
            roots[skeleton_id] += 1
        elif parent_id not in parents:
            _fail("treenode parent is outside the reserved batch", id=node_id)
        elif node_skeletons[parent_id] != skeleton_id:
            _fail("treenode parent belongs to another skeleton", id=node_id)
    bad_roots = {
        skeleton_id: count for skeleton_id, count in roots.items() if count != 1
    }
    if bad_roots:
        _fail("each skeleton must have exactly one root", roots=bad_roots)

    # Root and same-skeleton parent checks alone allow a disconnected cycle.
    # Following every parent chain verifies the copied graph is a rooted tree.
    reaches_root: set[int] = set()
    for start in parents:
        chain: list[int] = []
        chain_set: set[int] = set()
        current: int | None = start
        while current is not None and current not in reaches_root:
            if current in chain_set:
                _fail("treenode topology contains a cycle", id=current)
            chain.append(current)
            chain_set.add(current)
            current = parents[current]
        reaches_root.update(chain)

    return parents


def _validate_edges(
    rows: Sequence[Sequence[Any]],
    location_ids: Sequence[int],
    parents: Mapping[int, int | None],
    *,
    project_id: int,
) -> None:
    by_id = _rows_by_id(rows, location_ids, "treenode_edge")
    for row_id, row in by_id.items():
        _, actual_parent, actual_project = row
        normalized_parent = None if actual_parent is None else int(actual_parent)
        if int(actual_project) != project_id or normalized_parent != parents[row_id]:
            _fail("treenode_edge parent or project does not match", id=row_id)


def _validate_summaries(
    rows: Sequence[Sequence[Any]],
    skeletons: Sequence[Mapping[str, Any]],
    *,
    project_id: int,
    user_id: int,
    cable_abs_tolerance_nm: float,
    cable_rel_tolerance: float,
) -> None:
    expected = {int(item["skeleton_id"]): item for item in skeletons}
    by_id = _rows_by_id(rows, tuple(expected), "catmaid_skeleton_summary")
    for skeleton_id, row in by_id.items():
        _, actual_project, last_editor_id, node_count, cable_length = row
        item = expected[skeleton_id]
        if (
            int(actual_project) != project_id
            or int(last_editor_id) != user_id
            or int(node_count) != int(item["node_count"])
        ):
            _fail("skeleton summary ownership or node count does not match", id=skeleton_id)
        if not math.isclose(
            float(cable_length),
            float(item["cable_length_nm"]),
            rel_tol=cable_rel_tolerance,
            abs_tol=cable_abs_tolerance_nm,
        ):
            _fail(
                "skeleton summary cable length does not match",
                id=skeleton_id,
                expected=float(item["cable_length_nm"]),
                actual=float(cable_length),
                absolute_tolerance_nm=cable_abs_tolerance_nm,
                relative_tolerance=cable_rel_tolerance,
            )


def validate_batch(
    *,
    project_id: int,
    user_id: int,
    neuron_class_id: int,
    skeleton_class_id: int,
    annotation_class_id: int,
    model_of_relation_id: int,
    annotated_with_relation_id: int,
    skeletons: Sequence[Mapping[str, Any]],
    location_ids: Sequence[int],
    connection: Any | None = None,
    cable_abs_tolerance_nm: float = DEFAULT_CABLE_ABS_TOLERANCE_NM,
    cable_rel_tolerance: float = DEFAULT_CABLE_REL_TOLERANCE,
) -> dict[str, int]:
    """Validate exact batch ownership, topology, edges, and summaries."""

    normalized = _normalize_expected_skeletons(skeletons)
    normalized_locations = _normalize_location_ids(
        location_ids, sum(item["node_count"] for item in normalized)
    )
    if min(cable_abs_tolerance_nm, cable_rel_tolerance) < 0:
        raise ValueError("cable tolerances cannot be negative")

    connection = _default_connection(connection)
    class_ids = tuple(
        identifier
        for item in normalized
        for identifier in (item["neuron_id"], item["skeleton_id"], item["annotation_id"])
    )
    link_ids = tuple(identifier for item in normalized
                     for identifier in (item["link_id"], item["annotation_link_id"]))
    skeleton_ids = tuple(item["skeleton_id"] for item in normalized)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, user_id, project_id, class_id, name
            FROM class_instance
            WHERE id = ANY(%s::bigint[])
            ORDER BY id
            """,
            (list(class_ids),),
        )
        class_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT id, user_id, project_id, relation_id,
                   class_instance_a, class_instance_b
            FROM class_instance_class_instance
            WHERE id = ANY(%s::bigint[])
            ORDER BY id
            """,
            (list(link_ids),),
        )
        relationship_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT id, project_id, user_id, editor_id, skeleton_id, parent_id
            FROM treenode
            WHERE id = ANY(%s::bigint[]) OR skeleton_id = ANY(%s::bigint[])
            ORDER BY id
            """,
            (list(normalized_locations), list(skeleton_ids)),
        )
        treenode_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT id, parent_id, project_id
            FROM treenode_edge
            WHERE id = ANY(%s::bigint[])
            ORDER BY id
            """,
            (list(normalized_locations),),
        )
        edge_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT skeleton_id, project_id, last_editor_id, num_nodes,
                   cable_length
            FROM catmaid_skeleton_summary
            WHERE skeleton_id = ANY(%s::bigint[])
            ORDER BY skeleton_id
            """,
            (list(skeleton_ids),),
        )
        summary_rows = cursor.fetchall()
        _validate_fragment_metadata(
            cursor, normalized, project_id=project_id,
            annotation_class_id=annotation_class_id,
            annotated_with_relation_id=annotated_with_relation_id,
        )

    _validate_class_instances(
        class_rows,
        normalized,
        project_id=project_id,
        user_id=user_id,
        neuron_class_id=neuron_class_id,
        skeleton_class_id=skeleton_class_id,
        annotation_class_id=annotation_class_id,
    )
    _validate_relationships(
        relationship_rows,
        normalized,
        project_id=project_id,
        user_id=user_id,
        model_of_relation_id=model_of_relation_id,
        annotated_with_relation_id=annotated_with_relation_id,
    )
    parents = _validate_treenodes(
        treenode_rows,
        normalized,
        normalized_locations,
        project_id=project_id,
        user_id=user_id,
    )
    _validate_edges(
        edge_rows,
        normalized_locations,
        parents,
        project_id=project_id,
    )
    _validate_summaries(
        summary_rows,
        normalized,
        project_id=project_id,
        user_id=user_id,
        cable_abs_tolerance_nm=cable_abs_tolerance_nm,
        cable_rel_tolerance=cable_rel_tolerance,
    )
    skeleton_count = len(normalized)
    node_count = len(normalized_locations)
    return {
        "class_instance_count": skeleton_count * 3,
        "annotation_count": skeleton_count,
        "annotation_link_count": skeleton_count,
        "model_of_count": skeleton_count,
        "neuron_count": skeleton_count,
        "skeleton_count": skeleton_count,
        "relationship_count": skeleton_count * 2,
        "treenode_count": node_count,
        "edge_count": node_count,
        "summary_count": skeleton_count,
    }


def materialize_and_verify_batch(
    *,
    project_id: int,
    user_id: int,
    neuron_class_id: int,
    skeleton_class_id: int,
    annotation_class_id: int,
    model_of_relation_id: int,
    annotated_with_relation_id: int,
    skeletons: Sequence[Mapping[str, Any]],
    location_ids: Sequence[int],
    connection: Any | None = None,
    cursor: Any | None = None,
    treenode_file: TextIO,
    record_timing: Callable[[str, float], None] | None = None,
    cable_abs_tolerance_nm: float = DEFAULT_CABLE_ABS_TOLERANCE_NM,
    cable_rel_tolerance: float = DEFAULT_CABLE_REL_TOLERANCE,
) -> dict[str, int]:
    """Materialize and validate a batch inside the caller's transaction.

    The core COPY is intentionally owned by the loader.  Call this after COPY
    and after restoring ``session_replication_role=origin``. Edge creation uses
    only the prepared batch; summaries retain CATMAID's canonical function.
    """

    normalized = _normalize_expected_skeletons(skeletons)
    _normalize_location_ids(
        location_ids, sum(item["node_count"] for item in normalized)
    )
    skeleton_ids = tuple(int(item["skeleton_id"]) for item in normalized)
    if len(set(skeleton_ids)) != len(skeleton_ids):
        raise ValueError("skeleton IDs passed to materialization must be unique")

    connection = _default_connection(connection or getattr(cursor, "db", None))
    if not getattr(connection, "in_atomic_block", False):
        raise RuntimeError(
            "materialize_and_verify_batch requires an active outer transaction"
        )
    if cursor is None:
        with connection.cursor() as role_cursor:
            role_cursor.execute("SHOW session_replication_role")
            row = role_cursor.fetchone()
    else:
        cursor.execute("SHOW session_replication_role")
        row = cursor.fetchone()
    if row is None or row[0] != "origin":
        raise RuntimeError(
            "session_replication_role must be origin before materialization"
        )

    timing = record_timing or (lambda _name, _duration: None)
    materialization_started = time.monotonic()
    from .edges import create_batch_edges

    with (nullcontext(cursor) if cursor is not None else connection.cursor()) as work_cursor:
        create_batch_edges(
            cursor=work_cursor,
            treenode_file=treenode_file,
            location_ids=location_ids,
            skeletons=normalized,
            project_id=project_id,
            user_id=user_id,
            record_timing=timing,
        )
        summary_started = time.monotonic()
        work_cursor.execute(
            "SELECT public.refresh_skeleton_summary_table_selectively(%s::bigint[])",
            (list(skeleton_ids),),
        )
        timing("summary_refresh", time.monotonic() - summary_started)
    timing("materialization", time.monotonic() - materialization_started)

    validation_started = time.monotonic()
    result = validate_batch(
        project_id=project_id,
        user_id=user_id,
        neuron_class_id=neuron_class_id,
        skeleton_class_id=skeleton_class_id,
        annotation_class_id=annotation_class_id,
        model_of_relation_id=model_of_relation_id,
        annotated_with_relation_id=annotated_with_relation_id,
        skeletons=normalized,
        location_ids=location_ids,
        connection=connection,
        cable_abs_tolerance_nm=cable_abs_tolerance_nm,
        cable_rel_tolerance=cable_rel_tolerance,
    )
    timing("validation", time.monotonic() - validation_started)
    return result
