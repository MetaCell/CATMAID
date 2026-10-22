"""Read associated stack geometry and express its coverage in project nanometres."""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

from .errors import InvalidInputError


Point = tuple[float, float, float]
StackBounds = tuple[Point, Point]


def geometry_bounds(geometry: Mapping[str, Any]) -> tuple[StackBounds, ...]:
    """Apply CATMAID's XY/XZ/ZY axis permutation, then project translation."""
    bounds = []
    for stack in geometry["stacks"]:
        try:
            dimension = tuple(float(value) for value in stack["dimension"])
            resolution = tuple(float(value) for value in stack["resolution_nm"])
            minimum = tuple(float(value) for value in stack["translation_nm"])
            if not all(len(vector) == 3 for vector in (dimension, resolution, minimum)):
                raise ValueError("expected three coordinates")
            if not all(math.isfinite(value) and value > 0 for value in dimension + resolution):
                raise ValueError("invalid dimension or resolution")
            if not all(math.isfinite(value) for value in minimum):
                raise ValueError("invalid translation")
            extent = tuple(size * step for size, step in zip(dimension, resolution))
            # Match CATMAID.Stack.stackToProjectX/Y/Z in static/js/stack.js.
            order = {0: (0, 1, 2), 1: (0, 2, 1), 2: (2, 1, 0)}[stack["orientation"]]
            maximum = tuple(minimum[axis] + extent[index] for axis, index in enumerate(order))
            if not all(math.isfinite(high) and low < high for low, high in zip(minimum, maximum)):
                raise ValueError("invalid physical extent")
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise InvalidInputError(
                f"Target stack {stack.get('stack_id')} has invalid geometry: {exc}"
            ) from exc
        bounds.append((minimum, maximum))
    if not bounds:
        raise InvalidInputError(
            f"Target project {geometry['project_id']} needs at least one associated stack"
        )
    return tuple(bounds)


def read_project_stack_geometry(project_id: int, connection: Any = None) -> dict[str, Any]:
    """Read stack definitions and database identity without changing the target."""
    from .database import bootstrap_django, database_target_fingerprint

    connection = bootstrap_django() if connection is None else connection
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT ps.id, s.id, ps.orientation,
                   (s.dimension).x, (s.dimension).y, (s.dimension).z,
                   (s.resolution).x, (s.resolution).y, (s.resolution).z,
                   (ps.translation).x, (ps.translation).y, (ps.translation).z
            FROM project_stack ps JOIN stack s ON s.id=ps.stack_id
            WHERE ps.project_id=%s ORDER BY ps.id
        """, [project_id])
        stacks = [
            {"project_stack_id": row[0], "stack_id": row[1], "orientation": row[2],
             "dimension": list(row[3:6]), "resolution_nm": list(row[6:9]),
             "translation_nm": list(row[9:12])}
            for row in cursor.fetchall()
        ]
    geometry = {"schema_version": 1, "project_id": project_id, "stacks": stacks}
    geometry_bounds(geometry)
    geometry["database_target"] = database_target_fingerprint(connection)
    return geometry


def contains_point(point: Point, bounds: Sequence[StackBounds]) -> bool:
    """Check actual stack coverage: the envelope may include gaps between stacks."""
    return any(all(low <= value <= high for value, low, high in zip(point, minimum, maximum))
               for minimum, maximum in bounds)
