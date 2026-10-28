"""Insert new edges using only the prepared batch, never rebuild shared tables."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TextIO

from .errors import VerificationError


STAGING_TABLE = "pg_temp.catmaid_import_edge_nodes"


def create_batch_edges(
    *,
    cursor: Any,
    treenode_file: TextIO,
    location_ids: Sequence[int],
    skeletons: Sequence[Mapping[str, Any]],
    project_id: int,
    user_id: int,
    record_timing: Callable[[str, float], None] | None = None,
) -> int:
    """Stage COPY input and insert edges in the caller's origin transaction.

    This is create-only. A duplicate target edge is a database error, not a
    reason to overwrite it. The caller validates the actual treenodes and
    commits all batch work atomically.
    """
    from .loader import _copy_file
    from .materialization import _validate_treenodes

    timing = record_timing or (lambda _name, _duration: None)
    started = time.monotonic()
    # Explicit columns avoid inheriting defaults, constraints or triggers.
    # float4 matches values stored by the core treenode COPY.
    cursor.execute("""
        CREATE TEMPORARY TABLE catmaid_import_edge_nodes (
            id bigint PRIMARY KEY,
            project_id integer NOT NULL,
            location_x real NOT NULL,
            location_y real NOT NULL,
            location_z real NOT NULL,
            editor_id integer NOT NULL,
            user_id integer NOT NULL,
            skeleton_id bigint NOT NULL,
            radius real NOT NULL,
            parent_id bigint
        ) ON COMMIT DROP
    """)
    _copy_file(
        cursor,
        "COPY pg_temp.catmaid_import_edge_nodes "
        "(id, project_id, location_x, location_y, location_z, editor_id, "
        "user_id, skeleton_id, radius, parent_id) "
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        treenode_file,
    )
    treenode_file.seek(0)
    cursor.execute("ANALYZE pg_temp.catmaid_import_edge_nodes")
    cursor.execute("""
        SELECT id, project_id, user_id, editor_id, skeleton_id, parent_id
        FROM pg_temp.catmaid_import_edge_nodes
    """)
    # Also checked on real rows later. Check here before a missing parent can
    # silently disappear from the join, and before any edge is inserted.
    _validate_treenodes(
        cursor.fetchall(), skeletons, location_ids,
        project_id=project_id, user_id=user_id,
    )
    timing("edge_staging", time.monotonic() - started)

    started = time.monotonic()
    cursor.execute("""
        INSERT INTO treenode_edge (id, parent_id, project_id, edge)
        SELECT c.id, c.parent_id, c.project_id,
               ST_MakeLine(
                   ST_MakePoint(c.location_x, c.location_y, c.location_z),
                   ST_MakePoint(p.location_x, p.location_y, p.location_z))
        FROM pg_temp.catmaid_import_edge_nodes c
        JOIN pg_temp.catmaid_import_edge_nodes p ON p.id = c.parent_id
        WHERE c.parent_id IS NOT NULL
        UNION ALL
        SELECT r.id, r.parent_id, r.project_id,
               ST_MakeLine(
                   ST_MakePoint(r.location_x, r.location_y, r.location_z),
                   ST_MakePoint(r.location_x, r.location_y, r.location_z))
        FROM pg_temp.catmaid_import_edge_nodes r
        WHERE r.parent_id IS NULL
    """)
    inserted = cursor.rowcount
    if inserted != len(location_ids):
        raise VerificationError(
            "inserted edge count does not match the prepared batch",
            details={"expected": len(location_ids), "actual": inserted},
        )
    timing("edge_creation", time.monotonic() - started)
    return inserted
