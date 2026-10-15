"""Refresh and verify the spatial footprint of committed append batches."""

from __future__ import annotations

from contextlib import contextmanager
from itertools import product
import math
from typing import Any

from .errors import RetryableError, VerificationError


GRID_FIELDS = (
    "id", "project_id", "orientation", "cell_width", "cell_height", "cell_depth",
    "n_lod_levels", "lod_strategy", "lod_min_bucket_size",
    "n_largest_skeletons_limit", "n_last_edited_skeletons_limit",
    "hidden_last_editor_id", "allow_empty", "has_json_data",
    "has_json_text_data", "has_msgpack_data", "enabled", "ordering",
)


def intersected_cells(start, end, sizes):
    """Traverse a segment's closed grid cells, including touched faces/corners.

    PostGIS cache queries include boundary intersections. A node on a grid
    plane can therefore change both neighboring cells. Advance plane crossings
    without a tolerance: merging nearly equal crossings can skip thin cells.
    """
    def point_cells(point):
        indices = [math.floor(value / size) for value, size in zip(point, sizes)]
        return product(*[
            (index - 1, index) if value == index * size else (index,)
            for value, size, index in zip(point, sizes, indices)
        ])

    yield from point_cells(start)
    direction = [b - a for a, b in zip(start, end)]
    steps = [1 if delta > 0 else -1 if delta < 0 else 0 for delta in direction]
    current = [math.floor(value / size) for value, size in zip(start, sizes)]
    for axis in range(3):
        if steps[axis] < 0 and start[axis] == current[axis] * sizes[axis]:
            current[axis] -= 1
    while True:
        yield from product(*[
            (index - 1, index) if steps[axis] == 0 and
            start[axis] == index * sizes[axis] else (index,)
            for axis, index in enumerate(current)
        ])
        crossings = [
            ((current[axis] + (steps[axis] > 0)) * sizes[axis] - start[axis]) / delta
            if delta else math.inf
            for axis, delta in enumerate(direction)
        ]
        next_crossing = min(crossings)
        if next_crossing > 1:
            break
        axes = [axis for axis, crossing in enumerate(crossings) if crossing == next_crossing]
        yield from product(*[
            (index, index + steps[axis]) if axis in axes else
            (index - 1, index) if steps[axis] == 0 and
            start[axis] == index * sizes[axis] else (index,)
            for axis, index in enumerate(current)
        ])
        if next_crossing == 1:
            break
        for axis in axes:
            current[axis] += steps[axis]
    yield from point_cells(end)


class AppendCacheRefresh:
    """Keep refresh evidence for this run; resume rederives it from reserved IDs.

    No cache definitions or unrelated cells are removed. Each batch is already
    committed when its edges are read. Edges, including intermediate cells on
    long edges and cells touched at boundaries, define the refresh scope.
    """

    def __init__(self, project_id: int, connection: Any, *,
                 lock_timeout_ms: int, statement_timeout_ms: int):
        self.project_id = project_id
        self.connection = connection
        self.lock_timeout_ms = lock_timeout_ms
        self.statement_timeout_ms = statement_timeout_ms
        self.grids = self._read_grids()
        self.receipts: dict[tuple[int, int, int, int], dict[str, Any]] = {}

    def _read_grids(self) -> dict[int, dict[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(GRID_FIELDS)} FROM node_grid_cache "
                "WHERE project_id=%s ORDER BY id", [self.project_id],
            )
            grids = {row[0]: dict(zip(GRID_FIELDS, row)) for row in cursor.fetchall()}
        for grid in grids.values():
            if any(grid[name] is None or grid[name] <= 0 for name in (
                "cell_width", "cell_height", "cell_depth", "n_lod_levels", "lod_min_bucket_size"
            )):
                raise VerificationError(f"Grid {grid['id']} has invalid cell dimensions or LOD settings")
            if not any(grid[name] for name in (
                "has_json_data", "has_json_text_data", "has_msgpack_data"
            )):
                raise VerificationError(f"Grid {grid['id']} has no configured cache format")
        return grids

    def _check_grids(self) -> None:
        if self._read_grids() != self.grids:
            raise RetryableError("Project cache configuration changed; resume to refresh its current grids")

    def refresh_batch(self, location_ids: list[int]) -> None:
        self._check_grids()
        if not self.grids:
            return
        cells: set[tuple[int, int, int, int]] = set()
        edge_count = 0
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT ST_X(ST_StartPoint(edge)), ST_Y(ST_StartPoint(edge)),
                       ST_Z(ST_StartPoint(edge)), ST_X(ST_EndPoint(edge)),
                       ST_Y(ST_EndPoint(edge)), ST_Z(ST_EndPoint(edge))
                FROM treenode_edge WHERE project_id=%s AND id=ANY(%s::bigint[])
            """, [self.project_id, location_ids])
            while rows := cursor.fetchmany(2000):
                for row in rows:
                    edge_count += 1
                    for grid in self.grids.values():
                        for cell in intersected_cells(
                            row[:3], row[3:], (grid["cell_width"],
                            grid["cell_height"], grid["cell_depth"]),
                        ):
                            # Postgis3dNodeProvider uses distance to a middle-Z
                            # rectangle, so its candidates can extend beyond an
                            # exact box/edge intersection in X/Y. Include that
                            # halo, then apply the provider's exact predicates.
                            dx = math.ceil(grid["cell_depth"] / (2 * grid["cell_width"]))
                            dy = math.ceil(grid["cell_depth"] / (2 * grid["cell_height"]))
                            for ox, oy in product(range(-dx, dx + 1), range(-dy, dy + 1)):
                                cells.add((grid["id"], cell[0] + ox, cell[1] + oy, cell[2]))
        if edge_count != len(location_ids):
            raise VerificationError("Cannot refresh cache: imported treenode edges are missing")
        for cell in self._query_cells(sorted(cells), location_ids):
            # A later committed batch can affect a previously refreshed cell.
            self.receipts[cell] = self._refresh(cell)

    def _query_cells(self, candidates, location_ids):
        """Retain exactly cells whose node-provider query sees an imported edge."""
        for offset in range(0, len(candidates), 1000):
            chunk = candidates[offset:offset + 1000]
            values = ",".join(["(%s,%s,%s,%s)"] * len(chunk))
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    WITH candidates(grid_id,x,y,z) AS (VALUES {values}), boxes AS (
                        SELECT c.*, x*g.cell_width::float8 l, (x+1)*g.cell_width::float8 r,
                            y*g.cell_height::float8 t, (y+1)*g.cell_height::float8 b,
                            z*g.cell_depth::float8 z1, (z+1)*g.cell_depth::float8 z2
                        FROM candidates c JOIN node_grid_cache g ON g.id=c.grid_id
                    )
                    SELECT grid_id,x,y,z FROM boxes c WHERE EXISTS (
                        SELECT 1 FROM treenode_edge e
                        WHERE e.project_id=%s AND e.id=ANY(%s::bigint[])
                        AND e.edge &&& ST_MakeLine(ARRAY[
                            ST_MakePoint(l,b,z2), ST_MakePoint(r,t,z1)]::geometry[])
                        AND ST_3DDWithin(e.edge, ST_MakePolygon(ST_MakeLine(ARRAY[
                            ST_MakePoint(l,t,(z1+z2)/2), ST_MakePoint(r,t,(z1+z2)/2),
                            ST_MakePoint(r,b,(z1+z2)/2), ST_MakePoint(l,b,(z1+z2)/2),
                            ST_MakePoint(l,t,(z1+z2)/2)]::geometry[])), (z2-z1)/2)
                    ) ORDER BY grid_id,x,y,z
                """, [value for cell in chunk for value in cell] + [self.project_id, location_ids])
                yield from (tuple(row) for row in cursor.fetchall())

    def _refresh(self, cell: tuple[int, int, int, int]) -> dict[str, Any]:
        from django.conf import settings
        from catmaid.control.grid_cache import locked_grid_cell
        from catmaid.control.node import update_grid_cell

        grid = self.grids[cell[0]]
        params = {
            "project_id": self.project_id,
            "limit": settings.NODE_LIST_MAXIMUM_COUNT,
            "ordering": grid["ordering"],
        }
        for name in ("n_largest_skeletons_limit", "n_last_edited_skeletons_limit",
                     "hidden_last_editor_id"):
            if grid[name]:
                params[name] = grid[name]
        try:
            with locked_grid_cell(
                *cell, connection=self.connection,
                lock_timeout_ms=self.lock_timeout_ms,
                statement_timeout_ms=self.statement_timeout_ms,
            ) as (cursor, receipt):
                updated = update_grid_cell(
                    self.project_id, *cell, grid["cell_width"], grid["cell_height"],
                    grid["cell_depth"], params, grid["allow_empty"],
                    grid["n_lod_levels"], grid["lod_min_bucket_size"],
                    grid["lod_strategy"], grid["has_json_data"],
                    grid["has_json_text_data"], grid["has_msgpack_data"],
                    cursor=cursor, delete_empty=True,
                )
                if not updated:
                    raise RuntimeError("Cell updater did not publish a replacement")
            return receipt
        except Exception as exc:
            raise RetryableError(
                f"Cache cell {cell} refresh failed; committed batches are safe to resume: {exc}"
            ) from exc

    def _current(self, cell: tuple[int, int, int, int], receipt: dict[str, Any]) -> bool:
        grid = self.grids[cell[0]]
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT c.update_time,
                       c.json_data IS NOT NULL AND cardinality(c.json_data)=%s,
                       c.json_text_data IS NOT NULL AND cardinality(c.json_text_data)=%s,
                       c.msgpack_data IS NOT NULL AND cardinality(c.msgpack_data)=%s,
                       EXISTS (SELECT 1 FROM dirty_node_grid_cache_cell d
                           WHERE d.grid_id=%s AND d.x_index=%s
                             AND d.y_index=%s AND d.z_index=%s)
                FROM (SELECT 1) singleton
                LEFT JOIN node_grid_cache_cell c ON
                    c.grid_id=%s AND c.x_index=%s AND c.y_index=%s AND c.z_index=%s
            """, [grid["n_lod_levels"]] * 3 + list(cell) * 2)
            row = cursor.fetchone()
        if row[4]:
            return False
        if not receipt["present"]:
            # Empty cells may legitimately be omitted by a grid's filters.
            return row[0] is None
        return row[0] is not None and row[0] >= receipt["generated_at"] and all(
            not grid[flag] or row[index]
            for index, flag in enumerate(
                ("has_json_data", "has_json_text_data", "has_msgpack_data"), start=1
            )
        )

    @contextmanager
    def _snapshot(self):
        from django.db import transaction

        with transaction.atomic(using=self.connection.alias):
            with self.connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            yield

    def _stale_cells(self):
        if not self.receipts:
            self._check_grids()
            return []
        # All cells and grid definitions must pass in one database snapshot,
        # rather than a sequence of unrelated observations during live editing.
        # This read-only transaction takes no cell or project write locks.
        with self._snapshot():
            self._check_grids()
            return [cell for cell, receipt in self.receipts.items()
                    if not self._current(cell, receipt)]

    def verify(self) -> dict[str, Any]:
        """Require fresh payloads and no pending invalidation in the affected scope.

        Workers can invalidate a cell again during generation. Retry only stale
        cells, with a bound so busy editing cannot falsely mark an import done.
        The normal spatial workers continue handling subsequent project edits.
        """
        for attempt in range(4):
            stale = self._stale_cells()
            if not stale:
                return {
                    "managed_by": "catmaid", "rebuilt": False,
                    "strategy": "affected_grid_cells", "verified": True,
                    "grid_count": len(self.grids), "cell_count": len(self.receipts),
                }
            if attempt == 3:
                break
            for cell in stale:
                self.receipts[cell] = self._refresh(cell)
        raise RetryableError(
            "Affected cache cells are still changing; resume to verify them before completion"
        )
