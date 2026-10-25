"""Rebuild project grid payloads once, after offline bulk ingestion."""

from __future__ import annotations

from itertools import product
import math
from typing import Any

from .errors import OperationalFailureError, VerificationError


GRID_FIELDS = (
    "id", "project_id", "orientation", "cell_width", "cell_height", "cell_depth",
    "n_lod_levels", "lod_strategy", "lod_min_bucket_size",
    "n_largest_skeletons_limit", "n_last_edited_skeletons_limit",
    "hidden_last_editor_id", "allow_empty", "has_json_data",
    "has_json_text_data", "has_msgpack_data", "enabled", "ordering",
)


class OfflineGridCache:
    """Keep definitions unchanged; replace payloads from all project data.

    The Neuroglass managed layer must remain inactive throughout the run.
    This class does not make concurrent morphology edits safe.
    """

    def __init__(self, project_id: int, connection: Any, *,
                 lock_timeout_ms: int, statement_timeout_ms: int):
        self.project_id = project_id
        self.connection = connection
        self.lock_timeout_ms = lock_timeout_ms
        self.statement_timeout_ms = statement_timeout_ms
        self._provider = None
        self.grids = self._read_grids()

    def _read_grids(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(GRID_FIELDS)} FROM node_grid_cache "
                "WHERE project_id=%s ORDER BY id", [self.project_id],
            )
            grids = [dict(zip(GRID_FIELDS, row)) for row in cursor.fetchall()]
        for grid in grids:
            if any(grid[name] is None or grid[name] <= 0 for name in (
                "cell_width", "cell_height", "cell_depth", "n_lod_levels", "lod_min_bucket_size"
            )) or grid["lod_strategy"] not in {"linear", "quadratic", "exponential"}:
                raise VerificationError(f"Grid {grid['id']} has invalid dimensions or LOD settings")
            if not any(grid[name] for name in (
                "has_json_data", "has_json_text_data", "has_msgpack_data"
            )):
                raise VerificationError(f"Grid {grid['id']} has no configured cache format")
        return grids

    def ensure_import_grid(self, *, cell_size_nm, lod_levels, lod_bucket_size, lod_strategy):
        """Only a newly created project gets an importer-owned default grid."""
        if not self.grids:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO node_grid_cache (project_id, orientation,
                        cell_width, cell_height, cell_depth, n_lod_levels,
                        lod_min_bucket_size, lod_strategy, allow_empty,
                        has_json_data, has_json_text_data, has_msgpack_data, enabled)
                    VALUES (%s,0,%s,%s,%s,%s,%s,%s,false,false,false,true,true)
                """, [self.project_id, cell_size_nm, cell_size_nm, cell_size_nm,
                      lod_levels, lod_bucket_size, lod_strategy])
            self.grids = self._read_grids()

    def _check_grids(self):
        if self._read_grids() != self.grids:
            raise OperationalFailureError("Cache configuration changed; keep the layer inactive and inspect the failed import")

    def _bounds(self):
        # Include old and new skeletons, free connectors and negative coordinates.
        # Endpoints bound every edge, so long edges are covered as well.
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT min(x), min(y), min(z), max(x), max(y), max(z)
                FROM (
                    SELECT location_x x, location_y y, location_z z
                    FROM treenode WHERE project_id=%s
                    UNION ALL
                    SELECT location_x, location_y, location_z
                    FROM connector WHERE project_id=%s
                ) points
            """, [self.project_id, self.project_id])
            bounds = cursor.fetchone()
        return bounds if bounds and all(value is not None for value in bounds) else None

    @staticmethod
    def cell_ranges(bounds, grid):
        if bounds is None:
            return None
        sizes = [grid[name] for name in ("cell_width", "cell_height", "cell_depth")]
        # Both cells adjacent to an exact boundary can contain the same node.
        return [(math.ceil(low / size) - 1, math.floor(high / size))
                for low, high, size in zip(bounds[:3], bounds[3:], sizes)]

    def _refresh(self, grid, cell):
        from catmaid.control.grid_cache import locked_grid_cell
        from catmaid.control.node import Postgis3dNodeProvider, update_grid_cell

        if self._provider is None:
            self._provider = Postgis3dNodeProvider()
        params = {"project_id": self.project_id, "limit": None, "ordering": grid["ordering"]}
        for name in ("n_largest_skeletons_limit", "n_last_edited_skeletons_limit",
                     "hidden_last_editor_id"):
            if grid[name] is not None:
                params[name] = grid[name]
        with locked_grid_cell(
            grid["id"], *cell, connection=self.connection,
            lock_timeout_ms=self.lock_timeout_ms, statement_timeout_ms=self.statement_timeout_ms,
        ) as (cursor, _receipt):
            if not update_grid_cell(
                self.project_id, grid["id"], *cell, grid["cell_width"], grid["cell_height"],
                grid["cell_depth"], params, grid["allow_empty"], grid["n_lod_levels"],
                grid["lod_min_bucket_size"], grid["lod_strategy"], grid["has_json_data"],
                grid["has_json_text_data"], grid["has_msgpack_data"],
                provider=self._provider, cursor=cursor, delete_empty=True,
            ):
                raise OperationalFailureError(f"Grid {grid['id']} cell {cell} could not be rebuilt")

    def plan(self):
        """Persist coverage before clearing payloads, including intentional empty cells."""
        self._check_grids()
        bounds = self._bounds() if self.grids else None
        records = []
        for grid in self.grids:
            ranges = self.cell_ranges(bounds, grid)
            with self.connection.cursor() as cursor:
                # Retain intentionally cached empty coverage where requested.
                if grid["allow_empty"]:
                    cursor.execute("""
                        SELECT min(x_index), max(x_index), min(y_index), max(y_index),
                               min(z_index), max(z_index)
                        FROM node_grid_cache_cell WHERE grid_id=%s
                    """, [grid["id"]])
                    old = cursor.fetchone()
                    if old and old[0] is not None:
                        previous = list(zip(old[::2], old[1::2]))
                        ranges = previous if ranges is None else [
                            (min(a, c), max(b, d)) for (a, b), (c, d) in zip(ranges, previous)]
            records.append({"definition": grid, "ranges": ranges})
        return {"schema_version": 1, "project_id": self.project_id,
                "node_limit": None, "grids": records}

    def rebuild(self, log=lambda _message: None, *, plan=None, progress=None):
        """Rebuild once after loading; any failure terminates this import attempt."""
        if self.connection.in_atomic_block:
            raise RuntimeError("Cache rebuilding requires its own transactions")
        self._check_grids()
        plan = self.plan() if plan is None else plan
        if (plan.get("schema_version") != 1 or plan.get("project_id") != self.project_id
                or plan.get("node_limit") is not None
                or [record["definition"] for record in plan["grids"]] != self.grids):
            raise VerificationError("Cache rebuild plan changed; inspect the failed import")
        # Count all cells evaluated, including intentionally empty results.
        total = sum(math.prod(high - low + 1 for low, high in record["ranges"])
                    for record in plan["grids"] if record["ranges"] is not None)
        completed = 0
        if progress is not None:
            progress(0, total)
        records = []
        for planned in plan["grids"]:
            grid, ranges = planned["definition"], planned["ranges"]
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT clock_timestamp()")
                started_at = cursor.fetchone()[0].isoformat()
                # Definitions (and their IDs) are never deleted or rewritten.
                cursor.execute("DELETE FROM node_grid_cache_cell WHERE grid_id=%s", [grid["id"]])
                cursor.execute("DELETE FROM dirty_node_grid_cache_cell WHERE grid_id=%s", [grid["id"]])
            log(f"Rebuilding grid {grid['id']} after all import batches")
            if ranges is not None:
                for cell in product(*(range(low, high + 1) for low, high in ranges)):
                    self._refresh(grid, cell)
                    completed += 1
                    if progress is not None:
                        progress(completed, total)
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM node_grid_cache_cell WHERE grid_id=%s", [grid["id"]])
                cell_count = cursor.fetchone()[0]
            records.append({"definition": grid, "generated_at": started_at, "cell_count": cell_count})
        result = {"schema_version": 1, "project_id": self.project_id,
                  "strategy": "offline_full_grid_rebuild", "node_limit": None,
                  "grids": records}
        # The orchestrator verifies payloads once, in its final verification phase.
        return result

    def verify(self, receipt):
        """Verify preserved definitions and every stored payload without rebuilding."""
        self._check_grids()
        if (receipt.get("schema_version") != 1 or receipt.get("project_id") != self.project_id
                or receipt.get("node_limit") is not None
                or receipt.get("strategy") != "offline_full_grid_rebuild"
                or [record["definition"] for record in receipt["grids"]] != self.grids):
            raise VerificationError("Cache rebuild receipt does not match the target configuration")
        total = 0
        for record in receipt["grids"]:
            grid = record["definition"]
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT count(*), count(*) FILTER (WHERE update_time < %s::timestamptz
                        OR (%s AND (json_data IS NULL OR cardinality(json_data) <> %s))
                        OR (%s AND (json_text_data IS NULL OR cardinality(json_text_data) <> %s))
                        OR (%s AND (msgpack_data IS NULL OR cardinality(msgpack_data) <> %s)))
                    FROM node_grid_cache_cell WHERE grid_id=%s
                """, [record["generated_at"], grid["has_json_data"], grid["n_lod_levels"],
                      grid["has_json_text_data"], grid["n_lod_levels"],
                      grid["has_msgpack_data"], grid["n_lod_levels"], grid["id"]])
                count, invalid = cursor.fetchone()
                cursor.execute("SELECT count(*) FROM dirty_node_grid_cache_cell WHERE grid_id=%s", [grid["id"]])
                dirty = cursor.fetchone()[0]
            if count != record["cell_count"] or invalid or dirty:
                raise VerificationError(
                    f"Grid {grid['id']} rebuild is incomplete or stale; keep the layer inactive "
                    "and inspect the failed import")
            total += count
        return {"strategy": "offline_full_grid_rebuild", "rebuilt": True, "verified": True,
                "grid_count": len(self.grids), "cell_count": total}
