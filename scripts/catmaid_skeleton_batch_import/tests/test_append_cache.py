from __future__ import annotations

import ast
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
import importlib.util
import itertools
from pathlib import Path
import random
import sys
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import.append_cache import AppendCacheRefresh, GRID_FIELDS, intersected_cells
from catmaid_skeleton_batch_import.errors import RetryableError, VerificationError


ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "grid_cache_helpers", ROOT / "django/applications/catmaid/control/grid_cache.py"
)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)


def grid(identifier=11, **overrides):
    result = dict(zip(GRID_FIELDS, (
        identifier, 123, 0, 10, 20, 30, 1, "quadratic", 500,
        None, None, None, False, True, True, True, True, None,
    )))
    return result | overrides


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.connection.statements.append((sql, params))
        if "FROM node_grid_cache WHERE" in sql:
            self.rows = [tuple(g[name] for name in GRID_FIELDS) for g in self.connection.grids]
        elif "ST_X(ST_StartPoint(edge))" in sql:
            self.rows = list(self.connection.edges)
        elif "WITH candidates" in sql:
            # The real PostGIS predicate is covered by the guarded integration
            # test. Restrict this fake to a chosen exact edge footprint.
            candidates = list(zip(*[iter(params[:-2])] * 4))
            self.connection.candidates.update(candidates)
            self.rows = sorted(set(candidates) & self.connection.query_cells)
        else:
            self.rows = [self.connection.current]

    def fetchall(self):
        return self.rows

    def fetchmany(self, size):
        result, self.rows = self.rows[:size], self.rows[size:]
        return result

    def fetchone(self):
        return self.rows[0] if self.rows else None


class Connection:
    def __init__(self, grids=()):
        self.grids = list(grids)
        self.edges = []
        self.statements = []
        self.current = (NOW, True, True, True, False)
        self.query_cells = set()
        self.candidates = set()

    def cursor(self):
        return Cursor(self)


def tracker(connection):
    result = AppendCacheRefresh(123, connection, lock_timeout_ms=500, statement_timeout_ms=5000)
    result._snapshot = nullcontext
    return result


def intersects(start, end, cell, sizes):
    """Independent segment/closed-box slab oracle."""
    lower, upper = 0.0, 1.0
    for a, b, index, size in zip(start, end, cell, sizes):
        if a == b:
            if not index * size <= a <= (index + 1) * size:
                return False
        else:
            entry, exit = sorted(((index * size - a) / (b - a),
                                  ((index + 1) * size - a) / (b - a)))
            lower, upper = max(lower, entry), min(upper, exit)
            if lower > upper:
                return False
    return True


def test_traversal_matches_independent_geometry_including_negative_and_boundary_points():
    rng = random.Random(27)
    sizes = (10, 20, 30)
    segments = [((0, 0, 0), (30, 60, 90)), ((1, 1, 1), (99, 1, 1)),
                ((-10, -20, -30), (-10, -20, -30)),
                ((0, 20, 0), (-20, 20, 60)),
                ((0, 0, 0), (20, 40.0000001, 60))]
    segments += [(tuple(rng.randint(-40, 40) for _ in range(3)),
                  tuple(rng.randint(-40, 40) for _ in range(3))) for _ in range(100)]
    for start, end in segments:
        candidates = itertools.product(range(-6, 12), range(-4, 6), range(-3, 5))
        expected = {cell for cell in candidates if intersects(start, end, cell, sizes)}
        assert set(intersected_cells(start, end, sizes)) == expected
        assert set(intersected_cells(end, start, sizes)) == expected


def test_only_edge_query_cells_in_each_grid_are_refreshed_once_per_batch(monkeypatch):
    connection = Connection([grid(), grid(12, cell_width=20)])
    connection.edges = [(1, 2, 3, 35, 2, 3), (35, 2, 3, 35, 2, 3)]
    connection.query_cells = {(11, x, 0, 0) for x in range(4)} | {(12, x, 0, 0) for x in range(2)}
    refresh = tracker(connection)
    refreshed = []
    def update(cell):
        refreshed.append(cell)
        return {"generated_at": NOW, "present": True}
    monkeypatch.setattr(refresh, "_refresh", update)
    refresh.refresh_batch([100, 101])
    assert set(refreshed) == connection.query_cells
    assert len(refreshed) == 6
    assert (11, 2, 0, 0) in refreshed  # Interior cell contains no endpoint.
    assert (11, 999, 999, 999) not in connection.candidates
    assert refresh.verify()["cell_count"] == 6
    assert len(refreshed) == 6  # Fresh cells need no second refresh.


def test_provider_distance_halo_is_considered_and_then_filtered(monkeypatch):
    connection = Connection([grid()])
    connection.edges = [(1, 1, 1, 29, 59, 29)]
    # The provider can return this edge within depth/2 of the middle-Z
    # rectangle, even though it does not intersect the cell's exact box.
    cell = (11, 0, 2, 0)
    assert cell[1:] not in set(intersected_cells((1, 1, 1), (29, 59, 29), (10, 20, 30)))
    connection.query_cells = {cell}
    refresh = tracker(connection)
    monkeypatch.setattr(refresh, "_refresh", lambda _cell: {"present": True, "generated_at": NOW})
    refresh.refresh_batch([100])
    assert set(refresh.receipts) == {cell}


def test_current_check_requires_all_formats_new_enough_and_no_dirty_marker():
    connection = Connection([grid()])
    refresh = tracker(connection)
    receipt = {"generated_at": NOW, "present": True}
    cell = (11, 0, 0, 0)
    for row in [(NOW, True, True, True, False),
                (NOW + timedelta(seconds=1), True, True, True, False)]:
        connection.current = row
        assert refresh._current(cell, receipt)
    for row in [(NOW - timedelta(seconds=1), True, True, True, False),
                (NOW, True, False, True, False),
                (NOW, True, True, True, True), (None, False, False, False, False)]:
        connection.current = row
        assert not refresh._current(cell, receipt)
    connection.current = (None, False, False, False, False)
    assert refresh._current(cell, {"generated_at": NOW, "present": False})


def test_verification_retries_only_cells_invalidated_again_and_is_bounded(monkeypatch):
    refresh = tracker(Connection([grid()]))
    clean, dirty = (11, 0, 0, 0), (11, 1, 0, 0)
    refresh.receipts = {clean: {}, dirty: {}}
    refreshed = []
    monkeypatch.setattr(refresh, "_current", lambda cell, _receipt: cell == clean or bool(refreshed))
    monkeypatch.setattr(refresh, "_refresh", lambda cell: refreshed.append(cell) or {})
    assert refresh.verify()["verified"]
    assert refreshed == [dirty]
    refreshed.clear()
    monkeypatch.setattr(refresh, "_current", lambda cell, _receipt: cell == clean)
    with pytest.raises(RetryableError, match="still changing"):
        refresh.verify()
    assert refreshed == [dirty] * 3


def test_cache_configuration_change_and_missing_edges_prevent_completion(monkeypatch):
    connection = Connection([grid()])
    refresh = tracker(connection)
    with pytest.raises(VerificationError, match="edges are missing"):
        refresh.refresh_batch([100])
    connection.grids[0]["cell_width"] = 15
    with pytest.raises(RetryableError, match="configuration changed"):
        refresh.verify()


def test_invalid_existing_grid_is_rejected_before_ingestion():
    with pytest.raises(VerificationError, match="invalid cell dimensions"):
        tracker(Connection([grid(cell_width=0)]))


class LockedCursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.connection.statements.append((sql, params))
        if "SELECT id, xmin::text" in sql:
            self.row = self.connection.dirty
        elif "clock_timestamp" in sql:
            self.row = (NOW,)
        elif "UPDATE node_grid_cache_cell" in sql:
            self.row = (55,)
        elif "DELETE FROM dirty" in sql and tuple(params) == self.connection.dirty:
            self.connection.dirty = None

    def fetchone(self):
        return self.row


def test_cell_update_serializes_formats_and_retains_a_new_invalidation():
    connection = SimpleNamespace(in_atomic_block=False, dirty=(44, "10"), statements=[])
    connection.cursor = lambda: LockedCursor(connection)
    active = []
    @contextmanager
    def atomic():
        active.append(True)
        try:
            yield
        finally:
            active.pop()
    with helpers.locked_grid_cell(11, -1, 2, 3, connection=connection, atomic=atomic,
                                 lock_timeout_ms=500) as (_cursor, receipt):
        assert active
        assert any("pg_advisory_xact_lock" in sql for sql, _ in connection.statements)
        # Same dirty row is updated by a different transaction during generation.
        connection.dirty = (44, "11")
    assert receipt == {"generated_at": NOW, "present": True}
    assert connection.dirty == (44, "11")
    with helpers.locked_grid_cell(11, -1, 2, 3, connection=connection, atomic=atomic):
        pass
    assert connection.dirty is None


def test_failed_generation_does_not_acknowledge_invalidation():
    connection = SimpleNamespace(in_atomic_block=False, dirty=(44, "10"), statements=[])
    connection.cursor = lambda: LockedCursor(connection)
    with pytest.raises(RuntimeError, match="generation failed"):
        with helpers.locked_grid_cell(11, 0, 0, 0, connection=connection, atomic=nullcontext):
            raise RuntimeError("generation failed")
    assert connection.dirty == (44, "10")
    assert not any("UPDATE node_grid_cache_cell" in sql for sql, _ in connection.statements)


def test_refresh_preserves_grid_formats_lod_and_filters_and_passes_timeouts(monkeypatch):
    config = grid(n_lod_levels=3, n_largest_skeletons_limit=5,
                  n_last_edited_skeletons_limit=2, hidden_last_editor_id=7,
                  has_json_text_data=False, ordering="cable-desc")
    refresh = tracker(Connection([config]))
    calls = []
    @contextmanager
    def locked(*cell, **kwargs):
        calls.append((cell, kwargs))
        yield "cursor", {"present": True, "generated_at": NOW}
    def update(*args, **kwargs):
        calls.append((args, kwargs))
        return True
    monkeypatch.setitem(sys.modules, "django.conf", SimpleNamespace(
        settings=SimpleNamespace(NODE_LIST_MAXIMUM_COUNT=3500)))
    monkeypatch.setitem(sys.modules, "catmaid.control.grid_cache", SimpleNamespace(locked_grid_cell=locked))
    monkeypatch.setitem(sys.modules, "catmaid.control.node", SimpleNamespace(update_grid_cell=update))
    assert refresh._refresh((11, -1, 2, 3))["present"]
    cell, options = calls[0]
    assert cell == (11, -1, 2, 3)
    assert options["lock_timeout_ms"] == 500
    assert options["statement_timeout_ms"] == 5000
    args, options = calls[1]
    assert args[:8] == (123, 11, -1, 2, 3, 10, 20, 30)
    assert args[8] == {"project_id": 123, "limit": 3500, "ordering": "cable-desc",
                       "n_largest_skeletons_limit": 5, "n_last_edited_skeletons_limit": 2,
                       "hidden_last_editor_id": 7}
    assert args[9:] == (False, 3, 500, "quadratic", True, False, True)
    assert options == {"cursor": "cursor", "delete_empty": True}


def test_final_verification_uses_one_read_only_snapshot_for_the_whole_scope(monkeypatch):
    refresh = tracker(Connection([grid()]))
    refresh.receipts = {(11, 0, 0, 0): {}, (11, 1, 0, 0): {}}
    active = []
    @contextmanager
    def snapshot():
        active.append(True)
        try:
            yield
        finally:
            active.pop()
    def current(*_args):
        assert active
        return True
    monkeypatch.setattr(refresh, "_snapshot", snapshot)
    monkeypatch.setattr(refresh, "_current", current)
    assert refresh.verify()["verified"]
    assert not active


def test_rectangular_cell_bounds_used_by_real_update_function():
    # Exercise the production function without importing CATMAID's web stack.
    path = ROOT / "django/applications/catmaid/control/node.py"
    tree = ast.parse(path.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "update_grid_cell")
    bounds = []
    namespace = {"_node_list_tuples_query": lambda params, *_args, **_kwargs:
                 bounds.append(dict(params)) or [[], []]}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    namespace["update_grid_cell"](
        123, 11, 1, 2, 3, 10, 20, 30, {}, False, 1, 500, "linear",
        True, True, True, provider=object(), cursor=SimpleNamespace(execute=lambda *_: None))
    assert bounds[0] == {"left": 10, "right": 20, "top": 40, "bottom": 60, "z1": 90, "z2": 120}


def test_normal_worker_uses_same_cell_lock_and_retains_largest_skeleton_filter():
    path = ROOT / "django/applications/catmaid/management/commands/catmaid_cache_update_worker.py"
    tree = ast.parse(path.read_text())
    worker = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "GridWorker")
    config = SimpleNamespace(**grid(n_largest_skeletons_limit=5))
    events = []
    active = []
    @contextmanager
    def locked(*cell):
        assert cell == (11, 1, 2, 3)
        active.append(True)
        events.append("lock")
        try:
            yield "transaction_cursor", {}
        finally:
            active.pop()
            events.append("commit")
    def update(*args, **kwargs):
        assert active
        assert kwargs["cursor"] == "transaction_cursor"
        assert args[8]["n_largest_skeletons_limit"] == 5
        events.append("refresh")
        return True
    namespace = {
        "NodeGridCache": SimpleNamespace(objects=SimpleNamespace(filter=lambda **_kwargs: [config])),
        "Postgis3dNodeProvider": object,
        "settings": SimpleNamespace(NODE_LIST_MAXIMUM_COUNT=3500, SPATIAL_UPDATE_CLIENT_NOTIFICATIONS=False),
        "locked_grid_cell": locked, "update_grid_cell": update,
        "logger": SimpleNamespace(debug=lambda *_args: None),
    }
    exec(compile(ast.Module(body=[worker], type_ignores=[]), str(path), "exec"), namespace)
    namespace["GridWorker"]().update([{"grid_id": 11, "x": 1, "y": 2, "z": 3}], None)
    assert events == ["lock", "refresh", "commit"]
