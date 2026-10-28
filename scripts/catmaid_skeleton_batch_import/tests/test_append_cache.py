from __future__ import annotations

import ast
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import.cache_rebuild import GRID_FIELDS


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
