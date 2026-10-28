from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
import copy
import sys

import pytest

from catmaid_skeleton_batch_import.cache_rebuild import GRID_FIELDS, GridCacheRebuild
from catmaid_skeleton_batch_import.errors import OperationalFailureError, VerificationError


NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)


def grid(identifier=11, **overrides):
    return dict(zip(GRID_FIELDS, (
        identifier, 123, 0, 10, 20, 30, 2, "quadratic", 500,
        None, None, None, False, True, True, True, True, None,
    ))) | overrides


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def execute(self, sql, params=None):
        c = self.connection
        c.statements.append((sql, params))
        if "FROM node_grid_cache WHERE" in sql:
            self.rows = [tuple(g[name] for name in GRID_FIELDS) for g in c.grids]
        elif "SELECT min(x)," in sql:
            self.row = c.bounds
        elif "SELECT clock_timestamp" in sql:
            self.row = (NOW,)
        elif "min(x_index)" in sql:
            self.row = c.empty_extent
        elif "DELETE FROM node_grid_cache_cell" in sql:
            c.cells[params[0]] = set()
        elif "DELETE FROM dirty_node" in sql:
            c.dirty = 0
        elif "FROM dirty_node_grid" in sql:
            self.row = (c.dirty,)
        elif "count(*) FILTER" in sql:
            self.row = (len(c.cells[params[-1]]), c.invalid)
        elif "SELECT count(*)" in sql:
            self.row = (len(c.cells[params[0]]),)

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class Connection:
    in_atomic_block = False

    def __init__(self, grids=()):
        self.grids = list(grids)
        self.cells = {g["id"]: {(99, 99, 99)} for g in grids}
        self.statements = []
        self.bounds = (-10, 5, 3, 35, 25, 40)
        self.empty_extent = (0, 0, 0, 0, 0, 0)
        self.invalid = 0
        self.dirty = 0

    def cursor(self):
        return Cursor(self)


def tracker(connection):
    return GridCacheRebuild(123, connection, lock_timeout_ms=500, statement_timeout_ms=5000)


def test_full_rebuild_retains_all_grid_definitions_and_includes_whole_project(monkeypatch):
    connection = Connection([grid(), grid(12, cell_width=20, enabled=False)])
    before = copy.deepcopy(connection.grids)
    cache = tracker(connection)
    generated = []
    def refresh(config, cell):
        generated.append((config["id"], cell))
        connection.cells[config["id"]].add(cell)
    monkeypatch.setattr(cache, "_refresh", refresh)
    receipt = cache.rebuild()
    assert cache.verify(receipt)["grid_count"] == 2
    assert connection.grids == before
    assert (11, (-2, 0, 0)) in generated  # Negative coordinate on a cell boundary.
    assert (11, (3, 1, 1)) in generated
    assert (99, 99, 99) not in connection.cells[11]  # Stale payload was removed.
    assert len(generated) == len(set(generated))
    assert all("DELETE FROM node_grid_cache WHERE" not in sql for sql, _ in connection.statements)
    bounds_sql, params = next((s, p) for s, p in connection.statements if "SELECT min(x)," in s)
    assert "FROM treenode WHERE project_id=%s" in bounds_sql
    assert "FROM connector WHERE project_id=%s" in bounds_sql
    assert params == [123, 123]


def test_empty_coverage_is_retained_and_empty_projects_are_supported(monkeypatch):
    connection = Connection([grid(allow_empty=True)])
    connection.bounds = None
    connection.empty_extent = (-1, 0, 2, 2, 3, 3)
    cache = tracker(connection)
    monkeypatch.setattr(cache, "_refresh", lambda g, cell: connection.cells[g["id"]].add(cell))
    receipt = cache.rebuild()
    assert connection.cells[11] == {(-1, 2, 3), (0, 2, 3)}
    assert cache.verify(receipt)["cell_count"] == 2


def test_saved_plan_preserves_empty_coverage_after_an_interrupted_rebuild(monkeypatch):
    connection = Connection([grid(allow_empty=True)])
    connection.bounds = None
    connection.empty_extent = (-1, 0, 2, 2, 3, 3)
    cache = tracker(connection)
    plan = cache.plan()
    monkeypatch.setattr(cache, "_refresh", lambda *_args: (_ for _ in ()).throw(OperationalFailureError("interrupted")))
    with pytest.raises(OperationalFailureError, match="interrupted"):
        cache.rebuild(plan=plan)
    assert not connection.cells[11]
    connection.empty_extent = (None,) * 6
    resumed = tracker(connection)
    monkeypatch.setattr(resumed, "_refresh", lambda g, cell: connection.cells[g["id"]].add(cell))
    resumed.rebuild(plan=plan)
    assert connection.cells[11] == {(-1, 2, 3), (0, 2, 3)}


def test_project_without_grids_needs_no_cache_or_extent_queries():
    connection = Connection()
    cache = tracker(connection)
    assert cache.verify(cache.rebuild())["grid_count"] == 0
    assert not any("DELETE" in sql or "FROM treenode" in sql for sql, _ in connection.statements)


def test_failed_rebuild_can_restart_without_accepting_partial_payloads(monkeypatch):
    connection = Connection([grid()])
    cache = tracker(connection)
    calls = []
    def fail(config, cell):
        calls.append(cell)
        if len(calls) == 2:
            raise OperationalFailureError("injected generation failure")
        connection.cells[config["id"]].add(cell)
    monkeypatch.setattr(cache, "_refresh", fail)
    with pytest.raises(OperationalFailureError, match="generation failure"):
        cache.rebuild()
    monkeypatch.setattr(cache, "_refresh", lambda g, cell: connection.cells[g["id"]].add(cell))
    receipt = cache.rebuild()
    assert cache.verify(receipt)["cell_count"] > 1


@pytest.mark.parametrize("problem", ["missing", "invalid", "dirty", "configuration", "node_limit"])
def test_verification_rejects_incomplete_payloads_or_changed_settings(monkeypatch, problem):
    connection = Connection([grid()])
    cache = tracker(connection)
    monkeypatch.setattr(cache, "_refresh", lambda g, cell: connection.cells[g["id"]].add(cell))
    receipt = cache.rebuild()
    if problem == "missing":
        connection.cells[11].pop()
    elif problem == "invalid":
        connection.invalid = 1
    elif problem == "dirty":
        connection.dirty = 1
    elif problem == "node_limit":
        receipt["node_limit"] = 3500
    else:
        connection.grids[0]["ordering"] = "cable-desc"
    with pytest.raises((VerificationError, OperationalFailureError)):
        cache.verify(receipt)


def test_limited_cache_plan_is_rejected_before_existing_payloads_are_deleted():
    connection = Connection([grid()])
    cache = tracker(connection)
    plan = cache.plan()
    assert plan["node_limit"] is None
    plan["node_limit"] = 3500
    with pytest.raises(VerificationError, match="Cache rebuild plan changed"):
        cache.rebuild(plan=plan)
    assert not any("DELETE" in sql for sql, _params in connection.statements)


def test_rebuild_passes_original_formats_lod_filters_and_grid_id(monkeypatch):
    config = grid(orientation=2, enabled=False, allow_empty=True, has_json_text_data=False,
                  n_largest_skeletons_limit=5, n_last_edited_skeletons_limit=2,
                  hidden_last_editor_id=7, ordering="cable-desc")
    cache = tracker(Connection([config]))
    calls = []
    provider = object()
    provider_creations = []
    def make_provider():
        provider_creations.append(provider)
        return provider
    @contextmanager
    def locked(*args, **kwargs):
        calls.append((args, kwargs))
        yield "cursor", {}
    monkeypatch.setitem(sys.modules, "catmaid.control.grid_cache", SimpleNamespace(locked_grid_cell=locked))
    monkeypatch.setitem(sys.modules, "catmaid.control.node", SimpleNamespace(
        Postgis3dNodeProvider=make_provider,
        update_grid_cell=lambda *args, **kwargs: calls.append((args, kwargs)) or True))
    cache._refresh(config, (-1, 2, 3))
    assert calls[0][0] == (11, -1, 2, 3)
    assert calls[0][1]["statement_timeout_ms"] == 5000
    args, kwargs = calls[1]
    assert args[:8] == (123, 11, -1, 2, 3, 10, 20, 30)
    assert args[8] == {"project_id": 123, "limit": None, "ordering": "cable-desc",
                       "n_largest_skeletons_limit": 5, "n_last_edited_skeletons_limit": 2,
                       "hidden_last_editor_id": 7}
    assert args[9:] == (True, 2, 500, "quadratic", True, False, True)
    assert kwargs == {"provider": provider, "cursor": "cursor", "delete_empty": True}
    cache._refresh(config, (0, 2, 3))
    assert provider_creations == [provider]
    assert calls[3][1]["provider"] is provider


def test_invalid_grid_is_rejected_before_loading_any_batches():
    with pytest.raises(VerificationError, match="invalid dimensions"):
        tracker(Connection([grid(cell_width=0)]))


def test_cache_progress_counts_successful_cells_across_grids_including_empty(monkeypatch):
    connection = Connection([grid(), grid(12, cell_width=20)])
    cache = tracker(connection)
    plan = cache.plan()
    expected = sum(__import__("math").prod(high-low+1 for low,high in record["ranges"]) for record in plan["grids"])
    observed = []
    # An empty cell is still processed work even if no payload row is stored.
    monkeypatch.setattr(cache, "_refresh", lambda *_: None)
    cache.rebuild(plan=plan, progress=lambda done,total: observed.append((done,total)))
    assert observed == [(done,expected) for done in range(expected+1)]
    assert all(not rows for rows in connection.cells.values())


def test_cache_progress_does_not_count_failed_cell_and_resets_on_retry(monkeypatch):
    connection = Connection([grid()]); cache = tracker(connection)
    plan=cache.plan(); observed=[]; calls=[]
    def refresh(*_):
        calls.append(1)
        if len(calls)==2:
            raise OperationalFailureError("cell failure")
    monkeypatch.setattr(cache,"_refresh",refresh)
    with pytest.raises(OperationalFailureError, match="cell failure"):
        cache.rebuild(plan=plan, progress=lambda d,t:observed.append((d,t)))
    assert [d for d,t in observed] == [0,1]
    observed.clear(); monkeypatch.setattr(cache,"_refresh",lambda *_:None)
    cache.rebuild(plan=plan,progress=lambda d,t:observed.append((d,t)))
    assert observed[0][0] == 0
    assert observed[-1][0] == observed[-1][1]


def test_no_cache_grids_reports_zero_work():
    observed=[]
    tracker(Connection()).rebuild(progress=lambda d,t:observed.append((d,t)))
    assert observed == [(0,0)]
