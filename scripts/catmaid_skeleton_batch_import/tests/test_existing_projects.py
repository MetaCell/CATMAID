from __future__ import annotations

import json
import sys
import zipfile
from copy import deepcopy
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import import database, loader, materialization, offline_cache, orchestrator, project, project_checks
from catmaid_skeleton_batch_import.config import load_request, load_settings
from catmaid_skeleton_batch_import.artifacts import read_id_artifact
from catmaid_skeleton_batch_import.errors import ImmutableStateError, InvalidInputError, RetryableError
from catmaid_skeleton_batch_import.loader import ProjectContext
from catmaid_skeleton_batch_import.state import StateStore


SWC = "1 1 1 1 1 0.5 -1\n2 3 2 1 1 -1 1\n3 3 3 1 1 0.4 2\n"
CONTEXT = {
    "project_id": 123, "stack_id": None, "project_stack_id": None,
    "user_id": 7, "neuron_class_id": 31, "skeleton_class_id": 32,
    "annotation_class_id": 33, "model_of_relation_id": 41,
    "annotated_with_relation_id": 42, "existing_project": True,
}
COUNTS = {
    "neuron_count": 1, "skeleton_count": 1, "treenode_count": 3,
    "edge_count": 3, "summary_count": 1, "class_instance_count": 3,
    "relationship_count": 2, "annotation_count": 1,
    "annotation_link_count": 1, "model_of_count": 1,
}
STACK_GEOMETRY = {
    "schema_version": 1, "project_id": 123, "database_target": {"sha256": "test-cluster"},
    "stacks": [{"project_stack_id": 8, "stack_id": 9, "orientation": 0,
                "dimension": [100, 100, 100], "resolution_nm": [1000, 1000, 1000],
                "translation_nm": [0, 0, 0]}],
}


def request_file(tmp_path, *, target=123, member_path="alpha.swc"):
    archive = tmp_path / "input.zip"
    with zipfile.ZipFile(archive, "w") as source:
        source.writestr(member_path, SWC)
    path = tmp_path / "request.json"
    path.write_text(json.dumps({
        "schema_version": 1,
        "source": {"archive_path": str(archive), "coordinate_unit": "um"},
        "target": {"project_id": target},
    }))
    return path


def forbidden(*_args, **_kwargs):
    raise AssertionError("An append must not create a project, replace cache definitions, or validate unrelated data")


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
        if "pg_try_advisory_lock" in sql:
            self.rows = [(self.connection.available,)]
        elif "pg_advisory_unlock" in sql:
            self.rows = [(True,)]
        elif "SELECT id, name FROM class_instance" in sql:
            self.rows = [(identifier, name) for identifier, name in self.connection.annotations
                         if name in params[2]]
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class Connection:
    def __init__(self, annotations=(), *, available=True):
        self.statements = []
        self.annotations = annotations
        self.available = available
        self.in_atomic_block = False
        self.geometry = deepcopy(STACK_GEOMETRY)

    def cursor(self):
        return Cursor(self)


def test_existing_request_needs_only_project_and_physical_source(tmp_path):
    path = request_file(tmp_path)
    request = load_request(path)
    assert request.target_project_id == 123
    assert request.dimension is None
    assert request.resolution_nm is None
    assert request.scale_nm == 1000
    assert request.to_dict() == json.loads(path.read_text())
    with pytest.raises(ValueError, match="no stack geometry"):
        _ = request.max_nm


@pytest.mark.parametrize("target", [True, False, 0, -1, "123", 1.5, None, 2**31])
def test_invalid_existing_project_id_is_rejected(tmp_path, target):
    with pytest.raises(InvalidInputError, match="positive integer"):
        load_request(request_file(tmp_path, target=target))


def test_existing_request_cannot_also_change_stack_geometry(tmp_path):
    path = request_file(tmp_path)
    value = json.loads(path.read_text())
    value["stack"] = {"dimension": [1, 1, 1], "resolution_nm": [1, 1, 1]}
    path.write_text(json.dumps(value))
    with pytest.raises(InvalidInputError, match="unknown fields"):
        load_request(path)


def test_project_selection_is_read_only_and_requires_tracing():
    components = {
        "Project": SimpleNamespace(objects=SimpleNamespace(
            filter=lambda **_kwargs: SimpleNamespace(exists=lambda: True))),
        "check_tracing_setup": lambda _pid: True,
        "get_system_user": lambda: SimpleNamespace(id=7),
        "get_class_to_id_map": lambda *_args: {"neuron": 31, "skeleton": 32, "annotation": 33},
        "get_relation_to_id_map": lambda *_args: {"model_of": 41, "annotated_with": 42},
        "assign_perm": forbidden, "setup_tracing": forbidden,
        "validate_project_setup": forbidden,
    }
    assert project.existing_project_context(123, components=components) == CONTEXT
    components["check_tracing_setup"] = lambda _pid: False
    with pytest.raises(project.ProjectSetupError, match="incomplete tracing"):
        project.existing_project_context(123, components=components)
    components["Project"].objects.filter = lambda **_kwargs: SimpleNamespace(exists=lambda: False)
    with pytest.raises(project.ProjectSetupError, match="does not exist"):
        project.existing_project_context(123, components=components)


def test_existing_project_lock_is_released_on_failure_and_busy_is_retryable():
    connection = Connection()
    with pytest.raises(RuntimeError, match="failed batch"):
        with database.existing_project_import_lock(123, connection):
            raise RuntimeError("failed batch")
    assert "pg_advisory_unlock" in connection.statements[-1][0]
    with pytest.raises(RetryableError, match="Another skeleton import"):
        with database.existing_project_import_lock(123, Connection(available=False)):
            forbidden()


@pytest.mark.parametrize("existing", [False, True])
def test_both_target_modes_use_bulk_materialization_in_one_atomic_batch(monkeypatch, existing):
    connection = Connection()
    monkeypatch.setitem(sys.modules, "django.db", SimpleNamespace(
        connection=connection, transaction=SimpleNamespace(atomic=lambda **_kwargs: nullcontext())))
    copied = []
    events = []
    monkeypatch.setattr(loader, "_copy_file", lambda _cursor, sql, _source: copied.append(sql))
    monkeypatch.setattr(loader, "verify_exact_treenodes", lambda *_args: events.append("exact"))
    def materialize(**_kwargs):
        assert connection.statements[-1][0] == "SET LOCAL session_replication_role = origin"
        events.append("materialize")
        return dict(COUNTS)
    monkeypatch.setattr(materialization, "materialize_and_verify_batch", materialize)
    monkeypatch.setattr(materialization, "validate_batch", forbidden)
    prepared = SimpleNamespace(
        batch=SimpleNamespace(index=1, skeleton_count=1, node_count=3),
        class_file=None, relationship_file=None, treenode_file=None,
        expected_skeletons=[SimpleNamespace(to_validation_dict=lambda: {})],
        location_ids=(100, 101, 102),
    )
    context = CONTEXT | {"existing_project": existing}
    loader.execute_batch_transaction(prepared, ProjectContext.from_dict(context),
                                     load_settings(), lambda: events.append("commit"))
    assert len(copied) == 3
    assert [sql for sql, _params in connection.statements if "session_replication_role" in sql] == [
        "SET LOCAL session_replication_role = replica", "SET LOCAL session_replication_role = origin"]
    assert events == ["materialize", "exact", "commit"]


def configure_run(monkeypatch, connection):
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setitem(sys.modules, "django.conf", SimpleNamespace(
        settings=SimpleNamespace(NODE_LIST_MAXIMUM_COUNT=None)))
    monkeypatch.setattr(project, "existing_project_context", lambda _pid: dict(CONTEXT))
    monkeypatch.setattr(project, "create_hidden_project", forbidden)
    monkeypatch.setattr(database, "allocate_project_ids", forbidden)
    monkeypatch.setattr(project_checks, "verify_project_stack", forbidden)
    monkeypatch.setattr(project_checks, "verify_project_aggregate_counts", forbidden)
    monkeypatch.setattr(database, "analyze_tables", lambda *_args: {"count": 5})
    def database_context(_store, _state, *, require_replica_role):
        assert require_replica_role is True
        return connection, {}
    monkeypatch.setattr(orchestrator, "_database_context", database_context)
    monkeypatch.setattr(orchestrator, "read_project_stack_geometry",
                        lambda _pid, _connection=None: deepcopy(connection.geometry))
    monkeypatch.setattr(materialization, "validate_batch", lambda **_kwargs: dict(COUNTS))
    allocated = iter(range(1000, 10000))
    monkeypatch.setattr(database, "allocate_sequence_ids", lambda _seq, count, *_args:
                        tuple(next(allocated) for _ in range(count)))


def plan_append(path, state_dir):
    orchestrator.preflight_ingestion(path, state_dir)
    return orchestrator.plan_ingestion(path, state_dir)


def test_append_plan_needs_a_local_snapshot_without_attempting_database_access(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrator, "read_project_stack_geometry", forbidden)
    with pytest.raises(InvalidInputError, match="run preflight"):
        orchestrator.plan_ingestion(request_file(tmp_path), tmp_path / "state")
    assert not (tmp_path / "state" / "state.json").exists()


def test_preflight_only_reads_metadata_and_append_planning_is_entirely_offline(tmp_path, monkeypatch):
    connection = Connection()
    configure_run(monkeypatch, connection)
    path = request_file(tmp_path)
    state_dir = tmp_path / "state"
    with monkeypatch.context() as preflight:
        preflight.setattr(orchestrator, "scan_archive", forbidden)
        result = orchestrator.preflight_ingestion(path, state_dir)
    assert result["command"] == "preflight"
    assert result["required"] is True
    assert result["project_id"] == 123
    assert result["stack_count"] == 1
    assert not (state_dir / "state.json").exists()
    assert not (state_dir / "plan.jsonl.gz").exists()
    monkeypatch.setattr(orchestrator, "read_project_stack_geometry", forbidden)
    monkeypatch.setattr(database, "bootstrap_django", forbidden)
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "must.not.be.loaded")
    assert orchestrator.plan_ingestion(path, state_dir)["ok"]
    assert orchestrator.plan_ingestion(path, state_dir)["ok"]
    StateStore(state_dir).verify_artifact("stack_geometry")


@pytest.mark.parametrize("payload", ["not json", "[]", '{"schema_version": 2}',
                                     json.dumps(STACK_GEOMETRY | {"project_id": 999})])
def test_append_plan_rejects_invalid_or_wrong_project_snapshots(tmp_path, payload):
    path = request_file(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "stack-geometry.json").write_text(payload)
    with pytest.raises(InvalidInputError, match="Invalid preflight stack snapshot"):
        orchestrator.plan_ingestion(path, state_dir)
    assert not (state_dir / "state.json").exists()


def test_preflight_can_refresh_metadata_only_before_planning(tmp_path, monkeypatch):
    connection = Connection()
    configure_run(monkeypatch, connection)
    path = request_file(tmp_path)
    state_dir = tmp_path / "state"
    orchestrator.preflight_ingestion(path, state_dir)
    connection.geometry["stacks"][0]["dimension"][0] = 200
    orchestrator.preflight_ingestion(path, state_dir)
    assert json.loads((state_dir / "stack-geometry.json").read_text()) == connection.geometry
    orchestrator.plan_ingestion(path, state_dir)
    assert orchestrator.preflight_ingestion(path, state_dir)["ok"]
    saved = (state_dir / "stack-geometry.json").read_bytes()
    connection.geometry["stacks"][0]["dimension"][0] = 300
    with pytest.raises(ImmutableStateError, match="geometry or database identity changed"):
        orchestrator.preflight_ingestion(path, state_dir)
    assert (state_dir / "stack-geometry.json").read_bytes() == saved


def test_append_planning_rejects_outside_coordinates_without_persisting_a_plan(tmp_path, monkeypatch):
    connection = Connection()
    configure_run(monkeypatch, connection)
    connection.geometry["stacks"][0]["dimension"] = [1, 100, 100]
    state_dir = tmp_path / "state"
    with pytest.raises(InvalidInputError, match="outside all associated target stack bounds"):
        plan_append(request_file(tmp_path), state_dir)
    assert not (state_dir / "plan.jsonl.gz").exists()
    assert not (state_dir / "state.json").exists()
    assert connection.statements == []


@pytest.mark.parametrize("stage", ["preflight", "run"])
@pytest.mark.parametrize("change", ["geometry", "database"])
def test_changed_append_geometry_or_database_is_rejected_before_reserving_ids(tmp_path, monkeypatch, stage, change):
    connection = Connection()
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    path = request_file(tmp_path)
    plan_append(path, state_dir)
    if change == "geometry":
        connection.geometry["stacks"][0]["translation_nm"][0] = 10
    else:
        connection.geometry["database_target"]["sha256"] = "different-cluster"
    monkeypatch.setattr(database, "allocate_sequence_ids", forbidden)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", forbidden)
    # Offline planning intentionally keeps using the snapshot, even if the live
    # target has since changed. Preflight/run enforce freshness against CATMAID.
    assert orchestrator.plan_ingestion(path, state_dir)["ok"]
    with pytest.raises(ImmutableStateError, match="geometry or database identity changed"):
        if stage == "preflight":
            orchestrator.preflight_ingestion(path, state_dir)
        else:
            orchestrator.run_ingestion(state_dir)
    assert StateStore(state_dir).load()["batches"][0]["status"] == "planned"


def test_missing_stack_snapshot_fails_before_loading_or_reserving_ids(tmp_path, monkeypatch):
    from catmaid_skeleton_batch_import.util import atomic_write_json

    connection = Connection()
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path), state_dir)
    store = StateStore(state_dir)
    state = store.load()
    del state["artifacts"]["stack_geometry"]
    atomic_write_json(store.state_path, state)
    connection.geometry["stacks"][0]["dimension"] = [1, 100, 100]
    monkeypatch.setattr(database, "allocate_sequence_ids", forbidden)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", forbidden)
    with pytest.raises(ImmutableStateError, match="Missing state artifact: stack_geometry"):
        orchestrator.run_ingestion(state_dir)
    assert store.load()["batches"][0]["status"] == "planned"


def test_wrong_database_does_not_persist_a_conflicting_runtime_fingerprint(tmp_path, monkeypatch):
    database_context = orchestrator._database_context
    connection = Connection()
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path), state_dir)
    store = StateStore(state_dir)
    monkeypatch.setattr(database, "bootstrap_django", lambda: connection)
    monkeypatch.setattr(database, "preflight_database", lambda *_args, **_kwargs:
                        {"target": {"sha256": "wrong-cluster"}})
    with pytest.raises(ImmutableStateError, match="differs from append planning"):
        database_context(store, store.load())
    assert "database" not in store.load()["artifacts"]


def test_geometry_change_during_import_prevents_completion(tmp_path, monkeypatch):
    connection = Connection()
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path), state_dir)
    def commit(_prepared, _context, _settings, before_commit, _timings):
        before_commit()
        connection.geometry["stacks"][0]["dimension"][0] += 1
        return dict(COUNTS)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", commit)
    with pytest.raises(ImmutableStateError, match="geometry or database identity changed"):
        orchestrator.run_ingestion(state_dir)
    state = StateStore(state_dir).load()
    assert state["batches"][0]["status"] == "committed"
    assert state["status"] != "complete"


def test_complete_append_and_resume_are_scoped_and_preserve_target_configuration(tmp_path, monkeypatch):
    connection = Connection([(99, "source-fragment:unrelated-existing-skeleton")])
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path), state_dir)
    batches = []
    exact_checks = []
    def commit(prepared, context, _settings, before_commit, _timings):
        assert context.existing_project
        batches.append(prepared.batch.index)
        before_commit()
        return dict(COUNTS)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", commit)
    monkeypatch.setattr(loader, "verify_exact_treenodes", lambda *_args: exact_checks.append(True))
    first = orchestrator.run_ingestion(state_dir)
    descriptor = StateStore(state_dir).load()["batches"][0]["artifacts"]["concept_ids"]
    own_ids = read_id_artifact(state_dir, descriptor, expected_count=5)
    connection.annotations.append((own_ids[3], "source-fragment:alpha"))
    second = orchestrator.run_ingestion(state_dir)
    assert first == second
    assert first["project_id"] == 123
    assert first["stack_id"] is None
    assert first["status"] == "complete"
    assert first["existing_project"] is True
    assert batches == [1]  # Resume never reimports committed rows.
    assert len(exact_checks) == 2  # Coordinates/radii are checked on every run.
    result = json.loads((state_dir / "verification.json").read_text())
    assert result["cache"] == {
        "rebuilt": True, "strategy": "offline_full_grid_rebuild", "verified": True,
        "grid_count": 0, "cell_count": 0,
    }
    assert result["database"]["skeleton_count"] == 1
    state = StateStore(state_dir)
    assert "project_reservation" not in state.load()["artifacts"]
    with pytest.raises(ImmutableStateError, match="planned target"):
        state.select_existing_project(124)


@pytest.mark.parametrize("member_path", ["alpha.swc", "folder/ \talpha \u2003.swc"])
def test_provenance_collision_is_rejected_before_any_batch_is_loaded(tmp_path, monkeypatch, member_path):
    configure_run(monkeypatch, Connection([(99, "source-fragment:alpha")]))
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path, member_path=member_path), state_dir)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", forbidden)
    monkeypatch.setattr(database, "allocate_sequence_ids", forbidden)
    with pytest.raises(InvalidInputError, match="already exist in project 123"):
        orchestrator.run_ingestion(state_dir)
    assert StateStore(state_dir).load()["batches"][0]["status"] == "planned"


def test_interrupted_cache_refresh_resumes_committed_batch_and_requires_final_verification(tmp_path, monkeypatch):
    connection = Connection()
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path), state_dir)
    events = []
    def commit(_prepared, _context, _settings, before_commit, _timings):
        events.append("commit")
        before_commit()
        return dict(COUNTS)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", commit)
    monkeypatch.setattr(loader, "verify_exact_treenodes", lambda *_args: events.append("database_verification"))
    failure = ["refresh"]
    def rebuild(self, **_kwargs):
        assert StateStore(state_dir).load()["batches"][0]["status"] == "committed"
        events.append("refresh")
        if failure == ["refresh"]:
            raise RetryableError("injected cache interruption")
        return {"schema_version": 1, "project_id": 123, "grids": [],
                "strategy": "offline_full_grid_rebuild"}
    def verify(self, _receipt):
        events.append("cache_verification")
        if failure == ["verification"] and events[-2] == "database_verification":
            raise RetryableError("injected final cache invalidation")
        return {"verified": True, "cell_count": 1}
    monkeypatch.setattr(offline_cache.OfflineGridCache, "rebuild", rebuild)
    monkeypatch.setattr(offline_cache.OfflineGridCache, "verify", verify)
    with pytest.raises(RetryableError, match="cache interruption"):
        orchestrator.run_ingestion(state_dir)
    state = StateStore(state_dir).load()
    assert state["batches"][0]["status"] == "committed"
    assert state["status"] != "complete"
    assert state["phases"]["cache"]["status"] == "retryable"

    failure[:] = ["verification"]
    with pytest.raises(RetryableError, match="final cache invalidation"):
        orchestrator.run_ingestion(state_dir)
    state = StateStore(state_dir).load()
    assert state["status"] != "complete"
    assert state["phases"]["verification"]["status"] == "retryable"
    failure.clear()
    assert orchestrator.run_ingestion(state_dir)["status"] == "complete"
    assert events.count("commit") == 1
    assert events.count("refresh") == 2  # Completed cache stage is verified, not rebuilt again.
    assert events[-2:] == ["database_verification", "cache_verification"]


def test_cache_is_rebuilt_once_after_all_batches(tmp_path, monkeypatch):
    connection = Connection()
    configure_run(monkeypatch, connection)
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "3")
    path = request_file(tmp_path)
    with zipfile.ZipFile(tmp_path / "input.zip", "a") as source:
        source.writestr("beta.swc", SWC)
    state_dir = tmp_path / "state"
    plan_append(path, state_dir)
    events = []
    def commit(prepared, _context, _settings, before_commit, _timings):
        events.append(f"commit_{prepared.batch.index}")
        before_commit()
        return dict(COUNTS)
    monkeypatch.setattr(orchestrator, "execute_batch_transaction", commit)
    monkeypatch.setattr(loader, "verify_exact_treenodes", lambda *_args: None)
    original = offline_cache.OfflineGridCache.rebuild
    def rebuild(self, **kwargs):
        events.append("cache")
        return original(self, **kwargs) | {"rebuild_serial": events.count("cache")}
    monkeypatch.setattr(offline_cache.OfflineGridCache, "rebuild", rebuild)
    assert orchestrator.run_ingestion(state_dir)["status"] == "complete"
    assert events == ["commit_1", "commit_2", "cache"]
    assert orchestrator.run_ingestion(state_dir)["status"] == "complete"
    assert events == ["commit_1", "commit_2", "cache"]
    assert orchestrator.run_ingestion(state_dir, rebuild_cache=True)["status"] == "complete"
    assert events == ["commit_1", "commit_2", "cache", "cache"]


def test_project_context_round_trips():
    assert ProjectContext.from_dict(CONTEXT).to_dict() == CONTEXT


@pytest.mark.parametrize("interrupted", [False, True], ids=["explicit-rebuild", "resume"])
def test_cache_observation_is_reset_before_coverage_planning(tmp_path, monkeypatch, interrupted):
    connection = Connection()
    configure_run(monkeypatch, connection)
    state_dir = tmp_path / "state"
    plan_append(request_file(tmp_path), state_dir)
    commits = []

    def commit(prepared, _context, _settings, before_commit, _timings):
        commits.append(prepared.batch.index)
        before_commit()
        return dict(COUNTS)

    monkeypatch.setattr(orchestrator, "execute_batch_transaction", commit)
    monkeypatch.setattr(loader, "verify_exact_treenodes", lambda *_args: None)
    assert orchestrator.run_ingestion(state_dir)["status"] == "complete"

    observation_path = state_dir / "cache-progress.json"
    previous = {"schema_version": 1, "completed": 90, "total": 90}
    observation_path.write_text(json.dumps(previous))
    # A completed import that only verifies must not reset diagnostic counters.
    assert orchestrator.run_ingestion(state_dir)["status"] == "complete"
    assert json.loads(observation_path.read_text()) == previous

    store = StateStore(state_dir)
    if interrupted:
        store.prepare_cache_rebuild(reset_plan=True)
        store.mark_phase_running("cache")
        store.mark_phase_retryable("cache", "interrupted coverage planning")

    original_plan = offline_cache.OfflineGridCache.plan
    observations = []

    def check_before_planning(cache):
        assert store.load()["phases"]["cache"]["status"] == "running"
        current = json.loads(observation_path.read_text())
        observations.append(current)
        assert current == {"schema_version": 1, "completed": 0, "total": 0}
        return original_plan(cache)

    monkeypatch.setattr(offline_cache.OfflineGridCache, "plan", check_before_planning)
    assert orchestrator.run_ingestion(state_dir, rebuild_cache=not interrupted)["status"] == "complete"
    assert len(observations) == 1
    assert commits == [1]
    assert store.load()["phases"]["verification"]["status"] == "complete"
