"""Real PostgreSQL admission races, restricted to the disposable test database."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4
import os

import pytest

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("CATMAID_BATCH_IMPORT_E2E") != "1",
        reason="requires the guarded disposable CATMAID/PostGIS database",
    ),
]


@pytest.fixture
def projects(isolated_database):
    from catmaid.models import Project

    rows = [Project.objects.create(title=f"Project ingestion lock test {i}") for i in range(2)]
    try:
        yield tuple(row.pk for row in rows)
    finally:
        from catmaid.ingestion_lock import release_project_ingestion

        for row in rows:
            row.refresh_from_db()
            if row.active_skeleton_ingestion is not None:
                release_project_ingestion(row.pk, row.active_skeleton_ingestion)
            row.delete()


@pytest.fixture
def sessions(isolated_database):
    """Every task owns a thread-local Django connection and a bounded lifetime."""
    from django.db import connections

    gates = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        def gate():
            value = Event()
            gates.append(value)
            return value

        def start(operation):
            ready = Event()
            task = SimpleNamespace(pid=None)

            def execute():
                connection = connections["default"]
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("SET statement_timeout = '10s'")
                        cursor.execute("SET lock_timeout = '8s'")
                        cursor.execute("SELECT pg_backend_pid()")
                        task.pid = cursor.fetchone()[0]
                    ready.set()
                    return operation(connection)
                finally:
                    ready.set()
                    connection.close()

            task.future = executor.submit(execute)
            assert ready.wait(5), "Database session did not start"
            if task.pid is None:
                task.future.result(timeout=5)
            return task

        try:
            yield SimpleNamespace(start=start, gate=gate)
        finally:
            # Unblock holders even when an assertion fails; tasks also have SQL timeouts.
            for value in gates:
                value.set()


def wait_for_blocker(connection, task, blockers):
    """Observe an actual PostgreSQL lock wait instead of assuming a sleep raced."""
    deadline = monotonic() + 5
    while monotonic() < deadline:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_blocking_pids(%s)", [task.pid])
            if set(cursor.fetchone()[0]) & set(blockers):
                return
        if task.future.done():
            task.future.result()
            pytest.fail("Database operation finished before the expected lock wait")
        Event().wait(0.01)
    pytest.fail("PostgreSQL did not report the expected blocker")


def owner(connection, project_id):
    with connection.cursor() as cursor:
        cursor.execute("SELECT active_skeleton_ingestion FROM project WHERE id = %s", [project_id])
        return cursor.fetchone()[0]


def test_acquisition_drains_all_existing_concurrent_edits(isolated_database, projects, sessions):
    from catmaid.apps import get_system_user
    from catmaid.ingestion_lock import acquire_project_ingestion, project_write_guard
    from catmaid.models import Class

    project_id = projects[0]
    user_id = get_system_user().pk
    entered = [sessions.gate(), sessions.gate()]
    finish = [sessions.gate(), sessions.gate()]

    def edit(index):
        def execute(_connection):
            with project_write_guard(project_id):
                Class.objects.create(project_id=project_id, user_id=user_id,
                                     class_name=f"guarded-edit-{index}")
                entered[index].set()
                assert finish[index].wait(10), "Edit was not allowed to finish"
        return execute

    first = sessions.start(edit(0))
    assert entered[0].wait(5)
    second = sessions.start(edit(1))
    # The second edit enters while the first is still inside its guard.
    assert entered[1].wait(5)
    token = uuid4()
    importer = sessions.start(lambda _connection: acquire_project_ingestion(
        project_id, token, lock_timeout_ms=8000))
    wait_for_blocker(isolated_database, importer, [first.pid, second.pid])
    assert owner(isolated_database, project_id) is None

    finish[0].set()
    first.future.result(timeout=5)
    wait_for_blocker(isolated_database, importer, [second.pid])
    assert not importer.future.done()

    finish[1].set()
    second.future.result(timeout=5)
    assert importer.future.result(timeout=5) == token
    assert owner(isolated_database, project_id) == token
    assert Class.objects.filter(project_id=project_id, class_name__startswith="guarded-edit-").count() == 2


def test_waiting_edit_rechecks_the_flag_after_acquisition_commits(isolated_database, projects, sessions):
    from catmaid.ingestion_lock import (
        ProjectIngestionLocked, acquire_project_ingestion, project_write_guard,
    )
    from django.db import transaction

    project_id = projects[0]
    token = uuid4()
    acquired, commit = sessions.gate(), sessions.gate()
    body_entered = Event()

    def acquire(_connection):
        # Hold the real acquisition's outer transaction just before its COMMIT.
        with transaction.atomic():
            acquire_project_ingestion(project_id, token)
            acquired.set()
            assert commit.wait(10), "Acquisition was not allowed to commit"

    importer = sessions.start(acquire)
    assert acquired.wait(5)

    def edit(connection):
        with project_write_guard(project_id):
            body_entered.set()
            with connection.cursor() as cursor:
                cursor.execute("UPDATE project SET comment = 'must not be written' WHERE id = %s",
                               [project_id])

    mutation = sessions.start(edit)
    wait_for_blocker(isolated_database, mutation, [importer.pid])
    # Ordinary reads are not blocked by the exclusive project-row lock.
    assert owner(isolated_database, project_id) is None
    commit.set()
    importer.future.result(timeout=5)
    with pytest.raises(ProjectIngestionLocked):
        mutation.future.result(timeout=5)
    assert not body_entered.is_set()
    assert owner(isolated_database, project_id) == token
    with isolated_database.cursor() as cursor:
        cursor.execute("SELECT comment FROM project WHERE id = %s", [project_id])
        assert cursor.fetchone()[0] != "must not be written"


def test_simultaneous_importers_have_exactly_one_owner(isolated_database, projects, sessions):
    from catmaid.ingestion_lock import ProjectIngestionLocked, acquire_project_ingestion

    barrier = Barrier(2, timeout=5)
    tokens = [uuid4(), uuid4()]

    def acquire(token):
        def execute(_connection):
            barrier.wait()
            try:
                return acquire_project_ingestion(projects[0], token)
            except ProjectIngestionLocked:
                return None
        return execute

    tasks = [sessions.start(acquire(token)) for token in tokens]
    results = [task.future.result(timeout=5) for task in tasks]
    assert sum(result is not None for result in results) == 1
    assert owner(isolated_database, projects[0]) == next(result for result in results if result)


@pytest.mark.parametrize("same_owner", [False, True])
def test_persistent_owner_survives_disconnect_and_rejects_other_claims(
    isolated_database, projects, sessions, same_owner,
):
    from catmaid.ingestion_lock import (
        ProjectIngestionLocked, acquire_project_ingestion, project_write_guard,
        release_project_ingestion,
    )

    project_id = projects[0]
    token = uuid4()
    task = sessions.start(lambda _connection: acquire_project_ingestion(project_id, token))
    assert task.future.result(timeout=5) == token
    # The session that acquired the flag has closed. The committed owner remains.
    assert owner(isolated_database, project_id) == token
    assert not isolated_database.in_atomic_block

    claimant = token if same_owner else uuid4()
    task = sessions.start(lambda _connection: acquire_project_ingestion(project_id, claimant))
    with pytest.raises(ProjectIngestionLocked):
        task.future.result(timeout=5)
    with pytest.raises(ProjectIngestionLocked):
        release_project_ingestion(project_id, uuid4())
    assert owner(isolated_database, project_id) == token
    with pytest.raises(ProjectIngestionLocked):
        with project_write_guard(project_id):
            pytest.fail("A guarded mutation entered a locked project")

    release_project_ingestion(project_id, token)
    with project_write_guard(project_id):
        with isolated_database.cursor() as cursor:
            cursor.execute("UPDATE project SET comment = 'edits resumed' WHERE id = %s", [project_id])
    assert owner(isolated_database, project_id) is None


def test_locked_project_still_allows_reads_and_other_project_edits(
    isolated_database, projects, sessions,
):
    from catmaid.ingestion_lock import acquire_project_ingestion, project_write_guard

    token = uuid4()
    acquire_project_ingestion(projects[0], token)

    def read(connection):
        with connection.cursor() as cursor:
            cursor.execute("SELECT title FROM project WHERE id = %s", [projects[0]])
            return cursor.fetchone()[0]

    assert sessions.start(read).future.result(timeout=5) == "Project ingestion lock test 0"

    def edit_other(connection):
        with project_write_guard(projects[1]):
            with connection.cursor() as cursor:
                cursor.execute("UPDATE project SET comment = 'other project edit' WHERE id = %s",
                               [projects[1]])

    sessions.start(edit_other).future.result(timeout=5)
    assert owner(isolated_database, projects[0]) == token
    assert owner(isolated_database, projects[1]) is None
    with isolated_database.cursor() as cursor:
        cursor.execute("SELECT comment FROM project WHERE id = %s", [projects[1]])
        assert cursor.fetchone()[0] == "other project edit"


def test_acquisition_timeout_rolls_back_and_restores_connection_settings(
    isolated_database, projects, sessions,
):
    from catmaid.ingestion_lock import acquire_project_ingestion, project_write_guard
    from django.db import OperationalError

    entered, finish = sessions.gate(), sessions.gate()

    def hold(_connection):
        with project_write_guard(projects[0]):
            entered.set()
            assert finish.wait(10), "Guard was not allowed to finish"

    editor = sessions.start(hold)
    assert entered.wait(5)

    def time_out(connection):
        with connection.cursor() as cursor:
            cursor.execute("SHOW lock_timeout")
            previous = cursor.fetchone()[0]
        with pytest.raises(OperationalError) as error:
            acquire_project_ingestion(projects[0], uuid4(), lock_timeout_ms=100)
        assert error.value.__cause__.pgcode == "55P03"
        assert connection.get_autocommit()
        with connection.cursor() as cursor:
            cursor.execute("SHOW lock_timeout")
            assert cursor.fetchone()[0] == previous
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1

    sessions.start(time_out).future.result(timeout=5)
    assert owner(isolated_database, projects[0]) is None
    finish.set()
    editor.future.result(timeout=5)
    token = uuid4()
    assert acquire_project_ingestion(projects[0], token, lock_timeout_ms=100) == token


def test_guard_exception_rolls_back_edits_and_releases_its_row_lock(
    isolated_database, projects, sessions,
):
    from catmaid.apps import get_system_user
    from catmaid.ingestion_lock import acquire_project_ingestion, project_write_guard
    from catmaid.models import Class

    user_id = get_system_user().pk

    def fail_edit(_connection):
        with project_write_guard(projects[0]):
            Class.objects.create(project_id=projects[0], user_id=user_id, class_name="rolled-back-edit")
            raise RuntimeError("injected edit failure")

    task = sessions.start(fail_edit)
    with pytest.raises(RuntimeError, match="injected edit failure"):
        task.future.result(timeout=5)
    assert not Class.objects.filter(project_id=projects[0], class_name="rolled-back-edit").exists()
    token = uuid4()
    assert acquire_project_ingestion(projects[0], token, lock_timeout_ms=100) == token


@pytest.mark.parametrize("supplied_identity", [False, True])
@pytest.mark.parametrize("fail_loading", [False, True])
def test_importer_uses_retained_ingestion_uuid_and_embedded_lock_state(
    isolated_database, existing_project_factory, tmp_path, monkeypatch, supplied_identity, fail_loading,
):
    import json
    import zipfile
    from uuid import UUID

    from catmaid import ingestion_lock
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.errors import OperationalFailureError
    from catmaid_skeleton_batch_import.orchestrator import (
        plan_ingestion, preflight_ingestion, run_ingestion,
    )
    from catmaid_skeleton_batch_import.state import StateStore
    from catmaid_skeleton_batch_import.workflow_io import FinalizeConfig, derive_finalization

    identity = str(uuid4())
    if supplied_identity:
        monkeypatch.setenv("SKELETON_INGESTION_ID", identity)
    else:
        monkeypatch.delenv("SKELETON_INGESTION_ID", raising=False)
    project_id = existing_project_factory()
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w") as source:
        source.writestr("cell.swc", chr(10).join([
            "1 1 1 1 1 -1 -1", "2 3 2 2 2 1 1", "",
        ]))
    request = tmp_path / "request.json"
    request.write_text(json.dumps({
        "schema_version": 1,
        "source": {"archive_path": str(archive), "coordinate_unit": "nm"},
        "target": {"project_id": project_id},
    }))
    store = StateStore(tmp_path / "state")
    preflight_ingestion(request, store.state_dir)
    plan_ingestion(request, store.state_dir)
    state = store.load()
    if supplied_identity:
        assert state["ingestion_id"] == identity
    else:
        identity = state["ingestion_id"]
    assert str(UUID(identity)) == identity
    assert state["project_lock"] == {"project_id": project_id, "status": "pending"}

    acquire = ingestion_lock.acquire_project_ingestion
    release = ingestion_lock.release_project_ingestion
    materialize = materialization.materialize_and_verify_batch
    observed = []

    def acquire_with_state(target, token, **kwargs):
        assert target == project_id and token == identity
        assert store.load()["project_lock"]["status"] == "acquiring"
        result = acquire(target, token, **kwargs)
        assert owner(isolated_database, project_id) == UUID(identity)
        observed.append("acquired")
        return result

    def release_with_state(target, token, **kwargs):
        assert target == project_id and token == identity
        assert store.load()["project_lock"]["status"] == "locked"
        assert store.load()["run"]["status"] == "complete"
        release(target, token, **kwargs)
        assert owner(isolated_database, project_id) is None
        observed.append("released")

    def materialize_with_state(**kwargs):
        assert store.load()["project_lock"]["status"] == "locked"
        assert owner(isolated_database, project_id) == UUID(identity)
        result = materialize(**kwargs)
        if fail_loading:
            raise OperationalFailureError("injected loading failure")
        return result

    monkeypatch.setattr(ingestion_lock, "acquire_project_ingestion", acquire_with_state)
    monkeypatch.setattr(ingestion_lock, "release_project_ingestion", release_with_state)
    monkeypatch.setattr(materialization, "materialize_and_verify_batch", materialize_with_state)
    try:
        if fail_loading:
            with pytest.raises(OperationalFailureError, match="injected loading failure"):
                run_ingestion(store.state_dir)
        else:
            run_ingestion(store.state_dir)
        state = store.load()
        assert state["ingestion_id"] == identity
        assert state["project_lock"] == {
            "project_id": project_id, "status": "locked" if fail_loading else "released",
        }
        assert state["run"]["status"] == ("failed" if fail_loading else "complete")
        assert owner(isolated_database, project_id) == (UUID(identity) if fail_loading else None)
        assert observed == (["acquired"] if fail_loading else ["acquired", "released"])
        assert not (store.state_dir / "project-ingestion-lock.json").exists()
        decision = derive_finalization(FinalizeConfig(
            ingestion_id=identity, workspace=tmp_path, workflow_name="lock-state-test",
            workflow_uid="test-workflow", workflow_result="Failed" if fail_loading else "Succeeded",
        ))
        assert decision.payload["outcome"] == ("operator_attention" if fail_loading else "succeeded")
    finally:
        if owner(isolated_database, project_id) == UUID(identity):
            release(project_id, identity)


@pytest.mark.parametrize("scenario", ["wrapper_startup_failure", "success", "partial_import"])
def test_workflow_project_lock_covers_parent_to_run_handoff(
    isolated_database, existing_project_factory, tmp_path, monkeypatch, scenario,
):
    import json
    from pathlib import Path
    import zipfile
    from uuid import UUID

    from catmaid.ingestion_lock import release_project_ingestion
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.errors import OperationalFailureError
    from catmaid_skeleton_batch_import.orchestrator import (
        plan_ingestion, preflight_ingestion, run_ingestion,
    )
    from catmaid_skeleton_batch_import.project_lock import WorkflowProjectLock
    from catmaid_skeleton_batch_import.state import StateStore
    from catmaid_skeleton_batch_import.workflow_io import FinalizeConfig, derive_finalization

    identity = str(uuid4())
    project_id = existing_project_factory()
    monkeypatch.setenv("SKELETON_INGESTION_ID", identity)
    monkeypatch.setenv("SKELETON_INGESTION_TARGET_PROJECT_ID", str(project_id))
    monkeypatch.setenv("SKELETON_INGESTION_WORKFLOW_NAME", "project-lock-handoff-test")
    monkeypatch.setenv("CATMAID_PROJECTS_DIRECTORY",
                       str(Path(__file__).resolve().parents[3] / "django/projects"))
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "2")
    workflow_lock = WorkflowProjectLock(os.environ)
    try:
        workflow_lock.acquire()
        assert isolated_database.connection is None
        assert owner(isolated_database, project_id) == UUID(identity)

        archive = tmp_path / "source.zip"
        swc = chr(10).join(["1 1 1 1 1 -1 -1", "2 3 2 2 2 1 1", ""])
        with zipfile.ZipFile(archive, "w") as source:
            source.writestr("a.swc", swc)
            if scenario == "partial_import":
                source.writestr("b.swc", swc)
        request = tmp_path / "request.json"
        request.write_text(json.dumps({
            "schema_version": 1,
            "source": {"archive_path": str(archive), "coordinate_unit": "nm"},
            "target": {"project_id": project_id},
        }))
        store = StateStore(tmp_path / "state")
        preflight_ingestion(request, store.state_dir)
        plan_ingestion(request, store.state_dir)

        if scenario == "wrapper_startup_failure":
            (store.state_dir / "run-wrapper-result.json").write_text(json.dumps({
                "schema_version": 1, "command": "run-wrapper", "terminal": True,
                "attempts": 0, "importer_exit_code": 66,
            }))
        elif scenario == "success":
            run_ingestion(store.state_dir)
        else:
            original = materialization.materialize_and_verify_batch
            completed = []

            def fail_second(**kwargs):
                result = original(**kwargs)
                completed.append(result)
                if len(completed) == 2:
                    raise OperationalFailureError("injected second-batch failure")
                return result

            monkeypatch.setattr(materialization, "materialize_and_verify_batch", fail_second)
            with pytest.raises(OperationalFailureError, match="second-batch failure"):
                run_ingestion(store.state_dir)

        workflow_lock.finish(tmp_path, run_started=True)
        state = store.load()
        if scenario == "wrapper_startup_failure":
            assert state["run"]["status"] == "pending"
            assert [batch["status"] for batch in state["batches"]] == ["planned"]
        elif scenario == "success":
            assert state["run"]["status"] == "complete"
            assert [batch["status"] for batch in state["batches"]] == ["committed"]
        else:
            assert state["run"]["status"] == "failed"
            assert [batch["status"] for batch in state["batches"]] == ["committed", "failed"]
        assert state["project_lock"]["status"] == ("locked" if scenario == "partial_import" else "released")
        assert owner(isolated_database, project_id) == (
            UUID(identity) if scenario == "partial_import" else None
        )
        with isolated_database.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM treenode WHERE project_id=%s", [project_id])
            assert cursor.fetchone()[0] == (0 if scenario == "wrapper_startup_failure" else 2)
        decision = derive_finalization(FinalizeConfig(
            ingestion_id=identity, workspace=tmp_path, workflow_name="project-lock-handoff-test",
            workflow_uid="test-workflow", workflow_result="Succeeded" if scenario == "success" else "Failed",
        ))
        assert decision.payload["outcome"] == {
            "wrapper_startup_failure": "failed", "success": "succeeded",
            "partial_import": "operator_attention",
        }[scenario]
    finally:
        if owner(isolated_database, project_id) == UUID(identity):
            release_project_ingestion(project_id, identity)
