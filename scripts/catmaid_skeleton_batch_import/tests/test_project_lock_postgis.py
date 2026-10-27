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
