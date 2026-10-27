from types import SimpleNamespace
import sys
from unittest.mock import Mock

import pytest

from catmaid_skeleton_batch_import.domain import ImportSettings
from catmaid_skeleton_batch_import.errors import ProjectIngestionLockError
from catmaid_skeleton_batch_import.project_lock import project_ingestion_lock
from catmaid_skeleton_batch_import.state import ArchiveMetadata, StateStore
from catmaid_skeleton_batch_import.statuses import ProjectLockStatus
from catmaid_skeleton_batch_import.util import sha256_file

INGESTION_ID = "11111111-2222-4333-8444-555555555555"


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("SKELETON_INGESTION_ID", INGESTION_ID)
    archive = tmp_path / "source.zip"
    archive.write_bytes(b"archive")
    result = StateStore(tmp_path / "state")
    result.initialize_plan(
        {"schema_version": 1, "source": {"archive_path": str(archive), "coordinate_unit": "nm"},
         "target": {"project_id": 17}},
        ArchiveMetadata(archive, archive.stat().st_size, sha256_file(archive)),
        ImportSettings(max_nodes_per_batch=1, max_archive_bytes=1024,
                       max_swc_bytes=1024, max_total_nodes=None),
        [{"index": 1, "skeleton_count": 1, "node_count": 1}],
        plan_digest="a" * 64,
    )
    result.begin_run()
    return result


def helpers(monkeypatch):
    monkeypatch.delenv("SKELETON_INGESTION_WORKFLOW_NAME", raising=False)
    acquire, release = Mock(), Mock()
    monkeypatch.setitem(sys.modules, "catmaid.ingestion_lock", SimpleNamespace(
        acquire_project_ingestion=acquire, release_project_ingestion=release,
        require_project_ingestion=Mock()))
    return acquire, release


@pytest.mark.parametrize("batch_status, released", [
    ("planned", True), ("prepared", False), ("committed", False), ("failed", False),
])
def test_failure_retains_only_potentially_written_projects(store, monkeypatch, batch_status, released):
    acquire, release = helpers(monkeypatch)
    with pytest.raises(RuntimeError, match="injected"):
        with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            assert store.load()["project_lock"]["status"] == "locked"
            store.select_existing_project(17)
            if batch_status != "planned":
                store.mark_batch_prepared(1)
                if batch_status == "committed":
                    store.mark_batch_committed(1)
                elif batch_status == "failed":
                    store.fail_run("injected")
            raise RuntimeError("injected")
    store.fail_run("injected")
    state = store.load()
    assert state["ingestion_id"] == INGESTION_ID
    assert state["project_lock"] == {
        "project_id": 17, "status": "released" if released else "locked",
    }
    acquire.assert_called_once_with(17, INGESTION_ID, using="default", lock_timeout_ms=100)
    assert release.call_count == int(released)
    if released:
        release.assert_called_once_with(17, INGESTION_ID, using="default")


def test_success_records_unlock_after_run_completion_without_reopening_the_attempt(store, monkeypatch):
    from catmaid_skeleton_batch_import.errors import InvalidInputError

    acquire, release = helpers(monkeypatch)
    with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
        store.select_existing_project(17)
        store.mark_batch_prepared(1)
        store.mark_batch_committed(1)
        for phase in ("database_maintenance", "cache", "verification"):
            store.mark_phase_running(phase)
            store.mark_phase_complete(phase)
        store.finish_run()
        assert store.load()["project_lock"]["status"] == "locked"
    state = StateStore(store.state_dir).load()
    assert state["run"]["status"] == "complete"
    assert state["project_lock"] == {"project_id": 17, "status": "released"}
    release.assert_called_once_with(17, INGESTION_ID, using="default")
    assert not (store.state_dir / "project-ingestion-lock.json").exists()
    with pytest.raises(InvalidInputError, match="terminal"):
        store.mark_batch_prepared(1)


@pytest.mark.parametrize("failure, expected_status", [
    ("acquire", "acquiring"), ("record_locked", "acquiring"),
    ("release", "locked"), ("record_released", "locked"),
])
def test_uncertain_database_or_state_write_requires_inspection(
    store, monkeypatch, failure, expected_status,
):
    acquire, release = helpers(monkeypatch)
    if failure == "acquire":
        acquire.side_effect = RuntimeError("acquisition disconnected")
    elif failure == "release":
        release.side_effect = RuntimeError("release disconnected")
    transition = store.transition_project_lock

    def record(status):
        if ((failure == "record_locked" and status == ProjectLockStatus.LOCKED)
                or (failure == "record_released" and status == ProjectLockStatus.RELEASED)):
            raise OSError("state volume failed")
        return transition(status)

    monkeypatch.setattr(store, "transition_project_lock", record)
    with pytest.raises(ProjectIngestionLockError):
        with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            if failure in {"acquire", "record_locked"}:
                pytest.fail("No loading after uncertain acquisition")
    assert store.load()["project_lock"]["status"] == expected_status
    if expected_status == "acquiring":
        release.assert_not_called()


def test_failure_to_record_acquiring_never_touches_the_database(store, monkeypatch):
    acquire, release = helpers(monkeypatch)
    monkeypatch.setattr(store, "_persist", Mock(side_effect=OSError("state volume failed")))
    with pytest.raises(OSError, match="state volume"):
        with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            pytest.fail("No loading without a retained acquisition checkpoint")
    acquire.assert_not_called()
    release.assert_not_called()
    assert store.load()["project_lock"]["status"] == "pending"


def test_saved_lock_target_must_match_the_requested_project(store, monkeypatch):
    from catmaid_skeleton_batch_import.errors import ImmutableStateError

    acquire, release = helpers(monkeypatch)
    with pytest.raises(ImmutableStateError, match="requested target"):
        with project_ingestion_lock(store, 18, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            pytest.fail("No lock acquisition for a different project")
    acquire.assert_not_called()
    release.assert_not_called()
    assert store.load()["project_lock"]["status"] == "pending"


def workflow_lock(store, monkeypatch):
    from catmaid_skeleton_batch_import.project_lock import WorkflowProjectLock
    from catmaid_skeleton_batch_import import database

    acquire, release = helpers(monkeypatch)
    monkeypatch.setattr(database, "bootstrap_django", lambda: SimpleNamespace(alias="default"))
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "test-settings")
    lock = WorkflowProjectLock({"SKELETON_INGESTION_ID": INGESTION_ID,
                                "SKELETON_INGESTION_TARGET_PROJECT_ID": "17"})
    lock.acquire()
    return lock, acquire, release


def test_workflow_releases_a_confirmed_download_failure_before_state_exists(store, monkeypatch, tmp_path):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    lock.finish(tmp_path / "no-plan", run_started=False)
    assert lock.status == "released"
    release.assert_called_once_with(17, INGESTION_ID)


def test_workflow_hands_ownership_to_run_without_an_unlocked_gap(store, monkeypatch):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    monkeypatch.setenv("SKELETON_INGESTION_WORKFLOW_NAME", "workflow-test")
    require = sys.modules["catmaid.ingestion_lock"].require_project_ingestion
    with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
        assert lock.status == "locked"
        require.assert_called_once_with(17, INGESTION_ID, using="default")
        release.assert_not_called()
    lock.finish(store.state_dir.parent, run_started=True)
    assert lock.status == "released"
    assert acquire.call_count == 1
    assert release.call_count == 1  # Run released; outer workflow does not repeat it.


@pytest.mark.parametrize("status", ["pending", "acquiring", "locked"])
def test_workflow_releases_when_run_fails_before_any_batch_preparation(store, monkeypatch, status):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    if status != "pending":
        store.transition_project_lock(ProjectLockStatus.ACQUIRING)
    if status == "locked":
        store.transition_project_lock(ProjectLockStatus.LOCKED)
    store.fail_run("wrapper or startup failure")
    lock.finish(store.state_dir.parent, run_started=True)
    assert store.load()["project_lock"]["status"] == "released"
    release.assert_called_once_with(17, INGESTION_ID)


@pytest.mark.parametrize("status", ["prepared", "committed", "failed"])
def test_workflow_keeps_partial_or_uncertain_loading_locked(store, monkeypatch, status):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    store.transition_project_lock(ProjectLockStatus.ACQUIRING)
    store.transition_project_lock(ProjectLockStatus.LOCKED)
    store.select_existing_project(17)
    store.mark_batch_prepared(1)
    if status == "committed":
        store.mark_batch_committed(1)
    elif status == "failed":
        store.fail_run("commit outcome unknown")
    lock.finish(store.state_dir.parent, run_started=True)
    release.assert_not_called()
    assert lock.status == "locked"


def test_workflow_cannot_unlock_if_run_state_was_lost(store, monkeypatch, tmp_path):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    with pytest.raises(ProjectIngestionLockError, match="state is missing"):
        lock.finish(tmp_path / "missing", run_started=True)
    release.assert_not_called()


def test_workflow_cannot_unlock_another_ingestion_state(store, monkeypatch):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    lock.owner = "11111111-2222-4333-8444-555555555556"
    with pytest.raises(ProjectIngestionLockError, match="does not match"):
        lock.finish(store.state_dir.parent, run_started=False)
    release.assert_not_called()


def test_workflow_does_not_assume_rollback_after_uncertain_acquisition(store, monkeypatch):
    lock, acquire, release = workflow_lock(store, monkeypatch)
    lock.status = ProjectLockStatus.ACQUIRING
    with pytest.raises(ProjectIngestionLockError, match="not confirmed"):
        lock.finish(store.state_dir.parent, run_started=False)
    release.assert_not_called()
