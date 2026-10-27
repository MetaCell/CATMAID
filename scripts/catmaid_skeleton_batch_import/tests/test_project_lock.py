from types import SimpleNamespace
import json
import sys
from unittest.mock import Mock

import pytest

from catmaid_skeleton_batch_import.project_lock import project_ingestion_lock
from catmaid_skeleton_batch_import.errors import ProjectIngestionLockError


@pytest.mark.parametrize("batch_status, released", [("planned", True), ("prepared", False),
                                                   ("committed", False), ("failed", False)])
def test_failure_retains_only_potentially_written_projects(tmp_path, monkeypatch, batch_status, released):
    acquire, release = Mock(), Mock()
    monkeypatch.setitem(sys.modules, "catmaid.ingestion_lock", SimpleNamespace(
        acquire_project_ingestion=acquire, release_project_ingestion=release))
    store = SimpleNamespace(state_dir=tmp_path, load=lambda: {"batches": [{"status": batch_status}]})
    with pytest.raises(RuntimeError, match="injected"):
        with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            assert json.loads((tmp_path / "project-ingestion-lock.json").read_text())["status"] == "locked"
            raise RuntimeError("injected")
    receipt = json.loads((tmp_path / "project-ingestion-lock.json").read_text())
    assert receipt["status"] == ("released" if released else "locked")
    assert release.call_count == int(released)
    if released:
        assert release.call_args.args == (17, acquire.call_args.args[1])


def test_success_releases_the_recorded_owner(tmp_path, monkeypatch):
    acquire, release = Mock(), Mock()
    monkeypatch.setitem(sys.modules, "catmaid.ingestion_lock", SimpleNamespace(
        acquire_project_ingestion=acquire, release_project_ingestion=release))
    store = SimpleNamespace(state_dir=tmp_path)
    with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
        pass
    assert json.loads((tmp_path / "project-ingestion-lock.json").read_text())["status"] == "released"
    assert release.call_args.args == (17, acquire.call_args.args[1])


def test_uncertain_acquisition_and_release_require_inspection(tmp_path, monkeypatch):
    acquire, release = Mock(side_effect=RuntimeError("commit disconnected")), Mock()
    monkeypatch.setitem(sys.modules, "catmaid.ingestion_lock", SimpleNamespace(
        acquire_project_ingestion=acquire, release_project_ingestion=release))
    store = SimpleNamespace(state_dir=tmp_path)
    with pytest.raises(ProjectIngestionLockError):
        with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            pytest.fail("No import after uncertain lock acquisition")
    assert json.loads((tmp_path / "project-ingestion-lock.json").read_text())["status"] == "acquiring"
    release.assert_not_called()
    acquire.side_effect = None
    release.side_effect = RuntimeError("unlock disconnected")
    with pytest.raises(ProjectIngestionLockError):
        with project_ingestion_lock(store, 17, SimpleNamespace(alias="default"), lock_timeout_ms=100):
            pass
    assert json.loads((tmp_path / "project-ingestion-lock.json").read_text())["status"] == "locked"
