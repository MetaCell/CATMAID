from __future__ import annotations

import json
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.artifacts import read_id_artifact, write_id_artifact
from catmaid_skeleton_batch_import.errors import (
    ImmutableStateError,
    InvalidInputError,
)
from catmaid_skeleton_batch_import.state import (
    ArchiveMetadata,
    SCHEMA_VERSION,
    StateStore,
)
from catmaid_skeleton_batch_import.util import canonical_json_bytes, sha256_file


def make_store(tmp_path: Path, *, batch_count: int = 2) -> tuple[StateStore, dict]:
    archive = tmp_path / "source.zip"
    archive.write_bytes(b"stable archive bytes")
    request = {'schema_version': 1, 'source': {'archive_path': str(archive.resolve()), 'coordinate_unit': 'um'}, 'target': {'project_id': 101}}
    settings = {'max_nodes_per_batch': 10, 'max_archive_bytes': 1000000, 'max_swc_bytes': 10000, 'max_total_nodes': None}
    batches = [
        {
            "index": index,
            "first_member": f"cell-{index}.swc",
            "last_member": f"cell-{index}.swc",
            "skeleton_count": 1,
            "node_count": 4 + index,
        }
        for index in range(1, batch_count + 1)
    ]
    store = StateStore(tmp_path / "state")
    arguments = {
        "request": request,
        "archive": ArchiveMetadata(archive, archive.stat().st_size, sha256_file(archive)),
        "settings": settings,
        "batches": batches,
        "plan_digest": "a" * 64,
    }
    store.initialize_plan(**arguments)
    return store, arguments


def select_project(store: StateStore) -> None:
    store.select_existing_project(101)


def commit_batch(store: StateStore, index: int) -> None:
    store.mark_batch_prepared(index)
    store.mark_batch_committed(index)


def test_state_files_are_canonical_and_planning_is_single_use(tmp_path: Path) -> None:
    store, arguments = make_store(tmp_path)
    state = store.load()
    assert state["schema_version"] == SCHEMA_VERSION == 1
    assert "status" not in state
    assert "project" not in state
    assert state["selected_project_id"] is None
    assert store.load()["plan"]["planner_version"] == 1
    for path in (store.request_path, store.archive_path, store.state_path):
        assert path.read_bytes() == canonical_json_bytes(json.loads(path.read_bytes()))
    before = store.state_path.read_bytes()
    with pytest.raises(InvalidInputError, match="already has a plan"):
        store.initialize_plan(**arguments)
    assert store.state_path.read_bytes() == before


@pytest.mark.parametrize("source_change", ["removed", "replaced"])
def test_archive_audit_metadata_is_retained_without_requiring_the_zip(
    tmp_path: Path, source_change: str,
) -> None:
    store, arguments = make_store(tmp_path)
    archive = arguments["archive"].path
    metadata = store.archive_path.read_bytes()
    if source_change == "removed":
        archive.unlink()
    else:
        archive.write_bytes(b"replacement ZIP is not an execution input")

    store.verify_immutable()

    assert store.archive_path.read_bytes() == metadata
    assert json.loads(metadata)["sha256"] == arguments["archive"].sha256

    store.archive_path.write_bytes(metadata + b"tampered")
    with pytest.raises(ImmutableStateError, match="Archive metadata digest mismatch"):
        store.verify_immutable()


def test_artifacts_are_relative_compact_and_digest_verified(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    artifact = store.state_dir / "batches/000001/concept_ids.txt.gz"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"compressed-id-data")

    metadata = store.register_artifact(
        "concept_ids",
        "batches/000001/concept_ids.txt.gz",
        count=3,
        min_id=10,
        max_id=12,
        batch_index=1,
    )
    assert metadata.relative_path == "batches/000001/concept_ids.txt.gz"
    assert metadata.sha256 == sha256_file(artifact)
    persisted = store.load()["batches"][0]["artifacts"]["concept_ids"]
    assert persisted == {
        "schema_version": 1,
        "path": "batches/000001/concept_ids.txt.gz",
        "count": 3,
        "bytes": len(b"compressed-id-data"),
        "sha256": sha256_file(artifact),
        "min": 10,
        "max": 12,
        "compression": "gzip",
    }
    store.verify_artifact("concept_ids", batch_index=1)

    artifact.write_bytes(b"tampered-id-data!!")
    with pytest.raises(ImmutableStateError, match="digest changed"):
        store.verify_immutable()

    artifact.write_bytes(b"short")
    with pytest.raises(ImmutableStateError, match="byte size changed"):
        store.verify_immutable()

    outside = tmp_path / "outside.gz"
    outside.write_bytes(b"data")
    with pytest.raises(InvalidInputError, match="relative"):
        store.register_artifact("outside", outside, count=1)


def test_persisted_artifact_descriptor_is_accepted_by_artifact_reader(
    tmp_path: Path,
) -> None:
    store, _ = make_store(tmp_path)
    artifact = store.state_dir / "batches/000001/concept_ids.txt.gz"
    artifact.parent.mkdir(parents=True)
    write_id_artifact(artifact, [10, 11, 12])

    store.register_artifact(
        "concept_ids",
        "batches/000001/concept_ids.txt.gz",
        count=3,
        min_id=10,
        max_id=12,
        batch_index=1,
    )
    descriptor = store.load()["batches"][0]["artifacts"]["concept_ids"]

    assert read_id_artifact(store.state_dir, descriptor, expected_count=3) == [
        10,
        11,
        12,
    ]


def test_project_batch_and_phase_lifecycle(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    store.begin_run()
    select_project(store)
    commit_batch(store, 1)
    assert store.load()["run"]["status"] == "running"
    commit_batch(store, 2)
    for phase in ("database_maintenance", "cache", "verification"):
        store.mark_phase_running(phase)
        store.mark_phase_complete(phase)
    assert store.finish_run()["run"]["status"] == "complete"
    with pytest.raises(InvalidInputError, match="terminal"):
        store.begin_run()


def test_whole_batch_order_and_phase_prerequisites_are_enforced(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path, batch_count=2)

    with pytest.raises(ImmutableStateError, match="target selection"):
        store.mark_batch_prepared(1)
    select_project(store)
    with pytest.raises(ImmutableStateError, match="batch 1"):
        store.mark_batch_prepared(2)
    with pytest.raises(ImmutableStateError, match="every batch"):
        store.mark_phase_running("database_maintenance")


def test_selected_project_cannot_be_replaced(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    store.select_existing_project(101)
    with pytest.raises(ImmutableStateError, match="planned target"):
        store.select_existing_project(999)
    assert store.load()["selected_project_id"] == 101


def test_failed_and_interrupted_attempts_cannot_be_resumed(tmp_path):
    store, _ = make_store(tmp_path)
    store.begin_run()
    select_project(store)
    commit_batch(store, 1)
    store.mark_batch_prepared(2)
    with pytest.raises(InvalidInputError, match="already been run"):
        store.begin_run()
    state = store.fail_run("first error")
    assert state["run"]["status"] == "failed"
    assert [item["status"] for item in state["batches"]] == ["committed", "failed"]
    with pytest.raises(InvalidInputError, match="terminal"):
        store.begin_run()


def test_standalone_ingestion_uuid_is_generated_once_and_retained(tmp_path, monkeypatch):
    from uuid import UUID

    monkeypatch.delenv("SKELETON_INGESTION_ID", raising=False)
    store, _ = make_store(tmp_path)
    state = store.load()
    identity = state["ingestion_id"]
    assert str(UUID(identity)) == identity
    assert state["project_lock"] == {"project_id": 101, "status": "pending"}
    reopened = StateStore(store.state_dir)
    assert reopened.begin_run()["ingestion_id"] == identity


def test_supplied_ingestion_uuid_is_retained_when_run_environment_is_absent(tmp_path, monkeypatch):
    identity = "11111111-2222-4333-8444-555555555555"
    monkeypatch.setenv("SKELETON_INGESTION_ID", identity)
    store, _ = make_store(tmp_path)
    assert store.load()["ingestion_id"] == identity
    monkeypatch.delenv("SKELETON_INGESTION_ID")
    assert StateStore(store.state_dir).begin_run()["ingestion_id"] == identity


@pytest.mark.parametrize("identity", ["", "not-a-uuid", "11111111-2222-4333-8444-AAAAAAAAAAAA"])
def test_planning_rejects_invalid_ingestion_uuid_without_publishing_state(tmp_path, monkeypatch, identity):
    monkeypatch.setenv("SKELETON_INGESTION_ID", identity)
    with pytest.raises(InvalidInputError, match="canonical lowercase UUID"):
        make_store(tmp_path)
    assert not (tmp_path / "state/state.json").exists()


def test_run_rejects_a_different_ingestion_uuid_before_acquisition(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path)
    monkeypatch.setenv("SKELETON_INGESTION_ID", "11111111-2222-4333-8444-555555555555")
    with pytest.raises(ImmutableStateError, match="Ingestion UUID"):
        store.begin_run()
    state = store.load()
    assert state["run"]["status"] == "pending"
    assert state["project_lock"]["status"] == "pending"


def test_started_workspace_rejects_another_process_before_database_access(tmp_path):
    import subprocess
    import sys

    store, _ = make_store(tmp_path)
    store.begin_run()
    before = store.state_path.read_bytes()
    marker = tmp_path / "database-accessed"
    launcher = tmp_path / "second_importer.py"
    launcher.write_text("""
from pathlib import Path
import sys
from catmaid_skeleton_batch_import import orchestrator
from catmaid_skeleton_batch_import.errors import InvalidInputError

def unexpected_database(*_args, **_kwargs):
    Path(sys.argv[2]).touch()
    raise AssertionError("A started workspace must not reach the database")

orchestrator._database_context = unexpected_database
try:
    orchestrator.run_ingestion(Path(sys.argv[1]))
except InvalidInputError as error:
    print(str(error), file=sys.stderr)
    raise SystemExit(error.exit_code)
""")
    result = subprocess.run(
        [sys.executable, str(launcher), str(store.state_dir), str(marker)],
        capture_output=True, timeout=10,
    )
    assert result.returncode == 2, result.stderr.decode()
    assert b"This workspace has already been run" in result.stderr
    assert not marker.exists()
    assert store.state_path.read_bytes() == before


def test_failed_batch_retains_selection_committed_prefix_and_lock(tmp_path):
    from catmaid_skeleton_batch_import.statuses import ProjectLockStatus

    store, _ = make_store(tmp_path, batch_count=3)
    store.begin_run()
    store.transition_project_lock(ProjectLockStatus.ACQUIRING)
    store.transition_project_lock(ProjectLockStatus.LOCKED)
    select_project(store)
    commit_batch(store, 1)
    store.mark_batch_prepared(2)

    state = store.fail_run("Connection lost during COMMIT")

    assert "status" not in state
    assert "project" not in state
    assert state["selected_project_id"] == 101
    assert state["run"]["status"] == "failed"
    assert state["project_lock"]["status"] == "locked"
    assert [batch["status"] for batch in state["batches"]] == [
        "committed", "failed", "planned",
    ]
    assert all(phase["status"] == "pending" for phase in state["phases"].values())
