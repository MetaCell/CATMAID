from __future__ import annotations

import json
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.artifacts import read_id_artifact, write_id_artifact
from catmaid_skeleton_batch_import.errors import (
    ImmutableStateError,
    InvalidInputError,
    OperationalFailureError,
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


def commit_project(store: StateStore) -> None:
    store.select_existing_project(101)


def commit_batch(store: StateStore, index: int) -> None:
    store.mark_batch_prepared(index)
    store.mark_batch_committed(index)


def test_state_files_are_canonical_and_planning_is_single_use(tmp_path: Path) -> None:
    store, arguments = make_store(tmp_path)
    assert store.load()["schema_version"] == SCHEMA_VERSION
    for path in (store.request_path, store.archive_path, store.state_path):
        assert path.read_bytes() == canonical_json_bytes(json.loads(path.read_bytes()))
    before = store.state_path.read_bytes()
    with pytest.raises(InvalidInputError, match="already has a plan"):
        store.initialize_plan(**arguments)
    assert store.state_path.read_bytes() == before


def test_posix_state_lock_is_exclusive_and_reentrant(tmp_path: Path) -> None:
    first = StateStore(tmp_path / "state")
    second = StateStore(tmp_path / "state")

    with first.lock():
        with first.lock():
            with pytest.raises(OperationalFailureError):
                with second.lock():
                    pass

    with second.lock():
        pass


def test_archive_is_verified_before_loading(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    archive.write_bytes(b"archive")
    store = StateStore(tmp_path / "state")
    request = {'schema_version': 1, 'source': {'archive_path': str(archive.resolve()), 'coordinate_unit': 'nm'}, 'target': {'project_id': 101}}
    store.initialize_plan(
        request,
        ArchiveMetadata(archive, archive.stat().st_size, sha256_file(archive)),
        {},
        [],
        plan_digest="a" * 64,
    )
    store.verify_immutable()

    archive.write_bytes(b"changed")
    with pytest.raises(ImmutableStateError, match="archive digest changed"):
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
    commit_project(store)
    commit_batch(store, 1)
    assert store.load()["status"] == "database_in_progress"
    commit_batch(store, 2)
    for phase in ("database_maintenance", "cache", "verification"):
        store.mark_phase_running(phase)
        store.mark_phase_complete(phase)
    assert store.finish_run()["status"] == "complete"
    with pytest.raises(InvalidInputError, match="terminal"):
        store.begin_run()


def test_whole_batch_order_and_phase_prerequisites_are_enforced(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path, batch_count=2)

    with pytest.raises(ImmutableStateError, match="target selection"):
        store.mark_batch_prepared(1)
    commit_project(store)
    with pytest.raises(ImmutableStateError, match="batch 1"):
        store.mark_batch_prepared(2)
    with pytest.raises(ImmutableStateError, match="every batch"):
        store.mark_phase_running("database_maintenance")


def test_selected_project_cannot_be_replaced(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    store.select_existing_project(101)
    with pytest.raises(ImmutableStateError, match="planned target"):
        store.select_existing_project(999)
    assert store.load()["project"]["project_id"] == 101


def test_failed_and_interrupted_attempts_cannot_be_resumed(tmp_path):
    store, _ = make_store(tmp_path)
    store.begin_run()
    commit_project(store)
    commit_batch(store, 1)
    store.mark_batch_prepared(2)
    with pytest.raises(InvalidInputError, match="already been run"):
        store.begin_run()
    state = store.fail_run("first error")
    assert state["status"] == "failed"
    assert [item["status"] for item in state["batches"]] == ["committed", "failed"]
    with pytest.raises(InvalidInputError, match="terminal"):
        store.begin_run()
