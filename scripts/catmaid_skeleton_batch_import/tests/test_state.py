from __future__ import annotations

import json
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.artifacts import read_id_artifact, write_id_artifact
from catmaid_skeleton_batch_import.errors import (
    ImmutableStateError,
    InvalidInputError,
    OperatorAttentionError,
    RetryableError,
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
    request = {
        "schema_version": 1,
        "source": {
            "archive_path": str(archive.resolve()),
            "coordinate_unit": "um",
        },
        "stack": {
            "dimension": [10, 20, 30],
            "resolution_nm": [100.0, 100.0, 200.0],
        },
    }
    settings = {
        "max_nodes_per_batch": 10,
        "max_archive_bytes": 1_000_000,
        "max_swc_bytes": 10_000,
        "max_swc_count": 100,
        "max_total_nodes": None,
        "cache_cell_size_nm": 1_500_000,
        "lod_levels": 7,
        "lod_bucket_size": 500,
        "lod_strategy": "quadratic",
    }
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
        "archive": ArchiveMetadata.from_paths(archive),
        "settings": settings,
        "batches": batches,
    }
    store.initialize_plan(**arguments)
    return store, arguments


def commit_project(store: StateStore) -> None:
    store.mark_project_prepared(project_id=101, stack_id=202)
    store.mark_project_commit_attempted()
    store.mark_project_committed()


def commit_batch(store: StateStore, index: int) -> None:
    store.mark_batch_prepared(index)
    store.mark_batch_commit_attempted(index)
    store.mark_batch_committed(index)


def test_state_files_are_canonical_and_plan_is_idempotent(tmp_path: Path) -> None:
    store, arguments = make_store(tmp_path)

    assert store.load()["schema_version"] == SCHEMA_VERSION
    for path in (store.request_path, store.archive_path, store.state_path):
        parsed = json.loads(path.read_bytes())
        assert path.read_bytes() == canonical_json_bytes(parsed)

    store.mark_project_prepared(project_id=101, stack_id=202)
    before = store.state_path.read_bytes()
    resumed = store.initialize_plan(**arguments)
    assert store.state_path.read_bytes() == before
    assert resumed["project"] == {
        "status": "prepared",
        "project_id": 101,
        "stack_id": 202,
    }


def test_idempotent_plan_rejects_any_immutable_difference(tmp_path: Path) -> None:
    store, arguments = make_store(tmp_path)
    changed_request = json.loads(json.dumps(arguments["request"]))
    changed_request["source"]["coordinate_unit"] = "nm"

    with pytest.raises(ImmutableStateError):
        store.initialize_plan(**{**arguments, "request": changed_request})

    changed_batches = list(arguments["batches"])
    changed_batches[0] = {**changed_batches[0], "node_count": 99}
    with pytest.raises(ImmutableStateError):
        store.initialize_plan(**{**arguments, "batches": changed_batches})


def test_posix_state_lock_is_exclusive_and_reentrant(tmp_path: Path) -> None:
    first = StateStore(tmp_path / "state")
    second = StateStore(tmp_path / "state")

    with first.lock():
        with first.lock():
            with pytest.raises(RetryableError):
                with second.lock():
                    pass

    with second.lock():
        pass


def test_archive_and_mapping_are_verified_on_resume(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    mapping = tmp_path / "mapping.csv"
    archive.write_bytes(b"archive")
    mapping.write_text("member_path,external_skeleton_id\na.swc,a\n")
    store = StateStore(tmp_path / "state")
    request = {
        "schema_version": 1,
        "source": {
            "archive_path": str(archive.resolve()),
            "coordinate_unit": "nm",
            "external_skeleton_id_map_path": str(mapping.resolve()),
        },
        "stack": {"dimension": [1, 1, 1], "resolution_nm": [1, 1, 1]},
    }
    store.initialize_plan(
        request,
        ArchiveMetadata.from_paths(archive, mapping),
        {},
        [],
    )
    store.verify_immutable()

    mapping.write_text("member_path,external_skeleton_id\na.swc,changed\n")
    with pytest.raises(ImmutableStateError, match="mapping"):
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
    store, _ = make_store(tmp_path, batch_count=2)
    commit_project(store)

    store.mark_batch_prepared(1)
    store.mark_batch_retryable(1, "known COPY rollback")
    store.mark_batch_prepared(1)
    store.mark_batch_commit_attempted(1)
    state = store.mark_batch_committed(1)
    assert state["status"] == "database_in_progress"

    commit_batch(store, 2)
    assert store.load()["status"] == "database_complete"

    store.mark_phase_running("database_maintenance")
    store.mark_phase_complete("database_maintenance")
    store.mark_phase_running("cache")
    store.mark_phase_retryable("cache", "worker interruption")
    store.mark_phase_running("cache")
    store.mark_phase_complete("cache")
    store.mark_phase_running("verification")
    state = store.mark_phase_complete("verification")
    assert state["status"] == "ready_for_publication"


def test_whole_batch_order_and_phase_prerequisites_are_enforced(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path, batch_count=2)

    with pytest.raises(ImmutableStateError, match="project creation"):
        store.mark_batch_prepared(1)
    commit_project(store)
    with pytest.raises(ImmutableStateError, match="batch 1"):
        store.mark_batch_prepared(2)
    with pytest.raises(ImmutableStateError, match="every batch"):
        store.mark_phase_running("database_maintenance")


def test_phase_retry_clears_stale_failure_reason(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path, batch_count=1)
    commit_project(store)
    commit_batch(store, 1)
    store.mark_phase_running("database_maintenance")
    store.mark_phase_complete("database_maintenance")
    store.mark_phase_running("cache")
    store.mark_phase_complete("cache")
    store.mark_phase_running("verification")
    store.transition_phase("verification", "failed", reason="Verification failed")

    state = store.mark_phase_running("verification")
    assert "reason" not in state["phases"]["verification"]
    state = store.mark_phase_complete("verification")
    assert state["phases"]["verification"] == {"status": "complete"}


def test_explicit_cache_rebuild_preserves_database_and_invalidates_verification(
    tmp_path: Path,
) -> None:
    store, _ = make_store(tmp_path, batch_count=1)
    commit_project(store)
    commit_batch(store, 1)
    for phase in ("database_maintenance", "cache", "verification"):
        store.mark_phase_running(phase)
        store.mark_phase_complete(phase)
    verification_path = store.state_dir / "verification.json"
    verification_path.write_text('{"ok":true}\n')
    store.register_artifact("verification", "verification.json", count=1)

    state = store.prepare_cache_rebuild()

    assert state["status"] == "maintenance_complete"
    assert state["project"]["status"] == "committed"
    assert state["batches"][0]["status"] == "committed"
    assert state["phases"]["database_maintenance"]["status"] == "complete"
    assert state["phases"]["cache"] == {"status": "pending"}
    assert state["phases"]["verification"] == {"status": "pending"}
    assert "verification" not in state["artifacts"]


def test_cache_rebuild_requires_completed_database(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path, batch_count=1)
    commit_project(store)

    with pytest.raises(ImmutableStateError, match="every database batch"):
        store.prepare_cache_rebuild()


def test_uncertain_commit_becomes_operator_attention_and_blocks_mutation(
    tmp_path: Path,
) -> None:
    store, _ = make_store(tmp_path)
    commit_project(store)
    store.mark_batch_prepared(1)
    store.mark_batch_commit_attempted(1)

    with pytest.raises(OperatorAttentionError, match="not checkpointed"):
        store.block_on_uncertain_commit()

    state = store.load()
    assert state["status"] == "operator_attention"
    assert state["batches"][0]["status"] == "operator_attention"
    with pytest.raises(OperatorAttentionError, match="normal mutations are blocked"):
        store.mark_batch_committed(1)
    with pytest.raises(OperatorAttentionError):
        store.register_artifact("manifest", "manifest.csv.gz", count=0)


def test_known_commit_failure_can_be_marked_retryable_only_when_confirmed(
    tmp_path: Path,
) -> None:
    store, _ = make_store(tmp_path, batch_count=1)
    commit_project(store)
    store.mark_batch_prepared(1)
    store.mark_batch_commit_attempted(1)

    with pytest.raises(OperatorAttentionError, match="COMMIT outcome is uncertain"):
        store.mark_batch_retryable(1, "lost connection")

    state = store.mark_batch_retryable(
        1, "PostgreSQL rejected COMMIT and confirmed rollback", confirmed_rollback=True
    )
    assert state["batches"][0]["status"] == "retryable"


def test_reserved_project_ids_cannot_change(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    store.mark_project_prepared(project_id=101, stack_id=202)
    with pytest.raises(ImmutableStateError, match="project_id"):
        store.transition_project("prepared", project_id=999)
