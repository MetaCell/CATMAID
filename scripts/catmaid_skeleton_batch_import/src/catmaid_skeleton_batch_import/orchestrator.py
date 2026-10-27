"""The preflight/plan/run lifecycle for CATMAID skeleton ingestion."""

from __future__ import annotations

import sys
import time
from contextlib import ExitStack
from dataclasses import replace
import tempfile
import os
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import __version__
from .archive import ArchiveScan, scan_archive
from .artifacts import (
    artifact_descriptor,
    combine_manifests,
    manifest_rows,
    read_id_artifact,
    verify_node_map,
    write_id_artifact,
    write_manifest,
    write_node_map,
)
from .cache_progress import CacheProgressWriter
from .config import (
    load_operational_settings,
    load_request,
    load_settings,
)
from .domain import IngestionRequest, PlannedBatch, PlannedMember, SwcNode
from .errors import (
    ImporterError,
    ImmutableStateError,
    InvalidInputError,
    OperationalFailureError,
    VerificationError,
)
from .loader import PreparedBatch, ProjectContext, execute_batch_transaction, prepare_copy_rows
from .metrics import MetricsStore
from .planner import ImportPlan, plan_archive, read_plan, write_plan
from .batch_snapshot import BatchSnapshotReader, BatchSnapshotWriter
from .state import ArchiveMetadata, StateStore
from .statuses import DatabaseStatus, MetricsStatus, OperationStatus
from .stack_bounds import geometry_bounds, read_project_stack_geometry
from .util import atomic_write_json, load_json, sha256_file


PLAN_FILENAME = "plan.jsonl.gz"
DATABASE_FILENAME = "database.json"
PROJECT_CONTEXT_FILENAME = "project-context.json"
MANIFEST_FILENAME = "manifest.csv.gz"
VERIFICATION_FILENAME = "verification.json"
RESULT_FILENAME = "result.json"
STACK_GEOMETRY_FILENAME = "stack-geometry.json"


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _success_result(command: str, state_dir: Path, **values: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "ok": True,
        "command": command,
        "importer_version": __version__,
        "state_dir": str(state_dir.resolve()),
        **values,
    }


def _write_result(state_dir: Path, value: dict[str, object]) -> dict[str, object]:
    atomic_write_json(state_dir / RESULT_FILENAME, value)
    return value


def _archive_metadata(scan: ArchiveScan) -> ArchiveMetadata:
    return ArchiveMetadata(
        path=scan.archive_path,
        size_bytes=scan.archive_bytes,
        sha256=scan.archive_sha256,
    )


def _plan_result(store: StateStore, state: Mapping[str, Any]) -> dict[str, object]:
    plan = state["plan"]
    return _success_result(
        "plan",
        store.state_dir,
        status=state["status"],
        archive_sha256=state["archive"]["sha256"],
        plan_sha256=plan["digest"],
        batches=plan["batch_count"],
        skeletons=plan["skeleton_count"],
        nodes=plan["node_count"],
    )


def preflight_ingestion(request_path: Path, state_dir: Path) -> dict[str, object]:
    """Capture append metadata before planning, without modifying CATMAID."""
    store = StateStore(state_dir)
    with store.lock():
        if store.state_path.exists():
            raise InvalidInputError("Preflight must run before planning in a new state directory")
        request = load_request(request_path)
        geometry = _append_geometry(request)
        atomic_write_json(store.state_dir / STACK_GEOMETRY_FILENAME, geometry)
        return _write_result(store.state_dir, _success_result(
            "preflight", store.state_dir, required=True,
            project_id=request.target_project_id,
            stack_count=len(geometry["stacks"]),
        ))


def _planning_geometry(request: IngestionRequest, store: StateStore) -> dict[str, Any]:
    """Read only the local preflight snapshot; planning must not bootstrap Django."""
    path = store.state_dir / STACK_GEOMETRY_FILENAME
    if not path.is_file():
        raise InvalidInputError(
            "Append planning requires stack-geometry.json; run preflight with the "
            "same --request and --state-dir first"
        )
    try:
        geometry = load_json(path)
        if (not isinstance(geometry, dict) or geometry.get("schema_version") != 1
                or geometry.get("project_id") != request.target_project_id
                or not isinstance(geometry.get("database_target"), dict)
                or not geometry["database_target"].get("sha256")
                or not isinstance(geometry.get("stacks"), list)
                or not all(isinstance(stack, dict) for stack in geometry["stacks"])):
            raise ValueError("invalid metadata or different target project")
        geometry_bounds(geometry)
    except (OSError, ValueError, TypeError) as exc:
        raise InvalidInputError(f"Invalid preflight stack snapshot: {exc}") from exc
    return geometry


def plan_ingestion(request_path: Path, state_dir: Path) -> dict[str, object]:
    store = StateStore(state_dir)
    with store.lock():
        if store.state_path.exists() or (store.state_dir / PLAN_FILENAME).exists():
            raise InvalidInputError("This workspace already contains a planning attempt; use a new state directory")
        request = load_request(request_path)
        settings = load_settings()
        geometry = _planning_geometry(request, store)
        bounds = geometry_bounds(geometry)
        _progress(f"Scanning and validating local archive {request.archive_path}")
        started = time.monotonic()
        with tempfile.TemporaryDirectory(prefix=".batch-snapshot-", dir=store.state_dir) as staging:
            writer = BatchSnapshotWriter(Path(staging), settings.max_nodes_per_batch)
            try:
                scan = scan_archive(request, settings, bounds_nm=bounds, on_member=writer.add)
            finally:
                writer.close()
            plan = replace(plan_archive(scan, settings), batch_snapshots=tuple(writer.descriptors))
            for descriptor in plan.batch_snapshots:
                os.replace(Path(staging) / descriptor["path"], store.state_dir / descriptor["path"])
        plan_digest = write_plan(store.state_dir / PLAN_FILENAME, plan)
        artifacts = {"plan": artifact_descriptor(
            store.state_dir, store.state_dir / PLAN_FILENAME, count=plan.member_count)}
        batch_artifacts = {
            batch.index: {"batch_snapshot": artifact_descriptor(
                store.state_dir, store.state_dir / descriptor["path"], count=batch.node_count)}
            for batch, descriptor in zip(plan.batches, plan.batch_snapshots)
        }
        artifacts["stack_geometry"] = artifact_descriptor(
            store.state_dir, store.state_dir / STACK_GEOMETRY_FILENAME, count=len(geometry["stacks"]))
        metrics = MetricsStore(store.state_dir)
        metrics.initialize(planned_batches=plan.batch_count, planned_nodes=plan.total_nodes,
                           planned_skeletons=plan.member_count)
        metrics.record_phase("archive_scan_and_planning", duration_seconds=time.monotonic() - started,
                             node_count=plan.total_nodes, row_counts={
                                 "archive_bytes": scan.archive_bytes, "swc_members": plan.member_count,
                                 "planned_batches": plan.batch_count})
        # Publishing state is the final planning step: it references all required files.
        state = store.initialize_plan(
            request, _archive_metadata(scan), settings.plan_dict(), plan.batches,
            plan_digest=plan_digest, planner_version=plan.schema_version,
            artifacts=artifacts, batch_artifacts=batch_artifacts,
        )
        _progress(f"Planned {plan.member_count} skeletons and {plan.total_nodes} nodes in {plan.batch_count} batches")
        return _write_result(store.state_dir, _plan_result(store, state))


def _artifact_path(
    store: StateStore,
    state: Mapping[str, Any],
    name: str,
    *,
    batch_index: int | None = None,
) -> Path:
    if batch_index is None:
        descriptor = state["artifacts"].get(name)
    else:
        batch_state = next(
            batch for batch in state["batches"] if batch["index"] == batch_index
        )
        descriptor = batch_state["artifacts"].get(name)
    if descriptor is None:
        label = name if batch_index is None else f"batch {batch_index} {name}"
        raise ImmutableStateError(f"Missing state artifact: {label}")
    store.verify_artifact(name, batch_index=batch_index)
    return store.state_dir / descriptor["path"]


def _write_registered_json(
    store: StateStore,
    name: str,
    relative_path: str,
    value: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = dict(value)
    atomic_write_json(store.state_dir / relative_path, normalized)
    store.register_artifact(name, relative_path, count=1)
    return normalized


def _load_registered_json(
    store: StateStore,
    state: Mapping[str, Any],
    name: str,
) -> dict[str, Any]:
    value = load_json(_artifact_path(store, state, name))
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ImmutableStateError(f"Unsupported {name} artifact schema")
    return value


def _classified_runtime_error(
    exc: BaseException,
    *,
    context: str,
    preflight: bool = False,
) -> InvalidInputError | OperationalFailureError | None:
    """Map expected Django/database failures onto the public CLI contract."""

    if isinstance(exc, ModuleNotFoundError):
        return InvalidInputError(
            f"CATMAID application environment is incomplete during {context}: {exc}"
        )
    try:
        from django.core.exceptions import ImproperlyConfigured
        from django.db import DatabaseError, InterfaceError, OperationalError
    except ModuleNotFoundError:
        return InvalidInputError(
            f"Django is unavailable during {context}: {exc}"
        )
    if isinstance(exc, ImproperlyConfigured):
        return InvalidInputError(
            f"Django is not configured for {context}: {exc}"
        )
    if isinstance(exc, (OperationalError, InterfaceError)):
        return OperationalFailureError(f"Database operation failed during {context}: {exc}")
    if isinstance(exc, DatabaseError):
        cause = getattr(exc, "__cause__", None)
        sqlstate = getattr(cause, "pgcode", None) or getattr(
            getattr(cause, "diag", None), "sqlstate", None
        )
        if sqlstate == "42501":
            return InvalidInputError(
                f"Database role lacks a required privilege during {context}: {exc}"
            )
        if preflight:
            return InvalidInputError(
                f"CATMAID database preflight failed during {context}: {exc}"
            )
        return None
    return None


def _database_context(
    store: StateStore, state: Mapping[str, Any],
) -> tuple[Any, dict[str, Any]]:
    from .database import (
        DatabasePreflightError,
        bootstrap_django,
        preflight_database,
    )

    try:
        connection = bootstrap_django()
        current = {
            "schema_version": 1,
            **preflight_database(connection),
        }
    except DatabasePreflightError as exc:
        raise InvalidInputError(f"CATMAID database preflight failed: {exc}") from exc
    except Exception as exc:
        classified = _classified_runtime_error(
            exc, context="database preflight", preflight=True
        )
        if classified is not None:
            raise classified from exc
        raise

    # Bind this run to the database used for append planning before recording
    # its runtime fingerprint.
    geometry = _load_registered_json(store, state, "stack_geometry")
    if geometry["database_target"] != current["target"]:
        raise ImmutableStateError("Configured CATMAID database differs from append planning")
    _write_registered_json(store, "database", DATABASE_FILENAME, current)
    return connection, current


def _append_geometry(request: IngestionRequest, connection: Any = None) -> dict[str, Any]:
    try:
        return read_project_stack_geometry(request.target_project_id, connection)
    except ImporterError:
        raise
    except Exception as exc:
        classified = _classified_runtime_error(exc, context="target stack lookup", preflight=True)
        if classified is not None:
            raise classified from exc
        raise InvalidInputError(f"Could not read target stack geometry: {exc}") from exc


def _check_saved_geometry(
    store: StateStore,
    state: Mapping[str, Any],
    geometry: Mapping[str, Any],
) -> None:
    if _load_registered_json(store, state, "stack_geometry") != geometry:
        raise ImmutableStateError(
            "Target stack geometry or database identity changed since planning; use a new plan"
        )


def _ensure_existing_project(
    store: StateStore, project_id: int, metrics: MetricsStore,
) -> ProjectContext:
    from .project import ProjectSetupError, existing_project_context

    try:
        context = existing_project_context(project_id)
    except ProjectSetupError as exc:
        raise InvalidInputError(str(exc)) from exc
    except Exception as exc:
        classified = _classified_runtime_error(exc, context="existing project selection")
        if classified is not None:
            raise classified from exc
        raise
    _write_registered_json(
        store, "project_context", PROJECT_CONTEXT_FILENAME,
        {"schema_version": 1, **context},
    )
    store.select_existing_project(project_id)
    metrics.record_phase("existing_project_selection", duration_seconds=0.0)
    return ProjectContext.from_dict(context)


def _check_existing_fragment_conflicts(
    plan: ImportPlan,
    project: ProjectContext,
    connection: Any,
) -> None:
    """Reject every existing provenance collision before loading any batch."""
    from .fragments import fragment_annotation_name
    for batch in plan.batches:
        names = [fragment_annotation_name(member.fragment_id) for member in batch.members]
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, name FROM class_instance WHERE project_id = %s "
                "AND class_id = %s AND name = ANY(%s::text[])",
                [project.project_id, project.annotation_class_id, names],
            )
            conflicts = [name for _identifier, name in cursor.fetchall()]
        if conflicts:
            raise InvalidInputError(
                f"Source fragment identifiers already exist in project {project.project_id}: "
                + ", ".join(sorted(set(conflicts))[:10]))


def _batch_state(state: Mapping[str, Any], index: int) -> Mapping[str, Any]:
    for batch in state["batches"]:
        if int(batch["index"]) == index:
            return batch
    raise ImmutableStateError(f"State is missing batch {index}")


def _prepare_batch_artifacts(
    store: StateStore,
    batch: PlannedBatch,
    connection: Any,
) -> tuple[list[int], list[int]]:
    """Allocate once and retain the exact identities as audit evidence."""
    from .database import allocate_sequence_ids
    concept_ids = list(allocate_sequence_ids("concept_id_seq", batch.skeleton_count * 5, connection))
    location_ids = list(allocate_sequence_ids("location_id_seq", batch.node_count, connection))
    for name, values in (("concept_ids", concept_ids), ("location_ids", location_ids)):
        path = f"batches/{batch.index:06d}/{name}.txt.gz"
        write_id_artifact(store.state_dir / path, values)
        store.register_artifact(name, path, count=len(values), min_id=min(values),
                                max_id=max(values), compression="gzip", batch_index=batch.index)
    path = f"batches/{batch.index:06d}/manifest.csv.gz"
    rows = manifest_rows(batch, concept_ids)
    write_manifest(store.state_dir / path, rows)
    store.register_artifact("manifest", path, count=len(rows), compression="gzip", batch_index=batch.index)
    return concept_ids, location_ids


def _buffer_loader_timing(
    records: list[tuple[str, float, dict[str, int]]],
) -> Callable[[str, float, dict[str, int]], None]:
    def record(name: str, duration: float, counts: dict[str, int]) -> None:
        records.append((name, duration, dict(counts)))

    return record


def _flush_loader_timings(
    metrics: MetricsStore,
    batch_index: int,
    records: Sequence[tuple[str, float, dict[str, int]]],
) -> None:
    metrics.record_phases([
        {
            "name": f"batch_{batch_index:06d}_{name}",
            "duration_seconds": duration,
            "batch_index": batch_index,
            "batch_node_count": counts["nodes"],
            "batch_skeleton_count": counts["skeletons"],
            "node_count": counts["nodes"],
            "row_counts": {
                key: value
                for key, value in counts.items()
                if key.endswith("_rows")
            },
        }
        for name, duration, counts in records
    ])


def _record_completed_progress(
    metrics: MetricsStore,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Make committed filesystem state authoritative for progress metrics."""

    committed = [
        batch for batch in state["batches"] if batch["status"] == DatabaseStatus.COMMITTED
    ]
    return metrics.update_progress(
        completed_batches=len(committed),
        completed_nodes=sum(int(batch["node_count"]) for batch in committed),
        completed_skeletons=sum(
            int(batch["skeleton_count"]) for batch in committed
        ),
    )


def _verify_batch_node_map(store: StateStore, prepared: PreparedBatch) -> None:
    """Require the mapping saved before the batch's database transaction."""
    state = store.load()
    path = _artifact_path(store, state, "node_map", batch_index=prepared.batch.index)
    descriptor = _batch_state(state, prepared.batch.index)["artifacts"]["node_map"]
    if descriptor["count"] != prepared.batch.node_count:
        raise ImmutableStateError(f"Node mapping count changed for batch {prepared.batch.index}")
    verify_node_map(path, prepared.node_map_file)


def _save_batch_node_map(store: StateStore, prepared: PreparedBatch) -> None:
    batch = prepared.batch
    relative = Path("batches") / f"{batch.index:06d}" / "node-map.csv.gz"
    write_node_map(store.state_dir / relative, prepared.node_map_file)
    store.register_artifact("node_map", relative, count=batch.node_count, compression="gzip", batch_index=batch.index)


def _run_batch(
    store: StateStore,
    batch: PlannedBatch,
    request: IngestionRequest,
    timeouts: Mapping[str, int],
    project: ProjectContext,
    connection: Any,
    snapshots: BatchSnapshotReader,
    metrics: MetricsStore,
) -> None:
    concept_ids, location_ids = _prepare_batch_artifacts(store, batch, connection)
    store.mark_batch_prepared(batch.index)
    _progress(f"Preparing batch {batch.index}: {batch.skeleton_count} skeletons, {batch.node_count} nodes")
    started = time.monotonic()
    with prepare_copy_rows(batch, request, project, concept_ids, location_ids, snapshots.load) as prepared:
        metrics.record_phase(
            f"batch_{batch.index:06d}_row_generation", duration_seconds=time.monotonic() - started,
            batch_index=batch.index, batch_node_count=batch.node_count,
            batch_skeleton_count=batch.skeleton_count, node_count=batch.node_count)
        _save_batch_node_map(store, prepared)
        timings = []
        started = time.monotonic()
        try:
            counts = execute_batch_transaction(
                prepared, project, **timeouts, record_timing=_buffer_loader_timing(timings))
        except BaseException as exc:
            _flush_loader_timings(metrics, batch.index, timings)
            metrics.record_phase(
                f"batch_{batch.index:06d}_transaction", duration_seconds=time.monotonic() - started,
                status=OperationStatus.FAILED, batch_index=batch.index, batch_node_count=batch.node_count,
                batch_skeleton_count=batch.skeleton_count, error=str(exc))
            raise
    store.mark_batch_committed(batch.index)
    _flush_loader_timings(metrics, batch.index, timings)
    metrics.record_phase(
        f"batch_{batch.index:06d}_transaction", duration_seconds=time.monotonic() - started,
        batch_index=batch.index, batch_node_count=batch.node_count,
        batch_skeleton_count=batch.skeleton_count, node_count=batch.node_count, row_counts=counts)
    current = store.load()
    _record_completed_progress(metrics, current)
    _progress(f"Committed batch {batch.index}/{len(current['batches'])}")


def _combine_manifest(store: StateStore, state: Mapping[str, Any]) -> Path:
    paths = [
        _artifact_path(store, state, "manifest", batch_index=int(batch["index"]))
        for batch in state["batches"]
    ]
    destination = store.state_dir / MANIFEST_FILENAME
    count = combine_manifests(paths, destination)
    store.register_artifact(
        "manifest",
        MANIFEST_FILENAME,
        count=count,
        compression="gzip",
    )
    return destination


def _run_phase(
    store: StateStore,
    metrics: MetricsStore,
    phase: str,
    action: Callable[[], Mapping[str, Any]],
) -> dict[str, Any]:
    store.mark_phase_running(phase)
    started = time.monotonic()
    try:
        result = dict(action())
    except BaseException as exc:
        store.transition_phase(phase, OperationStatus.FAILED, reason=str(exc))
        metrics.record_phase(phase, duration_seconds=time.monotonic() - started,
                             status=OperationStatus.FAILED, error=str(exc))
        raise
    store.mark_phase_complete(phase)
    metrics.record_phase(phase, duration_seconds=time.monotonic() - started,
                         row_counts={key: value for key, value in result.items()
                                     if isinstance(value, int) and not isinstance(value, bool)})
    return result


def _verify_database_by_batch(
    store: StateStore,
    plan: ImportPlan,
    state: Mapping[str, Any],
    project: ProjectContext,
    connection: Any,
    load_nodes: Callable[[PlannedMember], Sequence[SwcNode]],
    request: IngestionRequest,
) -> dict[str, int]:
    """Verify exact rows with memory bounded by the planned batch size."""

    from .materialization import validate_batch
    from .loader import verify_exact_treenodes

    validated_batches = 0
    validated_skeletons = 0
    validated_nodes = 0
    imported_counts: dict[str, int] = {}
    for batch in plan.batches:
        _progress(
            f"Verifying batch {batch.index}/{plan.batch_count}: "
            f"{batch.skeleton_count} skeletons, {batch.node_count} nodes"
        )
        batch_state = _batch_state(state, batch.index)
        concept_ids = read_id_artifact(
            store.state_dir, batch_state["artifacts"]["concept_ids"],
            expected_count=batch.skeleton_count * 5,
        )
        location_ids = read_id_artifact(
            store.state_dir, batch_state["artifacts"]["location_ids"],
            expected_count=batch.node_count,
        )
        with prepare_copy_rows(
            batch, request, project, concept_ids, location_ids, load_nodes
        ) as prepared:
            counts = validate_batch(
                project_id=project.project_id,
                user_id=project.user_id,
                neuron_class_id=project.neuron_class_id,
                skeleton_class_id=project.skeleton_class_id,
                model_of_relation_id=project.model_of_relation_id,
                annotation_class_id=project.annotation_class_id,
                annotated_with_relation_id=project.annotated_with_relation_id,
                skeletons=[item.to_validation_dict() for item in prepared.expected_skeletons],
                location_ids=prepared.location_ids,
                connection=connection,
            )
            with connection.cursor() as cursor:
                verify_exact_treenodes(cursor, prepared)
            _verify_batch_node_map(store, prepared)
        for key, value in counts.items():
            imported_counts[key] = imported_counts.get(key, 0) + value
        if (
            counts["skeleton_count"] != batch.skeleton_count
            or counts["treenode_count"] != batch.node_count
        ):
            raise VerificationError(
                f"Batch {batch.index} verification counts do not match the plan",
                details={"expected": batch.to_summary_dict(), "actual": counts},
            )
        validated_batches += 1
        validated_skeletons += counts["skeleton_count"]
        validated_nodes += counts["treenode_count"]

    if (
        validated_batches != plan.batch_count
        or validated_skeletons != plan.member_count
        or validated_nodes != plan.total_nodes
    ):
        raise VerificationError(
            "Bounded database verification totals do not match the plan",
            details={
                "validated_batches": validated_batches,
                "validated_skeletons": validated_skeletons,
                "validated_nodes": validated_nodes,
            },
        )

    return imported_counts


def run_ingestion(state_dir: Path) -> dict[str, object]:
    """Execute this workspace once and surface the first failure immediately."""
    store = StateStore(state_dir)
    with store.lock():
        store.begin_run()
        try:
            with ExitStack() as resources:
                result = _execute_ingestion(store, resources)
                store.finish_run()
            return result
        except BaseException as exc:
            try:
                store.fail_run(str(exc) or "Interrupted")
                metrics = MetricsStore(store.state_dir)
                if metrics.path.exists():
                    metrics.set_status(MetricsStatus.FAILED)
            except Exception as reporting_error:
                _progress(f"Could not persist failure status: {reporting_error}")
            if isinstance(exc, ImporterError):
                raise
            classified = _classified_runtime_error(exc, context="ingestion")
            if classified is not None:
                raise classified from exc
            raise


def _execute_ingestion(store: StateStore, resources: ExitStack) -> dict[str, object]:
    state = store.verify_immutable()
    request = load_request(store.request_path)
    timeouts = load_operational_settings()
    plan_path = _artifact_path(store, state, "plan")
    plan = read_plan(plan_path)
    if sha256_file(plan_path) != state["plan"]["digest"]:
        raise ImmutableStateError("Plan artifact digest does not match state")
    if (
        plan.batch_count != state["plan"]["batch_count"]
        or plan.member_count != state["plan"]["skeleton_count"]
        or plan.total_nodes != state["plan"]["node_count"]
    ):
        raise ImmutableStateError("Plan artifact totals do not match state")

    metrics = MetricsStore(store.state_dir)
    metrics.initialize(
        planned_batches=plan.batch_count,
        planned_nodes=plan.total_nodes,
        planned_skeletons=plan.member_count,
    )
    metrics.record_operational_settings(**timeouts)
    metrics.set_status(MetricsStatus.IN_PROGRESS)
    connection, _preflight = _database_context(store, state)
    from .project_lock import project_ingestion_lock
    resources.enter_context(project_ingestion_lock(
        store, request.target_project_id, connection,
        lock_timeout_ms=timeouts["lock_timeout_ms"],
    ))
    geometry = _append_geometry(request, connection)
    _check_saved_geometry(store, store.load(), geometry)
    project = _ensure_existing_project(store, request.target_project_id, metrics)
    _check_existing_fragment_conflicts(plan, project, connection)
    from .cache_rebuild import GridCacheRebuild
    grid_cache = GridCacheRebuild(project.project_id, connection, **timeouts)
    _progress("Batch import: NeuroGlass project edits are locked through final verification")

    with BatchSnapshotReader(store.state_dir, plan) as snapshots:
        for batch in plan.batches:
            _run_batch(
                store,
                batch,
                request,
                timeouts,
                project,
                connection,
                snapshots,
                metrics,
            )

    state = store.load()
    _combine_manifest(store, state)

    from .database import analyze_tables

    _run_phase(
        store,
        metrics,
        "database_maintenance",
        lambda: analyze_tables(connection),
    )
    cache_progress = CacheProgressWriter(store.state_dir)
    cache_progress(0, 0)
    def build_cache() -> Mapping[str, Any]:
        _write_registered_json(store, "cache_plan", "cache-plan.json", grid_cache.plan())
        cache_plan = load_json(_artifact_path(store, store.load(), "cache_plan"))
        receipt = grid_cache.rebuild(
            log=_progress, plan=cache_plan, progress=cache_progress,
        )
        _write_registered_json(
            store, "cache_rebuild", "cache-rebuild.json", receipt,
        )
        return {"grid_count": len(receipt["grids"]),
                "cell_count": sum(grid["cell_count"] for grid in receipt["grids"])}

    _run_phase(
        store,
        metrics,
        "cache",
        build_cache,
    )

    state = store.load()

    def verify() -> Mapping[str, Any]:
        _check_saved_geometry(store, store.load(), _append_geometry(request, connection))
        from .project import ProjectSetupError, existing_project_context
        try:
            current_project = ProjectContext.from_dict(existing_project_context(project.project_id))
        except ProjectSetupError as exc:
            raise VerificationError(str(exc)) from exc
        if current_project != project:
            raise VerificationError("Existing project tracing context changed during import")
        project_result = {"project_id": project.project_id, "existing_project": True}
        with BatchSnapshotReader(store.state_dir, plan) as verification_snapshots:
            database_result = _verify_database_by_batch(
                store,
                plan,
                state,
                project,
                connection,
                verification_snapshots.load,
                request=request,
            )
        cache_result = grid_cache.verify(
            load_json(_artifact_path(store, store.load(), "cache_rebuild"))
        )
        result = {
            "project": project_result,
            "database": database_result,
            "cache": cache_result,
        }
        verification_value = {"schema_version": 1, **result}
        _write_registered_json(
            store,
            "verification",
            VERIFICATION_FILENAME,
            verification_value,
        )
        counts = {
            "skeletons": plan.member_count,
            "nodes": plan.total_nodes,
        }
        counts["cache_cells"] = result["cache"]["cell_count"]
        return counts

    _run_phase(
        store,
        metrics,
        "verification",
        verify,
    )

    state = store.load()
    metrics.set_status(MetricsStatus.COMPLETE)
    result = _success_result(
        "run",
        store.state_dir,
        status=state["status"],
        project_id=project.project_id,
        existing_project=True,
        batches=plan.batch_count,
        skeletons=plan.member_count,
        nodes=plan.total_nodes,
        manifest=str(_artifact_path(store, state, "manifest")),
        verification=str(_artifact_path(store, state, "verification")),
    )
    return _write_result(store.state_dir, result)
