"""The single plan/run lifecycle for CATMAID skeleton ingestion."""

from __future__ import annotations

import json
import math
import sys
import time
import zipfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import __version__
from .archive import ArchiveScan, scan_archive
from .artifacts import (
    combine_manifests,
    manifest_rows,
    read_id_artifact,
    write_id_artifact,
    write_manifest,
)
from .config import (
    load_operational_settings,
    load_request,
    load_settings,
    settings_from_state,
)
from .domain import ImportSettings, IngestionRequest, PlannedBatch, PlannedMember, SwcNode
from .errors import (
    ImmutableStateError,
    InvalidInputError,
    OperatorAttentionError,
    RetryableError,
    VerificationError,
)
from .loader import ProjectContext, execute_batch_transaction, prepare_copy_rows
from .metrics import MetricsStore
from .planner import ImportPlan, plan_archive, read_plan, write_plan
from .state import ArchiveMetadata, StateStore
from .swc import parse_swc
from .util import atomic_write_json, canonical_json_bytes, load_json, sha256_file


PLAN_FILENAME = "plan.jsonl.gz"
DATABASE_FILENAME = "database.json"
PROJECT_RESERVATION_FILENAME = "project-reservation.json"
PROJECT_CONTEXT_FILENAME = "project-context.json"
MANIFEST_FILENAME = "manifest.csv.gz"
VERIFICATION_FILENAME = "verification.json"
RESULT_FILENAME = "result.json"


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
        mapping_path=scan.mapping_path,
        mapping_size_bytes=scan.mapping_bytes,
        mapping_sha256=scan.mapping_sha256,
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


def _same_normalized_request(request: IngestionRequest, store: StateStore) -> bool:
    return canonical_json_bytes(request.to_dict()) == canonical_json_bytes(
        load_json(store.request_path)
    )


def plan_ingestion(request_path: Path, state_dir: Path) -> dict[str, object]:
    store = StateStore(state_dir)
    with store.lock():
        request = load_request(request_path)
        settings = load_settings()

        if store.state_path.exists():
            state = store.verify_immutable()
            if not _same_normalized_request(request, store):
                raise ImmutableStateError(
                    "Existing state directory contains a different normalized request"
                )
            if state["settings"] != settings.plan_dict():
                raise ImmutableStateError(
                    "Existing state directory was planned with different deployment settings"
                )
            if "plan" in state.get("artifacts", {}):
                store.verify_artifact("plan")
                result = _plan_result(store, state)
                return _write_result(store.state_dir, result)

        _progress(f"Scanning and validating local archive {request.archive_path}")
        planning_started = time.monotonic()
        scan = scan_archive(request, settings)
        plan = plan_archive(scan, settings)
        plan_path = store.state_dir / PLAN_FILENAME
        plan_digest = write_plan(plan_path, plan)
        planning_duration = time.monotonic() - planning_started

        state = store.initialize_plan(
            request,
            _archive_metadata(scan),
            settings,
            plan.batches,
            plan_digest=plan_digest,
            planner_version=1,
        )
        store.register_artifact(
            "plan",
            PLAN_FILENAME,
            count=plan.member_count,
            compression="gzip",
            expected_sha256=plan_digest,
        )
        metrics = MetricsStore(store.state_dir)
        metrics.initialize(
            planned_batches=plan.batch_count,
            planned_nodes=plan.total_nodes,
            planned_skeletons=plan.member_count,
        )
        if not metrics.load()["phases"]:
            metrics.record_phase(
                "archive_scan_and_planning",
                duration_seconds=planning_duration,
                node_count=plan.total_nodes,
                row_counts={
                    "archive_bytes": scan.archive_bytes,
                    "swc_members": plan.member_count,
                    "planned_batches": plan.batch_count,
                },
            )
        state = store.load()
        _progress(
            f"Planned {plan.member_count} skeletons and {plan.total_nodes} nodes "
            f"in {plan.batch_count} batches"
        )
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
    *,
    batch_index: int | None = None,
) -> dict[str, Any]:
    state = store.load()
    if batch_index is None:
        existing = state.get("artifacts", {}).get(name)
    else:
        existing = _batch_state(state, batch_index).get("artifacts", {}).get(name)
    normalized = dict(value)
    if existing is not None:
        persisted = _load_registered_json(
            store, state, name, batch_index=batch_index
        )
        if persisted != normalized:
            raise ImmutableStateError(
                f"Registered {name} artifact cannot be replaced with different content"
            )
        return normalized

    path = store.state_dir / relative_path
    atomic_write_json(path, normalized)
    store.register_artifact(
        name,
        relative_path,
        count=1,
        batch_index=batch_index,
    )
    return normalized


def _load_registered_json(
    store: StateStore,
    state: Mapping[str, Any],
    name: str,
    *,
    batch_index: int | None = None,
) -> dict[str, Any]:
    value = load_json(_artifact_path(store, state, name, batch_index=batch_index))
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ImmutableStateError(f"Unsupported {name} artifact schema")
    return value


def _classified_runtime_error(
    exc: BaseException,
    *,
    context: str,
    preflight: bool = False,
) -> InvalidInputError | RetryableError | None:
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
        return RetryableError(f"Database operation failed during {context}: {exc}")
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


def _database_context(store: StateStore, state: Mapping[str, Any]) -> tuple[Any, dict[str, Any]]:
    from .database import (
        DatabasePreflightError,
        bootstrap_django,
        preflight_database,
    )

    try:
        connection = bootstrap_django()
        current = {"schema_version": 1, **preflight_database(connection)}
    except DatabasePreflightError as exc:
        raise InvalidInputError(f"CATMAID database preflight failed: {exc}") from exc
    except Exception as exc:
        classified = _classified_runtime_error(
            exc, context="database preflight", preflight=True
        )
        if classified is not None:
            raise classified from exc
        raise

    persisted = state.get("artifacts", {}).get("database")
    if persisted is None:
        _write_registered_json(
            store,
            "database",
            DATABASE_FILENAME,
            current,
        )
    else:
        expected = _load_registered_json(store, state, "database")
        if expected != current:
            raise ImmutableStateError(
                "Configured CATMAID database target or compatibility fingerprint changed",
                details={
                    "expected_target": expected.get("target"),
                    "actual_target": current.get("target"),
                },
            )
    return connection, current


def _project_title(state: Mapping[str, Any], request: IngestionRequest) -> str:
    digest = str(state["request"]["sha256"])
    stem = request.archive_path.stem.strip() or "skeletons"
    return f"Skeleton import {stem} {digest[:12]}"


def _project_reservation(
    store: StateStore,
    state: Mapping[str, Any],
    connection: Any,
    request: IngestionRequest,
) -> dict[str, Any]:
    if "project_reservation" in state.get("artifacts", {}):
        return _load_registered_json(store, state, "project_reservation")

    from .database import allocate_project_ids

    try:
        ids = allocate_project_ids(connection)
    except Exception as exc:
        classified = _classified_runtime_error(
            exc, context="project and stack ID reservation"
        )
        if classified is not None:
            raise classified from exc
        raise
    value = {
        "schema_version": 1,
        **ids,
        "title": _project_title(state, request),
        "request_sha256": state["request"]["sha256"],
        "archive_sha256": state["archive"]["sha256"],
    }
    return _write_registered_json(
        store,
        "project_reservation",
        PROJECT_RESERVATION_FILENAME,
        value,
    )


def _ensure_project(
    store: StateStore,
    state: Mapping[str, Any],
    request: IngestionRequest,
    settings: ImportSettings,
    connection: Any,
    metrics: MetricsStore,
) -> ProjectContext:
    project_status = state["project"]["status"]
    reservation = _project_reservation(store, state, connection, request)

    if project_status == "planned":
        store.mark_project_prepared(
            int(reservation["project_id"]), int(reservation["stack_id"])
        )
        project_status = "prepared"
    elif project_status == "retryable":
        store.transition_project(
            "prepared",
            project_id=int(reservation["project_id"]),
            stack_id=int(reservation["stack_id"]),
        )
        project_status = "prepared"

    if project_status == "committed":
        context = _load_registered_json(store, store.load(), "project_context")
        return ProjectContext.from_dict(context)
    if project_status != "prepared":
        raise ImmutableStateError(f"Unsupported project state: {project_status}")

    from .project import create_hidden_project

    attempted = False
    started = time.monotonic()

    def before_commit() -> None:
        nonlocal attempted
        store.mark_project_commit_attempted()
        attempted = True

    _progress(f"Creating hidden CATMAID project {reservation['project_id']}")
    try:
        created = create_hidden_project(
            project_id=int(reservation["project_id"]),
            stack_id=int(reservation["stack_id"]),
            project_stack_id=int(reservation["project_stack_id"]),
            title=str(reservation["title"]),
            dimension=request.dimension,
            resolution_nm=request.resolution_nm,
            cache_cell_size_nm=settings.cache_cell_size_nm,
            comment=(
                "Hidden skeleton batch import. "
                f"Archive SHA-256: {state['archive']['sha256']}"
            ),
            before_commit=before_commit,
        )
    except BaseException as exc:
        duration = time.monotonic() - started
        if attempted:
            store.mark_operator_attention(
                "Project/stack COMMIT outcome is uncertain",
                entity="project",
                details={"error": str(exc)},
            )
            metrics.record_phase(
                "project_creation",
                duration_seconds=duration,
                status="operator_attention",
                error=str(exc),
            )
            metrics.set_status("operator_attention")
            raise OperatorAttentionError(
                "Project/stack COMMIT outcome is uncertain"
            ) from exc
        store.mark_project_retryable(str(exc), confirmed_rollback=True)
        metrics.record_phase(
            "project_creation",
            duration_seconds=duration,
            status="retryable",
            error=str(exc),
        )
        if isinstance(exc, (InvalidInputError, ImmutableStateError, RetryableError)):
            raise
        from .project import ProjectSetupError

        if isinstance(exc, ProjectSetupError):
            raise InvalidInputError(f"Project setup failed: {exc}") from exc
        if isinstance(exc, KeyboardInterrupt):
            raise RetryableError("Project creation was interrupted and rolled back") from exc
        classified = _classified_runtime_error(exc, context="project creation")
        if classified is not None:
            raise classified from exc
        raise

    context_value = {"schema_version": 1, **created}
    _write_registered_json(
        store,
        "project_context",
        PROJECT_CONTEXT_FILENAME,
        context_value,
    )
    store.mark_project_committed()
    metrics.record_phase(
        "project_creation",
        duration_seconds=time.monotonic() - started,
        row_counts={"project": 1, "stack": 1, "project_stack": 1},
    )
    return ProjectContext.from_dict(context_value)


def _batch_state(state: Mapping[str, Any], index: int) -> Mapping[str, Any]:
    for batch in state["batches"]:
        if int(batch["index"]) == index:
            return batch
    raise ImmutableStateError(f"State is missing batch {index}")


def _prepare_batch_artifacts(
    store: StateStore,
    state: Mapping[str, Any],
    batch: PlannedBatch,
    connection: Any,
) -> tuple[list[int], list[int]]:
    batch_dir = f"batches/{batch.index:06d}"
    batch_state = _batch_state(state, batch.index)
    artifacts = batch_state.get("artifacts", {})

    if "concept_ids" in artifacts:
        concept_ids = read_id_artifact(
            store.state_dir,
            artifacts["concept_ids"],
            expected_count=batch.skeleton_count * 3,
        )
    else:
        from .database import allocate_sequence_ids

        try:
            concept_ids = list(
                allocate_sequence_ids(
                    "concept_id_seq", batch.skeleton_count * 3, connection
                )
            )
        except Exception as exc:
            classified = _classified_runtime_error(
                exc, context=f"batch {batch.index} concept ID reservation"
            )
            if classified is not None:
                raise classified from exc
            raise
        concept_relative = f"{batch_dir}/concept_ids.txt.gz"
        write_id_artifact(store.state_dir / concept_relative, concept_ids)
        store.register_artifact(
            "concept_ids",
            concept_relative,
            count=len(concept_ids),
            min_id=min(concept_ids),
            max_id=max(concept_ids),
            compression="gzip",
            batch_index=batch.index,
        )

    state = store.load()
    batch_state = _batch_state(state, batch.index)
    artifacts = batch_state.get("artifacts", {})
    if "location_ids" in artifacts:
        location_ids = read_id_artifact(
            store.state_dir,
            artifacts["location_ids"],
            expected_count=batch.node_count,
        )
    else:
        from .database import allocate_sequence_ids

        try:
            location_ids = list(
                allocate_sequence_ids(
                    "location_id_seq", batch.node_count, connection
                )
            )
        except Exception as exc:
            classified = _classified_runtime_error(
                exc, context=f"batch {batch.index} location ID reservation"
            )
            if classified is not None:
                raise classified from exc
            raise
        location_relative = f"{batch_dir}/location_ids.txt.gz"
        write_id_artifact(store.state_dir / location_relative, location_ids)
        store.register_artifact(
            "location_ids",
            location_relative,
            count=len(location_ids),
            min_id=min(location_ids),
            max_id=max(location_ids),
            compression="gzip",
            batch_index=batch.index,
        )

    state = store.load()
    batch_state = _batch_state(state, batch.index)
    if "manifest" not in batch_state.get("artifacts", {}):
        manifest_relative = f"{batch_dir}/manifest.csv.gz"
        rows = manifest_rows(batch, concept_ids)
        write_manifest(store.state_dir / manifest_relative, rows)
        store.register_artifact(
            "manifest",
            manifest_relative,
            count=len(rows),
            compression="gzip",
            batch_index=batch.index,
        )
    return concept_ids, location_ids


class _RunArchive:
    def __init__(
        self,
        request: IngestionRequest,
        settings: ImportSettings,
    ) -> None:
        self.request = request
        self.settings = settings
        self.archive: zipfile.ZipFile | None = None

    def __enter__(self) -> "_RunArchive":
        try:
            self.archive = zipfile.ZipFile(self.request.archive_path, "r")
        except (OSError, zipfile.BadZipFile) as exc:
            raise ImmutableStateError(f"Could not reopen immutable archive: {exc}") from exc
        return self

    def __exit__(self, *_args: object) -> None:
        if self.archive is not None:
            self.archive.close()

    def load(self, member: PlannedMember) -> Sequence[SwcNode]:
        assert self.archive is not None
        try:
            info = self.archive.getinfo(member.member_path)
        except KeyError as exc:
            raise ImmutableStateError(
                f"Planned archive member is missing: {member.member_path}"
            ) from exc
        if info.file_size != member.uncompressed_bytes or info.CRC != member.crc32:
            raise ImmutableStateError(
                f"Planned archive member metadata changed: {member.member_path}"
            )
        try:
            with self.archive.open(info, "r") as source:
                parsed = parse_swc(
                    source,
                    f"{self.request.archive_path.name}:{member.member_path}",
                    max_bytes=self.settings.max_swc_bytes,
                    max_nodes=self.settings.max_nodes_per_batch,
                    scale_nm=self.request.scale_nm,
                )
        except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
            raise ImmutableStateError(
                f"Could not read planned archive member {member.member_path}: {exc}"
            ) from exc
        if parsed.summary.node_count != member.node_count:
            raise ImmutableStateError(
                f"Planned node count changed for {member.member_path}"
            )
        expected_cable = parsed.summary.cable_length_source_units * self.request.scale_nm
        if not math.isclose(
            expected_cable,
            member.cable_length_nm,
            rel_tol=1e-12,
            abs_tol=max(1e-6, abs(member.cable_length_nm) * 1e-12),
        ):
            raise ImmutableStateError(
                f"Planned cable length changed for {member.member_path}"
            )
        return parsed.nodes


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
    for name, duration, counts in records:
        metrics.record_phase(
            f"batch_{batch_index:06d}_{name}",
            duration_seconds=duration,
            batch_index=batch_index,
            batch_node_count=counts["nodes"],
            batch_skeleton_count=counts["skeletons"],
            node_count=counts["nodes"],
            row_counts={
                key: value
                for key, value in counts.items()
                if key.endswith("_rows")
            },
        )



def _reconcile_completed_progress(
    metrics: MetricsStore,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Make committed filesystem state authoritative for progress metrics."""

    committed = [
        batch for batch in state["batches"] if batch["status"] == "committed"
    ]
    return metrics.reconcile_progress(
        completed_batches=len(committed),
        completed_nodes=sum(int(batch["node_count"]) for batch in committed),
        completed_skeletons=sum(
            int(batch["skeleton_count"]) for batch in committed
        ),
    )


def _run_batch(
    store: StateStore,
    batch: PlannedBatch,
    request: IngestionRequest,
    settings: ImportSettings,
    project: ProjectContext,
    connection: Any,
    archive: _RunArchive,
    metrics: MetricsStore,
) -> None:
    state = store.load()
    batch_status = str(_batch_state(state, batch.index)["status"])
    if batch_status == "committed":
        return
    concept_ids, location_ids = _prepare_batch_artifacts(
        store, state, batch, connection
    )
    if batch_status == "planned":
        store.mark_batch_prepared(batch.index)
    elif batch_status == "retryable":
        store.transition_batch(batch.index, "prepared")
    elif batch_status != "prepared":
        raise ImmutableStateError(
            f"Unsupported state for batch {batch.index}: {batch_status}"
        )

    _progress(
        f"Preparing batch {batch.index}: {batch.skeleton_count} skeletons, "
        f"{batch.node_count} nodes"
    )
    generation_started = time.monotonic()
    try:
        prepared_context = prepare_copy_rows(
            batch,
            request,
            project,
            concept_ids,
            location_ids,
            archive.load,
        )
    except BaseException as exc:
        store.mark_batch_retryable(
            batch.index, f"Row preparation failed: {exc}", confirmed_rollback=True
        )
        metrics.record_phase(
            f"batch_{batch.index:06d}_row_generation",
            duration_seconds=time.monotonic() - generation_started,
            status="retryable",
            batch_index=batch.index,
            batch_node_count=batch.node_count,
            batch_skeleton_count=batch.skeleton_count,
            error=str(exc),
        )
        raise
    metrics.record_phase(
        f"batch_{batch.index:06d}_row_generation",
        duration_seconds=time.monotonic() - generation_started,
        batch_index=batch.index,
        batch_node_count=batch.node_count,
        batch_skeleton_count=batch.skeleton_count,
        node_count=batch.node_count,
        row_counts={
            "class_instance_rows": batch.skeleton_count * 2,
            "relationship_rows": batch.skeleton_count,
            "treenode_rows": batch.node_count,
        },
    )

    attempted = False
    transaction_started = time.monotonic()
    loader_timings: list[tuple[str, float, dict[str, int]]] = []

    def before_commit() -> None:
        nonlocal attempted
        store.mark_batch_commit_attempted(batch.index)
        attempted = True

    try:
        with prepared_context as prepared:
            counts = execute_batch_transaction(
                prepared,
                project,
                settings,
                before_commit,
                _buffer_loader_timing(loader_timings),
            )
    except BaseException as exc:
        duration = time.monotonic() - transaction_started
        if attempted:
            store.mark_operator_attention(
                f"Batch {batch.index} COMMIT outcome is uncertain",
                entity="batch",
                batch_index=batch.index,
                details={"error": str(exc)},
            )
            _flush_loader_timings(metrics, batch.index, loader_timings)
            metrics.record_phase(
                f"batch_{batch.index:06d}_transaction",
                duration_seconds=duration,
                status="operator_attention",
                batch_index=batch.index,
                batch_node_count=batch.node_count,
                batch_skeleton_count=batch.skeleton_count,
                error=str(exc),
            )
            metrics.set_status("operator_attention")
            raise OperatorAttentionError(
                f"Batch {batch.index} COMMIT outcome is uncertain"
            ) from exc
        store.mark_batch_retryable(
            batch.index, str(exc), confirmed_rollback=True
        )
        _flush_loader_timings(metrics, batch.index, loader_timings)
        metrics.record_phase(
            f"batch_{batch.index:06d}_transaction",
            duration_seconds=duration,
            status="retryable",
            batch_index=batch.index,
            batch_node_count=batch.node_count,
            batch_skeleton_count=batch.skeleton_count,
            error=str(exc),
        )
        if isinstance(exc, VerificationError):
            raise RetryableError(
                f"Batch {batch.index} validation failed and rolled back: {exc}",
                details=exc.details,
            ) from exc
        if isinstance(exc, (ImmutableStateError, InvalidInputError)):
            raise
        if isinstance(exc, RetryableError):
            raise
        if isinstance(exc, KeyboardInterrupt):
            raise RetryableError(
                f"Batch {batch.index} was interrupted and rolled back"
            ) from exc
        classified = _classified_runtime_error(
            exc, context=f"batch {batch.index} transaction"
        )
        if classified is not None:
            raise classified from exc
        raise

    store.mark_batch_committed(batch.index)
    _flush_loader_timings(metrics, batch.index, loader_timings)
    metrics.record_phase(
        f"batch_{batch.index:06d}_transaction",
        duration_seconds=time.monotonic() - transaction_started,
        batch_index=batch.index,
        batch_node_count=batch.node_count,
        batch_skeleton_count=batch.skeleton_count,
        node_count=batch.node_count,
        row_counts=counts,
    )
    current_state = store.load()
    _reconcile_completed_progress(metrics, current_state)
    _progress(f"Committed batch {batch.index}/{len(current_state['batches'])}")


def _combine_manifest(store: StateStore, state: Mapping[str, Any]) -> Path:
    if "manifest" in state.get("artifacts", {}):
        return _artifact_path(store, state, "manifest")
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
    *,
    rerun_complete: bool = False,
) -> dict[str, Any]:
    state = store.load()
    if state["phases"][phase]["status"] == "complete" and not rerun_complete:
        return {}
    store.mark_phase_running(phase)
    started = time.monotonic()
    try:
        result = dict(action())
    except VerificationError:
        duration = time.monotonic() - started
        store.transition_phase(phase, "failed", reason="Verification failed")
        metrics.record_phase(
            phase,
            duration_seconds=duration,
            status="failed",
            error="Verification failed",
        )
        raise
    except BaseException as exc:
        duration = time.monotonic() - started
        store.mark_phase_retryable(phase, str(exc))
        metrics.record_phase(
            phase,
            duration_seconds=duration,
            status="retryable",
            error=str(exc),
        )
        if isinstance(exc, (InvalidInputError, ImmutableStateError, RetryableError)):
            raise
        if isinstance(exc, KeyboardInterrupt):
            raise RetryableError(
                f"{phase} was interrupted and is safe to retry"
            ) from exc
        classified = _classified_runtime_error(exc, context=phase)
        if classified is not None:
            raise classified from exc
        raise
    store.mark_phase_complete(phase)
    metrics.record_phase(
        phase,
        duration_seconds=time.monotonic() - started,
        row_counts={
            key: value
            for key, value in result.items()
            if isinstance(value, int) and not isinstance(value, bool)
            and (
                key.endswith("_count")
                or key in {"count", "nodes", "skeletons", "cache_cells"}
            )
        },
    )
    return result


def _expected_batch_data(
    store: StateStore,
    batch: PlannedBatch,
    state: Mapping[str, Any],
) -> tuple[list[dict[str, object]], list[int]]:
    expected_skeletons: list[dict[str, object]] = []
    batch_state = _batch_state(state, batch.index)
    concept_ids = read_id_artifact(
        store.state_dir,
        batch_state["artifacts"]["concept_ids"],
        expected_count=batch.skeleton_count * 3,
    )
    location_ids = read_id_artifact(
        store.state_dir,
        batch_state["artifacts"]["location_ids"],
        expected_count=batch.node_count,
    )
    for member_index, member in enumerate(batch.members):
        neuron_id, skeleton_id, link_id = concept_ids[
            member_index * 3 : member_index * 3 + 3
        ]
        expected_skeletons.append(
            {
                "neuron_id": neuron_id,
                "skeleton_id": skeleton_id,
                "link_id": link_id,
                "name": member.display_name,
                "node_count": member.node_count,
                "cable_length_nm": member.cable_length_nm,
            }
        )
    return expected_skeletons, location_ids


def _verify_database_by_batch(
    store: StateStore,
    plan: ImportPlan,
    state: Mapping[str, Any],
    project: ProjectContext,
    connection: Any,
) -> dict[str, int]:
    """Verify exact rows with memory bounded by the planned batch size."""

    from .materialization import validate_batch
    from .verification import verify_project_aggregate_counts

    validated_batches = 0
    validated_skeletons = 0
    validated_nodes = 0
    for batch in plan.batches:
        _progress(
            f"Verifying batch {batch.index}/{plan.batch_count}: "
            f"{batch.skeleton_count} skeletons, {batch.node_count} nodes"
        )
        expected_skeletons, location_ids = _expected_batch_data(
            store, batch, state
        )
        counts = validate_batch(
            project_id=project.project_id,
            user_id=project.user_id,
            neuron_class_id=project.neuron_class_id,
            skeleton_class_id=project.skeleton_class_id,
            model_of_relation_id=project.model_of_relation_id,
            skeletons=expected_skeletons,
            location_ids=location_ids,
            connection=connection,
        )
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

    return verify_project_aggregate_counts(
        project_id=project.project_id,
        neuron_class_id=project.neuron_class_id,
        skeleton_class_id=project.skeleton_class_id,
        model_of_relation_id=project.model_of_relation_id,
        expected_skeleton_count=plan.member_count,
        expected_node_count=plan.total_nodes,
        connection=connection,
    )


def run_ingestion(
    state_dir: Path,
    *,
    rebuild_cache: bool = False,
) -> dict[str, object]:
    store = StateStore(state_dir)
    with store.lock():
        state = store.block_on_uncertain_commit()
        state = store.verify_immutable()
        request = load_request(store.request_path)
        operational_settings = load_operational_settings()
        settings = settings_from_state(state["settings"], operational_settings)
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
        recovered_phase = metrics.recover_active_phase()
        if recovered_phase is not None:
            _progress(
                f"Recovered interrupted metrics phase "
                f"{recovered_phase['name']}"
            )
        metrics.record_operational_settings(
            statement_timeout_ms=settings.statement_timeout_ms,
            lock_timeout_ms=settings.lock_timeout_ms,
        )
        _reconcile_completed_progress(metrics, state)
        metrics.set_status("in_progress")
        connection, _preflight = _database_context(store, state)
        state = store.load()
        project = _ensure_project(
            store, state, request, settings, connection, metrics
        )

        with _RunArchive(request, settings) as archive:
            for batch in plan.batches:
                _run_batch(
                    store,
                    batch,
                    request,
                    settings,
                    project,
                    connection,
                    archive,
                    metrics,
                )

        state = store.load()
        _combine_manifest(store, state)

        from .cache import build_grid_cache
        from .database import analyze_tables
        from .verification import verify_cache, verify_project_stack

        _run_phase(
            store,
            metrics,
            "database_maintenance",
            lambda: analyze_tables(connection),
        )
        if rebuild_cache:
            _progress("Invalidating cache and verification checkpoints")
            store.prepare_cache_rebuild()
        _run_phase(
            store,
            metrics,
            "cache",
            lambda: build_grid_cache(
                project_id=project.project_id,
                dimension=request.dimension,
                resolution_nm=request.resolution_nm,
                cache_cell_size_nm=settings.cache_cell_size_nm,
                lod_levels=settings.lod_levels,
                lod_bucket_size=settings.lod_bucket_size,
                lod_strategy=settings.lod_strategy,
                connection=connection,
                log=_progress,
            ),
        )

        state = store.load()

        def verify() -> Mapping[str, Any]:
            project_result = verify_project_stack(
                project_id=project.project_id,
                stack_id=project.stack_id,
                project_stack_id=project.project_stack_id,
                system_user_id=project.user_id,
                title=_project_title(state, request),
                dimension=request.dimension,
                resolution_nm=request.resolution_nm,
                cache_cell_size_nm=settings.cache_cell_size_nm,
                connection=connection,
            )
            database_result = _verify_database_by_batch(
                store, plan, state, project, connection
            )
            cache_result = verify_cache(
                project_id=project.project_id,
                dimension=request.dimension,
                resolution_nm=request.resolution_nm,
                expected_node_count=plan.total_nodes,
                cache_cell_size_nm=settings.cache_cell_size_nm,
                lod_levels=settings.lod_levels,
                lod_bucket_size=settings.lod_bucket_size,
                lod_strategy=settings.lod_strategy,
                connection=connection,
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
            return {
                "skeletons": plan.member_count,
                "nodes": plan.total_nodes,
                "cache_cells": result["cache"]["cell_count"],
            }

        _run_phase(
            store,
            metrics,
            "verification",
            verify,
            rerun_complete=True,
        )

        state = store.load()
        metrics.set_status("complete")
        result = _success_result(
            "run",
            store.state_dir,
            status=state["status"],
            project_id=project.project_id,
            stack_id=project.stack_id,
            batches=plan.batch_count,
            skeletons=plan.member_count,
            nodes=plan.total_nodes,
            manifest=str(_artifact_path(store, state, "manifest")),
            verification=str(_artifact_path(store, state, "verification")),
        )
        return _write_result(store.state_dir, result)
