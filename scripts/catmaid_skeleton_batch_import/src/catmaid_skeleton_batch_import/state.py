"""Atomic single-use import state and retained audit artifacts.

The state reports confirmed progress. Failed or interrupted runs are never
resumed; committed batches remain available for diagnosis.
"""

from __future__ import annotations

import contextlib
import copy
import errno
import fcntl
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import __version__
from .domain import ImportSettings, IngestionRequest, PlannedBatch
from .errors import (
    ImmutableStateError,
    InvalidInputError,
    OperationalFailureError,
)
from .statuses import DatabaseStatus, ImportStatus, OperationStatus
from .util import (
    atomic_write_json,
    canonical_json_bytes,
    load_json,
    sha256_bytes,
    sha256_file,
)


SCHEMA_VERSION = 3
METADATA_SCHEMA_VERSION = 1
REQUEST_FILENAME = "request.json"
ARCHIVE_FILENAME = "archive.json"
STATE_FILENAME = "state.json"
LOCK_FILENAME = ".state.lock"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PHASE_ORDER = ("database_maintenance", "cache", "verification")


def _canonical_value(value: Any) -> Any:
    """Return a detached JSON value and reject non-canonical JSON inputs."""
    try:
        return json.loads(canonical_json_bytes(value))
    except (TypeError, ValueError) as exc:
        raise InvalidInputError(f"Value is not valid canonical JSON: {exc}") from exc


def _validate_sha256(value: str, label: str) -> str:
    value = str(value).lower()
    if not _SHA256_RE.fullmatch(value):
        raise InvalidInputError(f"{label} must be a 64-character SHA-256 digest")
    return value


def _non_negative_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise InvalidInputError(f"{label} must be a non-negative integer")
    if isinstance(value, float) and not value.is_integer():
        raise InvalidInputError(f"{label} must be a non-negative integer")
    try:
        result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise InvalidInputError(f"{label} must be a non-negative integer") from exc
    if result < 0:
        raise InvalidInputError(f"{label} must be a non-negative integer")
    return result


@dataclass(frozen=True)
class ArchiveMetadata:
    """Identity of the local immutable ingestion package."""

    path: Path
    size_bytes: int
    sha256: str


    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": METADATA_SCHEMA_VERSION,
            "path": str(self.path.resolve()),
            "size_bytes": _non_negative_int(self.size_bytes, "archive size"),
            "sha256": _validate_sha256(self.sha256, "archive digest"),
        }
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArchiveMetadata":
        if int(value.get("schema_version", -1)) != METADATA_SCHEMA_VERSION:
            raise ImmutableStateError("Unsupported archive metadata schema")
        return cls(
            path=Path(str(value["path"])),
            size_bytes=_non_negative_int(value["size_bytes"], "archive size"),
            sha256=_validate_sha256(str(value["sha256"]), "archive digest"),
        )


@dataclass(frozen=True)
class ArtifactMetadata:
    """A compact reference to a potentially large state-directory artifact."""

    relative_path: str
    count: int
    size_bytes: int
    sha256: str
    min_id: int | None = None
    max_id: int | None = None
    compression: str | None = None

    def to_dict(self) -> dict[str, Any]:
        count = _non_negative_int(self.count, "artifact count")
        if (self.min_id is None) != (self.max_id is None):
            raise InvalidInputError("Artifact min_id and max_id must be supplied together")
        if self.min_id is not None and self.max_id is not None:
            if int(self.min_id) > int(self.max_id):
                raise InvalidInputError("Artifact min_id cannot be greater than max_id")
            if count == 0:
                raise InvalidInputError("An empty artifact cannot have min/max IDs")
        if self.compression not in (None, "gzip"):
            raise InvalidInputError("Artifact compression must be null or 'gzip'")

        result: dict[str, Any] = {
            "schema_version": METADATA_SCHEMA_VERSION,
            "path": self.relative_path,
            "count": count,
            "bytes": _non_negative_int(self.size_bytes, "artifact byte size"),
            "sha256": _validate_sha256(self.sha256, "artifact digest"),
        }
        if self.min_id is not None:
            result["min"] = int(self.min_id)
            result["max"] = int(self.max_id)  # type: ignore[arg-type]
        if self.compression is not None:
            result["compression"] = self.compression
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactMetadata":
        if int(value.get("schema_version", -1)) != METADATA_SCHEMA_VERSION:
            raise ImmutableStateError("Unsupported artifact metadata schema")
        return cls(
            relative_path=str(value["path"]),
            count=_non_negative_int(value["count"], "artifact count"),
            size_bytes=_non_negative_int(value["bytes"], "artifact byte size"),
            sha256=_validate_sha256(str(value["sha256"]), "artifact digest"),
            min_id=(int(value["min"]) if value.get("min") is not None else None),
            max_id=(int(value["max"]) if value.get("max") is not None else None),
            compression=(
                str(value["compression"])
                if value.get("compression") is not None
                else None
            ),
        )


class StateDirectoryLock:
    """An advisory POSIX writer lock retained for the context lifetime."""

    def __init__(self, state_dir: Path, *, blocking: bool = False):
        self.state_dir = state_dir.resolve()
        self.blocking = blocking
        self._descriptor: int | None = None

    def __enter__(self) -> "StateDirectoryLock":
        self.state_dir.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            self.state_dir / LOCK_FILENAME,
            os.O_RDWR | os.O_CREAT,
            0o600,
        )
        operation = fcntl.LOCK_EX
        if not self.blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(descriptor, operation)
        except OSError as exc:
            os.close(descriptor)
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise OperationalFailureError(
                    f"State directory is already locked: {self.state_dir}",
                    details={"state_dir": str(self.state_dir)},
                ) from exc
            raise
        self._descriptor = descriptor
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._descriptor is None:
            return
        try:
            fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        finally:
            os.close(self._descriptor)
            self._descriptor = None


class StateStore:
    """Own canonical request, archive, progress, and artifact metadata files."""

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir.resolve()
        self.request_path = self.state_dir / REQUEST_FILENAME
        self.archive_path = self.state_dir / ARCHIVE_FILENAME
        self.state_path = self.state_dir / STATE_FILENAME
        self._lock_depth = 0
        self._lock: StateDirectoryLock | None = None

    @contextlib.contextmanager
    def lock(self, *, blocking: bool = False) -> Iterator["StateStore"]:
        """Hold the single-writer lock, re-entrantly for this store instance."""
        if self._lock_depth:
            self._lock_depth += 1
            try:
                yield self
            finally:
                self._lock_depth -= 1
            return

        lock = StateDirectoryLock(self.state_dir, blocking=blocking)
        with lock:
            self._lock = lock
            self._lock_depth = 1
            try:
                yield self
            finally:
                self._lock_depth = 0
                self._lock = None

    @contextlib.contextmanager
    def _mutation_lock(self) -> Iterator[None]:
        if self._lock_depth:
            yield
        else:
            with self.lock():
                yield

    def initialize_plan(
        self,
        request: IngestionRequest | Mapping[str, Any],
        archive: ArchiveMetadata | Mapping[str, Any],
        settings: ImportSettings | Mapping[str, Any],
        batches: Sequence[PlannedBatch | Mapping[str, Any]],
        *,
        plan_digest: str,
        planner_version: int = 4,
        artifacts: Mapping[str, Mapping[str, Any]] | None = None,
        batch_artifacts: Mapping[int, Mapping[str, Mapping[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        """Publish one complete plan and its artifact references atomically."""
        request_value = self._normalize_request(request)
        archive_value = self._normalize_archive(archive)
        archive_identity = ArchiveMetadata.from_dict(archive_value)
        self._verify_source_file(
            archive_identity.path,
            archive_identity.size_bytes,
            archive_identity.sha256,
            "archive",
        )
        settings_value = self._normalize_settings(settings)
        batch_plan = self._normalize_batches(batches)
        planner_version = _non_negative_int(planner_version, "planner version")

        plan_digest = _validate_sha256(plan_digest, "plan digest")

        request_digest = sha256_bytes(canonical_json_bytes(request_value))
        archive_metadata_digest = sha256_bytes(canonical_json_bytes(archive_value))
        plan_value = {
            "planner_version": planner_version,
            "digest": plan_digest,
            "batches": batch_plan,
            "batch_count": len(batch_plan),
            "skeleton_count": sum(batch["skeleton_count"] for batch in batch_plan),
            "node_count": sum(batch["node_count"] for batch in batch_plan),
        }
        immutable = {
            "request": {"path": REQUEST_FILENAME, "sha256": request_digest},
            "archive": {
                "metadata_path": ARCHIVE_FILENAME,
                "metadata_sha256": archive_metadata_digest,
                "sha256": archive_value["sha256"],
                "size_bytes": archive_value["size_bytes"],
            },
            "settings": settings_value,
            "plan": plan_value,
        }

        with self._mutation_lock():
            if self.state_path.exists():
                raise InvalidInputError("This workspace already has a plan; use a new state directory")

            self._install_initial_metadata(
                self.request_path, request_value, request_digest, "normalized request"
            )
            self._install_initial_metadata(
                self.archive_path,
                archive_value,
                archive_metadata_digest,
                "archive metadata",
            )
            state = {
                "schema_version": SCHEMA_VERSION,
                "importer_version": __version__,
                "status": ImportStatus.PLANNED,
                **immutable,
                "project": {
                    "status": DatabaseStatus.PLANNED,
                    "project_id": None,
                },
                "batches": [
                    {
                        **copy.deepcopy(batch),
                        "status": DatabaseStatus.PLANNED,
                        "artifacts": copy.deepcopy((batch_artifacts or {}).get(batch["index"], {})),
                    }
                    for batch in batch_plan
                ],
                "phases": {
                    phase: {"status": OperationStatus.PENDING} for phase in _PHASE_ORDER
                },
                "artifacts": copy.deepcopy(dict(artifacts or {})),
                "run": {"status": OperationStatus.PENDING},
            }
            for name, artifact in state["artifacts"].items():
                self._verify_artifact_value(name, artifact)
            for batch in state["batches"]:
                for name, artifact in batch["artifacts"].items():
                    self._verify_artifact_value(name, artifact)
            self._persist(state)
            return copy.deepcopy(state)

    def _persist(self, state):
        atomic_write_json(self.state_path, state)

    def load(self) -> dict[str, Any]:
        if not self.state_path.is_file():
            raise InvalidInputError(
                f"State directory has not been planned: {self.state_dir}"
            )
        try:
            value = load_json(self.state_path)
        except (OSError, json.JSONDecodeError) as exc:
            raise ImmutableStateError(f"Cannot read state file: {exc}") from exc
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
            raise ImmutableStateError("Unsupported or malformed state schema")
        return value

    def verify_immutable(self) -> dict[str, Any]:
        """Verify retained metadata and artifacts; execution uses batch snapshots."""
        state = self.load()
        self._verify_metadata_file(
            self.request_path, state["request"]["sha256"], "normalized request"
        )
        self._verify_metadata_file(
            self.archive_path,
            state["archive"]["metadata_sha256"],
            "archive metadata",
        )

        for name, artifact in state.get("artifacts", {}).items():
            self._verify_artifact_value(name, artifact)
        for batch in state.get("batches", []):
            for name, artifact in batch.get("artifacts", {}).items():
                self._verify_artifact_value(
                    f"batch {batch['index']} artifact {name}", artifact
                )
        return state

    def register_artifact(
        self,
        name: str,
        relative_path: str | Path,
        *,
        count: int,
        min_id: int | None = None,
        max_id: int | None = None,
        compression: str | None = None,
        batch_index: int | None = None,
    ) -> ArtifactMetadata:
        """Atomically reference a completed artifact from state.

        Artifact bytes must already have been atomically installed.  Large ID
        lists normally use deterministic gzip and set ``compression='gzip'``.
        """
        self._assert_mutations_allowed()
        if not name or not isinstance(name, str):
            raise InvalidInputError("Artifact name must be a non-empty string")
        relative, absolute = self._artifact_path(relative_path)
        if not absolute.is_file():
            raise InvalidInputError(f"Artifact does not exist: {absolute}")
        if compression is None and relative.suffix == ".gz":
            compression = "gzip"
        metadata = ArtifactMetadata(
            relative_path=relative.as_posix(),
            count=count,
            size_bytes=absolute.stat().st_size,
            sha256=sha256_file(absolute),
            min_id=min_id,
            max_id=max_id,
            compression=compression,
        )
        metadata_value = metadata.to_dict()

        def mutate(state: dict[str, Any]) -> None:
            target = state["artifacts"]
            if batch_index is not None:
                target = self._find_batch(state, batch_index)["artifacts"]
            existing = target.get(name)
            if existing is not None and existing != metadata_value:
                raise ImmutableStateError(
                    f"Artifact metadata already exists with different content: {name}"
                )
            target[name] = metadata_value

        self._mutate(mutate)
        return metadata

    def verify_artifact(
        self, name: str, *, batch_index: int | None = None
    ) -> ArtifactMetadata:
        state = self.load()
        target = state.get("artifacts", {})
        label = name
        if batch_index is not None:
            target = self._find_batch(state, batch_index).get("artifacts", {})
            label = f"batch {batch_index} artifact {name}"
        if name not in target:
            raise ImmutableStateError(f"Unknown artifact: {label}")
        return self._verify_artifact_value(label, target[name])


    def select_existing_project(self, project_id: int) -> dict[str, Any]:
        """Checkpoint a read-only project selection, without a COMMIT window."""
        request = load_json(self.request_path)
        if request.get("target", {}).get("project_id") != project_id:
            raise ImmutableStateError("Existing project does not match the planned target")

        def mutate(state: dict[str, Any]) -> None:
            project = state["project"]
            if project["status"] != DatabaseStatus.PLANNED:
                raise ImmutableStateError("Cannot replace an already selected target project")
            self._set_immutable_id(project, "project_id", project_id)
            project["mode"] = "existing"
            project["status"] = DatabaseStatus.COMMITTED
            self._derive_status(state)

        return self._mutate(mutate)

    def transition_batch(
        self,
        batch_index: int,
        status: DatabaseStatus,
        *,
        reason: str | None = None,
    ) -> dict[str, Any]:
        try:
            status = DatabaseStatus(status)
        except (ValueError, TypeError) as exc:
            raise InvalidInputError(f"Unknown batch state: {status}") from exc

        def mutate(state: dict[str, Any]) -> None:
            batch = self._find_batch(state, batch_index)
            current = batch["status"]
            self._validate_database_transition(
                f"batch {batch_index}",
                current,
                status,
            )
            if status == DatabaseStatus.PREPARED:
                if state["project"]["status"] != DatabaseStatus.COMMITTED:
                    raise ImmutableStateError(
                        "Cannot prepare a batch before target selection is recorded"
                    )
                for prior in state["batches"]:
                    if prior["index"] == batch_index:
                        break
                    if prior["status"] != DatabaseStatus.COMMITTED:
                        raise ImmutableStateError(
                            f"Cannot prepare batch {batch_index} before batch "
                            f"{prior['index']} is committed"
                        )
            batch["status"] = status
            if reason is not None:
                batch["reason"] = str(reason)
            elif status in (DatabaseStatus.PREPARED, DatabaseStatus.COMMITTED):
                batch.pop("reason", None)
            self._derive_status(state)

        return self._mutate(mutate)

    def mark_batch_prepared(self, batch_index: int) -> dict[str, Any]:
        return self.transition_batch(batch_index, DatabaseStatus.PREPARED)

    def mark_batch_committed(self, batch_index: int) -> dict[str, Any]:
        return self.transition_batch(batch_index, DatabaseStatus.COMMITTED)

    def transition_phase(
        self, phase: str, status: OperationStatus, *, reason: str | None = None
    ) -> dict[str, Any]:
        if phase not in _PHASE_ORDER:
            raise InvalidInputError(f"Unknown lifecycle phase: {phase}")
        try:
            status = OperationStatus(status)
        except (ValueError, TypeError) as exc:
            raise InvalidInputError(f"Unknown phase state: {status}") from exc

        def mutate(state: dict[str, Any]) -> None:
            phase_state = state["phases"][phase]
            current = phase_state["status"]
            allowed = {
                OperationStatus.PENDING: {OperationStatus.RUNNING},
                OperationStatus.RUNNING: {OperationStatus.COMPLETE, OperationStatus.FAILED},
            }
            if status not in allowed.get(current, set()):
                raise ImmutableStateError(
                    f"Invalid {phase} transition: {current} -> {status}"
                )
            if status == OperationStatus.RUNNING:
                self._validate_phase_prerequisite(state, phase)
            phase_state["status"] = status
            if reason is not None:
                phase_state["reason"] = str(reason)
            elif status in (OperationStatus.RUNNING, OperationStatus.COMPLETE):
                phase_state.pop("reason", None)
            self._derive_status(state)

        return self._mutate(mutate)

    def mark_phase_running(self, phase: str) -> dict[str, Any]:
        return self.transition_phase(phase, OperationStatus.RUNNING)

    def mark_phase_complete(self, phase: str) -> dict[str, Any]:
        return self.transition_phase(phase, OperationStatus.COMPLETE)

    def begin_run(self) -> dict[str, Any]:
        """Consume this workspace once, before any CATMAID mutation."""

        def mutate(state):
            if state["run"]["status"] != OperationStatus.PENDING:
                raise InvalidInputError("This workspace has already been run; retries and resume are unsupported")
            state["run"] = {"status": OperationStatus.RUNNING}
            state["status"] = ImportStatus.IN_PROGRESS
        return self._mutate(mutate)

    def finish_run(self) -> dict[str, Any]:

        def mutate(state):
            if state["run"]["status"] != OperationStatus.RUNNING:
                raise InvalidInputError("Import completion requires an active run")
            if state["phases"]["verification"]["status"] != OperationStatus.COMPLETE:
                raise ImmutableStateError("Import completion requires final verification")
            state["run"] = {"status": OperationStatus.COMPLETE}
            self._derive_status(state)
        return self._mutate(mutate)

    def fail_run(self, reason: str) -> dict[str, Any]:
        """Retain confirmed progress and expose the first failed attempt."""
        with self._mutation_lock():
            state = self.load()
            if state["run"]["status"] != OperationStatus.RUNNING:
                return state
            state["run"] = {"status": OperationStatus.FAILED, "reason": reason}
            state["status"] = ImportStatus.FAILED
            for item in state["batches"]:
                if item["status"] == DatabaseStatus.PREPARED:
                    item["status"] = DatabaseStatus.FAILED
                    item["reason"] = reason
            for item in state["phases"].values():
                if item["status"] == OperationStatus.RUNNING:
                    item["status"] = OperationStatus.FAILED
                    item["reason"] = reason
            self._persist(state)
            return copy.deepcopy(state)

    def _mutate(self, mutator: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        with self._mutation_lock():
            self._assert_mutations_allowed()
            state = self.load()
            mutator(state)
            self._persist(state)
            return copy.deepcopy(state)

    def _assert_mutations_allowed(self) -> None:
        if (
            self.state_path.is_file()
            and self.load()["run"]["status"] in {OperationStatus.FAILED, OperationStatus.COMPLETE}
        ):
            raise InvalidInputError("This import attempt is terminal; the workspace cannot be reused")

    def _normalize_request(
        self, request: IngestionRequest | Mapping[str, Any]
    ) -> dict[str, Any]:
        value = request.to_dict() if isinstance(request, IngestionRequest) else dict(request)
        value = _canonical_value(value)
        if value.get("schema_version") != 1:
            raise InvalidInputError(
                "Request schema_version must be 1"
            )
        return value

    def _normalize_archive(
        self, archive: ArchiveMetadata | Mapping[str, Any]
    ) -> dict[str, Any]:
        if isinstance(archive, ArchiveMetadata):
            return archive.to_dict()
        value = dict(archive)
        value.setdefault("schema_version", METADATA_SCHEMA_VERSION)
        return ArchiveMetadata.from_dict(value).to_dict()

    def _normalize_settings(
        self, settings: ImportSettings | Mapping[str, Any]
    ) -> dict[str, Any]:
        value = settings.plan_dict() if isinstance(settings, ImportSettings) else dict(settings)
        return _canonical_value(value)

    def _normalize_batches(
        self, batches: Sequence[PlannedBatch | Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[int] = set()
        for raw in batches:
            value = raw.to_summary_dict() if isinstance(raw, PlannedBatch) else dict(raw)
            value.pop("status", None)
            value.pop("artifacts", None)
            index = _non_negative_int(value.get("index"), "batch index")
            if index in seen:
                raise InvalidInputError(f"Duplicate batch index: {index}")
            seen.add(index)
            value["index"] = index
            value["skeleton_count"] = _non_negative_int(
                value.get("skeleton_count"), f"batch {index} skeleton count"
            )
            value["node_count"] = _non_negative_int(
                value.get("node_count"), f"batch {index} node count"
            )
            result.append(_canonical_value(value))
        result.sort(key=lambda batch: batch["index"])
        return result

    def _install_initial_metadata(
        self, path: Path, value: Any, expected_digest: str, label: str
    ) -> None:
        if path.exists():
            self._verify_metadata_file(path, expected_digest, label)
        else:
            atomic_write_json(path, value)

    def _verify_metadata_file(
        self, path: Path, expected_digest: str, label: str
    ) -> None:
        if not path.is_file():
            raise ImmutableStateError(f"Missing {label}: {path}")
        actual = sha256_file(path)
        if actual != expected_digest:
            raise ImmutableStateError(
                f"{label.capitalize()} digest mismatch",
                details={"path": str(path), "expected": expected_digest, "actual": actual},
            )

    def _verify_source_file(
        self, path: Path, expected_size: int, expected_digest: str, label: str
    ) -> None:
        if not path.is_file():
            raise ImmutableStateError(f"Missing immutable {label}: {path}")
        actual_size = path.stat().st_size
        if actual_size != expected_size:
            raise ImmutableStateError(
                f"Immutable {label} size changed",
                details={
                    "path": str(path),
                    "expected": expected_size,
                    "actual": actual_size,
                },
            )
        actual_digest = sha256_file(path)
        if actual_digest != expected_digest:
            raise ImmutableStateError(
                f"Immutable {label} digest changed",
                details={
                    "path": str(path),
                    "expected": expected_digest,
                    "actual": actual_digest,
                },
            )

    def _artifact_path(self, value: str | Path) -> tuple[Path, Path]:
        relative = Path(value)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise InvalidInputError("Artifact paths must be relative to the state directory")
        absolute = (self.state_dir / relative).resolve()
        try:
            absolute.relative_to(self.state_dir)
        except ValueError as exc:
            raise InvalidInputError(
                "Artifact path resolves outside the state directory"
            ) from exc
        return relative, absolute

    def _verify_artifact_value(
        self, label: str, value: Mapping[str, Any]
    ) -> ArtifactMetadata:
        metadata = ArtifactMetadata.from_dict(value)
        _, absolute = self._artifact_path(metadata.relative_path)
        if not absolute.is_file():
            raise ImmutableStateError(f"Missing {label}: {absolute}")
        actual_size = absolute.stat().st_size
        if actual_size != metadata.size_bytes:
            raise ImmutableStateError(
                f"{label.capitalize()} byte size changed",
                details={
                    "path": str(absolute),
                    "expected": metadata.size_bytes,
                    "actual": actual_size,
                },
            )
        actual = sha256_file(absolute)
        if actual != metadata.sha256:
            raise ImmutableStateError(
                f"{label.capitalize()} digest changed",
                details={
                    "path": str(absolute),
                    "expected": metadata.sha256,
                    "actual": actual,
                },
            )
        return metadata

    def _find_batch(self, state: Mapping[str, Any], batch_index: int) -> dict[str, Any]:
        wanted = _non_negative_int(batch_index, "batch index")
        for batch in state["batches"]:
            if batch["index"] == wanted:
                return batch
        raise InvalidInputError(f"Unknown batch index: {wanted}")

    def _set_immutable_id(
        self, target: dict[str, Any], key: str, value: int | None
    ) -> None:
        if value is None:
            return
        value = _non_negative_int(value, key)
        existing = target.get(key)
        if existing is not None and existing != value:
            raise ImmutableStateError(f"Cannot change reserved {key}")
        target[key] = value

    def _validate_database_transition(
        self, label: str, current: DatabaseStatus | str, requested: DatabaseStatus
    ) -> None:
        allowed = {
            DatabaseStatus.PLANNED: {DatabaseStatus.PREPARED},
            DatabaseStatus.PREPARED: {DatabaseStatus.COMMITTED},
        }
        if requested not in allowed.get(current, set()):
            raise ImmutableStateError(f"Invalid {label} transition: {current} -> {requested}")

    def _validate_phase_prerequisite(
        self, state: Mapping[str, Any], phase: str
    ) -> None:
        if phase == "database_maintenance":
            if any(batch["status"] != DatabaseStatus.COMMITTED for batch in state["batches"]):
                raise ImmutableStateError(
                    "Database maintenance cannot start before every batch commits"
                )
        elif phase == "cache":
            if state["phases"]["database_maintenance"]["status"] != OperationStatus.COMPLETE:
                raise ImmutableStateError(
                    "Cache generation cannot start before database maintenance completes"
                )
        elif phase == "verification":
            if state["phases"]["cache"]["status"] != OperationStatus.COMPLETE:
                raise ImmutableStateError(
                    "Verification cannot start before cache generation completes"
                )

    def _derive_status(self, state: dict[str, Any]) -> None:
        if state["run"]["status"] == OperationStatus.FAILED:
            state["status"] = ImportStatus.FAILED
            return
        project_status = state["project"]["status"]
        if project_status != DatabaseStatus.COMMITTED:
            state["status"] = ImportStatus.PLANNED
            return
        if any(batch["status"] != DatabaseStatus.COMMITTED for batch in state["batches"]):
            state["status"] = ImportStatus.DATABASE_IN_PROGRESS
            return
        state["status"] = ImportStatus.DATABASE_COMPLETE
        if state["phases"]["database_maintenance"]["status"] != OperationStatus.COMPLETE:
            return
        state["status"] = ImportStatus.MAINTENANCE_COMPLETE
        if state["phases"]["cache"]["status"] != OperationStatus.COMPLETE:
            return
        state["status"] = ImportStatus.CACHE_COMPLETE
        if state["phases"]["verification"]["status"] == OperationStatus.COMPLETE:
            state["status"] = ImportStatus.COMPLETE
