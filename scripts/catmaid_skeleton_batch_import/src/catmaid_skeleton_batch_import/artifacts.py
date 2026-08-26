"""Schemas for reserved-ID and source-to-CATMAID artifacts."""

from __future__ import annotations

import csv
import gzip
import io
from pathlib import Path
from typing import Iterable, Sequence

from .domain import PlannedBatch
from .errors import ImmutableStateError
from .util import atomic_write_bytes, deterministic_gzip_bytes, sha256_file


MANIFEST_FIELDS = (
    "member_path",
    "external_skeleton_id",
    "neuron_id",
    "skeleton_id",
    "node_count",
    "name",
    "batch_index",
)


def artifact_descriptor(
    state_dir: Path,
    path: Path,
    *,
    count: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> dict[str, object]:
    resolved_root = state_dir.resolve()
    resolved_path = path.resolve()
    try:
        relative = resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"Artifact {path} is outside state directory {state_dir}") from exc
    result: dict[str, object] = {
        "schema_version": 1,
        "path": relative.as_posix(),
        "count": count,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }
    if minimum is not None:
        result["min"] = minimum
    if maximum is not None:
        result["max"] = maximum
    return result


def verify_artifact(state_dir: Path, descriptor: dict[str, object]) -> Path:
    relative = Path(str(descriptor["path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ImmutableStateError(f"Unsafe artifact path in state: {relative}")
    path = state_dir / relative
    if not path.is_file():
        raise ImmutableStateError(f"Required artifact is missing: {path}")
    if path.stat().st_size != int(descriptor["bytes"]):
        raise ImmutableStateError(f"Artifact byte size changed: {path}")
    if sha256_file(path) != descriptor["sha256"]:
        raise ImmutableStateError(f"Artifact digest changed: {path}")
    return path


def write_id_artifact(path: Path, ids: Sequence[int]) -> bytes:
    if not ids:
        raise ValueError("Cannot write an empty reserved-ID artifact")
    if len(set(ids)) != len(ids):
        raise ValueError("Reserved IDs must be unique")
    data = deterministic_gzip_bytes(f"{int(value)}\n" for value in ids)
    atomic_write_bytes(path, data)
    return data


def read_id_artifact(
    state_dir: Path,
    descriptor: dict[str, object],
    *,
    expected_count: int,
) -> list[int]:
    path = verify_artifact(state_dir, descriptor)
    ids: list[int] = []
    try:
        with gzip.open(path, "rt", encoding="ascii") as source:
            for line_number, raw in enumerate(source, 1):
                value = raw.strip()
                if not value:
                    continue
                try:
                    ids.append(int(value))
                except ValueError as exc:
                    raise ImmutableStateError(
                        f"Invalid reserved ID in {path} on line {line_number}"
                    ) from exc
    except OSError as exc:
        raise ImmutableStateError(f"Could not read reserved IDs from {path}: {exc}") from exc
    if len(ids) != expected_count or int(descriptor["count"]) != expected_count:
        raise ImmutableStateError(
            f"Reserved ID count mismatch for {path}: expected {expected_count}, "
            f"found {len(ids)}"
        )
    if len(set(ids)) != len(ids):
        raise ImmutableStateError(f"Reserved ID artifact has duplicates: {path}")
    if ids and (
        int(descriptor.get("min", min(ids))) != min(ids)
        or int(descriptor.get("max", max(ids))) != max(ids)
    ):
        raise ImmutableStateError(f"Reserved ID range changed: {path}")
    return ids


def manifest_rows(
    batch: PlannedBatch,
    concept_ids: Sequence[int],
) -> list[dict[str, object]]:
    if len(concept_ids) != batch.skeleton_count * 3:
        raise ValueError("Concept ID count does not match batch skeleton count")
    rows: list[dict[str, object]] = []
    for index, member in enumerate(batch.members):
        neuron_id, skeleton_id, _link_id = concept_ids[index * 3 : index * 3 + 3]
        rows.append(
            {
                "member_path": member.member_path,
                "external_skeleton_id": member.external_skeleton_id or "",
                "neuron_id": neuron_id,
                "skeleton_id": skeleton_id,
                "node_count": member.node_count,
                "name": member.display_name,
                "batch_index": batch.index,
            }
        )
    return rows


def _manifest_csv(rows: Iterable[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=list(MANIFEST_FIELDS),
        extrasaction="raise",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def write_manifest(path: Path, rows: Iterable[dict[str, object]]) -> None:
    csv_text = _manifest_csv(rows)
    atomic_write_bytes(path, deterministic_gzip_bytes([csv_text]))


def read_manifest(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != MANIFEST_FIELDS:
            raise ImmutableStateError(f"Unexpected manifest schema in {path}")
        return list(reader)


def combine_manifests(paths: Sequence[Path], destination: Path) -> int:
    rows: list[dict[str, str]] = []
    for path in paths:
        rows.extend(read_manifest(path))
    write_manifest(destination, rows)
    return len(rows)
