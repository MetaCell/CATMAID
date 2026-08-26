"""Deterministic whole-skeleton batch planning and plan persistence."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Iterator

from .archive import ArchiveScan
from .domain import ImportSettings, PlannedBatch, PlannedMember
from .errors import InvalidInputError
from .util import (
    atomic_write_bytes,
    canonical_json_bytes,
    deterministic_gzip_bytes,
    read_gzip_lines,
    sha256_bytes,
)


PLAN_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ImportPlan:
    """Stable member order and its deterministic atomic database batches."""

    schema_version: int
    max_nodes_per_batch: int
    members: tuple[PlannedMember, ...]
    batches: tuple[PlannedBatch, ...]

    @property
    def member_count(self) -> int:
        return len(self.members)

    @property
    def batch_count(self) -> int:
        return len(self.batches)

    @property
    def total_nodes(self) -> int:
        return sum(member.node_count for member in self.members)

    def header_dict(self) -> dict[str, Any]:
        return {
            "record_type": "header",
            "schema_version": self.schema_version,
            "max_nodes_per_batch": self.max_nodes_per_batch,
            "member_count": self.member_count,
            "batch_count": self.batch_count,
            "total_nodes": self.total_nodes,
        }


def _validate_member(member: PlannedMember) -> None:
    if not member.member_path:
        raise InvalidInputError("Planned member path must be non-empty")
    if member.node_count <= 0:
        raise InvalidInputError(
            f"Planned member {member.member_path!r} has no nodes"
        )
    if member.uncompressed_bytes < 0:
        raise InvalidInputError(
            f"Planned member {member.member_path!r} has a negative byte size"
        )
    if member.crc32 < 0 or member.crc32 > 0xFFFFFFFF:
        raise InvalidInputError(
            f"Planned member {member.member_path!r} has an invalid CRC32"
        )
    if not member.display_name:
        raise InvalidInputError(
            f"Planned member {member.member_path!r} has an empty display name"
        )
    if not math.isfinite(member.cable_length_nm) or member.cable_length_nm < 0:
        raise InvalidInputError(
            f"Planned member {member.member_path!r} has invalid cable length"
        )
    if (
        member.external_skeleton_id is not None
        and not member.external_skeleton_id.strip()
    ):
        raise InvalidInputError(
            f"Planned member {member.member_path!r} has an empty external ID"
        )


def build_plan(
    members: Iterable[PlannedMember],
    max_nodes_per_batch: int,
) -> ImportPlan:
    """Sort exact paths and greedily form hard-bounded whole-SWC batches."""

    if max_nodes_per_batch <= 0:
        raise ValueError("max_nodes_per_batch must be positive")
    ordered = sorted(members, key=lambda member: member.member_path)
    if not ordered:
        raise InvalidInputError("Cannot plan an archive with no SWC members")

    seen_paths: set[str] = set()
    seen_external_ids: set[str] = set()
    for member in ordered:
        _validate_member(member)
        if member.member_path in seen_paths:
            raise InvalidInputError(
                f"Plan contains duplicate member path {member.member_path!r}"
            )
        seen_paths.add(member.member_path)
        if member.external_skeleton_id is not None:
            if member.external_skeleton_id in seen_external_ids:
                raise InvalidInputError(
                    f"Plan contains duplicate external skeleton ID "
                    f"{member.external_skeleton_id!r}"
                )
            seen_external_ids.add(member.external_skeleton_id)
        if member.node_count > max_nodes_per_batch:
            raise InvalidInputError(
                f"SWC member {member.member_path!r} has {member.node_count} nodes; "
                f"batch limit is {max_nodes_per_batch}"
            )

    raw_batches: list[list[PlannedMember]] = []
    current: list[PlannedMember] = []
    current_nodes = 0
    for member in ordered:
        if current and current_nodes + member.node_count > max_nodes_per_batch:
            raw_batches.append(current)
            current = []
            current_nodes = 0
        current.append(member)
        current_nodes += member.node_count
    if current:
        raw_batches.append(current)

    planned_batches: list[PlannedBatch] = []
    planned_members: list[PlannedMember] = []
    for batch_index, batch_members in enumerate(raw_batches, 1):
        assigned = tuple(
            replace(member, batch_index=batch_index) for member in batch_members
        )
        planned_batches.append(PlannedBatch(index=batch_index, members=assigned))
        planned_members.extend(assigned)

    return ImportPlan(
        schema_version=PLAN_SCHEMA_VERSION,
        max_nodes_per_batch=max_nodes_per_batch,
        members=tuple(planned_members),
        batches=tuple(planned_batches),
    )


def plan_archive(scan: ArchiveScan, settings: ImportSettings) -> ImportPlan:
    return build_plan(scan.members, settings.max_nodes_per_batch)


def _plan_lines(plan: ImportPlan) -> Iterator[str]:
    yield canonical_json_bytes(plan.header_dict()).decode("utf-8")
    for member in plan.members:
        record = {"record_type": "member", **member.to_dict()}
        yield canonical_json_bytes(record).decode("utf-8")


def serialize_plan(plan: ImportPlan) -> bytes:
    """Return byte-for-byte deterministic gzip-compressed JSON Lines."""

    return deterministic_gzip_bytes(_plan_lines(plan))


def plan_sha256(plan: ImportPlan) -> str:
    return sha256_bytes(serialize_plan(plan))


def write_plan(path: Path, plan: ImportPlan) -> str:
    payload = serialize_plan(plan)
    atomic_write_bytes(path, payload)
    return sha256_bytes(payload)


def _load_json_record(line: str, path: Path, line_number: int) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number {value}")

    try:
        value = json.loads(line, parse_constant=reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise InvalidInputError(
            f"{path}:{line_number}: invalid plan JSON: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise InvalidInputError(f"{path}:{line_number}: plan record must be an object")
    return value


def _record_int(
    record: dict[str, Any], field: str, path: Path, line_number: int
) -> int:
    value = record[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidInputError(
            f"{path}:{line_number}: {field} must be an integer"
        )
    return value


def _record_string(
    record: dict[str, Any], field: str, path: Path, line_number: int
) -> str:
    value = record[field]
    if not isinstance(value, str):
        raise InvalidInputError(f"{path}:{line_number}: {field} must be a string")
    return value


def _member_from_record(
    record: dict[str, Any], path: Path, line_number: int
) -> PlannedMember:
    required = {
        "record_type",
        "member_path",
        "node_count",
        "uncompressed_bytes",
        "crc32",
        "display_name",
        "cable_length_nm",
        "batch_index",
    }
    allowed = required | {"external_skeleton_id"}
    unknown = set(record) - allowed
    missing = required - set(record)
    if unknown or missing or record.get("record_type") != "member":
        parts = []
        if unknown:
            parts.append(f"unknown fields: {', '.join(sorted(unknown))}")
        if missing:
            parts.append(f"missing fields: {', '.join(sorted(missing))}")
        if record.get("record_type") != "member":
            parts.append("record_type must be 'member'")
        raise InvalidInputError(f"{path}:{line_number}: " + "; ".join(parts))

    cable_length = record["cable_length_nm"]
    if isinstance(cable_length, bool) or not isinstance(cable_length, (int, float)):
        raise InvalidInputError(
            f"{path}:{line_number}: cable_length_nm must be numeric"
        )
    external_id = record.get("external_skeleton_id")
    if external_id is not None and not isinstance(external_id, str):
        raise InvalidInputError(
            f"{path}:{line_number}: external_skeleton_id must be a string"
        )
    member = PlannedMember(
        member_path=_record_string(record, "member_path", path, line_number),
        node_count=_record_int(record, "node_count", path, line_number),
        uncompressed_bytes=_record_int(
            record, "uncompressed_bytes", path, line_number
        ),
        crc32=_record_int(record, "crc32", path, line_number),
        display_name=_record_string(record, "display_name", path, line_number),
        cable_length_nm=float(cable_length),
        external_skeleton_id=external_id,
        batch_index=_record_int(record, "batch_index", path, line_number),
    )
    _validate_member(member)
    if member.batch_index <= 0:
        raise InvalidInputError(
            f"{path}:{line_number}: batch_index must be positive"
        )
    return member


def read_plan(path: Path) -> ImportPlan:
    """Read and fully validate a deterministic plan artifact."""

    lines = read_gzip_lines(path)
    try:
        try:
            header_line = next(lines)
        except StopIteration as exc:
            raise InvalidInputError(f"Plan is empty: {path}") from exc
        if not header_line.strip():
            raise InvalidInputError(f"Plan contains an empty record: {path}")
        header = _load_json_record(header_line, path, 1)
        persisted_members: list[PlannedMember] = []
        for line_number, line in enumerate(lines, 2):
            if not line.strip():
                raise InvalidInputError(f"Plan contains an empty record: {path}")
            persisted_members.append(
                _member_from_record(
                    _load_json_record(line, path, line_number), path, line_number
                )
            )
    except InvalidInputError:
        raise
    except (OSError, EOFError, UnicodeError) as exc:
        raise InvalidInputError(f"Could not read plan {path}: {exc}") from exc
    finally:
        lines.close()

    expected_header = {
        "record_type",
        "schema_version",
        "max_nodes_per_batch",
        "member_count",
        "batch_count",
        "total_nodes",
    }
    if set(header) != expected_header or header.get("record_type") != "header":
        raise InvalidInputError(f"{path}: invalid plan header")
    schema_version = _record_int(header, "schema_version", path, 1)
    max_nodes_per_batch = _record_int(header, "max_nodes_per_batch", path, 1)
    expected_member_count = _record_int(header, "member_count", path, 1)
    expected_batch_count = _record_int(header, "batch_count", path, 1)
    expected_total_nodes = _record_int(header, "total_nodes", path, 1)
    if schema_version != PLAN_SCHEMA_VERSION:
        raise InvalidInputError(
            f"{path}: unsupported plan schema version {schema_version}"
        )
    if max_nodes_per_batch <= 0:
        raise InvalidInputError(f"{path}: max_nodes_per_batch must be positive")
    if min(expected_member_count, expected_batch_count, expected_total_nodes) < 0:
        raise InvalidInputError(f"{path}: plan header counts cannot be negative")

    persisted_member_tuple = tuple(persisted_members)
    rebuilt = build_plan(persisted_member_tuple, max_nodes_per_batch)
    if persisted_member_tuple != rebuilt.members:
        raise InvalidInputError(
            f"{path}: member order or batch assignments are not deterministic"
        )
    if (
        rebuilt.member_count != expected_member_count
        or rebuilt.batch_count != expected_batch_count
        or rebuilt.total_nodes != expected_total_nodes
    ):
        raise InvalidInputError(f"{path}: plan header counts do not match records")
    return rebuilt
