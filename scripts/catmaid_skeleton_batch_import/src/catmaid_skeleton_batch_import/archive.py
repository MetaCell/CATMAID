"""Immutable local ZIP scanning and optional external-ID mapping."""

from __future__ import annotations

import csv
import math
import os
import stat
import zipfile
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any

from .domain import ImportSettings, IngestionRequest, PlannedMember
from .errors import InvalidInputError
from .swc import parse_swc
from .util import sha256_file


@dataclass(frozen=True)
class ArchiveScan:
    """Immutable source identity and all validated SWC member facts."""

    archive_path: Path
    archive_bytes: int
    archive_sha256: str
    archive_mtime_ns: int
    members: tuple[PlannedMember, ...]
    mapping_path: Path | None = None
    mapping_bytes: int | None = None
    mapping_sha256: str | None = None
    mapping_mtime_ns: int | None = None

    @property
    def swc_count(self) -> int:
        return len(self.members)

    @property
    def total_nodes(self) -> int:
        return sum(member.node_count for member in self.members)

    def identity_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "archive_path": str(self.archive_path),
            "archive_bytes": self.archive_bytes,
            "archive_sha256": self.archive_sha256,
            "archive_mtime_ns": self.archive_mtime_ns,
            "swc_count": self.swc_count,
            "total_nodes": self.total_nodes,
        }
        if self.mapping_path is not None:
            result.update(
                {
                    "mapping_path": str(self.mapping_path),
                    "mapping_bytes": self.mapping_bytes,
                    "mapping_sha256": self.mapping_sha256,
                    "mapping_mtime_ns": self.mapping_mtime_ns,
                }
            )
        return result


def _regular_file_stat(path: Path, label: str) -> os.stat_result:
    try:
        result = path.stat()
    except OSError as exc:
        raise InvalidInputError(f"Could not access {label} {path}: {exc}") from exc
    if not stat.S_ISREG(result.st_mode):
        raise InvalidInputError(f"{label} must be a regular file: {path}")
    try:
        with path.open("rb"):
            pass
    except OSError as exc:
        raise InvalidInputError(f"{label} is not readable: {path}: {exc}") from exc
    return result


def _same_file(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )


def _reject_if_changed(path: Path, before: os.stat_result, label: str) -> None:
    try:
        after = path.stat()
    except OSError as exc:
        raise InvalidInputError(f"Could not recheck {label} {path}: {exc}") from exc
    if not _same_file(before, after):
        raise InvalidInputError(f"{label} changed while it was being read: {path}")


def _sha256_file(path: Path, label: str) -> str:
    try:
        return sha256_file(path)
    except OSError as exc:
        raise InvalidInputError(f"Could not hash {label} {path}: {exc}") from exc


def _is_regular_zip_member(info: zipfile.ZipInfo) -> bool:
    if info.is_dir():
        return False
    if info.create_system != 3:
        return True
    mode = info.external_attr >> 16
    file_type = stat.S_IFMT(mode)
    # Many ZIP writers record Unix permissions without recording a file type.
    return file_type == 0 or stat.S_ISREG(mode)


def _read_mapping(path: Path) -> tuple[dict[str, str], int, str, int]:
    before = _regular_file_stat(path, "external skeleton-ID mapping")
    digest = _sha256_file(path, "external skeleton-ID mapping")
    mapping: dict[str, str] = {}
    external_ids: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", errors="strict", newline="") as source:
            reader = csv.DictReader(source)
            expected = ["member_path", "external_skeleton_id"]
            if reader.fieldnames != expected:
                raise InvalidInputError(
                    f"{path}: mapping header must be exactly "
                    "member_path,external_skeleton_id"
                )
            for row in reader:
                line_number = reader.line_num
                if None in row:
                    raise InvalidInputError(
                        f"{path}:{line_number}: mapping rows must have exactly "
                        "two columns"
                    )
                member_path = row.get("member_path")
                external_id_value = row.get("external_skeleton_id")
                if member_path is None or not member_path:
                    raise InvalidInputError(
                        f"{path}:{line_number}: member_path must be non-empty"
                    )
                if external_id_value is None or not external_id_value.strip():
                    raise InvalidInputError(
                        f"{path}:{line_number}: external_skeleton_id must be non-empty"
                    )
                external_id = external_id_value.strip()
                if member_path in mapping:
                    raise InvalidInputError(
                        f"{path}:{line_number}: duplicate member_path {member_path!r}"
                    )
                if external_id in external_ids:
                    raise InvalidInputError(
                        f"{path}:{line_number}: duplicate external_skeleton_id "
                        f"{external_id!r}"
                    )
                mapping[member_path] = external_id
                external_ids.add(external_id)
    except InvalidInputError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise InvalidInputError(f"Could not read mapping {path}: {exc}") from exc
    _reject_if_changed(path, before, "external skeleton-ID mapping")
    return mapping, before.st_size, digest, before.st_mtime_ns


def _collect_swc_infos(
    archive: zipfile.ZipFile, settings: ImportSettings
) -> list[zipfile.ZipInfo]:
    seen_names: set[str] = set()
    swc_infos: list[zipfile.ZipInfo] = []
    for info in archive.infolist():
        if info.filename in seen_names:
            raise InvalidInputError(
                f"Archive contains duplicate member path {info.filename!r}"
            )
        seen_names.add(info.filename)
        if info.flag_bits & 0x1:
            raise InvalidInputError(
                f"Archive contains encrypted member {info.filename!r}"
            )
        if not info.filename.lower().endswith(".swc"):
            continue
        if not _is_regular_zip_member(info):
            raise InvalidInputError(
                f"SWC member is not a regular file: {info.filename!r}"
            )
        if info.file_size > settings.max_swc_bytes:
            raise InvalidInputError(
                f"SWC member {info.filename!r} is {info.file_size} bytes; "
                f"limit is {settings.max_swc_bytes}"
            )
        swc_infos.append(info)
        if len(swc_infos) > settings.max_swc_count:
            raise InvalidInputError(
                f"Archive contains more than {settings.max_swc_count} SWC members"
            )
    if not swc_infos:
        raise InvalidInputError("Archive contains no regular .swc members")
    return sorted(swc_infos, key=lambda info: info.filename)


def scan_archive(
    request: IngestionRequest,
    settings: ImportSettings,
) -> ArchiveScan:
    """Hash and validate one local SWC ZIP without extracting its members."""

    archive_path = request.archive_path
    before = _regular_file_stat(archive_path, "SWC archive")
    if before.st_size > settings.max_archive_bytes:
        raise InvalidInputError(
            f"SWC archive is {before.st_size} bytes; "
            f"limit is {settings.max_archive_bytes}"
        )
    archive_digest = _sha256_file(archive_path, "SWC archive")
    _reject_if_changed(archive_path, before, "SWC archive")

    members: list[PlannedMember] = []
    total_nodes = 0
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            infos = _collect_swc_infos(archive, settings)
            for info in infos:
                label = f"{archive_path.name}:{info.filename}"
                with archive.open(info, "r") as member_source:
                    parsed = parse_swc(
                        member_source,
                        label,
                        max_bytes=settings.max_swc_bytes,
                        max_nodes=settings.max_nodes_per_batch,
                        scale_nm=request.scale_nm,
                    )
                if parsed.bytes_read != info.file_size:
                    raise InvalidInputError(
                        f"{label}: ZIP metadata reports {info.file_size} bytes but "
                        f"the member produced {parsed.bytes_read} bytes"
                    )
                total_nodes += parsed.summary.node_count
                if (
                    settings.max_total_nodes is not None
                    and total_nodes > settings.max_total_nodes
                ):
                    raise InvalidInputError(
                        f"Archive exceeds total node limit "
                        f"{settings.max_total_nodes}"
                    )
                cable_length_nm = (
                    parsed.summary.cable_length_source_units * request.scale_nm
                )
                if not math.isfinite(cable_length_nm):
                    raise InvalidInputError(
                        f"{label}: cable length is non-finite after conversion"
                    )
                members.append(
                    PlannedMember(
                        member_path=info.filename,
                        node_count=parsed.summary.node_count,
                        uncompressed_bytes=info.file_size,
                        crc32=info.CRC,
                        display_name=PurePosixPath(info.filename).stem[:255],
                        cable_length_nm=cable_length_nm,
                    )
                )
    except InvalidInputError:
        raise
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError) as exc:
        raise InvalidInputError(
            f"Could not read SWC archive {archive_path}: {exc}"
        ) from exc
    _reject_if_changed(archive_path, before, "SWC archive")

    mapping_path = request.external_skeleton_id_map_path
    mapping_bytes = None
    mapping_digest = None
    mapping_mtime_ns = None
    if mapping_path is not None:
        mapping, mapping_bytes, mapping_digest, mapping_mtime_ns = _read_mapping(
            mapping_path
        )
        member_paths = {member.member_path for member in members}
        mapping_paths = set(mapping)
        missing = sorted(member_paths - mapping_paths)
        unknown = sorted(mapping_paths - member_paths)
        if missing:
            raise InvalidInputError(
                f"{mapping_path}: missing mapping for archive member {missing[0]!r}"
            )
        if unknown:
            raise InvalidInputError(
                f"{mapping_path}: mapping references unknown archive member "
                f"{unknown[0]!r}"
            )
        members = [
            replace(member, external_skeleton_id=mapping[member.member_path])
            for member in members
        ]

    return ArchiveScan(
        archive_path=archive_path,
        archive_bytes=before.st_size,
        archive_sha256=archive_digest,
        archive_mtime_ns=before.st_mtime_ns,
        members=tuple(members),
        mapping_path=mapping_path,
        mapping_bytes=mapping_bytes,
        mapping_sha256=mapping_digest,
        mapping_mtime_ns=mapping_mtime_ns,
    )
