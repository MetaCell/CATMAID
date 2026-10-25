"""Immutable local ZIP scanning."""

from __future__ import annotations

import math
import os
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .domain import ImportSettings, IngestionRequest, PlannedMember
from .fragments import fragment_id_from_member_path
from .errors import InvalidInputError
from .swc import parse_swc
from .stack_bounds import StackBounds
from .util import sha256_file


@dataclass(frozen=True)
class ArchiveScan:
    """Immutable source identity and all validated SWC member facts."""

    archive_path: Path
    archive_bytes: int
    archive_sha256: str
    archive_mtime_ns: int
    members: tuple[PlannedMember, ...]

    @property
    def swc_count(self) -> int:
        return len(self.members)

    @property
    def total_nodes(self) -> int:
        return sum(member.node_count for member in self.members)


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


def _collect_swc_infos(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
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
        swc_infos.append(info)
    if not swc_infos:
        raise InvalidInputError("Archive contains no regular .swc members")
    return sorted(swc_infos, key=lambda info: info.filename)


def scan_archive(
    request: IngestionRequest,
    settings: ImportSettings,
    *, bounds_nm: tuple[StackBounds, ...],
    on_member=None,
) -> ArchiveScan:
    """Hash and validate one local SWC ZIP without extracting its members."""

    if not bounds_nm:
        raise InvalidInputError("Append validation requires associated target stack bounds")
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
    member_errors: dict[str, str] = {}
    fragment_paths: dict[str, list[str]] = {}
    total_nodes = 0
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            infos = _collect_swc_infos(archive)
            for info in infos:
                label = f"{archive_path.name}:{info.filename}"
                try:
                    if not _is_regular_zip_member(info):
                        raise InvalidInputError(
                            f"SWC member is not a regular file: {info.filename!r}"
                        )
                    if info.file_size > settings.max_swc_bytes:
                        raise InvalidInputError(
                            f"SWC member {info.filename!r} is {info.file_size} bytes; "
                            f"limit is {settings.max_swc_bytes}"
                        )
                    fragment_id = fragment_id_from_member_path(info.filename)
                    fragment_paths.setdefault(fragment_id, []).append(info.filename)
                    with archive.open(info, "r") as member_source:
                        parsed = parse_swc(
                            member_source,
                            label,
                            max_bytes=settings.max_swc_bytes,
                            max_nodes=settings.max_nodes_per_batch,
                            scale_nm=request.scale_nm,
                            bounds_nm=bounds_nm,
                        )
                    if parsed.bytes_read != info.file_size:
                        raise InvalidInputError(
                            f"{label}: ZIP metadata reports {info.file_size} bytes but "
                            f"the member produced {parsed.bytes_read} bytes"
                        )
                except InvalidInputError as exc:
                    # One useful error per member; continue so the operator can
                    # fix the whole archive in one pass. Never plan a subset.
                    member_errors[info.filename] = str(exc)
                    continue
                except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError) as exc:
                    member_errors[info.filename] = f"Could not read {label}: {exc}"
                    continue
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
                        fragment_id=fragment_id,
                        cable_length_nm=cable_length_nm,
                    )
                )
                if on_member is not None:
                    on_member(members[-1], parsed.nodes)
    except InvalidInputError:
        raise
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError) as exc:
        raise InvalidInputError(
            f"Could not read SWC archive {archive_path}: {exc}"
        ) from exc
    _reject_if_changed(archive_path, before, "SWC archive")

    for fragment_id, paths in fragment_paths.items():
        if len(paths) > 1:
            for index, member_path in enumerate(paths):
                other_path = paths[0] if index else paths[1]
                member_errors.setdefault(member_path,
                    f"Archive contains duplicate fragment identifier {fragment_id!r}; "
                    f"also used by {other_path!r}")
    if member_errors:
        errors = [{"member_path": path, "message": message}
                  for path, message in sorted(member_errors.items())]
        raise InvalidInputError(
            f"{len(errors)} SWC file(s) failed validation; "
            f"see error-details.json for all files. First error: {errors[0]['message']}",
            details={"invalid_swc_count": len(errors), "member_errors": errors},
        )

    return ArchiveScan(
        archive_path=archive_path,
        archive_bytes=before.st_size,
        archive_sha256=archive_digest,
        archive_mtime_ns=before.st_mtime_ns,
        members=tuple(members),
    )
