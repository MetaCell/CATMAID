from __future__ import annotations

import csv
import hashlib
import stat
import struct
import warnings
import zipfile
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.archive import scan_archive
from catmaid_skeleton_batch_import.domain import ImportSettings, IngestionRequest
from catmaid_skeleton_batch_import.errors import InvalidInputError


VALID_SWC = b"1 1 0 0 0 -1 -1\n2 3 3 4 0 2 1\n"


def request(archive: Path, mapping: Path | None = None) -> IngestionRequest:
    return IngestionRequest(
        schema_version=1,
        archive_path=archive,
        coordinate_unit="um",
        dimension=(1, 1, 1),
        resolution_nm=(1.0, 1.0, 1.0),
        external_skeleton_id_map_path=mapping,
    )


def settings(**overrides: int | None) -> ImportSettings:
    values = {
        "max_nodes_per_batch": 10,
        "max_archive_bytes": 1_000_000,
        "max_swc_bytes": 100_000,
        "max_swc_count": 10,
        "max_total_nodes": None,
        "cache_cell_size_nm": 1_500_000,
        "lod_levels": 7,
        "lod_bucket_size": 500,
        "lod_strategy": "quadratic",
        "statement_timeout_ms": 0,
        "lock_timeout_ms": 30_000,
    }
    values.update(overrides)
    return ImportSettings(**values)  # type: ignore[arg-type]


def write_zip(path: Path, members: list[tuple[str | zipfile.ZipInfo, bytes]]) -> None:
    with zipfile.ZipFile(path, "w", allowZip64=True) as archive:
        for name, content in members:
            archive.writestr(name, content)


def write_mapping(path: Path, rows: list[tuple[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(["member_path", "external_skeleton_id"])
        writer.writerows(rows)


def test_scan_archive_hashes_parses_and_maps_exact_member_paths(tmp_path: Path) -> None:
    archive = tmp_path / "cells.zip"
    write_zip(
        archive,
        [("z/cell.swc", VALID_SWC), ("notes.txt", b"ignored"), ("a.swc", VALID_SWC)],
    )
    mapping = tmp_path / "mapping.csv"
    write_mapping(mapping, [("a.swc", "external-a"), ("z/cell.swc", "external-z")])

    scanned = scan_archive(request(archive, mapping), settings())

    assert scanned.archive_sha256 == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert scanned.mapping_sha256 == hashlib.sha256(mapping.read_bytes()).hexdigest()
    assert [member.member_path for member in scanned.members] == ["a.swc", "z/cell.swc"]
    assert [member.external_skeleton_id for member in scanned.members] == [
        "external-a",
        "external-z",
    ]
    assert scanned.total_nodes == 4
    assert scanned.members[0].cable_length_nm == 5000
    assert scanned.members[0].display_name == "a"


def test_rejects_non_file_and_invalid_zip(tmp_path: Path) -> None:
    with pytest.raises(InvalidInputError, match="regular file"):
        scan_archive(request(tmp_path), settings())
    invalid = tmp_path / "invalid.zip"
    invalid.write_bytes(b"not a zip")
    with pytest.raises(InvalidInputError, match="Could not read SWC archive"):
        scan_archive(request(invalid), settings())


def test_rejects_duplicate_member_paths(tmp_path: Path) -> None:
    archive = tmp_path / "duplicate.zip"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        write_zip(archive, [("same.swc", VALID_SWC), ("same.swc", VALID_SWC)])
    with pytest.raises(InvalidInputError, match="duplicate member path"):
        scan_archive(request(archive), settings())


def test_rejects_nonregular_swc_member(tmp_path: Path) -> None:
    archive = tmp_path / "symlink.zip"
    info = zipfile.ZipInfo("link.swc")
    info.create_system = 3
    info.external_attr = (stat.S_IFLNK | 0o777) << 16
    write_zip(archive, [(info, b"target")])
    with pytest.raises(InvalidInputError, match="not a regular file"):
        scan_archive(request(archive), settings())


def test_rejects_encrypted_member_before_opening(tmp_path: Path) -> None:
    archive = tmp_path / "encrypted.zip"
    write_zip(archive, [("cell.swc", VALID_SWC)])
    data = bytearray(archive.read_bytes())
    local = data.index(b"PK\x03\x04")
    central = data.index(b"PK\x01\x02")
    local_flags = struct.unpack_from("<H", data, local + 6)[0] | 1
    central_flags = struct.unpack_from("<H", data, central + 8)[0] | 1
    struct.pack_into("<H", data, local + 6, local_flags)
    struct.pack_into("<H", data, central + 8, central_flags)
    archive.write_bytes(data)

    with pytest.raises(InvalidInputError, match="encrypted member"):
        scan_archive(request(archive), settings())


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"max_archive_bytes": 1}, "archive is"),
        ({"max_swc_bytes": 1}, "limit is 1"),
        ({"max_swc_count": 1}, "more than 1 SWC"),
        ({"max_total_nodes": 3}, "total node limit 3"),
        ({"max_nodes_per_batch": 1}, "exceeds 1 nodes"),
    ],
)
def test_enforces_deployment_limits(
    tmp_path: Path, overrides: dict[str, int], message: str
) -> None:
    archive = tmp_path / "limits.zip"
    write_zip(archive, [("a.swc", VALID_SWC), ("b.swc", VALID_SWC)])
    with pytest.raises(InvalidInputError, match=message):
        scan_archive(request(archive), settings(**overrides))


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([("a.swc", "one")], "missing mapping"),
        (
            [("a.swc", "one"), ("b.swc", "two"), ("unknown.swc", "three")],
            "unknown archive member",
        ),
        ([("a.swc", "same"), ("b.swc", "same")], "duplicate external_skeleton_id"),
        ([("a.swc", "one"), ("a.swc", "two")], "duplicate member_path"),
    ],
)
def test_mapping_must_be_complete_exact_and_one_to_one(
    tmp_path: Path, rows: list[tuple[str, str]], message: str
) -> None:
    archive = tmp_path / "mapping.zip"
    write_zip(archive, [("a.swc", VALID_SWC), ("b.swc", VALID_SWC)])
    mapping = tmp_path / "mapping.csv"
    write_mapping(mapping, rows)
    with pytest.raises(InvalidInputError, match=message):
        scan_archive(request(archive, mapping), settings())


def test_mapping_rejects_extra_row_columns(tmp_path: Path) -> None:
    archive = tmp_path / "mapping.zip"
    write_zip(archive, [("a.swc", VALID_SWC)])
    mapping = tmp_path / "mapping.csv"
    mapping.write_text(
        "member_path,external_skeleton_id\na.swc,source-a,unexpected\n",
        encoding="utf-8",
    )

    with pytest.raises(InvalidInputError, match="exactly two columns"):
        scan_archive(request(archive, mapping), settings())


def test_archive_ignores_directories_and_non_swc_files(tmp_path: Path) -> None:
    archive = tmp_path / "ignored.zip"
    directory = zipfile.ZipInfo("directory.swc/")
    directory.external_attr = (stat.S_IFDIR | 0o755) << 16
    write_zip(
        archive,
        [(directory, b""), ("readme.md", b"metadata"), ("real.SWC", VALID_SWC)],
    )
    scanned = scan_archive(request(archive), settings())
    assert [member.member_path for member in scanned.members] == ["real.SWC"]
