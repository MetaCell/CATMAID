from __future__ import annotations

import hashlib
import stat
import struct
import warnings
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.archive import scan_archive
from catmaid_skeleton_batch_import.domain import ImportSettings, IngestionRequest, PlannedMember
from catmaid_skeleton_batch_import.errors import InvalidInputError


VALID_SWC = b"1 1 0 0 0 -1 -1\n2 3 3 4 0 2 1\n"


def request(archive: Path) -> IngestionRequest:
    return IngestionRequest(
        schema_version=1,
        archive_path=archive,
        coordinate_unit="um",
        dimension=(10, 20, 30),
        resolution_nm=(1000.0, 1000.0, 1000.0),
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


def test_scan_archive_hashes_parses_and_preserves_exact_member_paths(tmp_path: Path) -> None:
    archive = tmp_path / "cells.zip"
    write_zip(
        archive,
        [("z/cell.swc", VALID_SWC), ("notes.txt", b"ignored"), ("a.swc", VALID_SWC)],
    )
    scanned = scan_archive(request(archive), settings())

    assert scanned.archive_sha256 == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert [member.member_path for member in scanned.members] == ["a.swc", "z/cell.swc"]
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


def test_trims_fragment_identifier_but_preserves_member_path_and_display_name(tmp_path: Path) -> None:
    archive = tmp_path / "cells.zip"
    path = "folder/ \t0021814 \u2003.swc"
    write_zip(archive, [(path, VALID_SWC)])
    scanned = scan_archive(request(archive), settings())
    assert scanned.members[0].member_path == path
    assert scanned.members[0].display_name == " \t0021814 \u2003"
    assert scanned.members[0].fragment_id == "0021814"


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


def test_reports_each_invalid_member_without_stopping_at_the_first(tmp_path: Path) -> None:
    archive = tmp_path / "mixed.zip"
    write_zip(archive, [
        ("a.swc", b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 -1\n"),
        ("b.swc", b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 99\n"),
        ("c.swc", b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 # missing parent\n"),
        ("valid.swc", VALID_SWC),
    ])
    with pytest.raises(InvalidInputError) as failure:
        scan_archive(request(archive), settings())
    details = failure.value.details
    assert details["invalid_swc_count"] == 3
    assert [error["member_path"] for error in details["member_errors"]] == [
        "a.swc", "b.swc", "c.swc",
    ]
    for error, reason in zip(details["member_errors"], [
        "exactly one root", "missing parent", "expected 7 columns",
    ]):
        assert reason in error["message"]


def test_planning_reports_all_members_with_negative_coordinates(tmp_path: Path) -> None:
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion
    import json

    archive = tmp_path / "negative.zip"
    write_zip(archive, [
        ("x.swc", b"1 1 -1 0 0 -1 -1\n"),
        ("y.swc", b"1 1 0 -2 0 -1 -1\n"),
        ("z.swc", b"1 1 0 0 -3 -1 -1\n"),
        ("valid.swc", VALID_SWC),
    ])
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request(archive).to_dict()))
    state_dir = tmp_path / "state"
    with pytest.raises(InvalidInputError) as failure:
        plan_ingestion(path, state_dir)
    errors = failure.value.details["member_errors"]
    assert [item["member_path"] for item in errors] == ["x.swc", "y.swc", "z.swc"]
    assert all(f"{axis} must be non-negative" in item["message"]
               for axis, item in zip("xyz", errors))
    assert not (state_dir / "state.json").exists()


@pytest.mark.parametrize("unit", ["nm", "um"])
def test_planning_reports_all_members_beyond_declared_stack_without_a_partial_plan(tmp_path, unit):
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion
    import json

    archive = tmp_path / "outside.zip"
    ingestion_request = replace(request(archive), coordinate_unit=unit)
    members = [("valid.swc", VALID_SWC)]
    for axis, extent in enumerate(ingestion_request.max_nm):
        coordinates = ["0", "0", "0"]
        coordinates[axis] = str((extent + 1) / ingestion_request.scale_nm)
        content = ("# header\n1 1 " + " ".join(coordinates) + " -1 -1\n").encode()
        members.append((f"{'xyz'[axis]}.swc", content))
    write_zip(archive, members)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(ingestion_request.to_dict()))
    state_dir = tmp_path / "state"

    with pytest.raises(InvalidInputError) as failure:
        plan_ingestion(path, state_dir)
    errors = failure.value.details["member_errors"]
    assert failure.value.details["invalid_swc_count"] == 3
    assert [item["member_path"] for item in errors] == ["x.swc", "y.swc", "z.swc"]
    for axis, item in zip("xyz", errors):
        assert f"outside.zip:{axis}.swc:2: {axis} coordinate" in item["message"]
        assert "exceeds declared stack extent" in item["message"]
    assert not (state_dir / "plan.jsonl.gz").exists()
    assert not (state_dir / "state.json").exists()


def test_run_rechecks_declared_extent_for_previously_planned_members(tmp_path):
    from catmaid_skeleton_batch_import.orchestrator import _RunArchive

    archive_path = tmp_path / "old-plan.zip"
    content = b"1 1 10.001 0 0 -1 -1\n"
    write_zip(archive_path, [("cell.swc", content)])
    with zipfile.ZipFile(archive_path) as archive:
        info = archive.getinfo("cell.swc")
    member = PlannedMember(
        member_path=info.filename, node_count=1, uncompressed_bytes=info.file_size,
        crc32=info.CRC, display_name="cell", fragment_id="cell", cable_length_nm=0,
    )
    with _RunArchive(request(archive_path), settings()) as archive:
        with pytest.raises(InvalidInputError, match="exceeds declared stack extent"):
            archive.load(member)


def test_append_without_declared_stack_keeps_no_upper_coordinate_limit(tmp_path):
    from catmaid_skeleton_batch_import.orchestrator import _RunArchive

    archive_path = tmp_path / "append.zip"
    write_zip(archive_path, [("cell.swc", b"1 1 1000000000 0 0 -1 -1\n")])
    ingestion_request = IngestionRequest(1, archive_path, "um", target_project_id=123)
    scanned = scan_archive(ingestion_request, settings())
    with _RunArchive(ingestion_request, settings()) as archive:
        assert archive.load(scanned.members[0])[0].x == 1_000_000_000


def test_reports_member_metadata_errors_and_fragment_collisions_together(tmp_path: Path) -> None:
    archive = tmp_path / "mixed.zip"
    symlink = zipfile.ZipInfo("bad-link.swc")
    symlink.create_system = 3
    symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
    write_zip(archive, [
        ("a/same.swc", VALID_SWC), ("b/ same .SWC", VALID_SWC),
        (symlink, b"target"), ("empty.swc", b"# no nodes\n"),
        ("oversize.swc", b"#" * 100),
    ])
    with pytest.raises(InvalidInputError) as failure:
        scan_archive(request(archive), settings(max_swc_bytes=90))
    errors = {item["member_path"]: item["message"]
              for item in failure.value.details["member_errors"]}
    assert len(errors) == failure.value.details["invalid_swc_count"] == 5
    assert "duplicate fragment identifier" in errors["a/same.swc"]
    assert "duplicate fragment identifier" in errors["b/ same .SWC"]
    assert "not a regular file" in errors["bad-link.swc"]
    assert "no SWC nodes" in errors["empty.swc"]
    assert "limit is 90" in errors["oversize.swc"]
