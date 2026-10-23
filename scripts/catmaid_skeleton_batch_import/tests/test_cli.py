from __future__ import annotations

import json
import zipfile
from decimal import Decimal
from pathlib import Path

from catmaid_skeleton_batch_import import cli, orchestrator
from catmaid_skeleton_batch_import.errors import InvalidInputError, VerificationError
from catmaid_skeleton_batch_import.util import atomic_write_json


def test_failure_result_is_json_safe_and_replaces_stale_success(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    atomic_write_json(state_dir / "result.json", {"ok": True})

    def fail(_state_dir: Path, **_kwargs: object) -> dict[str, object]:
        raise VerificationError(
            "verification failed",
            details={
                "decimal": Decimal("1.25"),
                "non_finite": float("nan"),
                "path": Path("artifact.json"),
            },
        )

    monkeypatch.setattr(orchestrator, "run_ingestion", fail)
    exit_code = cli.execute(["run", "--state-dir", str(state_dir)])

    assert exit_code == 6
    result = json.loads(capsys.readouterr().out)
    assert result["command"] == "run"
    assert result["error"]["category"] == "verification_failed"
    assert result["error"]["details"] == {
        "decimal": "1.25",
        "non_finite": "nan",
        "path": "artifact.json",
    }
    assert json.loads((state_dir / "result.json").read_text()) == result
    assert result["error"]["details_file"] == "error-details.json"
    diagnostics = json.loads((state_dir / "error-details.json").read_text())
    assert diagnostics["error"]["details"] == result["error"]["details"]


def test_argument_error_has_stable_versioned_json(capsys) -> None:
    exit_code = cli.execute(["run"])

    assert exit_code == 2
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == 1
    assert result["ok"] is False
    assert result["command"] is None
    assert result["error"]["category"] == "invalid_input"


def test_rebuild_cache_flag_is_forwarded(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    observed: dict[str, object] = {}

    def succeed(state_dir: Path, **kwargs: object) -> dict[str, object]:
        observed["state_dir"] = state_dir
        observed.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(orchestrator, "run_ingestion", succeed)
    state_dir = tmp_path / "state"

    exit_code = cli.execute(
        ["run", "--state-dir", str(state_dir), "--rebuild-cache"]
    )

    assert exit_code == 0
    assert observed == {
        "state_dir": state_dir,
        "rebuild_cache": True,
    }
    assert json.loads(capsys.readouterr().out) == {"ok": True}


def test_preflight_is_forwarded(tmp_path, monkeypatch, capsys):
    observed = []
    monkeypatch.setattr(orchestrator, "preflight_ingestion", lambda request, state_dir:
                        observed.append((request, state_dir)) or {"ok": True})
    request = tmp_path / "request.json"
    state_dir = tmp_path / "state"
    assert cli.execute(["preflight", "--request", str(request),
                        "--state-dir", str(state_dir)]) == 0
    assert observed == [(request, state_dir)]
    assert json.loads(capsys.readouterr().out) == {"ok": True}


def test_new_project_preflight_and_plan_do_not_initialize_django(tmp_path, monkeypatch):
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "must.not.be.loaded")
    def forbidden(*_args):
        raise AssertionError("Database lookup during planning")
    monkeypatch.setattr(orchestrator, "read_project_stack_geometry", forbidden)
    archive = tmp_path / "input.zip"
    with zipfile.ZipFile(archive, "w") as source:
        source.writestr("point.swc", "1 1 1 2 3 -1 -1\n")
    request = tmp_path / "request.json"
    atomic_write_json(request, {"schema_version": 1, "source": {
        "archive_path": str(archive), "coordinate_unit": "nm"},
        "stack": {"dimension": [10, 10, 10], "resolution_nm": [1, 1, 1]}})
    state_dir = tmp_path / "state"
    assert orchestrator.preflight_ingestion(request, state_dir)["required"] is False
    assert not (state_dir / "stack-geometry.json").exists()
    assert orchestrator.plan_ingestion(request, state_dir)["ok"]


def test_invalid_archive_reports_all_members_without_writing_a_partial_plan(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    archive = tmp_path / "mixed.zip"
    with zipfile.ZipFile(archive, "w") as source:
        source.writestr("a.swc", "1 1 0 0 0 1 -1\n2 3 1 0 0 1 -1\n")
        source.writestr("b.swc", "1 1 0 0 0 1 -1\n2 3 1 0 0 1 99\n")
        source.writestr("valid.swc", "42 1 1 2 3 1 -1 # point\n")
    request = tmp_path / "request.json"
    atomic_write_json(request, {"schema_version": 1, "source": {
        "archive_path": str(archive), "coordinate_unit": "nm"},
        "stack": {"dimension": [10, 10, 10], "resolution_nm": [1, 1, 1]}})
    state_dir = tmp_path / "state"
    assert cli.execute(["plan", "--request", str(request),
                        "--state-dir", str(state_dir)]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["error"]["category"] == "invalid_input"
    assert result["error"]["details"]["invalid_swc_count"] == 2
    assert "member_errors" not in result["error"]["details"]
    diagnostics = json.loads((state_dir / result["error"]["details_file"]).read_text())
    assert [error["member_path"] for error in
            diagnostics["error"]["details"]["member_errors"]] == ["a.swc", "b.swc"]
    assert not (state_dir / "plan.jsonl.gz").exists()
    assert not (state_dir / "state.json").exists()


def test_large_validation_result_keeps_every_error_outside_the_terminal_summary(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    errors = [
        {"member_path": f"fragment-{i:06d}.swc", "message": "missing parent 9999 " * 5}
        for i in range(30000)
    ]

    def fail(*_args):
        raise InvalidInputError(
            "30000 SWC file(s) failed validation. First error: missing parent 9999",
            details={"invalid_swc_count": len(errors), "member_errors": errors},
        )

    monkeypatch.setattr(orchestrator, "plan_ingestion", fail)
    assert cli.execute(["plan", "--request", str(tmp_path / "request.json"),
                        "--state-dir", str(state_dir)]) == 2
    stdout = capsys.readouterr().out
    result = json.loads(stdout)
    assert len(stdout.encode()) < 16 * 1024
    assert (state_dir / "result.json").stat().st_size < 16 * 1024
    assert result["error"]["category"] == "invalid_input"
    assert result["error"]["details"] == {"invalid_swc_count": 30000}
    assert "missing parent 9999" in result["error"]["message"]
    diagnostics = state_dir / result["error"]["details_file"]
    assert diagnostics.stat().st_size > 4 * 1024 * 1024
    assert json.loads(diagnostics.read_text())["error"]["details"]["member_errors"] == errors
    assert json.loads((state_dir / "result.json").read_text()) == result


def test_large_generic_diagnostics_and_message_do_not_expand_terminal_result(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    message = "verification mismatch " * 1000
    details = {"unexpected_nodes": list(range(100000))}

    def fail(*_args, **_kwargs):
        raise VerificationError(message, details=details)

    monkeypatch.setattr(orchestrator, "run_ingestion", fail)
    assert cli.execute(["run", "--state-dir", str(state_dir)]) == 6
    result = json.loads(capsys.readouterr().out)
    assert len(result["error"]["message"]) == cli.MAX_ERROR_MESSAGE_CHARS
    assert result["error"]["details"] == {}
    assert (state_dir / "result.json").stat().st_size < 16 * 1024
    diagnostics = json.loads((state_dir / result["error"]["details_file"]).read_text())
    assert diagnostics["error"]["message"] == message
    assert diagnostics["error"]["details"] == details


def test_diagnostic_write_failure_still_reports_original_error(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    write = cli.atomic_write_json

    def disk_failure(path, value):
        if path.name == cli.ERROR_DETAILS_FILENAME:
            raise OSError("no space for full diagnostics")
        write(path, value)

    def fail(*_args, **_kwargs):
        raise InvalidInputError("missing parent 9999")

    monkeypatch.setattr(cli, "atomic_write_json", disk_failure)
    monkeypatch.setattr(orchestrator, "run_ingestion", fail)
    assert cli.execute(["run", "--state-dir", str(state_dir)]) == 2
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["error"]["category"] == "invalid_input"
    assert result["error"]["message"] == "missing parent 9999"
    assert "details_file" not in result["error"]
    assert "Could not persist full failure details" in output.err
    assert json.loads((state_dir / "result.json").read_text()) == result
