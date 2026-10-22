from __future__ import annotations

import json
import zipfile
from decimal import Decimal
from pathlib import Path

from catmaid_skeleton_batch_import import cli, orchestrator
from catmaid_skeleton_batch_import.errors import VerificationError
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
    assert [error["member_path"] for error in
            result["error"]["details"]["member_errors"]] == ["a.swc", "b.swc"]
    assert not (state_dir / "plan.jsonl.gz").exists()
    assert not (state_dir / "state.json").exists()
