from __future__ import annotations

import json
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
