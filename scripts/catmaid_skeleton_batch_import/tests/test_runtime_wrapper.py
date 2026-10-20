from __future__ import annotations

import os
import json
import subprocess
import sys
from pathlib import Path


WRAPPER = Path(__file__).parents[2] / "docker" / "run_catmaid_skeleton_import.py"


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _base_environment(tmp_path: Path, importer: Path) -> dict[str, str]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    projects = tmp_path / "projects"
    projects.mkdir()
    media = tmp_path / "media"
    pg_isready = tmp_path / "pg_isready"
    _write_executable(pg_isready, "#!/bin/bash\nexit 0\n")

    return {
        **os.environ,
        "SKELETON_INGESTION_WORKSPACE": str(workspace),
        "CATMAID_SKELETON_IMPORTER_BIN": str(importer),
        "CATMAID_PG_ISREADY_BIN": str(pg_isready),
        "CATMAID_PROJECTS_DIRECTORY": str(projects),
        "CATMAID_INGESTION_MEDIA_ROOT": str(media),
        "DB_HOST": "catmaid-db",
        "DB_PORT": "5432",
        "DB_NAME": "catmaid",
        "DB_USER": "catmaid_ingestion",
        "DB_PASS": "test-password",
    }


def test_plan_uses_workspace_contract_without_initializing_django(tmp_path: Path) -> None:
    calls = tmp_path / "calls"
    importer = tmp_path / "importer"
    _write_executable(
        importer,
        """#!/bin/bash
printf '%s|%s\\n' "${DJANGO_SETTINGS_MODULE-unset}" "$*" >> "${CALLS_FILE}"
exit 0
""",
    )
    environment = _base_environment(tmp_path, importer)
    workspace = Path(environment["SKELETON_INGESTION_WORKSPACE"])
    (workspace / "request.json").write_text("{}", encoding="utf-8")
    environment["CALLS_FILE"] = str(calls)
    environment["DJANGO_SETTINGS_MODULE"] = "must.not.be.loaded"

    result = subprocess.run(
        [sys.executable, str(WRAPPER), "plan"],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert calls.read_text(encoding="utf-8").strip() == (
        f"unset|plan --request {workspace}/request.json --state-dir {workspace}/state"
    )


def test_run_does_not_retry_retryable_importer_exit(tmp_path: Path) -> None:
    calls = tmp_path / "calls"
    counter = tmp_path / "counter"
    importer = tmp_path / "importer"
    _write_executable(
        importer,
        """#!/bin/bash
count=0
if [[ -f "${COUNTER_FILE}" ]]; then count=$(<"${COUNTER_FILE}"); fi
count=$((count + 1))
printf '%s' "${count}" > "${COUNTER_FILE}"
printf '%s|%s\\n' "${DJANGO_SETTINGS_MODULE-unset}" "$*" >> "${CALLS_FILE}"
exit 3
""",
    )
    environment = _base_environment(tmp_path, importer)
    environment.update(
        {
            "CALLS_FILE": str(calls),
            "COUNTER_FILE": str(counter),
        }
    )

    result = subprocess.run(
        [sys.executable, str(WRAPPER), "run"],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 3, result.stderr
    assert calls.read_text(encoding="utf-8").splitlines() == [
        "mysite.ingestion_settings|run --state-dir "
        f"{environment['SKELETON_INGESTION_WORKSPACE']}/state",
    ]
    marker = json.loads(
        (
            Path(environment["SKELETON_INGESTION_WORKSPACE"])
            / "state"
            / "run-wrapper-result.json"
        ).read_text(encoding="utf-8")
    )
    assert marker == {
        "schema_version": 1,
        "command": "run-wrapper",
        "terminal": True,
        "importer_exit_code": 3,
        "attempts": 1,
    }


def test_run_exposes_catmaid_project_to_installed_importer(tmp_path: Path) -> None:
    importer = tmp_path / "bin" / "importer"
    importer.parent.mkdir()
    environment = _base_environment(tmp_path, importer)
    projects = Path(environment["CATMAID_PROJECTS_DIRECTORY"])
    mysite = projects / "mysite"
    mysite.mkdir()
    (mysite / "__init__.py").write_text("", encoding="utf-8")
    python_path_file = tmp_path / "python-path"
    _write_executable(
        importer,
        f"""#!{sys.executable}
import os
from pathlib import Path
import mysite

Path(os.environ["PYTHON_PATH_FILE"]).write_text(
    os.environ["PYTHONPATH"], encoding="utf-8"
)
""",
    )
    environment["PYTHON_PATH_FILE"] = str(python_path_file)
    environment["PYTHONPATH"] = str(tmp_path / "existing-python-path")

    result = subprocess.run(
        [sys.executable, str(WRAPPER), "run"],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert python_path_file.read_text(encoding="utf-8").split(os.pathsep) == [
        str(projects),
        str(tmp_path / "existing-python-path"),
    ]


def test_successful_retry_clears_previous_failure_before_starting_child(tmp_path: Path) -> None:
    importer = tmp_path / "importer"
    _write_executable(importer, f"""#!{sys.executable}
import os
import sys
from pathlib import Path
state_dir = Path(sys.argv[sys.argv.index('--state-dir') + 1])
if (state_dir / 'run-wrapper-result.json').exists():
    sys.exit(99)
sys.exit(int(os.environ['TEST_IMPORTER_EXIT']))
""")
    environment = _base_environment(tmp_path, importer)
    state_dir = Path(environment["SKELETON_INGESTION_WORKSPACE"]) / "state"
    state_dir.mkdir()
    checkpoint = state_dir / "state.json"
    checkpoint.write_text('{"checkpoint":"keep"}', encoding="utf-8")
    marker = state_dir / "run-wrapper-result.json"
    command = [sys.executable, str(WRAPPER), "run"]

    failed = subprocess.run(command, env=environment | {"TEST_IMPORTER_EXIT": "3"},
                            text=True, capture_output=True, check=False)
    assert failed.returncode == 3, failed.stderr
    assert json.loads(marker.read_text())["importer_exit_code"] == 3

    succeeded = subprocess.run(command, env=environment | {"TEST_IMPORTER_EXIT": "0"},
                               text=True, capture_output=True, check=False)
    assert succeeded.returncode == 0, succeeded.stderr
    assert not marker.exists()
    assert json.loads(checkpoint.read_text()) == {"checkpoint": "keep"}


def test_run_does_not_retry_operator_attention_exit(tmp_path: Path) -> None:
    calls = tmp_path / "calls"
    importer = tmp_path / "importer"
    _write_executable(
        importer,
        """#!/bin/bash
printf 'called\\n' >> "${CALLS_FILE}"
exit 5
""",
    )
    environment = _base_environment(tmp_path, importer)
    environment["CALLS_FILE"] = str(calls)

    result = subprocess.run(
        [sys.executable, str(WRAPPER), "run"],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 5
    assert calls.read_text(encoding="utf-8").splitlines() == ["called"]
    marker = json.loads(
        (
            Path(environment["SKELETON_INGESTION_WORKSPACE"])
            / "state"
            / "run-wrapper-result.json"
        ).read_text(encoding="utf-8")
    )
    assert marker == {
        "schema_version": 1,
        "command": "run-wrapper",
        "terminal": True,
        "importer_exit_code": 5,
        "attempts": 1,
    }
