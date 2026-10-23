#!/usr/bin/env python3
"""Run the skeleton importer safely inside a short-lived CATMAID job.

The importer CLI deliberately knows nothing about Kubernetes or CATMAID image
startup. This entrypoint supplies that boundary: ``plan`` stays independent
from Django; ``preflight`` and ``run`` wait for PostgreSQL and
select the ingestion-only Django settings. It forwards termination signals and
records a failed run as terminal for the workflow notifier. Argo invokes this script rather than
starting CATMAID's web process.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence


EX_USAGE = 64
EX_NOINPUT = 66
RETRYABLE_IMPORTER_EXIT = 3
REQUEST_FILENAME = "request.json"
STATE_DIRECTORY = "state"
RUN_RESULT_FILENAME = "run-wrapper-result.json"

_active_child: subprocess.Popen[bytes] | None = None


class EntrypointError(RuntimeError):
    def __init__(self, message: str, exit_code: int = EX_USAGE):
        super().__init__(message)
        self.exit_code = exit_code


def _environment_value(environ: Mapping[str, str], name: str) -> str:
    return str(environ.get(name, "")).strip()


def _required_environment_value(environ: Mapping[str, str], name: str) -> str:
    value = _environment_value(environ, name)
    if not value:
        raise EntrypointError(
            f"{name} is required for CATMAID skeleton ingestion"
        )
    return value


def _bounded_positive_integer(
    environ: Mapping[str, str],
    name: str,
    default: int,
    maximum: int,
) -> int:
    raw = _environment_value(environ, name) or str(default)
    try:
        value = int(raw)
    except ValueError as exc:
        raise EntrypointError(
            f"{name} must be an integer between 1 and {maximum}"
        ) from exc
    if value < 1 or value > maximum or raw != str(value):
        raise EntrypointError(
            f"{name} must be an integer between 1 and {maximum}"
        )
    return value


def _workspace(environ: Mapping[str, str]) -> Path:
    raw = _environment_value(environ, "SKELETON_INGESTION_WORKSPACE")
    workspace = Path(raw)
    if not raw or not workspace.is_absolute() or workspace == Path("/"):
        raise EntrypointError(
            "SKELETON_INGESTION_WORKSPACE must be a non-root absolute path"
        )
    if not workspace.is_dir():
        raise EntrypointError(
            f"Ingestion workspace does not exist: {workspace}",
            EX_NOINPUT,
        )
    return workspace


def _forward_signal(signum: int, _frame: object) -> None:
    child = _active_child
    if child is not None and child.poll() is None:
        child.send_signal(signum)
        return
    raise SystemExit(128 + signum)


def _run_child(command: Sequence[str], *, environ: Mapping[str, str]) -> int:
    global _active_child
    try:
        _active_child = subprocess.Popen(command, env=dict(environ))
    except OSError as exc:
        raise EntrypointError(f"Could not start {command[0]}: {exc}", EX_NOINPUT) from exc
    try:
        return_code = _active_child.wait()
    finally:
        _active_child = None
    return 128 - return_code if return_code < 0 else return_code


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_terminal_marker(
    state_directory: Path,
    *,
    importer_exit_code: int,
    attempts: int,
) -> None:
    state_directory.mkdir(parents=True, exist_ok=True)
    path = state_directory / RUN_RESULT_FILENAME
    payload = {
        "schema_version": 1,
        "command": "run-wrapper",
        "terminal": True,
        "importer_exit_code": importer_exit_code,
        "attempts": attempts,
    }
    data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor, temporary_name = tempfile.mkstemp(
        dir=state_directory,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(data)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        _fsync_directory(state_directory)
    finally:
        temporary.unlink(missing_ok=True)


def _clear_terminal_marker(state_directory: Path) -> None:
    """A new invocation must not inherit the previous wrapper's failure."""
    try:
        (state_directory / RUN_RESULT_FILENAME).unlink()
        _fsync_directory(state_directory)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise EntrypointError(
            f"Could not reset the previous CATMAID wrapper result: {exc}", EX_NOINPUT
        ) from exc


def _prepare(workspace: Path, importer: str, environ: Mapping[str, str], *, action: str) -> int:
    request = workspace / REQUEST_FILENAME
    if not request.is_file():
        raise EntrypointError(
            f"Ingestion request does not exist: {request}",
            EX_NOINPUT,
        )
    child_environment = dict(environ)
    if action == "preflight":
        _projects_directory, child_environment = _database_environment(environ)
    else:
        child_environment.pop("DJANGO_SETTINGS_MODULE", None)
    return _run_child(
        (
            importer,
            action,
            "--request",
            str(request),
            "--state-dir",
            str(workspace / STATE_DIRECTORY),
        ),
        environ=child_environment,
    )


def _wait_for_database(
    *,
    pg_isready: str,
    host: str,
    port: int,
    database: str,
    user: str,
    wait_seconds: int,
    environ: Mapping[str, str],
) -> bool:
    deadline = time.monotonic() + wait_seconds
    print(f"Waiting for CATMAID database {host}:{port}...", file=sys.stderr)
    command = (
        pg_isready,
        "--quiet",
        "--host",
        host,
        "--port",
        str(port),
        "--dbname",
        database,
        "--username",
        user,
    )
    while True:
        try:
            ready = subprocess.run(
                command,
                env=dict(environ),
                check=False,
            ).returncode == 0
        except OSError as exc:
            raise EntrypointError(
                f"Could not run PostgreSQL readiness check: {exc}", EX_NOINPUT
            ) from exc
        if ready:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def _database_environment(environ: Mapping[str, str]) -> tuple[Path, dict[str, str]]:
    database_host = _required_environment_value(environ, "DB_HOST")
    database_name = _required_environment_value(environ, "DB_NAME")
    database_user = _required_environment_value(environ, "DB_USER")
    _required_environment_value(environ, "DB_PASS")
    database_port = _bounded_positive_integer(environ, "DB_PORT", 5432, 65535)
    wait_seconds = _bounded_positive_integer(
        environ, "CATMAID_INGESTION_DB_WAIT_SECONDS", 120, 3600
    )
    projects_directory = Path(
        _environment_value(environ, "CATMAID_PROJECTS_DIRECTORY")
        or "/home/django/projects"
    )
    if not projects_directory.is_dir():
        raise EntrypointError(
            f"CATMAID projects directory does not exist: {projects_directory}",
            EX_NOINPUT,
        )
    media_root = Path(
        _environment_value(environ, "CATMAID_INGESTION_MEDIA_ROOT")
        or "/tmp/catmaid-skeleton-ingestion-media"
    )
    media_root.mkdir(parents=True, exist_ok=True)

    run_environment = dict(environ)
    existing_python_path = _environment_value(environ, "PYTHONPATH")
    python_path = str(projects_directory)
    if existing_python_path:
        python_path = os.pathsep.join((python_path, existing_python_path))
    run_environment.update(
        {
            "DB_PORT": str(database_port),
            "DJANGO_SETTINGS_MODULE": "mysite.ingestion_settings",
            # The importer is an installed console script, so Python otherwise
            # starts module discovery from /home/env/bin rather than CATMAID's
            # project root.  Changing cwd alone is not sufficient for imports
            # such as mysite.ingestion_settings.
            "PYTHONPATH": python_path,
            "PYTHONUNBUFFERED": "1",
        }
    )
    pg_isready = (
        _environment_value(environ, "CATMAID_PG_ISREADY_BIN")
        or "/usr/bin/pg_isready"
    )
    if not _wait_for_database(
        pg_isready=pg_isready,
        host=database_host,
        port=database_port,
        database=database_name,
        user=database_user,
        wait_seconds=wait_seconds,
        environ=run_environment,
    ):
        raise EntrypointError(
            f"CATMAID database was not ready after {wait_seconds} seconds", RETRYABLE_IMPORTER_EXIT,
        )
    return projects_directory, run_environment


def _run(workspace: Path, importer: str, environ: Mapping[str, str]) -> int:
    _clear_terminal_marker(workspace / STATE_DIRECTORY)
    projects_directory, run_environment = _database_environment(environ)

    original_directory = Path.cwd()
    try:
        os.chdir(projects_directory)
        status = _run_child(
            (
                importer,
                "run",
                "--state-dir",
                str(workspace / STATE_DIRECTORY),
            ),
            environ=run_environment,
        )
        if status != 0:
            try:
                _write_terminal_marker(
                    workspace / STATE_DIRECTORY,
                    importer_exit_code=status,
                    attempts=1,
                )
            except OSError as exc:
                print(
                    f"Could not persist the terminal CATMAID wrapper result: {exc}",
                    file=sys.stderr,
                )
        return status
    finally:
        os.chdir(original_directory)


def execute(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(prog="run-catmaid-skeleton-import")
    parser.add_argument("action", choices=("preflight", "plan", "run"))
    arguments = parser.parse_args(argv)
    environment = os.environ if environ is None else environ
    try:
        workspace = _workspace(environment)
        importer = (
            _environment_value(environment, "CATMAID_SKELETON_IMPORTER_BIN")
            or "/home/env/bin/catmaid-skeleton-import"
        )
        if arguments.action in {"preflight", "plan"}:
            return _prepare(workspace, importer, environment, action=arguments.action)
        return _run(workspace, importer, environment)
    except EntrypointError as exc:
        print(str(exc), file=sys.stderr)
        return exc.exit_code


def main() -> None:
    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)
    raise SystemExit(execute())


if __name__ == "__main__":
    main()
