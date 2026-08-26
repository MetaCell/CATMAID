"""Non-interactive command-line interface."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Sequence

from . import __version__
from .errors import ImporterError, InvalidInputError, RetryableError
from .util import atomic_write_json


class ImportArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise InvalidInputError(message)


def build_parser() -> argparse.ArgumentParser:
    parser = ImportArgumentParser(prog="catmaid-skeleton-import")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="validate inputs and persist a deterministic plan")
    plan.add_argument("--request", type=Path, required=True)
    plan.add_argument("--state-dir", type=Path, required=True)

    run = commands.add_parser("run", help="execute or resume the prepared ingestion")
    run.add_argument("--state-dir", type=Path, required=True)
    run.add_argument(
        "--rebuild-cache",
        action="store_true",
        help="cleanly rebuild the completed importer cache and rerun verification",
    )
    return parser


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _emit(value: dict[str, object]) -> None:
    print(
        json.dumps(
            _json_safe(value),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _error_result(
    exc: ImporterError,
    *,
    command: str | None,
    state_dir: Path | None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "schema_version": 1,
        "ok": False,
        "command": command,
        "importer_version": __version__,
        "error": {
            "category": exc.category,
            "message": str(exc),
            "details": _json_safe(exc.details),
        },
    }
    if state_dir is not None:
        result["state_dir"] = str(state_dir.resolve())
    return result


def _persist_error(state_dir: Path | None, result: dict[str, object]) -> None:
    if state_dir is None or not state_dir.is_dir():
        return
    try:
        atomic_write_json(state_dir / "result.json", _json_safe(result))
    except OSError as exc:
        print(f"Could not persist failure result: {exc}", file=sys.stderr)


def execute(argv: Sequence[str] | None = None) -> int:
    command: str | None = None
    state_dir: Path | None = None
    try:
        args = build_parser().parse_args(argv)
        command = args.command
        state_dir = args.state_dir
        from .orchestrator import plan_ingestion, run_ingestion

        if args.command == "plan":
            result = plan_ingestion(args.request, args.state_dir)
        else:
            result = run_ingestion(
                args.state_dir,
                rebuild_cache=args.rebuild_cache,
            )
        _emit(result)
        return 0
    except ImporterError as exc:
        print(str(exc), file=sys.stderr)
        result = _error_result(exc, command=command, state_dir=state_dir)
        _persist_error(state_dir, result)
        _emit(result)
        return exc.exit_code
    except KeyboardInterrupt:
        exc = RetryableError("Interrupted")
        print(str(exc), file=sys.stderr)
        result = _error_result(exc, command=command, state_dir=state_dir)
        _persist_error(state_dir, result)
        _emit(result)
        return exc.exit_code
    except Exception as exc:
        print(f"Unexpected importer failure: {exc}", file=sys.stderr)
        failure = ImporterError(str(exc))
        result = _error_result(
            failure,
            command=command,
            state_dir=state_dir,
        )
        _persist_error(state_dir, result)
        _emit(result)
        return 1


def main() -> None:
    raise SystemExit(execute())
