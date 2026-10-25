"""Strict append requests and environment-only importer configuration."""

from __future__ import annotations

import json
import os
from dataclasses import fields
from pathlib import Path
from typing import Any

from .domain import ImportSettings, IngestionRequest
from .errors import InvalidInputError


SETTING_NAMES = {
    "max_nodes_per_batch": "CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH",
    "max_archive_bytes": "CATMAID_BATCH_IMPORT_MAX_ARCHIVE_BYTES",
    "max_swc_bytes": "CATMAID_BATCH_IMPORT_MAX_SWC_BYTES",
    "max_total_nodes": "CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES",
    "statement_timeout_ms": "CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS",
    "lock_timeout_ms": "CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS",
}

DEFAULTS = {
    "max_nodes_per_batch": 250_000,
    "max_archive_bytes": 2 * 1024**3,
    "max_swc_bytes": 2 * 1024**3,
    "max_total_nodes": None,
}

OPERATIONAL_DEFAULTS = {"statement_timeout_ms": 0, "lock_timeout_ms": 30_000}


def _expect_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown:
        raise InvalidInputError(
            f"{label} contains unknown fields: {', '.join(sorted(unknown))}"
        )
    if missing:
        raise InvalidInputError(
            f"{label} is missing required fields: {', '.join(sorted(missing))}"
        )


def load_request(path: Path) -> IngestionRequest:
    try:
        with path.open("r", encoding="utf-8") as source:
            raw = json.load(source)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidInputError(f"Could not read request {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidInputError("request must be a JSON object")
    _expect_keys(raw, {"schema_version", "source", "target"}, "request")
    if raw["schema_version"] != 1:
        raise InvalidInputError("request.schema_version must be 1")
    target = raw["target"]
    if not isinstance(target, dict):
        raise InvalidInputError("request.target must be an object")
    _expect_keys(target, {"project_id"}, "request.target")
    project_id = target["project_id"]
    if (isinstance(project_id, bool) or not isinstance(project_id, int)
            or not 0 < project_id <= 2_147_483_647):
        raise InvalidInputError(
            "request.target.project_id must be a positive integer no greater than 2147483647"
        )
    source = raw["source"]
    if not isinstance(source, dict):
        raise InvalidInputError("request.source must be an object")
    _expect_keys(source, {"archive_path", "coordinate_unit"}, "request.source")
    archive_value = source["archive_path"]
    if not isinstance(archive_value, str) or not archive_value.strip():
        raise InvalidInputError("request.source.archive_path must be a non-empty string")
    unit = source["coordinate_unit"]
    if unit not in {"nm", "um"}:
        raise InvalidInputError("request.source.coordinate_unit must be 'nm' or 'um'")
    return IngestionRequest(
        schema_version=1,
        archive_path=Path(archive_value).expanduser().resolve(strict=False),
        coordinate_unit=unit,
        target_project_id=project_id,
    )


def _parse_int(value: Any, name: str, *, allow_none: bool = False) -> int | None:
    if allow_none and (value is None or (isinstance(value, str) and value.strip().lower() in {"", "none", "null"})):
        return None
    if isinstance(value, bool):
        raise InvalidInputError(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidInputError(f"{name} must be an integer") from exc
    if parsed < 0 or (parsed == 0 and name not in {
        SETTING_NAMES["statement_timeout_ms"],
        SETTING_NAMES["lock_timeout_ms"],
    }):
        raise InvalidInputError(f"{name} must be positive")
    return parsed


def load_settings() -> ImportSettings:
    """Resolve planning limits from the environment without importing Django."""
    values = {}
    for field in fields(ImportSettings):
        name = SETTING_NAMES[field.name]
        values[field.name] = _parse_int(
            os.environ.get(name, DEFAULTS[field.name]), name,
            allow_none=field.name == "max_total_nodes",
        )
    return ImportSettings(**values)


def load_operational_settings() -> dict[str, int]:
    """Resolve only the database timeouts used during this run."""
    result = {}
    for field_name, default in OPERATIONAL_DEFAULTS.items():
        name = SETTING_NAMES[field_name]
        value = _parse_int(os.environ.get(name, default), name)
        assert value is not None
        result[field_name] = value
    return result
