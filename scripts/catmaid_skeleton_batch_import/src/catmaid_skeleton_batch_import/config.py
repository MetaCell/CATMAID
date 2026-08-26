"""Strict request parsing and deployment setting resolution."""

from __future__ import annotations

import json
import math
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
    "max_swc_count": "CATMAID_BATCH_IMPORT_MAX_SWC_COUNT",
    "max_total_nodes": "CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES",
    "cache_cell_size_nm": "CATMAID_BATCH_IMPORT_CACHE_CELL_SIZE_NM",
    "lod_levels": "CATMAID_BATCH_IMPORT_LOD_LEVELS",
    "lod_bucket_size": "CATMAID_BATCH_IMPORT_LOD_BUCKET_SIZE",
    "lod_strategy": "CATMAID_BATCH_IMPORT_LOD_STRATEGY",
    "statement_timeout_ms": "CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS",
    "lock_timeout_ms": "CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS",
}


DEFAULTS: dict[str, int | str | None] = {
    "max_nodes_per_batch": 250_000,
    "max_archive_bytes": 10 * 1024**3,
    "max_swc_bytes": 2 * 1024**3,
    "max_swc_count": 250_000,
    "max_total_nodes": None,
    "cache_cell_size_nm": 1_500_000,
    "lod_levels": 7,
    "lod_bucket_size": 500,
    "lod_strategy": "quadratic",
    "statement_timeout_ms": 0,
    "lock_timeout_ms": 30_000,
}


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


def _positive_int_tuple(value: Any, label: str) -> tuple[int, int, int]:
    if not isinstance(value, list) or len(value) != 3:
        raise InvalidInputError(f"{label} must be an array of three positive integers")
    result: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise InvalidInputError(f"{label} must contain only positive integers")
        result.append(item)
    return tuple(result)  # type: ignore[return-value]


def _positive_number_tuple(value: Any, label: str) -> tuple[float, float, float]:
    if not isinstance(value, list) or len(value) != 3:
        raise InvalidInputError(f"{label} must be an array of three positive numbers")
    result: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise InvalidInputError(f"{label} must contain only positive numbers")
        number = float(item)
        if not math.isfinite(number) or number <= 0:
            raise InvalidInputError(f"{label} must contain only positive finite numbers")
        result.append(number)
    return tuple(result)  # type: ignore[return-value]


def load_request(path: Path) -> IngestionRequest:
    try:
        with path.open("r", encoding="utf-8") as source:
            raw = json.load(source)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidInputError(f"Could not read request {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise InvalidInputError("request must be a JSON object")
    _expect_keys(raw, {"schema_version", "source", "stack"}, "request")
    if raw["schema_version"] != 1:
        raise InvalidInputError("request.schema_version must be 1")
    if not isinstance(raw["source"], dict):
        raise InvalidInputError("request.source must be an object")
    if not isinstance(raw["stack"], dict):
        raise InvalidInputError("request.stack must be an object")

    source = raw["source"]
    required_source = {"archive_path", "coordinate_unit"}
    allowed_source = required_source | {"external_skeleton_id_map_path"}
    unknown_source = set(source) - allowed_source
    missing_source = required_source - set(source)
    if unknown_source:
        raise InvalidInputError(
            "request.source contains unknown fields: "
            + ", ".join(sorted(unknown_source))
        )
    if missing_source:
        raise InvalidInputError(
            "request.source is missing required fields: "
            + ", ".join(sorted(missing_source))
        )
    _expect_keys(raw["stack"], {"dimension", "resolution_nm"}, "request.stack")

    archive_value = source["archive_path"]
    if not isinstance(archive_value, str) or not archive_value.strip():
        raise InvalidInputError("request.source.archive_path must be a non-empty string")
    archive_path = Path(archive_value).expanduser().resolve(strict=False)

    unit = source["coordinate_unit"]
    if unit not in {"nm", "um"}:
        raise InvalidInputError("request.source.coordinate_unit must be 'nm' or 'um'")

    mapping_path = None
    if "external_skeleton_id_map_path" in source:
        mapping_value = source["external_skeleton_id_map_path"]
        if not isinstance(mapping_value, str) or not mapping_value.strip():
            raise InvalidInputError(
                "request.source.external_skeleton_id_map_path must be a non-empty string"
            )
        mapping_path = Path(mapping_value).expanduser().resolve(strict=False)

    return IngestionRequest(
        schema_version=1,
        archive_path=archive_path,
        coordinate_unit=unit,
        dimension=_positive_int_tuple(raw["stack"]["dimension"], "request.stack.dimension"),
        resolution_nm=_positive_number_tuple(
            raw["stack"]["resolution_nm"], "request.stack.resolution_nm"
        ),
        external_skeleton_id_map_path=mapping_path,
    )


_MISSING = object()


def _django_value(name: str) -> Any:
    if not os.environ.get("DJANGO_SETTINGS_MODULE"):
        return _MISSING
    try:
        from django.conf import settings

        return getattr(settings, name) if hasattr(settings, name) else _MISSING
    except Exception as exc:
        raise InvalidInputError(
            f"Could not initialize Django while resolving {name}: {exc}"
        ) from exc


def _raw_setting(field_name: str) -> Any:
    name = SETTING_NAMES[field_name]
    django_value = _django_value(name)
    if django_value is not _MISSING:
        return django_value
    if name in os.environ:
        return os.environ[name]
    return DEFAULTS[field_name]


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
    values: dict[str, Any] = {}
    for field in fields(ImportSettings):
        raw = _raw_setting(field.name)
        name = SETTING_NAMES[field.name]
        if field.name == "lod_strategy":
            strategy = str(raw)
            if strategy not in {"linear", "quadratic", "exponential"}:
                raise InvalidInputError(
                    f"{name} must be linear, quadratic, or exponential"
                )
            values[field.name] = strategy
        else:
            values[field.name] = _parse_int(
                raw, name, allow_none=field.name == "max_total_nodes"
            )

    return ImportSettings(**values)


def load_operational_settings() -> dict[str, int]:
    """Resolve only settings that are intentionally mutable between retries."""

    result: dict[str, int] = {}
    for field_name in ("statement_timeout_ms", "lock_timeout_ms"):
        name = SETTING_NAMES[field_name]
        value = _parse_int(_raw_setting(field_name), name)
        assert value is not None
        result[field_name] = value
    return result


def settings_from_state(
    plan_values: dict[str, Any], operational: dict[str, int]
) -> ImportSettings:
    """Combine immutable planned settings with current operational timeouts."""
    return ImportSettings(
        **plan_values,
        statement_timeout_ms=operational["statement_timeout_ms"],
        lock_timeout_ms=operational["lock_timeout_ms"],
    )
