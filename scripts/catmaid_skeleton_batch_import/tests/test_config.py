from __future__ import annotations

import json
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.config import (
    DEFAULTS,
    load_operational_settings,
    load_request,
    load_settings,
)
from catmaid_skeleton_batch_import.errors import InvalidInputError


def write_request(path: Path, **source_overrides: object) -> None:
    source = {
        "archive_path": str(path.parent / "input.zip"),
        "coordinate_unit": "um",
    }
    source.update(source_overrides)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": source,
                "stack": {
                    "dimension": [10, 20, 30],
                    "resolution_nm": [4, 5.5, 6],
                },
            }
        ),
        encoding="utf-8",
    )


def test_request_is_strict_and_normalized(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path)

    request = load_request(request_path)

    assert request.archive_path == (tmp_path / "input.zip").resolve()
    assert request.coordinate_unit == "um"
    assert request.scale_nm == 1000
    assert request.max_nm == (40.0, 110.0, 180.0)


@pytest.mark.parametrize("source_overrides", [
    {"coordinate_scale_nm": [1, 1, 1]},
    {"external_skeleton_id_map_path": "mapping.csv"},
])
def test_request_rejects_unknown_input_policy(
    tmp_path: Path, source_overrides: dict[str, object],
) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path, **source_overrides)

    with pytest.raises(InvalidInputError, match="unknown fields"):
        load_request(request_path)


@pytest.mark.parametrize("dimension,resolution", [
    ([10, 20, 30], [1e308, 1, 1]),
    ([10**400, 20, 30], [1, 1, 1]),
])
def test_request_rejects_nonfinite_physical_stack_extent(tmp_path, dimension, resolution):
    path = tmp_path / "request.json"
    write_request(path)
    value = json.loads(path.read_text())
    value["stack"] = {"dimension": dimension, "resolution_nm": resolution}
    path.write_text(json.dumps(value))
    with pytest.raises(InvalidInputError, match="positive finite extents"):
        load_request(path)


def test_settings_use_module_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    for name in (
        "CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH",
        "CATMAID_BATCH_IMPORT_MAX_ARCHIVE_BYTES",
        "CATMAID_BATCH_IMPORT_MAX_SWC_BYTES",
        "CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES",
        "CATMAID_BATCH_IMPORT_CACHE_CELL_SIZE_NM",
        "CATMAID_BATCH_IMPORT_LOD_LEVELS",
        "CATMAID_BATCH_IMPORT_LOD_BUCKET_SIZE",
        "CATMAID_BATCH_IMPORT_LOD_STRATEGY",
        "CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS",
        "CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = load_settings()

    assert settings.max_nodes_per_batch == DEFAULTS["max_nodes_per_batch"]
    assert settings.max_archive_bytes == 2 * 1024**3
    assert settings.max_total_nodes is None
    assert settings.cache_cell_size_nm == 1_500_000
    assert settings.lod_levels == 1


def test_environment_can_set_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "42")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES", "100")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_LOD_LEVELS", "7")

    settings = load_settings()

    assert settings.max_nodes_per_batch == 42
    assert settings.max_total_nodes == 100
    assert settings.lod_levels == 7


def test_run_resolves_only_operational_timeout_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_LOD_STRATEGY", "not-a-strategy")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "not-an-integer")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS", "123")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS", "456")

    assert load_operational_settings() == {
        "statement_timeout_ms": 123,
        "lock_timeout_ms": 456,
    }


def test_removed_swc_count_environment_setting_has_no_effect(monkeypatch):
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_SWC_COUNT", "1")
    settings = load_settings()
    assert not hasattr(settings, "max_swc_count")
    assert "max_swc_count" not in settings.plan_dict()


def test_legacy_settings_are_read_without_rewriting_obsolete_metadata(monkeypatch):
    from catmaid_skeleton_batch_import.config import settings_from_state
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    settings = load_settings()
    legacy = {**settings.plan_dict(), "max_swc_count": 1}
    restored = settings_from_state(legacy, settings.operational_dict())
    assert restored == settings
    assert legacy["max_swc_count"] == 1
