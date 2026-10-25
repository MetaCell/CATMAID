from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.config import (
    DEFAULTS, SETTING_NAMES, load_operational_settings, load_request, load_settings,
)
from catmaid_skeleton_batch_import.errors import InvalidInputError


def write_request(path: Path, **source_overrides: object) -> None:
    source = {"archive_path": str(path.parent / "input.zip"), "coordinate_unit": "um"}
    source.update(source_overrides)
    path.write_text(json.dumps({
        "schema_version": 1, "source": source, "target": {"project_id": 123},
    }))


def test_request_is_strict_and_normalized(tmp_path: Path) -> None:
    path = tmp_path / "request.json"
    write_request(path)
    request = load_request(path)
    assert request.archive_path == (tmp_path / "input.zip").resolve()
    assert request.coordinate_unit == "um"
    assert request.scale_nm == 1000
    assert request.target_project_id == 123
    assert request.to_dict()["target"] == {"project_id": 123}


@pytest.mark.parametrize("source_overrides", [
    {"coordinate_scale_nm": [1, 1, 1]},
    {"external_skeleton_id_map_path": "mapping.csv"},
])
def test_request_rejects_unknown_input_policy(tmp_path, source_overrides):
    path = tmp_path / "request.json"
    write_request(path, **source_overrides)
    with pytest.raises(InvalidInputError, match="unknown fields"):
        load_request(path)


@pytest.mark.parametrize("target", [None, [], {}, {"project_id": 0},
                                    {"project_id": True}, {"project_id": 1.5},
                                    {"project_id": 2147483648}])
def test_request_requires_an_existing_project_id(tmp_path, target):
    path = tmp_path / "request.json"
    write_request(path)
    value = json.loads(path.read_text())
    value["target"] = target
    path.write_text(json.dumps(value))
    with pytest.raises(InvalidInputError):
        load_request(path)


@pytest.mark.parametrize("keep_target", [False, True])
def test_stack_provisioning_requests_are_rejected(tmp_path, keep_target):
    path = tmp_path / "request.json"
    write_request(path)
    value = json.loads(path.read_text())
    value["stack"] = {"dimension": [10, 20, 30], "resolution_nm": [1, 1, 1]}
    if not keep_target:
        del value["target"]
    path.write_text(json.dumps(value))
    with pytest.raises(InvalidInputError, match="unknown fields.*stack"):
        load_request(path)


def test_settings_use_environment_defaults_without_loading_django(monkeypatch):
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "must.not.be.loaded")
    for name in SETTING_NAMES.values():
        monkeypatch.delenv(name, raising=False)
    assert asdict(load_settings()) == DEFAULTS
    assert load_operational_settings() == {"statement_timeout_ms": 0, "lock_timeout_ms": 30000}


def test_environment_can_set_planning_limits(monkeypatch):
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "42")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES", "100")
    settings = load_settings()
    assert settings.max_nodes_per_batch == 42
    assert settings.max_total_nodes == 100


def test_run_resolves_only_operational_timeout_settings(monkeypatch):
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "must.not.be.loaded")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "not-an-integer")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS", "123")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS", "456")
    assert load_operational_settings() == {"statement_timeout_ms": 123, "lock_timeout_ms": 456}
