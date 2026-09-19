from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from catmaid_skeleton_batch_import.config import (
    DEFAULTS,
    load_operational_settings,
    load_request,
    load_settings,
)
from catmaid_skeleton_batch_import.errors import InvalidInputError


def write_request(
    path: Path, annotations: object = None, **source_overrides: object
) -> None:
    source = {
        "archive_path": str(path.parent / "input.zip"),
        "coordinate_unit": "um",
    }
    source.update(source_overrides)
    request = {
        "schema_version": 1,
        "source": source,
        "stack": {
            "dimension": [10, 20, 30],
            "resolution_nm": [4, 5.5, 6],
        },
    }
    if annotations is not None:
        request["annotations"] = annotations
    path.write_text(json.dumps(request), encoding="utf-8")


def test_request_is_strict_and_normalized(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path, external_skeleton_id_map_path="mapping.csv")

    request = load_request(request_path)

    assert request.archive_path == (tmp_path / "input.zip").resolve()
    assert request.external_skeleton_id_map_path == Path("mapping.csv").resolve()
    assert request.coordinate_unit == "um"
    assert request.scale_nm == 1000
    assert request.max_nm == (40.0, 110.0, 180.0)


def test_request_rejects_unknown_input_policy(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path, coordinate_scale_nm=[1, 1, 1])

    with pytest.raises(InvalidInputError, match="unknown fields"):
        load_request(request_path)


def test_request_accepts_stem_annotation_template(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    templates = ["{stem}", "swc:{stem}"]
    write_request(request_path, annotations=templates)

    request = load_request(request_path)

    assert request.annotations == tuple(templates)
    assert request.to_dict()["annotations"] == templates
    assert request.annotation_names("191805/0000001.swc") == (
        "0000001",
        "swc:0000001",
    )


@pytest.mark.parametrize(
    ("annotations", "message"),
    [
        (["imported"], "must contain {stem}"),
        (["{stem}-{neuron_id}"], "may only use"),
        (["{stem:>8}"], "may only use"),
        (["{stem!r}"], "may only use"),
        (["{stem"], "is malformed"),
        (["{stem}", "{stem}"], "must not repeat"),
        ([""], "empty template"),
        ("{stem}", "array of strings"),
    ],
)
def test_request_rejects_unusable_annotation_templates(
    tmp_path: Path, annotations: object, message: str
) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path, annotations=annotations)

    with pytest.raises(InvalidInputError, match=re.escape(message)):
        load_request(request_path)


def test_settings_use_module_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    for name in (
        "CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH",
        "CATMAID_BATCH_IMPORT_MAX_ARCHIVE_BYTES",
        "CATMAID_BATCH_IMPORT_MAX_SWC_BYTES",
        "CATMAID_BATCH_IMPORT_MAX_SWC_COUNT",
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
    assert settings.max_total_nodes is None
    assert settings.cache_cell_size_nm == 1_500_000


def test_environment_can_set_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "42")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_TOTAL_NODES", "100")

    settings = load_settings()

    assert settings.max_nodes_per_batch == 42
    assert settings.max_total_nodes == 100


def test_run_resolves_only_operational_timeout_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_LOD_STRATEGY", "not-a-strategy")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_SWC_COUNT", "not-an-integer")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_STATEMENT_TIMEOUT_MS", "123")
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_LOCK_TIMEOUT_MS", "456")

    assert load_operational_settings() == {
        "statement_timeout_ms": 123,
        "lock_timeout_ms": 456,
    }
