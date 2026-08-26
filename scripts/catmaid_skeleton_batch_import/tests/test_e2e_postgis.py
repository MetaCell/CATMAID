from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

import pytest


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("CATMAID_BATCH_IMPORT_E2E") != "1",
        reason="set CATMAID_BATCH_IMPORT_E2E=1 to mutate a configured test database",
    ),
]


def test_known_rollback_preserves_prior_batch_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.errors import RetryableError
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion, run_ingestion
    from catmaid_skeleton_batch_import.state import StateStore

    archive_path = tmp_path / "skeletons.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "alpha.swc",
            "1 1 1 1 1 -1 -1\n2 3 2 1 1 0.5 1\n3 3 3 2 1 0.4 2\n",
        )
        archive.writestr(
            "beta.swc",
            "10 1 4 4 2 1 -1\n11 3 4 5 2 0.7 10\n12 3 5 6 2.5 0.6 11\n",
        )
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": {
                    "archive_path": str(archive_path),
                    "coordinate_unit": "um",
                },
                "stack": {
                    "dimension": [100, 100, 100],
                    "resolution_nm": [1000, 1000, 1000],
                },
            }
        ),
        encoding="utf-8",
    )
    state_dir = tmp_path / "state"
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "3")
    plan_ingestion(request_path, state_dir)

    original = materialization.materialize_and_verify_batch
    calls = 0

    def fail_second_batch(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RetryableError("injected materialization failure")
        return original(**kwargs)

    monkeypatch.setattr(
        materialization, "materialize_and_verify_batch", fail_second_batch
    )
    with pytest.raises(RetryableError, match="injected materialization failure"):
        run_ingestion(state_dir)

    state = StateStore(state_dir).load()
    assert [batch["status"] for batch in state["batches"]] == [
        "committed",
        "retryable",
    ]

    monkeypatch.setattr(materialization, "materialize_and_verify_batch", original)
    result = run_ingestion(state_dir)
    assert result["status"] == "ready_for_publication"
    assert result["nodes"] == 6
    assert result["skeletons"] == 2
    assert [batch["status"] for batch in StateStore(state_dir).load()["batches"]] == [
        "committed",
        "committed",
    ]
