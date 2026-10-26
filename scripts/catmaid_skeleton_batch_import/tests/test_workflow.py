import json
import os
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from uuid import uuid4

from catmaid_skeleton_batch_import import workflow, workflow_io as io


def environment(tmp_path):
    ident = str(uuid4())
    return {
        "SKELETON_INGESTION_ID": ident,
        "SKELETON_INGESTION_WORKSPACE": str(tmp_path / ident),
        "SKELETON_INGESTION_WORKFLOW_NAME": "test-workflow",
        "SKELETON_INGESTION_WORKFLOW_UID": "test-uid",
    }


def decision(outcome):
    value = {"workflow_name": "test-workflow", "workflow_uid": "test-uid", "outcome": outcome}
    if outcome == "succeeded":
        value.update(project_id=81, batches=2, skeletons=3, nodes=6,
                     archive={"sha256": "a" * 64, "size_bytes": 12})
    else:
        value["error"] = {"category": "operation_failed", "message": "Failed"}
    return io.FinalizationDecision(value, outcome == "succeeded")


def test_all_stages_run_once_in_one_process_and_expose_terminal_evidence(tmp_path):
    env = environment(tmp_path)
    with patch.object(io, "execute", return_value=0) as download, \
         patch.object(workflow.progress, "run", return_value=0) as run, \
         patch.object(io, "derive_finalization", return_value=decision("succeeded")):
        assert workflow.execute(env) == 0
    assert download.call_count == 1
    assert [c.args[0] for c in run.call_args_list] == ["preflight", "plan", "run"]
    record = json.loads((Path(env["SKELETON_INGESTION_WORKSPACE"]) / workflow.TERMINAL_FILENAME).read_text())
    assert record["schema_version"] == 1 and record["terminal"]
    assert record["result"]["outcome"] == "succeeded"


def test_download_failure_skips_every_database_stage(tmp_path):
    env = environment(tmp_path)
    with patch.object(io, "execute", return_value=2), \
         patch.object(workflow.progress, "run") as run, \
         patch.object(io, "derive_finalization", return_value=decision("failed")):
        assert workflow.execute(env) == 2
    run.assert_not_called()


def test_failed_stage_stops_without_retry_and_retains_evidence(tmp_path):
    env = environment(tmp_path)
    with patch.object(io, "execute", return_value=0), \
         patch.object(workflow.progress, "run", side_effect=[0, 2]) as run, \
         patch.object(io, "derive_finalization", return_value=decision("failed")):
        assert workflow.execute(env) == 2
    assert [c.args[0] for c in run.call_args_list] == ["preflight", "plan"]
    assert (Path(env["SKELETON_INGESTION_WORKSPACE"]) / workflow.TERMINAL_FILENAME).exists()


def test_existing_workspace_is_not_relabelled_or_overwritten(tmp_path):
    env = environment(tmp_path)
    workspace = Path(env["SKELETON_INGESTION_WORKSPACE"])
    workspace.mkdir()
    (workspace / "workflow-started").touch()
    terminal = workspace / workflow.TERMINAL_FILENAME
    terminal.write_text("previous evidence")
    with patch.object(io, "execute") as download:
        assert workflow.execute(env) == 4
    download.assert_not_called()
    assert terminal.read_text() == "previous evidence"


def test_preflight_readiness_failure_is_confirmed_before_loading(tmp_path):
    env = environment(tmp_path)
    with patch.object(io, "execute", return_value=0), \
         patch.object(workflow.progress, "run", return_value=3), \
         patch.object(io, "derive_finalization", return_value=decision("operator_attention")):
        assert workflow.execute(env) == 3
    record = json.loads((Path(env["SKELETON_INGESTION_WORKSPACE"]) / workflow.TERMINAL_FILENAME).read_text())
    assert record["result"]["outcome"] == "failed"
