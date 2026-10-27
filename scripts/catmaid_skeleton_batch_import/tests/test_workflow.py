import json
import os
import signal
import subprocess
import sys

import pytest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from uuid import uuid4

from catmaid_skeleton_batch_import import workflow, workflow_io as io


@pytest.fixture(autouse=True)
def project_lock():
    with patch.object(workflow, "WorkflowProjectLock") as lock:
        yield lock.return_value


def environment(tmp_path):
    ident = str(uuid4())
    return {
        "SKELETON_INGESTION_ID": ident,
        "SKELETON_INGESTION_WORKSPACE": str(tmp_path / ident),
        "SKELETON_INGESTION_WORKFLOW_NAME": "test-workflow",
        "SKELETON_INGESTION_WORKFLOW_UID": "test-uid",
        "SKELETON_INGESTION_TARGET_PROJECT_ID": "81",
    }


def decision(outcome):
    value = {"workflow_name": "test-workflow", "workflow_uid": "test-uid", "outcome": outcome}
    if outcome == "succeeded":
        value.update(project_id=81, batches=2, skeletons=3, nodes=6,
                     archive={"sha256": "a" * 64, "size_bytes": 12})
    else:
        value["error"] = {"category": "operation_failed", "message": "Failed"}
    return io.FinalizationDecision(value)


def test_all_stages_run_once_in_one_process_and_expose_terminal_evidence(tmp_path):
    env = environment(tmp_path)
    with patch.object(io, "execute", return_value=0) as download, \
         patch.object(workflow.progress, "run", return_value=0) as run, \
         patch.object(io, "derive_finalization", return_value=decision("succeeded")):
        assert workflow.execute(env) == 0
    download.assert_called_once_with(["download"], environ=env)
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


@pytest.mark.parametrize("stage,exits", [
    ("preflight", [3]),
    ("plan", [0, 3]),
])
def test_preloading_failure_reports_the_cli_stage_name(tmp_path, project_lock, stage, exits):
    env = environment(tmp_path)
    result = decision("operator_attention")
    result.payload["error"]["category"] = "workspace_evidence_missing"
    with patch.object(io, "execute", return_value=0), \
         patch.object(workflow.progress, "run", side_effect=exits) as run, \
         patch.object(io, "derive_finalization", return_value=result):
        assert workflow.execute(env) == 3

    assert run.call_args.args == (stage,)
    project_lock.finish.assert_called_once_with(
        Path(env["SKELETON_INGESTION_WORKSPACE"]), run_started=False,
    )
    terminal = json.loads(
        (Path(env["SKELETON_INGESTION_WORKSPACE"]) / workflow.TERMINAL_FILENAME).read_text()
    )
    assert terminal["result"]["outcome"] == "failed"
    assert terminal["result"]["error"]["message"] == (
        f"CATMAID {stage} exited with code 3 before loading"
    )


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize("after_stage", ["preflight", "plan"])
def test_signal_between_stages_stops_before_loading(tmp_path, signum, after_stage):
    env = environment(tmp_path)
    stage = tmp_path / "stage.py"
    stage.write_text(
        "import os, sys; from pathlib import Path; "
        "workspace=Path(os.environ['SKELETON_INGESTION_WORKSPACE']); "
        "output=(workspace/'executed-stages').open('a'); "
        "output.write(sys.argv[1]+chr(10)); output.close(); "
        "(workspace/'loading-started').touch() if sys.argv[1]=='run' else None"
    )
    launcher = tmp_path / "workflow.py"
    launcher.write_text("""
import os
import signal
import sys
from catmaid_skeleton_batch_import import workflow, workflow_progress

workflow_progress.CATMAID_WRAPPER = sys.argv[1]
workflow.io.execute = lambda *_args, **_kwargs: 0
from unittest.mock import Mock
workflow.WorkflowProjectLock = lambda *_args: Mock()
original_run = workflow_progress.run

def run(action):
    code = original_run(action)
    if action == sys.argv[3]:
        os.kill(os.getpid(), int(sys.argv[2]))
    return code

workflow_progress.run = run
raise SystemExit(workflow.execute())
""")
    child = subprocess.run(
        [sys.executable, str(launcher), str(stage), str(signum), after_stage],
        env={**os.environ, **env}, capture_output=True, timeout=15,
    )
    workspace = Path(env["SKELETON_INGESTION_WORKSPACE"])
    assert child.returncode == 128 + signum, child.stderr.decode()
    assert not (workspace / "loading-started").exists()
    assert (workspace / "executed-stages").read_text().splitlines() == (
        ["preflight"] if after_stage == "preflight" else ["preflight", "plan"]
    )
    result = json.loads((workspace / workflow.TERMINAL_FILENAME).read_text())
    assert result["terminal"] is True
    assert result["result"]["outcome"] == "failed"


def test_download_and_all_stages_start_after_acquiring_the_lock(tmp_path, project_lock):
    env = environment(tmp_path)
    events = []
    project_lock.acquire.side_effect = lambda: events.append("lock")
    project_lock.finish.side_effect = lambda *_a, **_kw: events.append("finish")
    with patch.object(io, "execute", side_effect=lambda *_a, **_kw: events.append("download") or 0), \
         patch.object(workflow.progress, "run", side_effect=lambda action: events.append(action) or 0), \
         patch.object(io, "derive_finalization", return_value=decision("succeeded")):
        assert workflow.execute(env) == 0
    assert events == ["lock", "download", "preflight", "plan", "run", "finish"]


@pytest.mark.parametrize("failure", ["acquire", "finish"])
def test_lock_uncertainty_cannot_be_reported_as_a_retryable_preparation_failure(tmp_path, project_lock, failure):
    env = environment(tmp_path)
    if failure == "acquire":
        project_lock.acquire.side_effect = RuntimeError("lock connection lost")
    project_lock.finish.side_effect = RuntimeError("lock ownership uncertain")
    with patch.object(io, "execute", return_value=2) as download, \
         patch.object(io, "derive_finalization", return_value=decision("failed")):
        assert workflow.execute(env) != 0
    if failure == "acquire":
        download.assert_not_called()
    record = json.loads((Path(env["SKELETON_INGESTION_WORKSPACE"]) / workflow.TERMINAL_FILENAME).read_text())
    assert record["result"]["outcome"] == "operator_attention"
    assert record["result"]["error"]["category"] == "project_ingestion_lock"
