"""One finite CATMAID pod: download, preflight, plan and import.

Kafka and study state belong to the caller. This command exposes one bounded
terminal result for the workflow executor; retained audit files stay on disk.
"""
from __future__ import annotations

import json
import os
import sys
from enum import Enum

from . import workflow_io as io, workflow_runner as runner
from .project_lock import WorkflowProjectLock

TERMINAL_FILENAME = "terminal-result.json"


class WorkflowStage(Enum):
    """Execution steps, distinct from Argo's five reported progress stages."""

    PROJECT_LOCK = "project_lock"
    DOWNLOAD = "download"
    PREFLIGHT = "preflight"
    PLAN = "plan"
    RUN = "run"


@runner.termination_signals()
def execute(environ=None):
    environment = os.environ if environ is None else environ
    workspace = None
    code = 1
    claimed = False
    stage = WorkflowStage.PROJECT_LOCK
    project_lock = None
    lock_error = None
    try:
        ingestion_id = io._validate_ingestion_id(io._required_env(environment, "SKELETON_INGESTION_ID"))
        workspace = io._workspace_from_environment(environment, ingestion_id, create=True)
        if any(workspace.iterdir()):
            raise io.ImmutableWorkspaceError("This workflow workspace is single-use")
        (workspace / "workflow-started").touch(exist_ok=False)
        claimed = True
        project_lock = WorkflowProjectLock(environment)
        project_lock.acquire()
        stage = WorkflowStage.DOWNLOAD
        code = io.execute([stage.value], environ=environment)
        if code == 0:
            runner.write_native_progress(*runner.stage_units(1))
            for stage in (WorkflowStage.PREFLIGHT, WorkflowStage.PLAN, WorkflowStage.RUN):
                code = runner.run(stage.value)
                if code != 0:
                    break
    except runner.WorkflowInterrupted as exc:
        print(f"Workflow interrupted by signal {exc.signum}", file=sys.stderr)
        code = exc.code
    except io.WorkerError as exc:
        print(io._sanitize_message(exc, "Workflow failed"), file=sys.stderr)
        code = exc.exit_code
        if workspace is not None and claimed:
            io._persist_download_failure(
                environment,
                io._error_result(
                    WorkflowStage.DOWNLOAD.value,
                    environment.get("SKELETON_INGESTION_ID"),
                    exc,
                ),
            )
    except (Exception, KeyboardInterrupt) as exc:
        print(io._sanitize_message(exc, "Workflow interrupted"), file=sys.stderr)
        code = 1
    finally:
        if workspace is not None and claimed:
            try:
                if project_lock is not None:
                    try:
                        project_lock.finish(workspace, run_started=stage == WorkflowStage.RUN)
                    except Exception as exc:
                        lock_error = io._sanitize_message(exc, "Project ingestion lock release was not confirmed")
                decision = io.derive_finalization(io.FinalizeConfig(
                    ingestion_id=environment["SKELETON_INGESTION_ID"],
                    workspace=workspace,
                    workflow_name=io._required_env(environment, "SKELETON_INGESTION_WORKFLOW_NAME"),
                    workflow_uid=io._required_env(environment, "SKELETON_INGESTION_WORKFLOW_UID"),
                    workflow_result="Succeeded" if code == 0 else "Failed",
                ))
                # This process owns a fresh workspace and has not invoked run.
                # Read-only preflight and offline planning cannot write morphology.
                if lock_error is not None:
                    decision.payload.update(outcome="operator_attention", error={
                        "category": "project_ingestion_lock", "message": lock_error,
                    })
                elif code != 0 and stage != WorkflowStage.RUN:
                    decision.payload["outcome"] = "failed"
                    if decision.payload.get("error", {}).get("category") == "workspace_evidence_missing":
                        decision.payload["error"] = {
                            "category": "operation_failed",
                            "message": f"CATMAID {stage.value} exited with code {code} before loading",
                        }
                if code != 0 and stage == WorkflowStage.RUN and decision.payload["outcome"] == "succeeded":
                    decision.payload.update(outcome="operator_attention", error={
                        "category": "workflow_failed",
                        "message": "Import verified, but the processing command did not finish successfully",
                    })
                if decision.payload["outcome"] != "succeeded" and code == 0:
                    code = 1
                record = {"schema_version": 1, "terminal": True, "result": decision.payload}
                if len(json.dumps(record).encode()) > 16 * 1024:
                    raise ValueError("Terminal result exceeds the output-parameter limit")
                io._atomic_write_json(workspace / TERMINAL_FILENAME, record)
                # Retain all files. A notifier without a volume mount cannot
                # perform acknowledgement-dependent source cleanup.
            except Exception as exc:
                print(io._sanitize_message(exc, "Could not publish terminal evidence"), file=sys.stderr)
                if code == 0:
                    code = 1
    return code


if __name__ == "__main__":
    raise SystemExit(execute())
