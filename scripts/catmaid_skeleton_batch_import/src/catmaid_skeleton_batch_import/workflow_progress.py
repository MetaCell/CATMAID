"""Finite CATMAID subprocess with retained logs; no NeuroGlass transport."""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading

CATMAID_WRAPPER = "/home/scripts/docker/run_catmaid_skeleton_import.py"


FINAL_PHASES = ("database_maintenance", "cache", "verification")
STAGE_COUNT = 5
PROGRESS_ANNOTATION = "neuroglass.metacell.us/ingestion-progress"
PROGRESS_VERSION = "stages-v1"
MAX_STATE_BYTES = 4 * 1024 * 1024
BATCH_STATUSES = {"planned", "prepared", "committed", "failed"}


def stage_units(stage, completed=0, total=0):
    """Encode stage and exact counters in Argo's sole N/M progress field.

    Each of five stages owns total+1 units. The extra unit distinguishes
    a fully counted stage from the start of the next stage. This is a work
    position, never an estimate of elapsed time.
    """
    width = total + 1
    return (stage - 1) * width + completed, STAGE_COUNT * width


def write_native_progress(completed, total, *, filename=None):
    """Return whether the best-effort observation was successfully published."""
    filename = filename or os.environ.get("ARGO_PROGRESS_FILE")
    if not filename:
        return False
    destination = Path(filename)
    temporary = destination.with_name(destination.name + ".tmp")
    try:
        temporary.write_text(f"{completed}/{total}")
        os.replace(temporary, destination)
    except OSError:
        return False
    return True


def progress_units(state, cache_progress=None):
    """Stage position with confirmed batch counts or successful cell counts."""
    if not isinstance(state, dict) or state.get("schema_version") != 1:
        return None
    plan = state.get("plan")
    total = plan.get("batch_count") if isinstance(plan, dict) else None
    batches = state.get("batches")
    if (
        type(total) is not int
        or total <= 0
        or not isinstance(batches, list)
        or len(batches) != total
    ):
        return None
    if any(
        not isinstance(b, dict)
        or not isinstance(b.get("status"), str)
        or b["status"] not in BATCH_STATUSES
        for b in batches
    ):
        return None
    completed = sum(b["status"] == "committed" for b in batches)
    if completed < total:
        return stage_units(2, completed, total)
    phases = state.get("phases")
    if not isinstance(phases, dict):
        return None
    for stage, name in enumerate(FINAL_PHASES, start=3):
        phase = phases.get(name)
        if not isinstance(phase, dict):
            return None
        if phase.get("status") != "complete":
            if name == "cache" and phase.get("status") == "running":
                value = cache_progress
                if isinstance(value, dict) and value.get("schema_version") == 1:
                    done, count = value.get("completed"), value.get("total")
                    if type(done) is int and type(count) is int and 0 <= done <= count:
                        return stage_units(stage, done, count)
            return stage_units(stage)
    return STAGE_COUNT, STAGE_COUNT


def _cache_progress(workspace):
    try:
        with (workspace / "state" / "cache-progress.json").open("rb") as source:
            data = source.read(1025)
        return json.loads(data) if len(data) <= 1024 else None
    except (OSError, ValueError):
        return None


def _report_progress(workspace, filename, stop):
    """Best effort native Argo reporting; never change import exit status."""
    state_path = workspace / "state" / "state.json"
    previous = None
    while True:
        try:
            with state_path.open("rb") as source:
                data = source.read(MAX_STATE_BYTES + 1)
            units = (
                progress_units(json.loads(data), _cache_progress(workspace))
                if len(data) <= MAX_STATE_BYTES
                else None
            )
            if units is not None and units != previous:
                if write_native_progress(*units, filename=filename):
                    previous = units
        except (OSError, ValueError, TypeError):
            pass
        if stop.wait(1):
            return


def run(action):
    workspace = Path(os.environ["SKELETON_INGESTION_WORKSPACE"])
    logs = workspace / "logs"
    logs.mkdir(exist_ok=True)
    # Open before starting CATMAID: a broken volume cannot start an unlogged import.
    with (logs / f"{action}.log").open("ab", buffering=0) as output:
        child = subprocess.Popen(
            [sys.executable, CATMAID_WRAPPER, action],
            start_new_session=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        def forward(signum, _frame):
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signum)
                except ProcessLookupError:
                    pass

        signal.signal(signal.SIGTERM, forward)
        signal.signal(signal.SIGINT, forward)
        stop = threading.Event()
        reporter = None
        if action == "run" and os.environ.get("ARGO_PROGRESS_FILE"):
            reporter = threading.Thread(
                target=_report_progress,
                args=(workspace, os.environ["ARGO_PROGRESS_FILE"], stop),
                daemon=True,
            )
            reporter.start()
        try:
            while chunk := child.stdout.read1(64 * 1024):
                output.write(chunk)
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            code = child.wait()
        except BaseException:
            forward(signal.SIGTERM, None)
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                forward(signal.SIGKILL, None)
                child.wait()
            raise
        finally:
            stop.set()
            if reporter is not None:
                reporter.join(timeout=2)
            child.stdout.close()
    return 128 - code if code < 0 else code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("preflight", "plan", "run"))
    raise SystemExit(run(parser.parse_args().action))
