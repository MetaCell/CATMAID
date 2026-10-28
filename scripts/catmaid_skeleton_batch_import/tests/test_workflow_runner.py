"""Exercise the real child process, retained logs and termination behavior."""

import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import json
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch
from contextlib import nullcontext

import pytest

from catmaid_skeleton_batch_import import workflow_runner as runner


class RetainedRunnerTests(TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.workspace = Path(self.temporary.name)
        self.child_script = self.workspace / "child.py"

    def start(self, source, setup="", **extra_env):
        self.child_script.write_text(source)
        code = (
            "from catmaid_skeleton_batch_import import workflow_runner as r; "
            f"r.CATMAID_WRAPPER={str(self.child_script)!r}; "
            + (setup + "; " if setup else "")
            + "raise SystemExit(r.run('run'))"
        )
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={
                **os.environ,
                "SKELETON_INGESTION_WORKSPACE": str(self.workspace),
                **extra_env,
            },
        )
        self.addCleanup(self.stop, child)
        return child

    @staticmethod
    def stop(child):
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=20)

    def test_logs_stdout_and_stderr_and_preserves_exit_status(self):
        child = self.start(
            "import sys; print('output', flush=True); print('error', file=sys.stderr, flush=True); sys.exit(7)"
        )
        stdout, stderr = child.communicate(timeout=20)
        self.assertEqual(child.returncode, 7, stderr)
        self.assertEqual(stdout, b"output\nerror\n")
        self.assertEqual((self.workspace / "logs/run.log").read_bytes(), stdout)

    def test_progress_streams_before_child_finishes_and_sigterm_is_forwarded(self):
        child = self.start("import time; print('running', flush=True); time.sleep(120)")
        ready, _, _ = select.select([child.stdout], [], [], 10)
        self.assertTrue(ready, "Progress was buffered until the child finished")
        self.assertEqual(child.stdout.readline(), b"running\n")
        child.send_signal(signal.SIGTERM)
        _, stderr = child.communicate(timeout=20)
        self.assertEqual(child.returncode, 143, stderr)
        self.assertEqual((self.workspace / "logs/run.log").read_bytes(), b"running\n")

    def test_unwritable_log_prevents_import_from_starting(self):
        (self.workspace / "logs").mkdir()
        (self.workspace / "logs/run.log").mkdir()
        marker = self.workspace / "started"
        child = self.start(f"from pathlib import Path; Path({str(marker)!r}).touch()")
        _, stderr = child.communicate(timeout=20)
        self.assertNotEqual(child.returncode, 0)
        self.assertIn(b"IsADirectoryError", stderr)
        self.assertFalse(marker.exists())

    def test_log_failure_stops_and_reaps_running_import(self):
        marker = self.workspace / "pid"
        source = f"import os, time; from pathlib import Path; Path({str(marker)!r}).write_text(str(os.getpid())); print('ready', flush=True); time.sleep(120)"
        setup = "from unittest.mock import patch, MagicMock; output=MagicMock(); output.__enter__.return_value=output; output.write.side_effect=OSError('volume full'); patch.object(r.Path, 'open', return_value=output).start()"
        child = self.start(source, setup=setup)
        _, stderr = child.communicate(timeout=20)
        self.assertNotEqual(child.returncode, 0)
        self.assertIn(b"volume full", stderr)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(marker.read_text()), 0)

    def test_native_progress_changes_while_the_child_is_running(self):
        native = self.workspace / "argo-progress"
        state_dir = self.workspace / "state"
        state_dir.mkdir()
        state = {
            "schema_version": 1, "run": {"status": "running"},
            "plan": {"batch_count": 2},
            "batches": [{"status": "committed"}, {"status": "prepared"}],
            "phases": {
                p: {"status": "pending"}
                for p in ("database_maintenance", "cache", "verification")
            },
        }
        (state_dir / "state.json").write_text(json.dumps(state))
        child = self.start(
            "import time; print('running', flush=True); time.sleep(120)",
            ARGO_PROGRESS_FILE=str(native),
        )
        self.assertEqual(child.stdout.readline(), b"running\n")

        def wait_for(value):
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if native.exists() and native.read_text() == value:
                    return
                time.sleep(0.05)
            self.fail(f"Native Argo progress did not become {value}")

        wait_for("4/15")
        state["batches"][1]["status"] = "committed"
        state["phases"]["database_maintenance"]["status"] = "complete"
        replacement = state_dir / "state.tmp"
        replacement.write_text(json.dumps(state))
        replacement.replace(state_dir / "state.json")
        wait_for("3/5")
        state["phases"]["cache"]["status"] = "running"
        replacement.write_text(json.dumps(state))
        replacement.replace(state_dir / "state.json")
        (state_dir / "cache-progress.json").write_text(json.dumps({
            "schema_version": 1, "completed": 1, "total": 2,
        }))
        wait_for("10/15")
        self.assertIsNone(child.poll())
        child.send_signal(signal.SIGTERM)
        _, stderr = child.communicate(timeout=20)
        self.assertEqual(child.returncode, 143, stderr)

    def test_unwritable_native_progress_does_not_fail_the_import(self):
        destination = self.workspace / "absent" / "progress"
        child = self.start(
            "import sys; print('finished', flush=True); sys.exit(7)",
            ARGO_PROGRESS_FILE=str(destination),
        )
        stdout, stderr = child.communicate(timeout=20)
        self.assertEqual(child.returncode, 7, stderr)
        self.assertEqual(stdout, b"finished\n")


    def test_cancellation_stays_nonzero_when_the_child_exits_successfully(self):
        source = (
            "import signal, sys, time; "
            "signal.signal(signal.SIGTERM, lambda *_args: sys.exit(0)); "
            "signal.signal(signal.SIGINT, lambda *_args: sys.exit(0)); "
            "print('running', flush=True); time.sleep(120)"
        )
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signum=signum):
                child = self.start(source)
                ready, _, _ = select.select([child.stdout], [], [], 10)
                self.assertTrue(ready, "Child did not start")
                self.assertEqual(child.stdout.readline(), b"running\n")
                child.send_signal(signum)
                _, stderr = child.communicate(timeout=20)
                self.assertEqual(child.returncode, 128 + signum, stderr)



class NativeProgressWriteTests(TestCase):
    def test_transient_write_failure_retries_unchanged_counters_then_deduplicates(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = Path(root)
            (workspace / "state").mkdir()
            state_path = workspace / "state/state.json"
            state_path.write_text(json.dumps({
                "schema_version": 1, "run": {"status": "running"}, "plan": {"batch_count": 2},
                "batches": [{"status": "committed"}, {"status": "planned"}],
            }))
            native = workspace / "argo-progress"
            replace_file = os.replace
            attempts = []

            def fail_once_then_replace(source, destination):
                attempts.append(1)
                if len(attempts) == 1:
                    raise OSError("temporary progress write failure")
                replace_file(source, destination)

            stop = Mock()
            stop.wait.side_effect = [False, False, True]
            with patch.object(runner.os, "replace", side_effect=fail_once_then_replace):
                runner._report_progress(workspace, str(native), stop)

            self.assertEqual(len(attempts), 2)
            self.assertEqual(native.read_text(), "4/15")
            self.assertEqual(stop.wait.call_count, 3)
            self.assertEqual(json.loads(state_path.read_text())["batches"][1]["status"], "planned")


@pytest.mark.parametrize("fail_write", [False, True])
def test_stage_restores_callers_signal_handlers_on_success_and_error(tmp_path, monkeypatch, fail_write):
    child = tmp_path / "child.py"
    child.write_text("print('output', flush=True)")
    monkeypatch.setenv("SKELETON_INGESTION_WORKSPACE", str(tmp_path))
    monkeypatch.setattr(runner, "CATMAID_WRAPPER", str(child))
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}

    def caller_handler(_signum, _frame):
        pass

    try:
        for signum in previous:
            signal.signal(signum, caller_handler)
        output = MagicMock()
        output.__enter__.return_value = output
        output.write.side_effect = OSError("volume full")
        with patch.object(runner.Path, "open", return_value=output) if fail_write else nullcontext():
            if fail_write:
                with pytest.raises(OSError, match="volume full"):
                    runner.run("plan")
            else:
                assert runner.run("plan") == 0
        assert all(signal.getsignal(signum) is caller_handler for signum in previous)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
