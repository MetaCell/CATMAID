"""Focused tests for the standalone skeleton-ingestion worker."""

from __future__ import annotations

import hashlib
import io
import json
import tempfile
from collections.abc import Mapping
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from botocore import UNSIGNED
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from catmaid_skeleton_batch_import import workflow_io as worker
from catmaid_skeleton_batch_import import archive_sources as sources
from test_archive_sources import (
    AWS_URL,
    GCS_URL,
    TEST_KEY,
)

INGESTION_ID = "0198f4af-0b6e-7f57-b55f-644215c4e6c2"


def _write_json(path: Path, value: dict[str, object]) -> bytes:
    """Write fixture JSON in the same canonical form used by the worker."""

    payload = (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return payload


def _finalization_payload(workspace, workflow_name, workflow_uid=None, workflow_result=None):
    return worker.derive_finalization(worker.FinalizeConfig(
        ingestion_id=workspace.name,
        workspace=workspace,
        workflow_name=workflow_name,
        workflow_uid=workflow_uid,
        workflow_result=workflow_result or "",
    )).payload


class _StreamingBody:
    """Small S3 streaming-body double that records bounded reads and closing."""

    def __init__(self, chunks: list[bytes], *, error_after: int | None = None):
        self._chunks = list(chunks)
        self._reads = 0
        self._error_after = error_after
        self.read_sizes: list[int] = []
        self.closed = False

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        if self._error_after is not None and self._reads >= self._error_after:
            raise OSError("fixture stream disconnected")
        self._reads += 1
        return self._chunks.pop(0) if self._chunks else b""

    def close(self) -> None:
        self.closed = True


class _S3Client:
    """Minimal get_object double for the downloader."""

    def __init__(self, response: dict[str, object]):
        self.response = response
        self.calls: list[dict[str, object]] = []

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        return self.response


class _HttpResponse(_StreamingBody):
    def __init__(self, chunks, *, length=None, error_after=None, status=200):
        super().__init__(chunks, error_after=error_after)
        self.status = status
        self.headers = {} if length is None else {"Content-Length": str(length)}


class SkeletonIngestionWorkerTestCase(TestCase):
    """Provide isolated ingestion workspaces and representative configurations."""

    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)
        self.root = Path(self._temporary_directory.name)
        self.workspace = self.root / INGESTION_ID
        self.workspace.mkdir()

    def download_config(
        self,
        *,
        max_archive_bytes: int = 1024,
        uri: str = "s3://public-neurons/archive/skeletons.zip",
    ) -> worker.DownloadConfig:
        source = worker.parse_public_s3_uri(uri)
        return worker.DownloadConfig(
            ingestion_id=INGESTION_ID,
            workspace=self.workspace,
            source=source,
            max_archive_bytes=max_archive_bytes,
            coordinate_unit="um",
            target_project_id=41,
        )

    def download_environment(self) -> dict[str, str]:
        return {
            "SKELETON_INGESTION_ID": INGESTION_ID,
            "SKELETON_INGESTION_WORKSPACE": str(self.workspace),
            "SKELETON_INGESTION_S3_URI": ("s3://public-neurons/archive/skeletons.zip"),
            "SKELETON_INGESTION_COORDINATE_UNIT": "um",
            "SKELETON_INGESTION_TARGET_PROJECT_ID": "41",
            "SKELETON_INGESTION_MAX_ARCHIVE_BYTES": "1048576",
        }

    def write_importer_artifact(
        self, filename: str, value: Mapping[str, object]
    ) -> dict[str, object]:
        state_directory = self.workspace / worker.STATE_DIRECTORY
        payload = _write_json(state_directory / filename, dict(value))
        return {
            "schema_version": worker.SCHEMA_VERSION,
            "path": filename,
            "count": 1,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def write_completed_append_state(self) -> dict[str, object]:
        _write_json(self.workspace / worker.STATE_DIRECTORY / "project-ingestion-lock.json", {
            "schema_version": 1, "project_id": 41, "owner": INGESTION_ID, "status": "released",
        })
        _write_json(
            self.workspace / worker.REQUEST_FILENAME,
            {"schema_version": 1, "source": {"archive_path": str(self.workspace / "source.zip"), "coordinate_unit": "um"}, "target": {"project_id": 41}},
        )
        context = {
            "schema_version": worker.SCHEMA_VERSION,
            "project_id": 41,
            "existing_project": True,
        }
        verification = {
            "schema_version": worker.SCHEMA_VERSION,
            "verified": True,
        }
        project_context = self.write_importer_artifact("project-context.json", context)
        verification_artifact = self.write_importer_artifact(
            "verification.json", verification
        )
        state: dict[str, object] = {
            "schema_version": worker.SCHEMA_VERSION,
            "status": "complete",
            "archive": {"sha256": "a" * 64, "size_bytes": 27},
            "plan": {
                "batch_count": 2,
                "skeleton_count": 7,
                "node_count": 101,
            },
            "project": {
                "status": "committed",
                "mode": "existing",
                "project_id": 41,
            },
            "batches": [
                {"status": "committed"},
                {"status": "committed"},
            ],
            "phases": {
                "database_maintenance": {"status": "complete"},
                "cache": {"status": "complete"},
                "verification": {"status": "complete"},
            },
            "artifacts": {
                "project_context": project_context,
                "verification": verification_artifact,
            },
            "run": {"status": "complete"},
        }
        _write_json(
            self.workspace / worker.STATE_DIRECTORY / worker.IMPORTER_STATE_FILENAME,
            state,
        )
        return state

    def write_incomplete_append_state(self, statuses, *, project_id=41):
        state = self.write_completed_append_state()
        state["status"] = "failed"
        state["run"] = {"status": "failed", "reason": "Source fragment already exists"}
        state["batches"] = [{"status": status} for status in statuses]
        state["project"].update(project_id=project_id)
        state["phases"] = {
            name: {"status": "pending"} for name in worker._REQUIRED_PHASES
        }
        state["artifacts"].pop("verification")
        state["artifacts"]["project_context"] = self.write_importer_artifact(
            "project-context.json",
            {"schema_version": 1, "project_id": project_id, "existing_project": True},
        )
        _write_json(
            self.workspace / worker.REQUEST_FILENAME,
            {"schema_version": 1, "source": {
                "archive_path": str(self.workspace / "source.zip"), "coordinate_unit": "um",
            }, "target": {"project_id": project_id}},
        )
        _write_json(self.workspace / "state/state.json", state)
        _write_json(self.workspace / "state/result.json", {
            "schema_version": 1, "command": "run", "ok": False,
            "error": {"category": "invalid_input", "message": "Source fragment already exists"},
        })
        return state

    def write_planned_state(self) -> dict[str, object]:
        state: dict[str, object] = {
            "schema_version": worker.SCHEMA_VERSION,
            "status": "planned",
            "archive": {"sha256": "a" * 64, "size_bytes": 27},
            "plan": {
                "batch_count": 1,
                "skeleton_count": 1,
                "node_count": 10,
            },
            "project": {
                "status": "planned",
                "project_id": None,
                "stack_id": None,
            },
            "batches": [{"status": "planned"}],
            "phases": {
                "database_maintenance": {"status": "pending"},
                "cache": {"status": "pending"},
                "verification": {"status": "pending"},
            },
            "artifacts": {},
            "run": {"status": "pending"},
        }
        _write_json(
            self.workspace / worker.STATE_DIRECTORY / worker.IMPORTER_STATE_FILENAME,
            state,
        )
        return state


class PublicS3ConfigurationTests(SkeletonIngestionWorkerTestCase):
    """Reject ambiguous locations before the worker makes any network request."""

    def test_parses_only_canonical_public_s3_object_locations(self) -> None:
        parsed = worker.parse_public_s3_uri("s3://public-neurons/path/to/skeletons.zip")

        self.assertEqual(
            parsed.bucket,
            "public-neurons",
            "The bucket should be extracted without rewriting it",
        )
        self.assertEqual(
            parsed.key,
            "path/to/skeletons.zip",
            "The complete object key should be retained",
        )
        self.assertEqual(
            parsed.uri,
            "s3://public-neurons/path/to/skeletons.zip",
            "The immutable source URI should remain available for provenance",
        )

        invalid_uris = [
            "https://public-neurons.s3.amazonaws.com/skeletons.zip",
            "S3://public-neurons/skeletons.zip",
            "s3://public-neurons",
            "s3://user@public-neurons/skeletons.zip",
            "s3://public-neurons:443/skeletons.zip",
            "s3://public-neurons/skeletons.zip?versionId=secret",
            "s3://public-neurons/skeletons.zip#fragment",
            "s3://Bad_Bucket/skeletons.zip",
            "s3://127.0.0.1/skeletons.zip",
            "s3://bad..bucket/skeletons.zip",
            "s3://public-neurons/key\nname",
        ]
        for uri in invalid_uris:
            with self.subTest(uri=uri):
                with self.assertRaises(
                    worker.InvalidWorkerInput,
                    msg=f"The worker must reject the unsafe or ambiguous URI {uri!r}",
                ):
                    worker.parse_public_s3_uri(uri)

    def test_load_download_config_normalizes_the_full_environment_contract(
        self,
    ) -> None:
        config = worker.load_download_config(self.download_environment())

        self.assertEqual(
            config.ingestion_id,
            INGESTION_ID,
            "The canonical ingestion UUID should be preserved",
        )
        self.assertEqual(
            config.workspace,
            self.workspace.resolve(),
            "The validated ingestion workspace should be absolute and resolved",
        )
        self.assertEqual(config.target_project_id, 41)
        self.assertEqual(
            config.max_archive_bytes,
            1048576,
            "The archive limit should be parsed as a positive integer",
        )

    def test_load_download_config_rejects_noncanonical_values(self) -> None:
        invalid_overrides = {
            "uppercase UUID": {
                "SKELETON_INGESTION_ID": INGESTION_ID.upper(),
            },
            "workspace for another ingestion": {
                "SKELETON_INGESTION_WORKSPACE": str(self.root / "another"),
            },
            "home-relative workspace": {
                "SKELETON_INGESTION_WORKSPACE": f"~/{INGESTION_ID}",
            },
            "implicit coordinate unit": {
                "SKELETON_INGESTION_COORDINATE_UNIT": "voxel",
            },
            "missing managed target": {"SKELETON_INGESTION_TARGET_PROJECT_ID": ""},
            "noninteger target": {"SKELETON_INGESTION_TARGET_PROJECT_ID": "1.5"},
            "nonpositive target": {"SKELETON_INGESTION_TARGET_PROJECT_ID": "0"},
            "zero archive limit": {
                "SKELETON_INGESTION_MAX_ARCHIVE_BYTES": "0",
            },
        }
        for description, override in invalid_overrides.items():
            environment = {**self.download_environment(), **override}
            with self.subTest(case=description):
                with self.assertRaises(
                    worker.InvalidWorkerInput,
                    msg=f"Configuration must reject {description}",
                ):
                    worker.load_download_config(environment)

    def test_unsigned_s3_client_never_passes_credential_arguments(self) -> None:
        client = object()
        client_factory = Mock(return_value=client)
        fake_boto3 = SimpleNamespace(client=client_factory)

        with patch.object(worker, "boto3", fake_boto3):
            actual = worker._new_unsigned_s3_client()

        self.assertIs(
            actual,
            client,
            "The configured boto3 client should be returned unchanged",
        )
        self.assertEqual(
            client_factory.call_count,
            1,
            "The boto3 client factory should be invoked exactly once",
        )
        positional, keyword = client_factory.call_args
        self.assertEqual(
            positional,
            ("s3",),
            "The worker should construct only an S3 client",
        )
        self.assertEqual(
            set(keyword),
            {"config"},
            "No credential, profile, endpoint, or region argument may be injected",
        )
        config = keyword["config"]
        self.assertEqual(
            config.signature_version,
            UNSIGNED,
            "Public-object access must explicitly disable request signing",
        )
        self.assertTrue(
            config.ignore_configured_endpoint_urls,
            "Configured endpoint overrides must not redirect the public S3 client",
        )
        self.assertEqual(
            config.retries["total_max_attempts"],
            1,
            "The outer downloader owns retries, without multiplying SDK attempts",
        )


class DownloadWorkerTests(SkeletonIngestionWorkerTestCase):
    """Exercise bounded streaming, atomic publication, and retry idempotency."""

    def test_streams_archive_and_publishes_request_and_result_atomically(self) -> None:
        archive = b"PK\x03\x04fixture-skeleton-archive"
        body = _StreamingBody([archive[:8], archive[8:]])
        s3 = _S3Client(
            {
                "Body": body,
                "ContentLength": len(archive),
                "ETag": '"etag-value"',
                "VersionId": "public-version",
            }
        )

        result = worker.download_public_archive(self.download_config(), s3_client=s3)

        self.assertEqual(
            s3.calls,
            [{"Bucket": "public-neurons", "Key": "archive/skeletons.zip"}],
            "The worker should address exactly the validated bucket and object key",
        )
        self.assertTrue(
            body.closed,
            "The S3 streaming body must close after a successful download",
        )
        self.assertTrue(
            body.read_sizes,
            "The archive must be consumed through the streaming read interface",
        )
        self.assertTrue(
            all(
                size == worker.DEFAULT_DOWNLOAD_CHUNK_BYTES for size in body.read_sizes
            ),
            "Every body read should use the configured bounded chunk size",
        )
        self.assertEqual(
            (self.workspace / worker.SOURCE_FILENAME).read_bytes(),
            archive,
            "Only the complete archive should be visible at source.zip",
        )
        self.assertFalse(
            (self.workspace / worker.PART_FILENAME).exists(),
            "The staging .part file must disappear after atomic installation",
        )
        expected_digest = hashlib.sha256(archive).hexdigest()
        self.assertEqual(
            result["archive"],
            {
                "path": worker.SOURCE_FILENAME,
                "size_bytes": len(archive),
                "sha256": expected_digest,
                "etag": "etag-value",
                "version_id": "public-version",
            },
            "The durable result must identify the exact downloaded bytes",
        )
        request = json.loads((self.workspace / worker.REQUEST_FILENAME).read_text())
        self.assertEqual(
            request,
            {
                "schema_version": worker.SCHEMA_VERSION,
                "source": {
                    "archive_path": str(
                        (self.workspace / worker.SOURCE_FILENAME).resolve()
                    ),
                    "coordinate_unit": "um",
                },
                "target": {"project_id": 41},
            },
            "The importer request should contain only normalized local inputs",
        )
        persisted_result = json.loads(
            (self.workspace / worker.DOWNLOAD_RESULT_FILENAME).read_text()
        )
        self.assertEqual(
            persisted_result,
            result,
            "download-result.json should be the durable completion marker",
        )

    def test_completed_download_is_verified_without_recontacting_s3(self) -> None:
        archive = b"repeatable archive"
        first_body = _StreamingBody([archive])
        config = self.download_config()
        first_result = worker.download_public_archive(
            config,
            s3_client=_S3Client({"Body": first_body, "ContentLength": len(archive)}),
        )
        forbidden_client = Mock()

        second_result = worker.download_public_archive(
            config, s3_client=forbidden_client
        )

        self.assertEqual(
            second_result,
            first_result,
            "A verified completion marker should make download retries idempotent",
        )
        self.assertEqual(
            forbidden_client.get_object.call_count,
            0,
            "An idempotent retry must not issue another S3 request",
        )

    def test_completed_download_rejects_a_changed_source(self) -> None:
        archive = b"immutable archive"
        original = self.download_config()
        worker.download_public_archive(
            original,
            s3_client=_S3Client(
                {"Body": _StreamingBody([archive]), "ContentLength": len(archive)}
            ),
        )

        with self.assertRaises(
            worker.ImmutableWorkspaceError,
            msg="A retry must not relabel durable bytes as a different S3 object",
        ):
            worker.download_public_archive(
                self.download_config(uri="s3://public-neurons/archive/different.zip"),
                s3_client=Mock(),
            )

        self.assertEqual(
            (self.workspace / worker.SOURCE_FILENAME).read_bytes(),
            archive,
            "An immutable-workspace error must preserve the completed archive",
        )

    def test_rejects_advertised_oversize_before_reading_or_writing(self) -> None:
        body = _StreamingBody([b"never read"])

        with self.assertRaises(
            worker.InvalidWorkerInput,
            msg="An advertised size above the configured cap must fail terminally",
        ):
            worker.download_public_archive(
                self.download_config(max_archive_bytes=8),
                s3_client=_S3Client({"Body": body, "ContentLength": 9}),
            )

        self.assertEqual(
            body.read_sizes,
            [],
            "Oversized advertised content should be rejected before consuming bytes",
        )
        self.assertTrue(body.closed, "Rejected S3 response bodies must still close")
        self.assertFalse(
            (self.workspace / worker.SOURCE_FILENAME).exists(),
            "No final archive should exist after an early size rejection",
        )
        self.assertFalse(
            (self.workspace / worker.PART_FILENAME).exists(),
            "No partial file should remain after an early size rejection",
        )

    def test_enforces_actual_size_when_content_length_understates_the_body(
        self,
    ) -> None:
        body = _StreamingBody([b"1234", b"5678"])

        with self.assertRaises(
            worker.InvalidWorkerInput,
            msg="Actual streamed bytes must remain authoritative over ContentLength",
        ):
            worker.download_public_archive(
                self.download_config(max_archive_bytes=6),
                s3_client=_S3Client({"Body": body, "ContentLength": 4}),
            )

        self.assertTrue(body.closed, "An over-limit stream must always be closed")
        self.assertFalse(
            (self.workspace / worker.PART_FILENAME).exists(),
            "Over-limit partial bytes must be removed",
        )
        self.assertFalse(
            (self.workspace / worker.SOURCE_FILENAME).exists(),
            "Over-limit bytes must never be promoted to source.zip",
        )

    def test_public_s3_interrupted_stream_restarts_without_appending_bytes(self):
        archive = b"PKcomplete-archive"
        raw_first = _StreamingBody([b"partial"], error_after=1)
        first = StreamingBody(raw_first, len(archive))
        raw_second = io.BytesIO(archive)
        second = StreamingBody(raw_second, len(archive))
        client = SimpleNamespace(get_object=Mock(side_effect=[
            {"Body": first, "ContentLength": len(archive)},
            {"Body": second, "ContentLength": len(archive)},
        ]))
        with patch.object(worker.time, "sleep") as sleep:
            result = worker.download_public_archive(self.download_config(), s3_client=client)
        self.assertEqual(client.get_object.call_count, 2)
        sleep.assert_called_once_with(1)
        self.assertTrue(raw_first.closed and raw_second.closed)
        self.assertEqual((self.workspace / worker.SOURCE_FILENAME).read_bytes(), archive)
        self.assertEqual(result["archive"]["sha256"], hashlib.sha256(archive).hexdigest())
        self.assertFalse((self.workspace / worker.PART_FILENAME).exists())

    def test_public_s3_short_body_is_retried_before_publishing(self):
        archive = b"complete"
        first = _StreamingBody([b"short"])
        second = _StreamingBody([archive])
        client = SimpleNamespace(get_object=Mock(side_effect=[
            {"Body": first, "ContentLength": len(archive)},
            {"Body": second, "ContentLength": len(archive)},
        ]))
        with patch.object(worker.time, "sleep"):
            result = worker.download_public_archive(self.download_config(), s3_client=client)
        self.assertTrue(result["ok"])
        self.assertEqual(client.get_object.call_count, 2)
        self.assertTrue(first.closed and second.closed)
        self.assertEqual((self.workspace / worker.SOURCE_FILENAME).read_bytes(), archive)

    def test_public_s3_stream_retries_are_bounded_and_clean_partial_bytes(self):
        bodies = [_StreamingBody([b"partial"], error_after=1)
                  for _ in range(worker.DEFAULT_DOWNLOAD_MAX_ATTEMPTS)]
        client = SimpleNamespace(get_object=Mock(side_effect=[{"Body": b} for b in bodies]))
        with patch.object(worker.time, "sleep") as sleep, self.assertRaises(
            worker.RetryableWorkerError
        ):
            worker.download_public_archive(self.download_config(), s3_client=client)
        self.assertEqual(client.get_object.call_count, 3)
        self.assertEqual([c.args for c in sleep.call_args_list], [(1,), (2,)])
        self.assertTrue(all(b.closed for b in bodies))
        self.assertFalse((self.workspace / worker.PART_FILENAME).exists())
        self.assertFalse((self.workspace / worker.SOURCE_FILENAME).exists())
        self.assertFalse((self.workspace / worker.REQUEST_FILENAME).exists())

    def test_public_s3_transient_request_failure_uses_the_same_retry_budget(self):
        error = ClientError({"Error": {"Code": "SlowDown"}}, "GetObject")
        body = _StreamingBody([b"archive"])
        client = SimpleNamespace(get_object=Mock(side_effect=[
            error, error, {"Body": body, "ContentLength": 7},
        ]))
        with patch.object(worker.time, "sleep") as sleep:
            result = worker.download_public_archive(self.download_config(), s3_client=client)
        self.assertTrue(result["ok"])
        self.assertEqual(client.get_object.call_count, 3)
        self.assertEqual([c.args for c in sleep.call_args_list], [(1,), (2,)])

    def test_public_s3_terminal_request_failures_are_never_retried(self):
        for code in ("AccessDenied", "NoSuchKey"):
            with self.subTest(code=code):
                error = ClientError({"Error": {"Code": code}}, "GetObject")
                client = SimpleNamespace(get_object=Mock(side_effect=error))
                with patch.object(worker.time, "sleep") as sleep, self.assertRaises(
                    worker.InvalidWorkerInput
                ):
                    worker.download_public_archive(self.download_config(), s3_client=client)
                self.assertEqual(client.get_object.call_count, 1)
                sleep.assert_not_called()
                self.assertFalse((self.workspace / worker.SOURCE_FILENAME).exists())


class FinalizationPayloadTests(SkeletonIngestionWorkerTestCase):
    """Current single-attempt state and real wrapper markers decide finalization."""

    def test_success_requires_confirmed_project_lock_release(self):
        for status in ("acquiring", "locked", None):
            with self.subTest(status=status):
                self.write_completed_append_state()
                receipt = self.workspace / worker.STATE_DIRECTORY / "project-ingestion-lock.json"
                if status is None:
                    receipt.unlink()
                else:
                    _write_json(receipt, {"schema_version": 1, "project_id": 41,
                                          "owner": INGESTION_ID, "status": status})
                result = _finalization_payload(self.workspace, "test-workflow", "test-uid", "Succeeded")
                self.assertEqual(result["outcome"], "operator_attention")

    def test_preloading_failure_with_unreleased_project_lock_is_not_retryable(self):
        state = self.write_completed_append_state()
        state["status"] = "failed"
        state["run"] = {"status": "failed", "reason": "Interrupted before batch loading"}
        for phase in state["phases"].values():
            phase["status"] = "pending"
        for batch in state["batches"]:
            batch["status"] = "planned"
        _write_json(self.workspace / worker.STATE_DIRECTORY / worker.IMPORTER_STATE_FILENAME, state)
        receipt = self.workspace / worker.STATE_DIRECTORY / "project-ingestion-lock.json"
        for status, outcome in (("acquiring", "operator_attention"), ("locked", "operator_attention"),
                                ("released", "failed")):
            with self.subTest(status=status):
                _write_json(receipt, {"schema_version": 1, "project_id": 41,
                                      "owner": INGESTION_ID, "status": status})
                self.assertEqual(_finalization_payload(self.workspace, "wf", "uid", "Failed")["outcome"], outcome)


    def save(self, state):
        _write_json(self.workspace / "state/state.json", state)

    def result(self, command="run", *, ok=False, category="operation_failed", message="database unavailable"):
        _write_json(self.workspace / "state/result.json", {
            "schema_version": 1, "command": command, "ok": ok,
            **({"error": {"category": category, "message": message}} if not ok else {}),
        })

    def wrapper(self, attempts=0, code=66):
        _write_json(self.workspace / "state/run-wrapper-result.json", {
            "schema_version": 1, "command": "run-wrapper", "terminal": True,
            "attempts": attempts, "importer_exit_code": code,
        })

    def decision(self, workflow_result="Failed"):
        return worker.derive_finalization(worker.FinalizeConfig(
            ingestion_id=INGESTION_ID, workspace=self.workspace, workflow_name="workflow",
            workflow_uid="workflow-uid", workflow_result=workflow_result,
        ))

    def assert_attention(self, category=None):
        decision = self.decision()
        self.assertEqual(decision.payload["outcome"], "operator_attention")
        if category:
            self.assertEqual(decision.payload["error"]["category"], category)
        return decision

    def test_verified_append_and_run_complete_are_authoritative(self):
        self.write_completed_append_state()
        self.result(category="internal_error", message="stale diagnostic")
        decision = self.decision()
        self.assertEqual(decision.payload["outcome"], "succeeded")
        self.assertEqual(decision.payload["project_id"], 41)
        self.assertEqual(decision.payload["nodes"], 101)
        self.assertNotIn("stack_id", decision.payload)

    def test_overall_complete_while_run_running_is_not_terminal_success(self):
        state = self.write_completed_append_state()
        state["run"] = {"status": "running"}
        self.save(state)
        self.result(ok=True)
        result = self.assert_attention().payload
        self.assertNotEqual(result["error"]["category"], "invalid_workspace_state")
        self.assertEqual(self.decision("Succeeded").payload["outcome"], "operator_attention")

    def test_successful_result_and_workflow_alone_never_establish_success(self):
        self.result(ok=True)
        self.assertEqual(self.decision("Succeeded").payload["outcome"], "operator_attention")

    def test_success_requires_every_committed_batch_and_final_phase(self):
        for missing in ("batch", *worker._REQUIRED_PHASES, "run"):
            with self.subTest(missing=missing):
                state = self.write_completed_append_state()
                if missing == "batch":
                    state["batches"][1]["status"] = "prepared"
                elif missing == "run":
                    state.pop("run")
                else:
                    state["phases"][missing]["status"] = "pending"
                self.save(state)
                self.assert_attention("invalid_workspace_state")

    def test_success_requires_matching_target_and_registered_verification(self):
        for missing in ("target", "verification", "digest"):
            with self.subTest(missing=missing):
                state = self.write_completed_append_state()
                if missing == "target":
                    _write_json(self.workspace / "request.json", {
                        "schema_version": 1, "target": {"project_id": 99},
                    })
                elif missing == "verification":
                    state["artifacts"].pop("verification")
                    self.save(state)
                else:
                    (self.workspace / "state/verification.json").write_text("{}")
                self.assert_attention("invalid_workspace_state")

    def test_unknown_state_schema_and_corrupt_state_are_not_safe_failures(self):
        for version in (0, 2, 3, 4, 99):
            with self.subTest(schema_version=version):
                state = self.write_completed_append_state()
                state["schema_version"] = version
                self.save(state)
                self.assert_attention("invalid_workspace_state")
        (self.workspace / "state/state.json").write_text("{broken")
        self.assert_attention("invalid_workspace_state")

    def test_symlinked_state_directory_cannot_supply_success(self):
        self.write_completed_append_state()
        state_dir = self.workspace / "state"
        external = self.root / "external-state"
        state_dir.rename(external)
        state_dir.symlink_to(external, target_is_directory=True)
        self.assert_attention("invalid_workspace_state")

    def test_invalid_planning_input_preserves_bounded_summary_and_full_diagnostics(self):
        self.result("plan", category="invalid_input", message="30000 invalid SWCs; first: missing parent 9999")
        diagnostics = self.workspace / "state/error-details.json"
        diagnostics.write_bytes(b"x" * (worker.MAX_JSON_BYTES + 1))
        decision = self.decision()
        self.assertEqual(decision.payload["outcome"], "failed")
        self.assertEqual(decision.payload["error"]["category"], "invalid_input")
        self.assertIn("missing parent 9999", decision.payload["error"]["message"])
        self.assertNotIn("member_errors", json.dumps(decision.payload))
        self.assertTrue(diagnostics.exists())

    def test_wrapper_setup_failure_supersedes_stale_plan_success(self):
        self.write_planned_state()
        self.result("plan", ok=True)
        self.wrapper(attempts=0, code=66)
        decision = self.decision()
        self.assertEqual(decision.payload["outcome"], "failed")
        self.assertEqual(decision.payload["error"]["category"], "operation_failed")
        self.assertIn("66", decision.payload["error"]["message"])
        self.assertIn("before launching", decision.payload["error"]["message"])
        self.assertTrue((self.workspace / "state/run-wrapper-result.json").exists())

    def test_wrapper_before_child_without_state_is_confirmed_preloading_failure(self):
        self.result("plan", ok=True)
        self.wrapper(attempts=0, code=64)
        self.assertEqual(self.decision().payload["outcome"], "failed")

    def test_launched_wrapper_without_state_requires_inspection(self):
        self.wrapper(attempts=1, code=1)
        result = self.assert_attention("operation_failed").payload
        self.assertIn("code 1", result["error"]["message"])

    def test_launched_wrapper_cannot_use_old_planning_failure_to_claim_no_writes(self):
        self.result("plan", category="invalid_input", message="old planning failure")
        self.wrapper(attempts=1, code=3)
        self.assert_attention("operation_failed")

    def test_run_failure_without_state_requires_inspection_even_with_download_error(self):
        self.result("run", category="internal_error")
        _write_json(self.workspace / "download-result.json", {
            "schema_version": 1, "command": "download", "ok": False,
            "error": {"category": "invalid_input", "message": "stale failure"},
        })
        self.assert_attention("internal_error")

    def test_wrapper_cannot_claim_no_child_after_run_started(self):
        self.write_incomplete_append_state(["planned", "planned"])
        self.wrapper(attempts=0)
        self.assert_attention("invalid_workspace_state")

    def test_completed_run_conflicting_with_wrapper_failure_is_not_cleaned(self):
        self.write_completed_append_state()
        self.wrapper(attempts=1)
        self.assert_attention("invalid_workspace_state")

    def test_wrapper_marker_fields_are_validated(self):
        for field, value in (("attempts", 2), ("attempts", True),
                             ("importer_exit_code", 0), ("terminal", False),
                             ("command", "plan")):
            self.wrapper()
            path = self.workspace / "state/run-wrapper-result.json"
            marker = json.loads(path.read_text())
            marker[field] = value
            _write_json(path, marker)
            self.assert_attention("invalid_workspace_state")

    def test_preloading_failure_with_all_batches_planned_allows_fresh_submission(self):
        state = self.write_incomplete_append_state(["planned", "planned"])
        self.wrapper(attempts=1, code=2)
        decision = self.decision()
        self.assertEqual(decision.payload["outcome"], "failed")
        self.assertEqual(decision.payload["project_id"], 41)
        self.assertEqual(decision.payload["error"], {
            "category": "invalid_input", "message": "Source fragment already exists",
        })
        self.assertEqual(state["run"]["status"], "failed")

    def test_prepared_or_failed_first_batch_can_have_uncertain_commit(self):
        for status in ("prepared", "failed"):
            with self.subTest(status=status):
                self.write_incomplete_append_state([status, "planned"])
                self.assert_attention("invalid_input")

    def test_committed_prefix_followed_by_failure_requires_inspection(self):
        self.write_incomplete_append_state(["committed", "failed"])
        result = self.assert_attention("invalid_input").payload
        self.assertEqual(result["project_id"], 41)
        self.assertEqual(result["batches"], 2)

    def test_post_loading_phase_failures_preserve_diagnostics(self):
        for index, name in enumerate(worker._REQUIRED_PHASES):
            with self.subTest(phase=name):
                state = self.write_completed_append_state()
                state["status"] = "failed"
                state["run"] = {"status": "failed", "reason": name + " interrupted"}
                state["phases"] = {
                    phase: {"status": "complete" if i < index else "failed" if i == index else "pending"}
                    for i, phase in enumerate(worker._REQUIRED_PHASES)
                }
                self.save(state)
                self.result(category="verification_failed" if name == "verification" else "operation_failed",
                            message=name + " failed")
                result = self.assert_attention().payload
                self.assertEqual(result["error"]["message"], name + " failed")
                self.assertNotEqual(result["error"]["category"], "invalid_workspace_state")

    def test_run_reason_is_used_when_result_diagnostic_is_missing(self):
        self.write_incomplete_append_state(["failed", "planned"])
        (self.workspace / "state/result.json").unlink()
        result = self.assert_attention("operation_failed").payload
        self.assertEqual(result["error"]["message"], "Source fragment already exists")

    def test_interrupted_run_with_prepared_batch_remains_blocked(self):
        state = self.write_incomplete_append_state(["prepared", "planned"])
        state["status"] = "database_in_progress"
        state["run"] = {"status": "running"}
        self.save(state)
        (self.workspace / "state/result.json").unlink()
        self.assert_attention("workflow_failed")

    def test_corrupt_optional_result_does_not_hide_failed_run_reason(self):
        self.write_incomplete_append_state(["failed", "planned"])
        (self.workspace / "state/result.json").write_text("{broken")
        self.assert_attention("operation_failed")

    def test_missing_execution_evidence_is_never_safe_to_resubmit(self):
        self.assert_attention("workspace_evidence_missing")
        self.result("plan", ok=True)
        self.assert_attention("workspace_evidence_missing")

    def test_contradictory_run_batch_and_phase_order_are_invalid(self):
        cases = ("run", "batch", "phase", "project")
        for case in cases:
            with self.subTest(case=case):
                state = self.write_incomplete_append_state(["planned", "planned"])
                if case == "run":
                    state["run"]["status"] = "running"
                elif case == "batch":
                    state["batches"] = [{"status": "planned"}, {"status": "committed"}]
                elif case == "phase":
                    state["phases"]["cache"]["status"] = "complete"
                else:
                    state["project"]["status"] = "prepared"
                self.save(state)
                self.assert_attention("invalid_workspace_state")

    def test_error_categories_messages_and_controls_remain_bounded(self):
        self.result("plan", category="x" * 1000, message="bad\n" + "x" * 3000)
        error = self.decision().payload["error"]
        self.assertEqual(len(error["category"]), worker.MAX_ERROR_CATEGORY_CHARS)
        self.assertEqual(len(error["message"]), worker.MAX_ERROR_MESSAGE_CHARS)
        self.assertNotIn("\n", error["message"])

    def test_download_failure_before_planning_is_reported(self):
        _write_json(self.workspace / "download-result.json", {
            "schema_version": 1, "command": "download", "ok": False,
            "error": {"category": "invalid_input", "message": "NoSuchKey"},
        })
        decision = self.decision()
        self.assertEqual(decision.payload["outcome"], "failed")
        self.assertEqual(decision.payload["error"]["message"], "NoSuchKey")


class WorkerCommandContractTests(SkeletonIngestionWorkerTestCase):
    """Keep the downloader CLI output durable and stable."""

    def test_download_command_emits_and_persists_a_stable_failure_result(self) -> None:
        environment = {
            **self.download_environment(),
            "SKELETON_INGESTION_S3_URI": "https://example.test/archive.zip",
        }
        stdout = io.StringIO()
        stderr = io.StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = worker.execute(["download"], environ=environment)

        emitted = json.loads(stdout.getvalue())
        persisted = json.loads(
            (self.workspace / worker.DOWNLOAD_RESULT_FILENAME).read_text()
        )
        expected = {
            "schema_version": worker.SCHEMA_VERSION,
            "ok": False,
            "command": "download",
            "worker_version": worker.WORKER_VERSION,
            "ingestion_id": INGESTION_ID,
            "error": {
                "category": "invalid_input",
                "message": "S3 URI must use the lowercase s3:// scheme",
                "details": {},
            },
        }
        self.assertEqual(
            exit_code,
            worker.InvalidWorkerInput.exit_code,
            "Invalid source configuration should produce the stable terminal exit code",
        )
        self.assertEqual(
            emitted,
            expected,
            "The machine-readable stdout record should have a stable failure schema",
        )
        self.assertEqual(
            persisted,
            expected,
            "The same failure record should survive in download-result.json",
        )
        self.assertIn(
            expected["error"]["message"],  # type: ignore[index]
            stderr.getvalue(),
            "Human-readable stderr should explain the terminal input failure",
        )

    def test_download_command_never_overwrites_a_successful_marker(self) -> None:
        successful_marker = {
            "schema_version": worker.SCHEMA_VERSION,
            "ok": True,
            "command": "download",
            "worker_version": worker.WORKER_VERSION,
            "ingestion_id": INGESTION_ID,
            "sentinel": "already-complete",
        }
        original_bytes = _write_json(
            self.workspace / worker.DOWNLOAD_RESULT_FILENAME,
            successful_marker,
        )
        environment = {
            **self.download_environment(),
            "SKELETON_INGESTION_S3_URI": "not-an-s3-uri",
        }

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            exit_code = worker.execute(["download"], environ=environment)

        self.assertEqual(
            exit_code,
            worker.InvalidWorkerInput.exit_code,
            "The new invalid attempt should still be reported to its caller",
        )
        self.assertEqual(
            (self.workspace / worker.DOWNLOAD_RESULT_FILENAME).read_bytes(),
            original_bytes,
            "Failure persistence must never replace an acknowledged success marker",
        )


class SignedArchiveDownloadTests(SkeletonIngestionWorkerTestCase):
    def signed_environment(self, url=AWS_URL):
        environment = self.download_environment()
        environment.pop("SKELETON_INGESTION_S3_URI")
        environment[sources.SOURCE_ENCRYPTED_ENV] = sources.encrypt_source_url(
            url, INGESTION_ID, TEST_KEY
        )
        environment[sources.SOURCE_KEY_ENV] = TEST_KEY
        return environment

    def test_signed_get_preserves_url_and_keeps_artifacts_and_output_safe(self):
        """Both providers use an exact GET, bounded streaming and the unchanged local CATMAID request."""
        for url in (AWS_URL, GCS_URL):
            with self.subTest(provider=sources.parse_archive_source(url).provider):
                for child in self.workspace.iterdir():
                    child.unlink()
                body = _HttpResponse([b"zip", b"-bytes"], length=9)
                opener = SimpleNamespace(open=Mock(return_value=body))
                output, errors = io.StringIO(), io.StringIO()
                with redirect_stdout(output), redirect_stderr(errors):
                    exit_code = worker.execute(
                        ["download"],
                        environ=self.signed_environment(url),
                        http_opener=opener,
                    )
                self.assertEqual(
                    exit_code,
                    0,
                    "Valid signed downloads should succeed for either cloud",
                )
                request = opener.open.call_args.args[0]
                self.assertEqual(
                    request.full_url,
                    url,
                    "Query order and percent escapes are signature-sensitive",
                )
                self.assertEqual(
                    request.get_method(),
                    "GET",
                    "A presigned GET must never be probed with HEAD",
                )
                self.assertEqual(
                    request.headers,
                    {},
                    "No ambient cloud Authorization header should be added",
                )
                self.assertTrue(body.closed, "Download connections must always close")
                self.assertTrue(
                    all(
                        size == worker.DEFAULT_DOWNLOAD_CHUNK_BYTES
                        for size in body.read_sizes
                    )
                )
                artifacts = (
                    output.getvalue()
                    + errors.getvalue()
                    + "".join(
                        path.read_text() for path in self.workspace.glob("*.json")
                    )
                )
                self.assertNotIn(
                    "private-signature",
                    artifacts,
                    "No output or workspace JSON may contain bearer credentials",
                )
                self.assertNotIn(
                    "private%2Btoken",
                    artifacts,
                    "Temporary AWS tokens must also remain private",
                )
                importer_request = json.loads(
                    (self.workspace / worker.REQUEST_FILENAME).read_text()
                )
                self.assertEqual(
                    set(importer_request["source"]),
                    {"archive_path", "coordinate_unit"},
                    "CATMAID receives a local path, never a cloud URL",
                )
                opener.open.reset_mock()
                worker.download_public_archive(
                    worker.load_download_config(self.signed_environment(url)),
                    http_opener=opener,
                )
                opener.open.assert_not_called()

    def test_retry_restarts_interrupted_download_and_does_not_append_bytes(self):
        """A transient read error restarts from byte zero within the bounded attempt budget."""
        first = _HttpResponse([b"partial"], error_after=1)
        second = _HttpResponse([b"complete"], length=8)
        opener = SimpleNamespace(open=Mock(side_effect=[first, second]))
        with patch.object(worker.time, "sleep"):
            worker.download_public_archive(
                worker.load_download_config(self.signed_environment()),
                http_opener=opener,
            )
        self.assertEqual(
            (self.workspace / worker.SOURCE_FILENAME).read_bytes(),
            b"complete",
            "Retries must replace partial content",
        )
        self.assertTrue(
            first.closed and second.closed, "Every attempt must close its response"
        )
        self.assertEqual(opener.open.call_count, 2)

    def test_terminal_http_failures_are_safe_and_never_retried(self):
        """Expired URLs and redirects must fail without publishing their URL or response body."""
        for code in (301, 302, 307, 308, 400, 403, 404):
            error = HTTPError(AWS_URL, code, AWS_URL, {}, io.BytesIO(AWS_URL.encode()))
            opener = SimpleNamespace(open=Mock(side_effect=error))
            output, errors = io.StringIO(), io.StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                exit_code = worker.execute(
                    ["download"], environ=self.signed_environment(), http_opener=opener
                )
            self.assertEqual(exit_code, worker.InvalidWorkerInput.exit_code)
            self.assertEqual(
                opener.open.call_count, 1, "Terminal HTTP failures must not retry"
            )
            self.assertNotIn(
                "private-signature",
                output.getvalue() + errors.getvalue(),
                "HTTP exceptions must never echo their credential-bearing URL",
            )
            self.assertFalse((self.workspace / worker.PART_FILENAME).exists())

    def test_network_failures_retry_boundedly_without_leaking_exception_text(self):
        """Transport errors can contain the raw URL and must be replaced with a safe message."""
        opener = SimpleNamespace(open=Mock(side_effect=URLError(AWS_URL)))
        with patch.object(worker.time, "sleep"), self.assertRaises(
            worker.RetryableWorkerError
        ) as raised:
            worker.download_public_archive(
                worker.load_download_config(self.signed_environment()),
                http_opener=opener,
            )
        self.assertEqual(
            opener.open.call_count,
            3,
            "Transient failures must have a finite retry budget",
        )
        self.assertNotIn("private-signature", str(raised.exception))

    def test_transient_http_statuses_retry_before_succeeding(self):
        """Provider throttling and service outages may recover without a new ingestion."""
        for code in (429, 503):
            for child in self.workspace.iterdir():
                child.unlink()
            error = HTTPError(AWS_URL, code, AWS_URL, {}, io.BytesIO(AWS_URL.encode()))
            opener = SimpleNamespace(
                open=Mock(side_effect=[error, _HttpResponse([b"zip"], length=3)])
            )
            with patch.object(worker.time, "sleep"):
                result = worker.download_public_archive(
                    worker.load_download_config(self.signed_environment()),
                    http_opener=opener,
                )
            self.assertTrue(
                result["ok"], "Transient HTTP statuses should permit a bounded retry"
            )
            self.assertEqual(opener.open.call_count, 2)

    def test_signed_download_enforces_declared_and_actual_size_caps(self):
        """A signed URL must not bypass the public-download resource limits."""
        for chunks, advertised in (([b"data"], 100), ([b"too much"], None)):
            config = worker.load_download_config(self.signed_environment())
            config = worker.DownloadConfig(
                **{**config.__dict__, "max_archive_bytes": 4}
            )
            body = _HttpResponse(chunks, length=advertised)
            with self.assertRaises(worker.InvalidWorkerInput):
                worker.download_public_archive(
                    config, http_opener=SimpleNamespace(open=Mock(return_value=body))
                )
            self.assertTrue(body.closed)
            self.assertFalse((self.workspace / worker.SOURCE_FILENAME).exists())

    def test_rejects_changed_signed_source_after_completed_download(self):
        """Resume must not relabel downloaded bytes as another object or signature."""
        config = worker.load_download_config(self.signed_environment())
        worker.download_public_archive(
            config,
            http_opener=SimpleNamespace(
                open=Mock(return_value=_HttpResponse([b"zip"]))
            ),
        )
        changed = worker.load_download_config(
            self.signed_environment(AWS_URL + "&versionId=other")
        )
        with self.assertRaises(worker.ImmutableWorkspaceError):
            worker.download_public_archive(changed, http_opener=Mock())

    def test_real_http_opener_disables_proxies_and_refuses_redirects(self):
        """Defense lives in the actual urllib opener, not only mocked HTTP responses."""
        opener = worker.build_opener(worker.ProxyHandler({}), worker._RejectRedirects())
        self.assertFalse(
            any(
                isinstance(handler, worker.ProxyHandler) for handler in opener.handlers
            ),
            "An empty proxy map must install no proxy handler",
        )
        handler = next(
            handler
            for handler in opener.handlers
            if isinstance(handler, worker._RejectRedirects)
        )
        self.assertIsNone(
            handler.redirect_request(
                None, None, 302, "redirect", {}, "https://evil.example"
            ),
            "Redirects must never forward a signed URL",
        )

    def test_unexpected_errors_do_not_echo_signed_urls(self):
        """The outer CLI boundary must also sanitize errors from unanticipated library failures."""
        output, errors = io.StringIO(), io.StringIO()
        with patch.object(
            worker, "download_public_archive", side_effect=RuntimeError(AWS_URL)
        ), redirect_stdout(output), redirect_stderr(errors):
            worker.execute(["download"], environ=self.signed_environment())
        self.assertNotIn("private-signature", output.getvalue() + errors.getvalue())


class DownloadProgressTests(SkeletonIngestionWorkerTestCase):
    def test_real_stream_updates_byte_progress_before_download_finishes(self):
        from catmaid_skeleton_batch_import import workflow_progress as runner
        native = self.root / "native-progress"
        observed = []
        class Body(_StreamingBody):
            def read(inner, size):
                if native.exists():
                    observed.append(native.read_text())
                return super().read(size)
        body = Body([b"abc", b"def"])
        with patch.dict("os.environ", {"ARGO_PROGRESS_FILE":str(native)}), patch.object(worker.time, "monotonic", side_effect=[0, 2, 4]):
            result = worker.download_public_archive(self.download_config(), s3_client=_S3Client({"Body":body,"ContentLength":6}))
        self.assertEqual(observed, ["0/35", "3/35", "6/35"])
        self.assertEqual(result["archive"]["size_bytes"], 6)
        self.assertEqual(native.read_text(), "6/35")

    def test_unknown_length_and_broken_progress_destination_do_not_break_download(self):
        for destination in (self.root / "unknown-progress", self.root / "absent" / "progress"):
            # Each trial uses a fresh workspace to exercise the streaming path.
            for name in ("source.zip", "request.json", "download-result.json"):
                (self.workspace/name).unlink(missing_ok=True)
            with patch.dict("os.environ", {"ARGO_PROGRESS_FILE":str(destination)}):
                result = worker.download_public_archive(self.download_config(), s3_client=_S3Client({"Body":_StreamingBody([b"abc"])}))
            self.assertTrue(result["ok"])
            if destination.exists():
                self.assertEqual(destination.read_text(), "0/5")
