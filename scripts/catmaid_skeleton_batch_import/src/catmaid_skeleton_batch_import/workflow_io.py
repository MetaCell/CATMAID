"""Download and retained-workspace evidence for the CATMAID workflow."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, NoReturn, cast
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from botocore import UNSIGNED
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import BotoCoreError, ClientError

from . import archive_sources
from .archive_sources import (
    PublicS3Object,
    SignedArchiveSource,
)

try:
    import boto3
except ImportError:  # pragma: no cover - the production image installs boto3
    boto3 = None


SCHEMA_VERSION = 1
WORKER_VERSION = "1"

SOURCE_FILENAME = "source.zip"
PART_FILENAME = "source.zip.part"
DOWNLOAD_RESULT_FILENAME = "download-result.json"
REQUEST_FILENAME = "request.json"
STATE_DIRECTORY = "state"
IMPORTER_STATE_FILENAME = "state.json"
IMPORTER_RESULT_FILENAME = "result.json"
WRAPPER_RESULT_FILENAME = "run-wrapper-result.json"

DEFAULT_DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DEFAULT_S3_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_S3_READ_TIMEOUT_SECONDS = 60
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 3
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_ERROR_CATEGORY_CHARS = 100
MAX_ERROR_MESSAGE_CHARS = 1000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ERROR_RE = re.compile(r"[\x00-\x1f\x7f]+")
_TERMINAL_S3_ERROR_CODES = {
    "AccessDenied",
    "AllAccessDisabled",
    "InvalidAccessKeyId",
    "InvalidBucketName",
    "InvalidObjectState",
    "NoSuchBucket",
    "NoSuchKey",
    "NotFound",
    "403",
    "404",
}
_RETRYABLE_S3_ERROR_CODES = {
    "InternalError",
    "InternalServerError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "500",
    "502",
    "503",
    "504",
}
_PROJECT_STATUSES = {"planned", "committed"}
_BATCH_STATUSES = {"planned", "prepared", "committed", "failed"}
_PHASE_STATUSES = {"pending", "running", "complete", "failed"}
_IMPORTER_STATUSES = {
    "planned", "in_progress", "database_in_progress", "database_complete",
    "maintenance_complete", "cache_complete", "complete", "failed",
}
_REQUIRED_PHASES = ("database_maintenance", "cache", "verification")


class WorkerError(Exception):
    exit_code = 1
    category = "internal_error"

    def __init__(self, message: str, *, details: Mapping[str, object] | None = None):
        super().__init__(message)
        self.details = dict(details or {})


class InvalidWorkerInput(WorkerError):
    exit_code = 2
    category = "invalid_input"


class RetryableWorkerError(WorkerError):
    exit_code = 3
    category = "retryable_failure"


class ImmutableWorkspaceError(WorkerError):
    exit_code = 4
    category = "immutable_workspace_mismatch"


class WorkspaceStateError(WorkerError):
    exit_code = 4
    category = "invalid_workspace_state"


@dataclass(frozen=True)
class DownloadConfig:
    ingestion_id: str
    workspace: Path
    source: PublicS3Object | SignedArchiveSource
    max_archive_bytes: int
    coordinate_unit: str
    target_project_id: int


@dataclass(frozen=True)
class FinalizeConfig:
    ingestion_id: str
    workspace: Path
    workflow_name: str
    workflow_result: str
    workflow_uid: str | None = None


@dataclass(frozen=True)
class FinalizationDecision:
    payload: dict[str, object]


class WorkerArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise InvalidWorkerInput(message)


def _required_env(environ: Mapping[str, str], name: str) -> str:
    value = str(environ.get(name, "")).strip()
    if not value:
        raise InvalidWorkerInput(f"Missing required environment variable {name}")
    return value


def _positive_int(value: str, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidWorkerInput(f"{label} must be a positive integer") from exc
    if parsed <= 0 or str(value).strip() != str(parsed):
        raise InvalidWorkerInput(f"{label} must be a positive integer")
    return parsed


def _validate_ingestion_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise InvalidWorkerInput("SKELETON_INGESTION_ID must be a UUID") from exc
    canonical = str(parsed)
    if value != canonical:
        raise InvalidWorkerInput(
            "SKELETON_INGESTION_ID must use canonical lowercase UUID form"
        )
    return canonical


def _workspace_from_environment(
    environ: Mapping[str, str], ingestion_id: str, *, create: bool
) -> Path:
    raw = _required_env(environ, "SKELETON_INGESTION_WORKSPACE")
    workspace = Path(raw)
    if not workspace.is_absolute():
        raise InvalidWorkerInput("SKELETON_INGESTION_WORKSPACE must be absolute")
    if workspace.name != ingestion_id:
        raise InvalidWorkerInput(
            "SKELETON_INGESTION_WORKSPACE must end with SKELETON_INGESTION_ID"
        )
    if create:
        try:
            workspace.mkdir(mode=0o770, parents=True, exist_ok=True)
        except OSError as exc:
            raise _filesystem_error(exc, "create ingestion workspace") from exc
    if workspace.is_symlink():
        raise InvalidWorkerInput("SKELETON_INGESTION_WORKSPACE cannot be a symlink")
    if not workspace.is_dir():
        raise InvalidWorkerInput("SKELETON_INGESTION_WORKSPACE must be a directory")
    return workspace.resolve()


def parse_public_s3_uri(uri: str) -> PublicS3Object:
    try:
        return archive_sources.parse_public_s3_uri(uri)
    except archive_sources.InvalidArchiveSource as exc:
        raise InvalidWorkerInput(str(exc)) from None


def load_download_config(environ: Mapping[str, str]) -> DownloadConfig:
    ingestion_id = _validate_ingestion_id(
        _required_env(environ, "SKELETON_INGESTION_ID")
    )
    workspace = _workspace_from_environment(environ, ingestion_id, create=True)
    encrypted_source = environ.get(archive_sources.SOURCE_ENCRYPTED_ENV, "")
    if encrypted_source:
        if environ.get("SKELETON_INGESTION_S3_URI"):
            raise InvalidWorkerInput("Only one archive source may be supplied")
        try:
            source = archive_sources.decrypt_source_url(
                encrypted_source,
                ingestion_id,
                _required_env(environ, archive_sources.SOURCE_KEY_ENV),
            )
        except archive_sources.InvalidArchiveSource as exc:
            raise InvalidWorkerInput(str(exc)) from None
    else:
        source = parse_public_s3_uri(
            _required_env(environ, "SKELETON_INGESTION_S3_URI")
        )
    coordinate_unit = _required_env(environ, "SKELETON_INGESTION_COORDINATE_UNIT")
    if coordinate_unit not in {"nm", "um"}:
        raise InvalidWorkerInput(
            "SKELETON_INGESTION_COORDINATE_UNIT must be 'nm' or 'um'"
        )
    target_project_id = _positive_int(
        _required_env(environ, "SKELETON_INGESTION_TARGET_PROJECT_ID"),
        "SKELETON_INGESTION_TARGET_PROJECT_ID",
    )
    return DownloadConfig(
        ingestion_id=ingestion_id,
        workspace=workspace,
        source=source,
        max_archive_bytes=_positive_int(
            _required_env(environ, "SKELETON_INGESTION_MAX_ARCHIVE_BYTES"),
            "SKELETON_INGESTION_MAX_ARCHIVE_BYTES",
        ),
        coordinate_unit=coordinate_unit,
        target_project_id=target_project_id,
    )


def _filesystem_error(exc: OSError, action: str) -> WorkerError:
    if isinstance(exc, (PermissionError, IsADirectoryError)):
        return InvalidWorkerInput(f"Cannot {action}: {exc}")
    return RetryableWorkerError(f"Could not {action}: {exc}")


def _canonical_json_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, value: Mapping[str, object]) -> str:
    payload = _canonical_json_bytes(value)
    temporary_path: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary_path = Path(raw_path)
        with os.fdopen(descriptor, "wb") as target:
            os.fchmod(target.fileno(), 0o660)
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
        _fsync_directory(path.parent)
    except OSError as exc:
        raise _filesystem_error(exc, f"write {path.name}") from exc
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
    return hashlib.sha256(payload).hexdigest()


def _regular_file_size(path: Path, label: str) -> int:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise WorkspaceStateError(f"Could not inspect {label}: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise WorkspaceStateError(f"{label} must be a regular file")
    return metadata.st_size


def _path_entry_exists(path: Path) -> bool:
    """Return true for regular entries and broken symlinks alike."""

    return path.exists() or path.is_symlink()


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    size = _regular_file_size(path, label)
    if size > MAX_JSON_BYTES:
        raise WorkspaceStateError(f"{label} exceeds {MAX_JSON_BYTES} bytes")
    try:
        with path.open("r", encoding="utf-8", errors="strict") as source:
            value = json.load(source)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkspaceStateError(f"Could not read {label}: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
        raise WorkspaceStateError(f"{label} has an unsupported schema")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(DEFAULT_DOWNLOAD_CHUNK_BYTES):
                digest.update(chunk)
    except OSError as exc:
        raise _filesystem_error(exc, f"hash {path.name}") from exc
    return digest.hexdigest()


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise _filesystem_error(exc, f"remove {path.name}") from exc


def _new_unsigned_s3_client() -> object:
    if boto3 is None:
        raise InvalidWorkerInput("boto3 is not installed in the worker image")
    config = BotocoreConfig(
        signature_version=UNSIGNED,
        connect_timeout=DEFAULT_S3_CONNECT_TIMEOUT_SECONDS,
        read_timeout=DEFAULT_S3_READ_TIMEOUT_SECONDS,
        # Retry the whole transfer, including body reads, in download_public_archive.
        # Disable SDK retries so nested policies cannot multiply the attempt budget.
        retries={"total_max_attempts": 1, "mode": "standard"},
        ignore_configured_endpoint_urls=True,
    )
    return boto3.client("s3", config=config)


def _s3_error(exc: Exception) -> WorkerError:
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = str(error.get("Code") or "")
        if code in _TERMINAL_S3_ERROR_CODES:
            return InvalidWorkerInput(
                f"Public S3 object is unavailable ({code or 'client error'})"
            )
        if code in _RETRYABLE_S3_ERROR_CODES:
            return RetryableWorkerError(
                f"Public S3 request failed transiently ({code})"
            )
        return InvalidWorkerInput(f"Public S3 request was rejected ({code or 'error'})")
    if isinstance(exc, (BotoCoreError, OSError)):
        return RetryableWorkerError(f"Public S3 request failed: {exc}")
    return RetryableWorkerError(f"Public S3 response failed: {exc}")


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _signed_http_error(exc: Exception) -> WorkerError:
    if isinstance(exc, HTTPError):
        code = exc.code
        exc.close()
        if code in {408, 429, 500, 502, 503, 504}:
            return RetryableWorkerError(
                f"Signed archive request failed transiently (HTTP {code})"
            )
        if 300 <= code < 400:
            return InvalidWorkerInput(
                "Signed archive redirects are not allowed; use the object's direct storage URL"
            )
        if code in {401, 403}:
            return InvalidWorkerInput(
                "Signed archive URL was rejected or expired; submit a fresh signed GET URL"
            )
        return InvalidWorkerInput(f"Signed archive request was rejected (HTTP {code})")
    return RetryableWorkerError("Signed archive connection or stream failed")


def _open_signed_archive(
    source: SignedArchiveSource, opener=None
) -> Mapping[str, object]:
    # Avoid ambient proxies, cloud credentials, URL rewriting and redirects.
    opener = opener or build_opener(ProxyHandler({}), _RejectRedirects())
    try:
        response = opener.open(
            Request(source.url, method="GET"), timeout=DEFAULT_S3_READ_TIMEOUT_SECONDS
        )
    except (HTTPError, URLError, HTTPException, OSError) as exc:
        raise _signed_http_error(exc) from None
    if response.status != 200:
        response.close()
        raise InvalidWorkerInput(
            "Signed archive endpoint did not return a complete object (HTTP 200 required)"
        )
    return {
        "Body": response,
        "ContentLength": response.headers.get("Content-Length"),
    }


def _normalized_request(
    config: DownloadConfig, archive_path: Path
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source": {
            "archive_path": str(archive_path.resolve()),
            "coordinate_unit": config.coordinate_unit,
        },
        "target": {"project_id": config.target_project_id},
    }


def _validate_completed_download(
    config: DownloadConfig, result: Mapping[str, Any]
) -> dict[str, object]:
    if result.get("ok") is not True or result.get("command") != "download":
        raise ImmutableWorkspaceError("Download completion marker is malformed")
    if result.get("ingestion_id") != config.ingestion_id:
        raise ImmutableWorkspaceError("Download belongs to a different ingestion")
    source = result.get("source")
    archive = result.get("archive")
    request = result.get("request")
    if not all(isinstance(value, dict) for value in (source, archive, request)):
        raise ImmutableWorkspaceError("Download completion metadata is incomplete")
    assert (
        isinstance(source, dict)
        and isinstance(archive, dict)
        and isinstance(request, dict)
    )
    expected_source = config.source.metadata()
    if source != expected_source:
        raise ImmutableWorkspaceError(
            "Completed download has a different archive source"
        )
    archive_path = config.workspace / SOURCE_FILENAME
    request_path = config.workspace / REQUEST_FILENAME
    if (
        archive.get("path") != SOURCE_FILENAME
        or request.get("path") != REQUEST_FILENAME
    ):
        raise ImmutableWorkspaceError("Completed download contains unexpected paths")
    try:
        archive_size = _regular_file_size(archive_path, "completed archive")
        expected_size_value = archive["size_bytes"]
        expected_sha256 = str(archive["sha256"])
    except KeyError as exc:
        raise ImmutableWorkspaceError("Completed archive metadata is invalid") from exc
    if (
        not isinstance(expected_size_value, int)
        or isinstance(expected_size_value, bool)
        or expected_size_value < 0
        or not _SHA256_RE.fullmatch(expected_sha256)
    ):
        raise ImmutableWorkspaceError("Completed archive metadata is invalid")
    expected_size = expected_size_value
    if expected_size > config.max_archive_bytes:
        raise ImmutableWorkspaceError("Completed archive exceeds the configured limit")
    if archive_size != expected_size or _sha256_file(archive_path) != expected_sha256:
        raise ImmutableWorkspaceError("Completed archive bytes changed")
    expected_request = _normalized_request(config, archive_path)
    try:
        persisted_request = _load_json_object(request_path, "importer request")
    except WorkspaceStateError as exc:
        raise ImmutableWorkspaceError(str(exc)) from exc
    if persisted_request != expected_request:
        raise ImmutableWorkspaceError("Completed importer request changed")
    request_sha256 = hashlib.sha256(_canonical_json_bytes(expected_request)).hexdigest()
    if request.get("sha256") != request_sha256:
        raise ImmutableWorkspaceError("Completed importer request digest changed")
    return dict(result)


def _download_archive_once(
    config: DownloadConfig,
    *,
    s3_client: object | None = None,
    http_opener=None,
) -> dict[str, object]:
    archive_path = config.workspace / SOURCE_FILENAME
    part_path = config.workspace / PART_FILENAME
    result_path = config.workspace / DOWNLOAD_RESULT_FILENAME
    request_path = config.workspace / REQUEST_FILENAME

    if _path_entry_exists(result_path):
        existing = _load_json_object(result_path, "download result")
        if existing.get("ok") is True:
            return _validate_completed_download(config, existing)

    for uncommitted_path in (part_path, archive_path, request_path):
        _safe_unlink(uncommitted_path)

    signed = isinstance(config.source, SignedArchiveSource)
    body: BinaryIO | None = None
    archive_installed = False
    try:
        try:
            if signed:
                response = _open_signed_archive(config.source, http_opener)
            else:
                client = s3_client or _new_unsigned_s3_client()
                response = client.get_object(  # type: ignore[attr-defined]
                    Bucket=config.source.bucket,
                    Key=config.source.key,
                )
        except WorkerError:
            raise
        except Exception as exc:
            raise (_signed_http_error(exc) if signed else _s3_error(exc)) from None
        if not isinstance(response, Mapping):
            raise RetryableWorkerError("Archive storage returned a malformed response")
        body_value = response.get("Body")
        if body_value is None or not hasattr(body_value, "read"):
            raise RetryableWorkerError("Archive response has no streaming body")
        body = cast(BinaryIO, body_value)
        content_length = response.get("ContentLength")
        if content_length is not None:
            try:
                advertised_size = int(content_length)
            except (TypeError, ValueError) as exc:
                raise RetryableWorkerError(
                    "Archive response has an invalid ContentLength"
                ) from exc
            if advertised_size < 0:
                raise RetryableWorkerError(
                    "Archive response has an invalid ContentLength"
                )
            if advertised_size > config.max_archive_bytes:
                raise InvalidWorkerInput(
                    f"Object exceeds the {config.max_archive_bytes}-byte archive limit"
                )
        else:
            advertised_size = None

        digest = hashlib.sha256()
        downloaded = 0
        from .workflow_progress import write_native_progress, stage_units
        write_native_progress(*stage_units(1, 0, advertised_size or 0))
        last_progress = time.monotonic()
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(part_path, flags, 0o660)
            with os.fdopen(descriptor, "wb") as target:
                while True:
                    try:
                        chunk = body.read(DEFAULT_DOWNLOAD_CHUNK_BYTES)
                    except Exception as exc:
                        raise (
                            _signed_http_error(exc) if signed else _s3_error(exc)
                        ) from None
                    if not chunk:
                        break
                    if not isinstance(chunk, bytes):
                        raise RetryableWorkerError(
                            "Archive body returned non-byte content"
                        )
                    downloaded += len(chunk)
                    if downloaded > config.max_archive_bytes:
                        raise InvalidWorkerInput(
                            f"Object exceeds the {config.max_archive_bytes}-byte archive limit"
                        )
                    target.write(chunk)
                    digest.update(chunk)
                    now = time.monotonic()
                    if advertised_size is not None and downloaded <= advertised_size and (
                        downloaded == advertised_size or now - last_progress >= 1
                    ):
                        write_native_progress(*stage_units(1, downloaded, advertised_size))
                        last_progress = now
                target.flush()
                os.fsync(target.fileno())
        except WorkerError:
            raise
        except OSError as exc:
            raise _filesystem_error(exc, "write archive") from exc

        if advertised_size is not None and downloaded != advertised_size:
            raise RetryableWorkerError(
                "Archive body length does not match ContentLength"
            )
        try:
            os.replace(part_path, archive_path)
            archive_installed = True
            _fsync_directory(config.workspace)
        except OSError as exc:
            raise _filesystem_error(exc, "install downloaded archive") from exc

        request_value = _normalized_request(config, archive_path)
        request_sha256 = _atomic_write_json(request_path, request_value)
        archive_value: dict[str, object] = {
            "path": SOURCE_FILENAME,
            "size_bytes": downloaded,
            "sha256": digest.hexdigest(),
        }
        etag = response.get("ETag")
        if isinstance(etag, str) and etag:
            archive_value["etag"] = etag.strip('"')
        version_id = response.get("VersionId")
        if isinstance(version_id, str) and version_id:
            archive_value["version_id"] = version_id
        result = {
            "schema_version": SCHEMA_VERSION,
            "ok": True,
            "command": "download",
            "worker_version": WORKER_VERSION,
            "ingestion_id": config.ingestion_id,
            "source": config.source.metadata(),
            "archive": archive_value,
            "request": {
                "path": REQUEST_FILENAME,
                "sha256": request_sha256,
            },
        }
        _atomic_write_json(result_path, result)
        return result
    except BaseException:
        try:
            _safe_unlink(part_path)
            completed = (
                result_path.exists()
                and _load_json_object(result_path, "download result").get("ok") is True
            )
            if archive_installed and not completed:
                _safe_unlink(archive_path)
                _safe_unlink(request_path)
        except WorkerError:
            # Preserve the primary download/interruption error. A retry performs
            # the same bounded cleanup before it opens the object stream.
            pass
        raise
    finally:
        if body is not None and hasattr(body, "close"):
            try:
                body.close()
            except Exception:
                pass


def download_public_archive(
    config: DownloadConfig,
    *,
    s3_client: object | None = None,
    http_opener=None,
) -> dict[str, object]:
    # One bounded policy for public S3 and signed HTTP, including streamed bodies.
    attempts = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    for attempt in range(attempts):
        try:
            return _download_archive_once(
                config, s3_client=s3_client, http_opener=http_opener
            )
        except RetryableWorkerError:
            if attempt + 1 == attempts:
                raise
            time.sleep(2**attempt)
    raise AssertionError("Unreachable download attempt")


def _sanitize_message(value: object, fallback: str) -> str:
    message = _SAFE_ERROR_RE.sub(" ", str(value or "")).strip()
    if not message:
        message = fallback
    return message[:MAX_ERROR_MESSAGE_CHARS]


def _sanitize_category(value: object, fallback: str) -> str:
    return _sanitize_message(value, fallback)[:MAX_ERROR_CATEGORY_CHARS]


def _validated_archive(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    sha256 = str(value.get("sha256") or "")
    size_bytes = value.get("size_bytes")
    if (
        not _SHA256_RE.fullmatch(sha256)
        or not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes < 0
    ):
        return None
    return {"sha256": sha256, "size_bytes": size_bytes}


def _registered_json_artifact(
    state_directory: Path,
    state: Mapping[str, Any],
    name: str,
) -> tuple[dict[str, Any], str]:
    artifacts = state.get("artifacts")
    descriptor = artifacts.get(name) if isinstance(artifacts, Mapping) else None
    if not isinstance(descriptor, Mapping):
        raise WorkspaceStateError(f"Importer state is missing {name} metadata")
    if (
        descriptor.get("schema_version") != SCHEMA_VERSION
        or descriptor.get("count") != 1
    ):
        raise WorkspaceStateError(f"Importer {name} metadata is invalid")
    relative = Path(str(descriptor.get("path") or ""))
    if not relative.parts or relative.is_absolute() or ".." in relative.parts:
        raise WorkspaceStateError(f"Importer {name} path is unsafe")
    path = state_directory / relative
    try:
        path.resolve(strict=True).relative_to(state_directory.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise WorkspaceStateError(f"Importer {name} path is unsafe") from exc
    expected_sha256 = str(descriptor.get("sha256") or "")
    expected_bytes = descriptor.get("bytes")
    if (
        not _SHA256_RE.fullmatch(expected_sha256)
        or not isinstance(expected_bytes, int)
        or isinstance(expected_bytes, bool)
        or expected_bytes < 0
    ):
        raise WorkspaceStateError(f"Importer {name} metadata is invalid")
    if _regular_file_size(path, f"importer {name}") != expected_bytes:
        raise WorkspaceStateError(f"Importer {name} byte size changed")
    if _sha256_file(path) != expected_sha256:
        raise WorkspaceStateError(f"Importer {name} digest changed")
    return _load_json_object(path, f"importer {name}"), expected_sha256


def _error_from_result(
    result: Mapping[str, Any] | None,
) -> tuple[str, str] | None:
    if not result or result.get("ok") is not False:
        return None
    error = result.get("error")
    if not isinstance(error, Mapping):
        return "worker_failed", "Worker command failed"
    category = _sanitize_category(error.get("category"), "worker_failed")
    message = _sanitize_message(error.get("message"), "Worker command failed")
    return category, message


def _load_optional_json(path: Path, label: str) -> dict[str, Any] | None:
    if not _path_entry_exists(path):
        return None
    return _load_json_object(path, label)


def _base_finalization_payload(config: FinalizeConfig) -> dict[str, object]:
    payload: dict[str, object] = {"workflow_name": config.workflow_name}
    if config.workflow_uid:
        payload["workflow_uid"] = config.workflow_uid
    return payload


def _wrapper_result(path: Path) -> dict[str, Any] | None:
    result = _load_optional_json(path, "run wrapper result")
    if result is not None and (
        result.get("command") != "run-wrapper"
        or result.get("terminal") is not True
        or type(result.get("importer_exit_code")) is not int
        or not 1 <= result["importer_exit_code"] <= 255
        or type(result.get("attempts")) is not int
        or result["attempts"] not in (0, 1)
    ):
        raise WorkspaceStateError("Run wrapper result is malformed")
    return result


def _checkpoint_status(record, allowed, label):
    if (not isinstance(record, Mapping)
            or not isinstance(record.get("status"), str)
            or record["status"] not in allowed):
        raise WorkspaceStateError(f"Importer state has an invalid {label} checkpoint")
    return record["status"]


def _validate_state_directory(workspace: Path, state_directory: Path) -> None:
    try:
        metadata = state_directory.lstat()
        workspace_root = workspace.resolve(strict=True)
        resolved_state = state_directory.resolve(strict=True)
    except OSError as exc:
        raise WorkspaceStateError(
            f"Could not inspect importer state directory: {exc}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise WorkspaceStateError("Importer state path must be a real directory")
    try:
        resolved_state.relative_to(workspace_root)
    except ValueError as exc:
        raise WorkspaceStateError(
            "Importer state directory escapes its workspace"
        ) from exc


def _validate_incomplete_state(state: Mapping[str, Any]) -> None:
    """Validate current checkpoints, including a failed run's retained progress."""
    status = _checkpoint_status(state, _IMPORTER_STATUSES, "overall")
    run_status = _checkpoint_status(state.get("run"), _PHASE_STATUSES, "run")
    project = state.get("project")
    project_status = _checkpoint_status(project, _PROJECT_STATUSES, "project")
    batches = state.get("batches")
    if not isinstance(batches, list) or not batches:
        raise WorkspaceStateError("Importer state has invalid batch checkpoints")
    batch_statuses = [
        _checkpoint_status(batch, _BATCH_STATUSES, "batch") for batch in batches
    ]
    phases = state.get("phases")
    if not isinstance(phases, Mapping):
        raise WorkspaceStateError("Importer state has invalid phase checkpoints")
    phase_statuses = [
        _checkpoint_status(phases.get(name), _PHASE_STATUSES, name)
        for name in _REQUIRED_PHASES
    ]
    plan = state.get("plan")
    if not isinstance(plan, Mapping):
        raise WorkspaceStateError("Importer state has no immutable plan")
    for name, minimum in (("batch_count", 1), ("skeleton_count", 0), ("node_count", 0)):
        if type(plan.get(name)) is not int or plan[name] < minimum:
            raise WorkspaceStateError(f"Importer plan has invalid {name}")
    if plan["batch_count"] != len(batches):
        raise WorkspaceStateError("Importer plan and batch checkpoints disagree")
    if _validated_archive(state.get("archive")) is None:
        raise WorkspaceStateError("Importer state has invalid archive metadata")
    if (project.get("mode") not in (None, "existing")
            or project.get("stack_id") is not None):
        raise WorkspaceStateError("Importer state is not an existing-project append")
    if project_status == "committed" and (
        project.get("mode") != "existing"
        or type(project.get("project_id")) is not int
        or project["project_id"] <= 0
    ):
        raise WorkspaceStateError("Selected importer project is invalid")
    if project_status == "planned" and project.get("project_id") is not None:
        raise WorkspaceStateError("Unselected importer project already has an ID")

    # One committed prefix, at most one active/failed batch, then planned batches.
    tail = False
    for value in batch_statuses:
        if (value == "committed" and tail) or (value in {"prepared", "failed"} and tail):
            raise WorkspaceStateError("Importer batches are out of order")
        if value != "committed":
            tail = True
    loaded = all(value == "committed" for value in batch_statuses)
    if project_status != "committed":
        if any(value != "planned" for value in batch_statuses):
            raise WorkspaceStateError("Importer batches advanced before target selection")
    if project_status != "committed" or not loaded:
        if any(value != "pending" for value in phase_statuses):
            raise WorkspaceStateError("Importer phases advanced before every batch committed")
    else:
        unfinished = False
        for value in phase_statuses:
            if unfinished and value != "pending":
                raise WorkspaceStateError("Importer final phases are out of order")
            if value != "complete":
                unfinished = True

    if (status == "failed") != (run_status == "failed"):
        raise WorkspaceStateError("Importer failure status disagrees with its run")
    if run_status == "pending":
        if (status != "planned" or project_status != "planned"
                or any(value != "planned" for value in batch_statuses)):
            raise WorkspaceStateError("Importer checkpoints advanced before its run")
        return
    if run_status == "failed":
        return  # A normal failure retains the last batch/phase checkpoints.
    if run_status == "complete" and (
        status != "complete" or not loaded
        or any(value != "complete" for value in phase_statuses)
    ):
        raise WorkspaceStateError("Completed importer run has incomplete checkpoints")
    if project_status != "committed":
        expected = "in_progress"
    elif not loaded:
        expected = "database_in_progress"
    else:
        expected = "database_complete"
        if phase_statuses[0] == "complete":
            expected = "maintenance_complete"
            if phase_statuses[1] == "complete":
                expected = "complete" if phase_statuses[2] == "complete" else "cache_complete"
    if status != expected:
        raise WorkspaceStateError("Importer state status disagrees with its checkpoints")


def _partial_state_payload(
    payload: dict[str, object],
    state_directory: Path,
    state: Mapping[str, Any],
) -> None:
    archive = state.get("archive")
    validated_archive = _validated_archive(archive)
    if validated_archive:
        payload["archive"] = validated_archive
    plan = state.get("plan")
    if isinstance(plan, Mapping):
        for source_name, payload_name in (
            ("batch_count", "batches"),
            ("skeleton_count", "skeletons"),
            ("node_count", "nodes"),
        ):
            value = plan.get(source_name)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                payload[payload_name] = value
    project = state.get("project")
    if not isinstance(project, Mapping) or project.get("status") != "committed":
        return
    try:
        context, _ = _registered_json_artifact(
            state_directory, state, "project_context"
        )
    except WorkspaceStateError:
        return
    value = context.get("project_id")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        payload["project_id"] = value


def _success_payload(
    config: FinalizeConfig,
    state_directory: Path,
    state: Mapping[str, Any],
    download_result: Mapping[str, Any] | None,
) -> dict[str, object]:
    run = state.get("run")
    if (state.get("status") != "complete" or not isinstance(run, Mapping)
            or run.get("status") != "complete"):
        raise WorkspaceStateError("Importer success requires a completed run")
    project = state.get("project")
    batches = state.get("batches")
    plan = state.get("plan")
    phases = state.get("phases")
    if not isinstance(project, Mapping) or project.get("status") != "committed":
        raise WorkspaceStateError("Completed importer state has no committed project")
    if not isinstance(plan, Mapping):
        raise WorkspaceStateError("Completed importer state has no immutable plan")
    planned_batch_count = plan.get("batch_count")
    if (
        not isinstance(planned_batch_count, int)
        or isinstance(planned_batch_count, bool)
        or planned_batch_count <= 0
    ):
        raise WorkspaceStateError("Completed importer state has an invalid batch count")
    if (
        not isinstance(batches, list)
        or not batches
        or any(
            not isinstance(batch, Mapping) or batch.get("status") != "committed"
            for batch in batches
        )
    ):
        raise WorkspaceStateError("Completed importer state has incomplete batches")
    if len(batches) != planned_batch_count:
        raise WorkspaceStateError("Completed importer state batch count changed")
    if not isinstance(phases, Mapping) or any(
        not isinstance(phases.get(name), Mapping)
        or phases[name].get("status") != "complete"
        for name in _REQUIRED_PHASES
    ):
        raise WorkspaceStateError("Completed importer state has incomplete phases")

    context, _ = _registered_json_artifact(state_directory, state, "project_context")
    _verification, _ = _registered_json_artifact(state_directory, state, "verification")
    if (
        state.get("status") != "complete"
        or context.get("existing_project") is not True
        or project.get("mode") != "existing"
    ):
        raise WorkspaceStateError(
            "Importer result must be a completed existing-project append"
        )
    project_id = context.get("project_id")
    if type(project_id) is not int or project_id <= 0:
        raise WorkspaceStateError("Importer project context has invalid project_id")
    if project.get("project_id") != project_id:
        raise WorkspaceStateError(
            "Importer state and project context disagree on project_id"
        )
    if context.get("stack_id") is not None or project.get("stack_id") is not None:
        raise WorkspaceStateError("Append state unexpectedly contains a new stack")
    request = _load_json_object(config.workspace / REQUEST_FILENAME, "append request")
    target = request.get("target")
    if (
        not isinstance(target, Mapping)
        or type(target.get("project_id")) is not int
        or target["project_id"] != project_id
    ):
        raise WorkspaceStateError(
            "Append result does not match the requested managed project"
        )
    payload = _base_finalization_payload(config)
    payload["outcome"] = "succeeded"
    _partial_state_payload(payload, state_directory, state)
    required_payload_fields = {"archive", "batches", "skeletons", "nodes", "project_id"}
    if not required_payload_fields.issubset(payload):
        raise WorkspaceStateError("Completed importer state is missing result metadata")
    if download_result and download_result.get("ok") is True:
        download_archive = _validated_archive(download_result.get("archive"))
        if download_archive and download_archive != payload["archive"]:
            raise WorkspaceStateError(
                "Downloader and importer archive metadata disagree"
            )
    return payload


def _invalid_workspace_decision(
    config: FinalizeConfig,
    error: WorkspaceStateError,
    *,
    state_directory: Path | None = None,
    state: Mapping[str, Any] | None = None,
) -> FinalizationDecision:
    payload = _base_finalization_payload(config)
    payload.update(
        {
            "outcome": "operator_attention",
            "error": {
                "category": "invalid_workspace_state",
                "message": _sanitize_message(error, "Importer state is invalid"),
            },
        }
    )
    if state_directory is not None and state is not None:
        _partial_state_payload(payload, state_directory, state)
    return FinalizationDecision(payload=payload)


def derive_finalization(config: FinalizeConfig) -> FinalizationDecision:
    """Classify one attempt; filesystem failure never proves database rollback."""
    state_directory = config.workspace / STATE_DIRECTORY
    payload = _base_finalization_payload(config)
    try:
        if _path_entry_exists(state_directory):
            _validate_state_directory(config.workspace, state_directory)
        state = (
            _load_json_object(
                state_directory / IMPORTER_STATE_FILENAME, "importer state",
            )
            if _path_entry_exists(state_directory / IMPORTER_STATE_FILENAME) else None
        )
        wrapper = _wrapper_result(state_directory / WRAPPER_RESULT_FILENAME)
        if state is not None:
            _validate_incomplete_state(state)
    except WorkspaceStateError as exc:
        return _invalid_workspace_decision(config, exc)

    try:
        download_result = _load_optional_json(
            config.workspace / DOWNLOAD_RESULT_FILENAME, "download result"
        )
    except WorkspaceStateError as exc:
        return _invalid_workspace_decision(config, exc, state_directory=state_directory, state=state)
    try:
        importer_result = _load_optional_json(
            state_directory / IMPORTER_RESULT_FILENAME, "importer result"
        )
    except WorkspaceStateError:
        importer_result = None  # Diagnostic output; valid execution state remains authoritative.

    if state is not None:
        _partial_state_payload(payload, state_directory, state)
        run_status = state["run"]["status"]
        if wrapper and wrapper["attempts"] == 0 and run_status != "pending":
            return _invalid_workspace_decision(
                config, WorkspaceStateError("Wrapper says no child launched but importer run advanced"),
                state_directory=state_directory, state=state,
            )
        if state["status"] == "complete" and run_status == "complete":
            if wrapper:
                return _invalid_workspace_decision(
                    config, WorkspaceStateError("Completed importer run conflicts with wrapper failure"),
                    state_directory=state_directory, state=state,
                )
            try:
                success = _success_payload(config, state_directory, state, download_result)
            except WorkspaceStateError as exc:
                return _invalid_workspace_decision(config, exc, state_directory=state_directory, state=state)
            return FinalizationDecision(payload=success)
        # Preparation/failed includes a possible COMMIT whose outcome was not saved.
        loading_started = any(batch["status"] != "planned" for batch in state["batches"])
        outcome = "operator_attention" if loading_started else "failed"
        if (config.workflow_result == "Succeeded"
                or (importer_result and importer_result.get("ok") is True
                    and importer_result.get("command") == "run")):
            outcome = "operator_attention"
    else:
        # Only explicit evidence of a pre-loading failure is safe to resubmit.
        preparation_failed = (
            importer_result and importer_result.get("command") in {"preflight", "plan"}
            and importer_result.get("ok") is False
        )
        download_failed = _error_from_result(download_result) is not None
        if wrapper:
            outcome = "failed" if wrapper["attempts"] == 0 else "operator_attention"
        elif importer_result and importer_result.get("command") == "run":
            outcome = "operator_attention"
        else:
            outcome = "failed" if preparation_failed or download_failed else "operator_attention"

    importer_error = _error_from_result(importer_result)
    if wrapper and importer_result and importer_result.get("command") != "run":
        importer_error = None  # A prior preparation command cannot describe this run.
    # A wrapper setup failure supersedes the prior plan's result.json.
    if wrapper and wrapper["attempts"] == 0:
        category, message = "operation_failed", (
            f"CATMAID run wrapper exited with code {wrapper['importer_exit_code']} "
            "before launching the importer"
        )
    elif importer_error:
        category, message = importer_error
    elif state is not None and state["run"].get("reason"):
        category, message = "operation_failed", state["run"]["reason"]
    elif wrapper:
        category, message = "operation_failed", (
            f"CATMAID importer exited with code {wrapper['importer_exit_code']}"
        )
    elif _error_from_result(download_result):
        category, message = _error_from_result(download_result)
    elif state is None:
        category, message = "workspace_evidence_missing", (
            "No valid importer execution evidence is available. Inspect CATMAID before another import."
        )
    else:
        category, message = "workflow_failed", (
            f"Workflow {config.workflow_result or 'did not complete'} without a completed importer run"
        )
    payload["outcome"] = outcome
    payload["error"] = {
        "category": _sanitize_category(category, "workflow_failed"),
        "message": _sanitize_message(message, "Skeleton ingestion failed"),
    }
    if "archive" not in payload and download_result:
        archive = _validated_archive(download_result.get("archive"))
        if archive:
            payload["archive"] = archive
    return FinalizationDecision(payload=payload)


def _error_result(
    command: str | None,
    ingestion_id: str | None,
    error: WorkerError,
) -> dict[str, object]:
    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "ok": False,
        "command": command,
        "worker_version": WORKER_VERSION,
        "error": {
            "category": error.category,
            "message": _sanitize_message(error, "Worker command failed"),
            "details": dict(error.details),
        },
    }
    if ingestion_id:
        result["ingestion_id"] = ingestion_id
    return result


def _persist_download_failure(
    environ: Mapping[str, str], result: Mapping[str, object]
) -> None:
    try:
        ingestion_id = _validate_ingestion_id(
            _required_env(environ, "SKELETON_INGESTION_ID")
        )
        workspace = _workspace_from_environment(environ, ingestion_id, create=True)
        result_path = workspace / DOWNLOAD_RESULT_FILENAME
        if _path_entry_exists(result_path):
            existing = _load_json_object(result_path, "download result")
            if existing.get("ok") is True:
                return
        _atomic_write_json(result_path, result)
    except WorkerError:
        return


def _emit(value: Mapping[str, object]) -> None:
    print(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = WorkerArgumentParser(prog="skeleton-ingestion-worker")
    parser.add_argument("--version", action="version", version=WORKER_VERSION)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "download", help="download and prepare a public S3 or signed S3/GCS archive"
    )
    return parser


def execute(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    s3_client: object | None = None,
    http_opener=None,
) -> int:
    environment = os.environ if environ is None else environ
    command: str | None = None
    ingestion_id = str(environment.get("SKELETON_INGESTION_ID", "")).strip() or None
    try:
        args = build_parser().parse_args(argv)
        command = args.command
        config = load_download_config(environment)
        result = download_public_archive(
            config, s3_client=s3_client, http_opener=http_opener
        )
        _emit(result)
        return 0
    except WorkerError as exc:
        result = _error_result(command, ingestion_id, exc)
        if command == "download":
            _persist_download_failure(environment, result)
        print(str(exc), file=sys.stderr)
        _emit(result)
        return exc.exit_code
    except KeyboardInterrupt:
        interruption = RetryableWorkerError("Worker interrupted")
        result = _error_result(command, ingestion_id, interruption)
        if command == "download":
            _persist_download_failure(environment, result)
        print(str(interruption), file=sys.stderr)
        _emit(result)
        return interruption.exit_code
    except Exception:
        error = WorkerError("Unexpected skeleton ingestion worker failure")
        result = _error_result(command, ingestion_id, error)
        if command == "download":
            _persist_download_failure(environment, result)
        print(str(error), file=sys.stderr)
        _emit(result)
        return error.exit_code


def main() -> None:
    raise SystemExit(execute())


if __name__ == "__main__":
    main()
