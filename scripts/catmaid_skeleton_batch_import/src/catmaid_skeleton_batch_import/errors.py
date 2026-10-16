"""Stable importer error categories and exit codes."""

from __future__ import annotations


class ImporterError(Exception):
    """Base class for errors that have a stable process contract."""

    exit_code = 1
    category = "internal_error"

    def __init__(self, message: str, *, details: dict[str, object] | None = None):
        super().__init__(message)
        self.details = details or {}


class InvalidInputError(ImporterError):
    exit_code = 2
    category = "invalid_input"


class RetryableError(ImporterError):
    exit_code = 3
    category = "retryable_failure"


class ImmutableStateError(ImporterError):
    exit_code = 4
    category = "immutable_state_mismatch"


class OperatorAttentionError(ImporterError):
    exit_code = 5
    category = "operator_attention"


class VerificationError(ImporterError):
    exit_code = 6
    category = "verification_failed"
