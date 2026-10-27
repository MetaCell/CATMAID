"""Typed status vocabularies with stable JSON string values."""

from enum import Enum


class _Status(str, Enum):
    def __str__(self) -> str:
        return self.value


class BatchStatus(_Status):
    """One atomic batch's database progress."""
    PLANNED = "planned"
    PREPARED = "prepared"
    COMMITTED = "committed"
    FAILED = "failed"


class OperationStatus(_Status):
    """The run, post-loading phases, and metrics share this lifecycle."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class ProjectLockStatus(_Status):
    PENDING = "pending"
    ACQUIRING = "acquiring"
    LOCKED = "locked"
    RELEASED = "released"
