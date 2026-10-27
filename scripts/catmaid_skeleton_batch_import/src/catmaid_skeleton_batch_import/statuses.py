"""Typed status vocabularies with stable JSON string values."""

from enum import Enum


class _Status(str, Enum):
    def __str__(self) -> str:
        return self.value


class DatabaseStatus(_Status):
    """Project and batch database progress."""
    PLANNED = "planned"
    PREPARED = "prepared"
    COMMITTED = "committed"
    FAILED = "failed"


class OperationStatus(_Status):
    """The run and each post-loading phase share this lifecycle."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class ProjectLockStatus(_Status):
    PENDING = "pending"
    ACQUIRING = "acquiring"
    LOCKED = "locked"
    RELEASED = "released"


class ImportStatus(_Status):
    """Overall ingestion progress exposed in state and command results."""
    PLANNED = "planned"
    IN_PROGRESS = "in_progress"
    DATABASE_IN_PROGRESS = "database_in_progress"
    DATABASE_COMPLETE = "database_complete"
    MAINTENANCE_COMPLETE = "maintenance_complete"
    CACHE_COMPLETE = "cache_complete"
    COMPLETE = "complete"
    FAILED = "failed"


class MetricsStatus(_Status):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"
