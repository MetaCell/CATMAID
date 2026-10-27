"""Durable project ingestion ownership and admission of NeuroGlass writes.

Shared project-row locks allow concurrent API edits but drain them before an
importer sets the persistent flag. No long-running database transaction spans
an import. Callers must explicitly release their own flag after safe completion.
"""
from contextlib import contextmanager
from uuid import UUID

from django.db import connections, transaction


class ProjectIngestionLocked(RuntimeError):
    pass


def acquire_project_ingestion(project_id, owner, *, using="default", lock_timeout_ms=30000):
    owner = UUID(str(owner))
    with transaction.atomic(using=using):
        with connections[using].cursor() as cursor:
            cursor.execute("SELECT set_config('lock_timeout', %s, true)", [f"{int(lock_timeout_ms)}ms"])
            cursor.execute(
                "SELECT active_skeleton_ingestion FROM project WHERE id = %s FOR UPDATE",
                [project_id],
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Project {project_id} does not exist")
            if row[0] is not None:
                raise ProjectIngestionLocked(f"Project {project_id} already has an ingestion lock")
            cursor.execute(
                "UPDATE project SET active_skeleton_ingestion = %s WHERE id = %s",
                [str(owner), project_id],
            )
    return owner


def release_project_ingestion(project_id, owner, *, using="default"):
    # Compare ownership atomically; never clear another attempt's lock.
    with transaction.atomic(using=using):
        with connections[using].cursor() as cursor:
            cursor.execute(
                "UPDATE project SET active_skeleton_ingestion = NULL "
                "WHERE id = %s AND active_skeleton_ingestion = %s",
                [project_id, str(UUID(str(owner)))],
            )
            if cursor.rowcount != 1:
                raise ProjectIngestionLocked(f"Project {project_id} ingestion lock ownership changed")


@contextmanager
def project_write_guard(project_id, *, using="default"):
    with transaction.atomic(using=using):
        with connections[using].cursor() as cursor:
            cursor.execute(
                "SELECT active_skeleton_ingestion FROM project WHERE id = %s FOR SHARE",
                [project_id],
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                raise ProjectIngestionLocked("Skeleton ingestion is in progress; project edits are temporarily disabled")
        yield
