"""Keep the CATMAID project locked across batch commits and final verification."""
from contextlib import contextmanager

from .errors import ImmutableStateError, ProjectIngestionLockError
from .statuses import ProjectLockStatus


@contextmanager
def project_ingestion_lock(store, project_id, connection, *, lock_timeout_ms):
    from catmaid.ingestion_lock import acquire_project_ingestion, release_project_ingestion

    state = store.load()
    owner = state["ingestion_id"]
    if state["project_lock"]["project_id"] != project_id:
        raise ImmutableStateError("Project ingestion lock does not match the requested target")
    # Checkpoint before the database call: its COMMIT may have an uncertain outcome.
    store.transition_project_lock(ProjectLockStatus.ACQUIRING)
    try:
        acquire_project_ingestion(project_id, owner, using=connection.alias,
                                  lock_timeout_ms=lock_timeout_ms)
        store.transition_project_lock(ProjectLockStatus.LOCKED)
    except Exception as exc:
        raise ProjectIngestionLockError(
            f"Could not confirm acquisition of project {project_id}'s ingestion lock; inspect before retrying",
            details={"reason": str(exc)},
        ) from exc
    try:
        yield
    except BaseException:
        # Prepared/failed batches include uncertain COMMIT outcomes. Never infer rollback.
        if all(batch["status"] == "planned" for batch in store.load()["batches"]):
            try:
                release_project_ingestion(project_id, owner, using=connection.alias)
                store.transition_project_lock(ProjectLockStatus.RELEASED)
            except Exception as exc:
                raise ProjectIngestionLockError(
                    f"Could not confirm release of project {project_id}'s ingestion lock", details={"reason": str(exc)},
                ) from exc
        raise
    else:
        try:
            release_project_ingestion(project_id, owner, using=connection.alias)
            store.transition_project_lock(ProjectLockStatus.RELEASED)
        except Exception as exc:
            raise ProjectIngestionLockError(
                f"Could not confirm release of project {project_id}'s ingestion lock", details={"reason": str(exc)},
            ) from exc
