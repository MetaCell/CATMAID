"""Keep the CATMAID project locked across batch commits and final verification."""
from contextlib import contextmanager
from uuid import uuid4

from .errors import ProjectIngestionLockError
from .util import atomic_write_json


@contextmanager
def project_ingestion_lock(store, project_id, connection, *, lock_timeout_ms):
    from catmaid.ingestion_lock import acquire_project_ingestion, release_project_ingestion

    owner = uuid4()
    # Persist the owner before acquisition, including an uncertain metadata COMMIT.
    def record(status):
        atomic_write_json(store.state_dir / "project-ingestion-lock.json", {
            "schema_version": 1, "project_id": project_id, "owner": str(owner), "status": status,
        })
    record("acquiring")
    try:
        acquire_project_ingestion(project_id, owner, using=connection.alias,
                                  lock_timeout_ms=lock_timeout_ms)
        record("locked")
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
                record("released")
            except Exception as exc:
                raise ProjectIngestionLockError(
                    f"Could not confirm release of project {project_id}'s ingestion lock", details={"reason": str(exc)},
                ) from exc
        raise
    else:
        try:
            release_project_ingestion(project_id, owner, using=connection.alias)
            record("released")
        except Exception as exc:
            raise ProjectIngestionLockError(
                f"Could not confirm release of project {project_id}'s ingestion lock", details={"reason": str(exc)},
            ) from exc
