"""Keep the CATMAID project locked across batch commits and final verification."""
from contextlib import contextmanager
import os
import sys

from .errors import ImmutableStateError, ProjectIngestionLockError
from .statuses import ProjectLockStatus


@contextmanager
def project_ingestion_lock(store, project_id, connection, *, lock_timeout_ms):
    from catmaid.ingestion_lock import (
        acquire_project_ingestion, require_project_ingestion, release_project_ingestion,
    )

    state = store.load()
    owner = state["ingestion_id"]
    if state["project_lock"]["project_id"] != project_id:
        raise ImmutableStateError("Project ingestion lock does not match the requested target")
    # Checkpoint before the database call: its COMMIT may have an uncertain outcome.
    store.transition_project_lock(ProjectLockStatus.ACQUIRING)
    try:
        if os.environ.get("SKELETON_INGESTION_WORKFLOW_NAME"):
            # The fresh workflow acquired this same owner before downloading.
            require_project_ingestion(project_id, owner, using=connection.alias)
        else:
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


class WorkflowProjectLock:
    """Acquire before preparation; hand the same owner to the normal run lock.

    No separate recovery file is needed: the workflow UUID owns the database
    flag even if planning has not produced state.json yet. A lost pod retains
    its flag and has no successful terminal result.
    """

    def __init__(self, environment):
        from . import workflow_io as io

        self.owner = io._validate_ingestion_id(io._required_env(environment, "SKELETON_INGESTION_ID"))
        self.project_id = io._positive_int(
            io._required_env(environment, "SKELETON_INGESTION_TARGET_PROJECT_ID"),
            "SKELETON_INGESTION_TARGET_PROJECT_ID",
        )
        self.environment = environment
        self.status = ProjectLockStatus.PENDING

    def acquire(self):
        from .config import load_operational_settings
        from .database import bootstrap_django

        # Match the finite job wrapper's isolated settings without starting the
        # web application or changing the database credentials supplied by MNP.
        projects = self.environment.get("CATMAID_PROJECTS_DIRECTORY", "/home/django/projects")
        sys.path.insert(0, str(projects))
        os.environ["DJANGO_SETTINGS_MODULE"] = "mysite.ingestion_settings"
        connection = bootstrap_django()
        from catmaid.ingestion_lock import acquire_project_ingestion

        timeouts = load_operational_settings()
        self.status = ProjectLockStatus.ACQUIRING
        acquire_project_ingestion(
            self.project_id, self.owner, using=connection.alias,
            lock_timeout_ms=timeouts["lock_timeout_ms"],
        )
        self.status = ProjectLockStatus.LOCKED

    def finish(self, workspace, *, run_started):
        from . import workflow_io as io
        from .state import StateStore

        if self.status == ProjectLockStatus.PENDING:
            return  # No database acquisition was attempted.
        if self.status != ProjectLockStatus.LOCKED:
            raise ProjectIngestionLockError("Project ingestion lock acquisition was not confirmed")
        state_path = workspace / "state" / "state.json"
        store = None
        if io._path_entry_exists(state_path):
            state = io._load_json_object(state_path, "importer state")
            io._validate_incomplete_state(state)
            if (state.get("ingestion_id") != self.owner
                    or state["project_lock"]["project_id"] != self.project_id):
                raise ProjectIngestionLockError("Project ingestion lock state does not match this workflow")
            if state["project_lock"]["status"] == ProjectLockStatus.RELEASED:
                self.status = ProjectLockStatus.RELEASED
                return  # The run already released and recorded its own flag.
            if any(batch["status"] != "planned" for batch in state["batches"]):
                return  # Partial or uncertain loading retains the flag.
            store = StateStore(workspace / "state")
        elif run_started:
            raise ProjectIngestionLockError("Importer state is missing after invoking run; inspect before unlocking")
        from catmaid.ingestion_lock import release_project_ingestion

        release_project_ingestion(self.project_id, self.owner)
        if store is not None:
            store.transition_project_lock(ProjectLockStatus.RELEASED)
        self.status = ProjectLockStatus.RELEASED
