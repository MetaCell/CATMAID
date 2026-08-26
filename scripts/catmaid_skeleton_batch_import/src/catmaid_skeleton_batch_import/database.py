"""Small, state-agnostic helpers for the CATMAID database boundary.

This module deliberately imports Django only inside functions.  Planning and
the dependency-free CLI can therefore be imported without a CATMAID checkout
or a configured Django settings module.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from collections.abc import Iterator, Sequence
from typing import Any

from .util import canonical_json_bytes


REQUIRED_TABLES = (
    "project",
    "stack",
    "project_stack",
    "class",
    "relation",
    "class_instance",
    "class_instance_class_instance",
    "treenode",
    "treenode_edge",
    "catmaid_skeleton_summary",
    "node_grid_cache",
    "node_grid_cache_cell",
    "dirty_node_grid_cache_cell",
)

REQUIRED_SEQUENCES = (
    "project_id_seq",
    "stack_id_seq",
    "project_stack_id_seq",
    "concept_id_seq",
    "location_id_seq",
)

REQUIRED_FUNCTIONS = (
    "public.refresh_skeleton_summary_table_selectively(bigint[])",
)

ANALYZE_TABLES = (
    "class_instance",
    "class_instance_class_instance",
    "treenode",
    "treenode_edge",
    "catmaid_skeleton_summary",
)

_SEQUENCE_NAMES = frozenset(REQUIRED_SEQUENCES)
_ANALYZE_TABLE_NAMES = frozenset(ANALYZE_TABLES)


class DatabasePreflightError(RuntimeError):
    """The configured database is not compatible with this importer."""


def bootstrap_django(settings_module: str | None = None) -> Any:
    """Initialize Django once and return its default database connection.

    ``settings_module`` is optional so a CATMAID container can provide the
    conventional ``DJANGO_SETTINGS_MODULE`` environment variable.  A caller
    cannot silently replace a different, already configured module.
    """

    if settings_module:
        configured = os.environ.get("DJANGO_SETTINGS_MODULE")
        if configured and configured != settings_module:
            raise DatabasePreflightError(
                "DJANGO_SETTINGS_MODULE is already set to a different value"
            )
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)

    import django
    from django.apps import apps

    if not apps.ready:
        django.setup()

    from django.db import connection

    return connection


def _default_connection(connection: Any | None) -> Any:
    if connection is not None:
        return connection
    from django.db import connection as django_connection

    return django_connection


def _fetch_scalar(cursor: Any, sql: str, params: Any = None) -> Any:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        raise DatabasePreflightError("Database query returned no row")
    return row[0]


def database_target_fingerprint(connection: Any | None = None) -> dict[str, Any]:
    """Return a non-secret, stable identity for the configured database target.

    PostgreSQL's system identifier distinguishes clusters without relying on a
    pod IP.  The configured host/port distinguish routing changes, while the
    database and role distinguish logical targets within the cluster.  No
    password or connection options are included.
    """

    connection = _default_connection(connection)
    settings = getattr(connection, "settings_dict", {})
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database(), current_user,
                   current_setting('server_version_num'),
                   (SELECT system_identifier::text FROM pg_control_system())
            """
        )
        row = cursor.fetchone()
    if row is None or len(row) != 4:
        raise DatabasePreflightError("Could not identify the PostgreSQL target")

    identity = {
        "engine": str(settings.get("ENGINE", "")),
        "configured_host": str(settings.get("HOST", "")),
        "configured_port": str(settings.get("PORT", "")),
        "database": str(row[0]),
        "role": str(row[1]),
        "server_version_num": str(row[2]),
        "system_identifier": str(row[3]),
    }
    return {
        "identity": identity,
        "sha256": hashlib.sha256(canonical_json_bytes(identity)).hexdigest(),
    }


def _migration_status(connection: Any, executor_factory: Any | None) -> dict[str, Any]:
    if executor_factory is None:
        from django.db.migrations.executor import MigrationExecutor

        executor_factory = MigrationExecutor
    executor = executor_factory(connection)
    targets = executor.loader.graph.leaf_nodes()
    plan = executor.migration_plan(targets)
    if plan:
        unapplied = [f"{migration.app_label}.{migration.name}" for migration, _ in plan]
        raise DatabasePreflightError(
            "Unapplied Django migrations: " + ", ".join(unapplied)
        )
    catmaid_leaves = sorted(
        migration_name
        for app_label, migration_name in targets
        if app_label == "catmaid"
    )
    if not catmaid_leaves:
        raise DatabasePreflightError("No CATMAID migration leaf was found")
    return {"catmaid_leaf_migrations": catmaid_leaves, "unapplied": []}


def _check_replica_role(connection: Any, atomic_factory: Any | None) -> None:
    if atomic_factory is None:
        from django.db import transaction

        atomic_factory = transaction.atomic

    # SET LOCAL is scoped to this harmless transaction.  Explicitly restoring
    # origin also verifies that both transitions accepted by the batch loader
    # work for this role.
    with atomic_factory():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL session_replication_role = replica")
            role = _fetch_scalar(cursor, "SHOW session_replication_role")
            if role != "replica":
                raise DatabasePreflightError(
                    "Database role could not enable session_replication_role=replica"
                )
            cursor.execute("SET LOCAL session_replication_role = origin")


def preflight_database(
    connection: Any | None = None,
    *,
    executor_factory: Any | None = None,
    atomic_factory: Any | None = None,
) -> dict[str, Any]:
    """Verify migrations, schema objects, functions, role, and target identity.

    The checks do not create application rows.  Sequence allocation is kept as
    an explicit later operation because ``nextval`` is intentionally not
    transactional.
    """

    connection = _default_connection(connection)
    migrations = _migration_status(connection, executor_factory)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT name, to_regclass('public.' || name) IS NOT NULL
            FROM unnest(%s::text[]) AS required(name)
            ORDER BY name
            """,
            (list(REQUIRED_TABLES + REQUIRED_SEQUENCES),),
        )
        object_rows = cursor.fetchall()
        missing_objects = [str(name) for name, present in object_rows if not present]

        cursor.execute(
            """
            SELECT signature, to_regprocedure(signature) IS NOT NULL
            FROM unnest(%s::text[]) AS required(signature)
            ORDER BY signature
            """,
            (list(REQUIRED_FUNCTIONS),),
        )
        function_rows = cursor.fetchall()
        missing_functions = [
            str(signature) for signature, present in function_rows if not present
        ]

    if missing_objects:
        raise DatabasePreflightError(
            "Missing CATMAID database objects: " + ", ".join(missing_objects)
        )
    if missing_functions:
        raise DatabasePreflightError(
            "Missing CATMAID database functions: " + ", ".join(missing_functions)
        )

    # The edge materializer is a shipped CATMAID Python function, not a stable
    # database-function dependency of the importer.
    try:
        from catmaid.control.edge import rebuild_edges_selectively

        if not callable(rebuild_edges_selectively):
            raise TypeError("not callable")
    except Exception as exc:
        raise DatabasePreflightError(
            "CATMAID rebuild_edges_selectively() is unavailable"
        ) from exc

    try:
        from catmaid.apps import get_system_user

        system_user = get_system_user()
    except Exception as exc:
        raise DatabasePreflightError("CATMAID system user is unavailable") from exc

    _check_replica_role(connection, atomic_factory)
    target = database_target_fingerprint(connection)
    try:
        from mysite.utils import get_version

        catmaid_version = str(get_version())
    except Exception as exc:
        raise DatabasePreflightError(
            "CATMAID application version is unavailable"
        ) from exc
    return {
        "target": target,
        "catmaid_version": catmaid_version,
        "migrations": migrations,
        "objects": sorted(REQUIRED_TABLES + REQUIRED_SEQUENCES),
        "functions": sorted(REQUIRED_FUNCTIONS),
        "system_user_id": int(system_user.id),
        "replica_role": True,
    }


def allocate_sequence_ids(
    sequence: str,
    count: int,
    connection: Any | None = None,
) -> tuple[int, ...]:
    """Consume and return the exact next values from an approved CATMAID sequence."""

    if sequence not in _SEQUENCE_NAMES:
        raise ValueError(f"Unsupported CATMAID sequence: {sequence}")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("count must be a positive integer")

    connection = _default_connection(connection)
    # The sequence name is selected from the allow-list above.  It cannot be a
    # bind parameter because nextval(regclass) resolves an identifier.
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT nextval('{sequence}'::regclass) FROM generate_series(1, %s)",
            (count,),
        )
        ids = tuple(int(row[0]) for row in cursor.fetchall())
    if len(ids) != count or len(set(ids)) != count:
        raise RuntimeError(
            f"Sequence {sequence} returned {len(ids)} values for requested {count}"
        )
    return ids


def allocate_project_ids(connection: Any | None = None) -> dict[str, int]:
    """Reserve explicit IDs for one project, stack, and project-stack link."""

    connection = _default_connection(connection)
    return {
        "project_id": allocate_sequence_ids("project_id_seq", 1, connection)[0],
        "stack_id": allocate_sequence_ids("stack_id_seq", 1, connection)[0],
        "project_stack_id": allocate_sequence_ids(
            "project_stack_id_seq", 1, connection
        )[0],
    }


def allocate_batch_ids(
    skeleton_count: int,
    node_count: int,
    connection: Any | None = None,
) -> dict[str, tuple[int, ...]]:
    """Reserve exact concept and location IDs for one whole-skeleton batch."""

    if isinstance(skeleton_count, bool) or skeleton_count <= 0:
        raise ValueError("skeleton_count must be positive")
    if isinstance(node_count, bool) or node_count <= 0:
        raise ValueError("node_count must be positive")
    connection = _default_connection(connection)
    return {
        "concept_ids": allocate_sequence_ids(
            "concept_id_seq", skeleton_count * 3, connection
        ),
        "location_ids": allocate_sequence_ids(
            "location_id_seq", node_count, connection
        ),
    }


def set_local_timeouts(
    statement_timeout_ms: int,
    lock_timeout_ms: int,
    connection: Any | None = None,
) -> None:
    """Set the two operational timeouts for the current transaction."""

    if min(statement_timeout_ms, lock_timeout_ms) < 0:
        raise ValueError("database timeouts cannot be negative")
    connection = _default_connection(connection)
    if not getattr(connection, "in_atomic_block", False):
        raise RuntimeError("SET LOCAL timeouts require an active transaction")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('statement_timeout', %s, true), "
            "set_config('lock_timeout', %s, true)",
            (str(statement_timeout_ms), str(lock_timeout_ms)),
        )


@contextlib.contextmanager
def replica_load(connection: Any | None = None) -> Iterator[None]:
    """Temporarily disable ordinary triggers for COPY in an outer transaction.

    On a successful body the role is restored before materialization.  On an
    exception the surrounding transaction must roll back; attempting another
    statement in an aborted transaction would only mask the original error.
    ``SET LOCAL`` guarantees rollback restores ``origin``.
    """

    connection = _default_connection(connection)
    if not getattr(connection, "in_atomic_block", False):
        raise RuntimeError("replica_load requires an active outer transaction")
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL session_replication_role = replica")
    try:
        yield
    except BaseException:
        raise
    else:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL session_replication_role = origin")


def analyze_tables(
    connection: Any | None = None,
    *,
    tables: Sequence[str] = ANALYZE_TABLES,
) -> dict[str, Any]:
    """Run the one-time post-ingestion query-planner maintenance phase."""

    connection = _default_connection(connection)
    if getattr(connection, "in_atomic_block", False):
        raise RuntimeError("ANALYZE must run outside the batch transaction")
    normalized = tuple(tables)
    unsupported = sorted(set(normalized) - _ANALYZE_TABLE_NAMES)
    if unsupported:
        raise ValueError("Unsupported ANALYZE tables: " + ", ".join(unsupported))
    with connection.cursor() as cursor:
        for table in normalized:
            cursor.execute(f"ANALYZE {table}")
    return {"tables": list(normalized), "count": len(normalized)}
