"""Exercise migration 0127 on a temporary clone of the guarded disposable database."""
from __future__ import annotations

from copy import deepcopy
import os
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("CATMAID_BATCH_IMPORT_E2E") != "1",
        reason="requires the guarded disposable CATMAID/PostGIS database",
    ),
]

BEFORE = ("catmaid", "0126_fix_single_node_selective_summary")
AFTER = ("catmaid", "0127_project_active_skeleton_ingestion")
COLUMN = "active_skeleton_ingestion"


@pytest.fixture
def migration_database(isolated_database):
    """Never downgrade the shared fixture: partial-import tests may retain locks."""
    import psycopg2
    from psycopg2 import sql
    from django.db import connections

    alias = "project_ingestion_migration"
    database_name = "catmaid_lock_migration_" + uuid4().hex
    configuration = deepcopy(isolated_database.settings_dict)
    parameters = isolated_database.get_connection_params()
    template_name = configuration["NAME"]
    parameters["dbname"] = "postgres"
    parameters["connect_timeout"] = 5
    administrator = psycopg2.connect(**parameters)
    administrator.autocommit = True
    created = False
    connection = None
    # PostgreSQL requires the template to have no other sessions. Other tests keep
    # this wrapper, which reconnects normally after this fixture has finished.
    isolated_database.close()
    try:
        with administrator.cursor() as cursor:
            cursor.execute("SET statement_timeout = '10s'")
            cursor.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(database_name), sql.Identifier(template_name),
                )
            )
        created = True
        configuration["NAME"] = database_name
        connections.databases[alias] = configuration
        connection = connections[alias]
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'project'
                  AND column_name = %s
            """, [COLUMN])
            if cursor.fetchone() is not None:
                # These are copied flags in our new disposable clone, never the
                # original database's owners. Start each migration trial unlocked.
                cursor.execute("UPDATE project SET active_skeleton_ingestion = NULL "
                               "WHERE active_skeleton_ingestion IS NOT NULL")
        yield connection
    finally:
        if connection is not None:
            connection.close()
            del connections[alias]
            del connections.databases[alias]
        try:
            if created:
                with administrator.cursor() as cursor:
                    cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(database_name)))
        finally:
            administrator.close()


def migrate(connection, target):
    from django.db.migrations.executor import MigrationExecutor

    executor = MigrationExecutor(connection)
    executor.migrate([target])
    return executor.loader.project_state([target]).apps.get_model("catmaid", "Project")


def columns(connection, relation):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """, [relation])
        return {name: (kind, nullable) for name, kind, nullable in cursor.fetchall()}


def history_rows(connection, project_id):
    with connection.cursor() as cursor:
        cursor.execute("SELECT title, comment FROM project__history WHERE id = %s "
                       "ORDER BY title, comment", [project_id])
        return cursor.fetchall()


def test_0127_forward_and_reverse_preserve_projects_history_and_tracking(migration_database):
    connection = migration_database
    OldProject = migrate(connection, BEFORE)
    assert COLUMN not in columns(connection, "project")
    project = OldProject.objects.using(connection.alias).create(
        title="Before migration 0127", comment="original")
    project.comment = "edited before migration"
    project.save(using=connection.alias, update_fields=["comment"])
    original_history = history_rows(connection, project.pk)
    assert (project.title, "original") in original_history

    NewProject = migrate(connection, AFTER)
    field = NewProject._meta.get_field(COLUMN)
    assert field.get_internal_type() == "UUIDField"
    assert field.null and field.blank and not field.editable
    for relation in ("project", "project__history", "project__with_history"):
        assert columns(connection, relation)[COLUMN] == ("uuid", "YES")
    restored = NewProject.objects.using(connection.alias).get(pk=project.pk)
    assert restored.comment == project.comment
    assert restored.active_skeleton_ingestion is None
    assert history_rows(connection, project.pk) == original_history

    # Exercise the actual generated history triggers with the new UUID column.
    from catmaid.ingestion_lock import acquire_project_ingestion, release_project_ingestion

    token = uuid4()
    acquire_project_ingestion(project.pk, token, using=connection.alias)
    release_project_ingestion(project.pk, token, using=connection.alias)
    with connection.cursor() as cursor:
        cursor.execute("SELECT active_skeleton_ingestion FROM project__history WHERE id = %s",
                       [project.pk])
        assert token in {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT active_skeleton_ingestion FROM project__with_history WHERE id = %s",
                       [project.pk])
        assert token in {row[0] for row in cursor.fetchall()}
    before_reverse = history_rows(connection, project.pk)

    OldProject = migrate(connection, BEFORE)
    for relation in ("project", "project__history", "project__with_history"):
        assert COLUMN not in columns(connection, relation)
    assert history_rows(connection, project.pk) == before_reverse
    restored = OldProject.objects.using(connection.alias).get(pk=project.pk)
    assert restored.title == project.title and restored.comment == project.comment
    restored.comment = "edited after rollback"
    restored.save(using=connection.alias, update_fields=["comment"])
    after_reverse = history_rows(connection, project.pk)
    assert len(after_reverse) == len(before_reverse) + 1
    assert (project.title, project.comment) in after_reverse
    with connection.cursor() as cursor:
        cursor.execute("SELECT comment FROM project__with_history WHERE id = %s", [project.pk])
        assert "edited after rollback" in {row[0] for row in cursor.fetchall()}


def test_0127_reverse_refuses_active_owner_without_changing_schema_or_history(migration_database):
    from catmaid.ingestion_lock import acquire_project_ingestion, release_project_ingestion
    from django.db import DatabaseError
    from django.db.migrations.recorder import MigrationRecorder

    connection = migration_database
    OldProject = migrate(connection, BEFORE)
    project = OldProject.objects.using(connection.alias).create(title="Locked rollback trial")
    migrate(connection, AFTER)
    token = uuid4()
    acquire_project_ingestion(project.pk, token, using=connection.alias)
    before_reverse = history_rows(connection, project.pk)

    with pytest.raises(DatabaseError, match="Resolve project ingestion locks"):
        migrate(connection, BEFORE)

    assert AFTER in MigrationRecorder(connection).applied_migrations()
    for relation in ("project", "project__history", "project__with_history"):
        assert COLUMN in columns(connection, relation)
    with connection.cursor() as cursor:
        cursor.execute("SELECT active_skeleton_ingestion FROM project WHERE id = %s", [project.pk])
        assert cursor.fetchone()[0] == token
    assert history_rows(connection, project.pk) == before_reverse

    # Tracking must still work after the rejected reverse; clearing the owner is
    # recorded in history, and a subsequent rollback is allowed.
    release_project_ingestion(project.pk, token, using=connection.alias)
    with connection.cursor() as cursor:
        cursor.execute("SELECT active_skeleton_ingestion FROM project__history WHERE id = %s",
                       [project.pk])
        assert token in {row[0] for row in cursor.fetchall()}
    migrate(connection, BEFORE)
    assert AFTER not in MigrationRecorder(connection).applied_migrations()
    assert COLUMN not in columns(connection, "project")
