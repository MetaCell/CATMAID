"""Opt-in database tests must name and identify a disposable database explicitly."""
from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def isolated_database():
    assert os.environ.get("CATMAID_BATCH_IMPORT_E2E") == "1"
    assert os.environ.get("CATMAID_BATCH_IMPORT_LOCAL_TEST") == "isolated", (
        "database tests require CATMAID_BATCH_IMPORT_LOCAL_TEST=isolated"
    )
    expected = {
        name: os.environ.get(f"CATMAID_BATCH_IMPORT_TEST_DB_{name}", "")
        for name in ("HOST", "NAME", "SYSTEM_IDENTIFIER")
    }
    assert all(expected.values()), (
        "set CATMAID_BATCH_IMPORT_TEST_DB_HOST, CATMAID_BATCH_IMPORT_TEST_DB_NAME "
        "and CATMAID_BATCH_IMPORT_TEST_DB_SYSTEM_IDENTIFIER for the disposable database"
    )

    from catmaid_skeleton_batch_import.database import bootstrap_django

    connection = bootstrap_django()
    assert str(connection.settings_dict["HOST"]) == expected["HOST"]
    assert str(connection.settings_dict["NAME"]) == expected["NAME"]
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT current_database(), system_identifier::text FROM pg_control_system()"
        )
        assert cursor.fetchone() == (expected["NAME"], expected["SYSTEM_IDENTIFIER"])
    return connection


@pytest.fixture
def micrometre_zigzag_swc():
    """Valid source geometry that exposed the redundant cable-length comparison."""
    return "".join(
        f"{index + 1} 3 {6000 + (index % 2) * 0.35:.8f} "
        f"{index * 0.01:.8f} 0 1 {-1 if index == 0 else index}" + chr(10)
        for index in range(10001)
    )


@pytest.fixture(scope="module")
def existing_project_factory(isolated_database):
    """Provision targets through ordinary CATMAID models, outside the importer."""
    from catmaid.apps import get_system_user
    from catmaid.control.tracing import setup_tracing
    from catmaid.models import NodeGridCache, Project, ProjectStack, Stack
    from django.db import transaction
    from guardian.shortcuts import assign_perm

    def create(*, dimension=(100, 100, 100), resolution_nm=(1000, 1000, 1000)):
        system_user = get_system_user()
        with transaction.atomic():
            project = Project.objects.create(title="Existing append regression target")
            setup_tracing(project.id, system_user)
            stack = Stack.objects.create(
                title="Existing regression stack", dimension=dimension,
                resolution=resolution_nm, downsample_factors=None,
                metadata={"cache_provider": "cached_msgpack_grid", "read_only": False,
                          "spatial": [{"chunk_size": [1500000] * 3, "limit": 0}]},
            )
            ProjectStack.objects.create(project=project, stack=stack)
            NodeGridCache.objects.create(
                project=project, orientation=0, cell_width=1500000,
                cell_height=1500000, cell_depth=1500000, has_msgpack_data=True,
                hidden_last_editor_id=None,
            )
            for permission in ("can_browse", "can_annotate", "can_import"):
                assign_perm(permission, system_user, project)
        return project.id

    return create
