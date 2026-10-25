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
