from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

import pytest


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.usefixtures("isolated_database"),
    pytest.mark.skipif(
        os.environ.get("CATMAID_BATCH_IMPORT_E2E") != "1",
        reason="set CATMAID_BATCH_IMPORT_E2E=1 to mutate a configured test database",
    ),
]


def test_known_rollback_preserves_prior_batch_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.errors import RetryableError
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion, run_ingestion
    from catmaid_skeleton_batch_import.state import StateStore

    archive_path = tmp_path / "skeletons.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "alpha.swc",
            "1 1 1 1 1 -1 -1\n2 3 2 1 1 0.5 1\n3 3 3 2 1 0.4 2\n",
        )
        archive.writestr(
            "beta.swc",
            "10 1 4 4 2 1 -1\n11 3 4 5 2 0.7 10\n12 3 5 6 2.5 0.6 11\n",
        )
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": {
                    "archive_path": str(archive_path),
                    "coordinate_unit": "um",
                },
                "stack": {
                    "dimension": [100, 100, 100],
                    "resolution_nm": [1000, 1000, 1000],
                },
            }
        ),
        encoding="utf-8",
    )
    state_dir = tmp_path / "state"
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "3")
    plan_ingestion(request_path, state_dir)

    original = materialization.materialize_and_verify_batch
    calls = 0

    def fail_second_batch(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RetryableError("injected materialization failure")
        return original(**kwargs)

    monkeypatch.setattr(
        materialization, "materialize_and_verify_batch", fail_second_batch
    )
    with pytest.raises(RetryableError, match="injected materialization failure"):
        run_ingestion(state_dir)

    state = StateStore(state_dir).load()
    assert [batch["status"] for batch in state["batches"]] == [
        "committed",
        "retryable",
    ]

    monkeypatch.setattr(materialization, "materialize_and_verify_batch", original)
    result = run_ingestion(state_dir)
    assert result["status"] == "ready_for_publication"
    assert result["nodes"] == 6
    assert result["skeletons"] == 2
    assert [batch["status"] for batch in StateStore(state_dir).load()["batches"]] == [
        "committed",
        "committed",
    ]


def test_append_to_populated_project_preserves_old_data_stacks_permissions_and_caches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_database,
) -> None:
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion, run_ingestion
    from guardian.models import UserObjectPermission, GroupObjectPermission
    from catmaid.models import Project
    from django.contrib.contenttypes.models import ContentType
    from catmaid.control import node as node_control
    import msgpack
    import psycopg2

    connection = isolated_database
    def request(directory, name, target=None):
        directory.mkdir()
        archive = directory / "input.zip"
        with zipfile.ZipFile(archive, "w") as source:
            source.writestr(name + ".swc", "1 1 1 1 1 0.5 -1\n2 3 2 1 1 -1 1\n")
        value = {"schema_version": 1, "source": {
            "archive_path": str(archive), "coordinate_unit": "um"}}
        if target is None:
            value["stack"] = {"dimension": [100, 100, 100], "resolution_nm": [1000] * 3}
        else:
            value["target"] = {"project_id": target}
        path = directory / "request.json"
        path.write_text(json.dumps(value))
        plan_ingestion(path, directory / "state")
        return directory / "state"

    seed = run_ingestion(request(tmp_path / "seed", "existing"))
    pid = seed["project_id"]
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM node_grid_cache WHERE project_id=%s", [pid])
        grid_id = cursor.fetchone()[0]
        # A valid empty cache cell far from the imported geometry is a sentinel
        # for accidental project-wide regeneration or deletion.
        cursor.execute("""
            INSERT INTO node_grid_cache_cell
                (grid_id,x_index,y_index,z_index,msgpack_data)
            VALUES (%s,99,99,99,%s::bytea[])
        """, [grid_id, [psycopg2.Binary(msgpack.packb([[], [], {}, False, {}, {}]))]])
    def cache_cells():
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT x_index,y_index,z_index,update_time,msgpack_data
                FROM node_grid_cache_cell WHERE grid_id=%s ORDER BY x_index,y_index,z_index
            """, [grid_id])
            return {(x, y, z): (updated, [None if value is None else bytes(value) for value in data])
                    for x, y, z, updated, data in cursor.fetchall()}
    cells_before = cache_cells()
    visible_during_refresh = []
    original_update = node_control.update_grid_cell
    # A second DB session must be able to browse the old payload while the
    # importer has already written (but not yet committed) the replacement.
    observer = psycopg2.connect(**connection.get_connection_params())
    observer.autocommit = True
    def observe_update(*args, **kwargs):
        result = original_update(*args, **kwargs)
        if args[1:5] == (grid_id, 0, 0, 0):
            with observer.cursor() as cursor:
                cursor.execute("SET statement_timeout='2s'")
                cursor.execute("""
                    SELECT msgpack_data FROM node_grid_cache_cell
                    WHERE grid_id=%s AND x_index=0 AND y_index=0 AND z_index=0
                """, [grid_id])
                payload = [None if value is None else bytes(value) for value in cursor.fetchone()[0]]
            visible_during_refresh.append(payload)
        return result
    content_type = ContentType.objects.get_for_model(Project)
    def snapshot():
        with connection.cursor() as cursor:
            cursor.execute("SELECT id, title, comment FROM project WHERE id=%s", [pid])
            project_row = cursor.fetchall()
            cursor.execute("SELECT * FROM project_stack WHERE project_id=%s ORDER BY id", [pid])
            stacks = cursor.fetchall()
            cursor.execute("SELECT * FROM node_grid_cache WHERE project_id=%s ORDER BY id", [pid])
            grids = cursor.fetchall()
            cursor.execute("SELECT * FROM treenode WHERE project_id=%s ORDER BY id", [pid])
            nodes = cursor.fetchall()
        permissions = {"content_type": content_type, "object_pk": str(pid)}
        users = list(UserObjectPermission.objects.filter(**permissions).order_by("id").values())
        groups = list(GroupObjectPermission.objects.filter(**permissions).order_by("id").values())
        return project_row, stacks, grids, nodes, users, groups

    before = snapshot()
    append_state = request(tmp_path / "append", "new-fragment", pid)
    monkeypatch.setattr(node_control, "update_grid_cell", observe_update)
    try:
        result = run_ingestion(append_state)
    finally:
        observer.close()
        monkeypatch.setattr(node_control, "update_grid_cell", original_update)
    after = snapshot()
    assert result["status"] == "complete"
    assert result["project_id"] == pid
    assert result["stack_id"] is None
    assert after[:3] == before[:3]
    assert after[4:] == before[4:]
    assert after[3][:len(before[3])] == before[3]
    assert len(after[3]) == len(before[3]) + 2
    cells_after = cache_cells()
    assert cells_after[(99, 99, 99)] == cells_before[(99, 99, 99)]
    assert cells_after[(0, 0, 0)][0] > cells_before[(0, 0, 0)][0]
    assert visible_during_refresh == [cells_before[(0, 0, 0)][1]]
    cached_ids = {
        row[0] for bucket in cells_after[(0, 0, 0)][1] if bucket is not None
        for row in msgpack.unpackb(bucket, strict_map_key=False)[0]
    }
    assert cached_ids == {row[0] for row in after[3]}
    report = json.loads(Path(result["verification"]).read_text())
    assert report["cache"]["verified"] is True
    assert report["cache"]["cell_count"] == 1
    assert run_ingestion(append_state) == result
    assert snapshot() == after

    # A fresh workspace must not duplicate an earlier import's source IDs.
    collision = request(tmp_path / "collision", "new-fragment", pid)
    from catmaid_skeleton_batch_import.errors import InvalidInputError
    with pytest.raises(InvalidInputError, match="already exist"):
        run_ingestion(collision)
    assert snapshot() == after
