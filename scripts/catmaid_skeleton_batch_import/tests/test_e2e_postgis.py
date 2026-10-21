from __future__ import annotations

import json
import csv
import gzip
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


@pytest.mark.parametrize("coordinate_unit", ["nm", "um"])
@pytest.mark.parametrize("existing_project", [False, True])
def test_single_node_import_rolls_back_resumes_and_is_cached(
    tmp_path, monkeypatch, isolated_database, coordinate_unit, existing_project,
):
    import msgpack
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.artifacts import read_manifest
    from catmaid_skeleton_batch_import.errors import RetryableError
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion, run_ingestion
    from catmaid_skeleton_batch_import.state import StateStore

    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "2")
    ordinary = "1 1 10 20 30 -1 -1 # root\n2 3 11 20 30 1 1 # child\n"

    def workspace(name, members, project_id=None):
        directory = tmp_path / name
        directory.mkdir()
        archive = directory / "input.zip"
        with zipfile.ZipFile(archive, "w") as source:
            for member, content in members.items():
                source.writestr(member, content)
        request = {"schema_version": 1, "source": {
            "archive_path": str(archive), "coordinate_unit": coordinate_unit}}
        if project_id is None:
            request["stack"] = {"dimension": [100, 100, 100], "resolution_nm": [1000] * 3}
        else:
            request["target"] = {"project_id": project_id}
        path = directory / "request.json"
        path.write_text(json.dumps(request))
        plan_ingestion(path, directory / "state")
        return directory / "state"

    seed_id = None
    if existing_project:
        seed = run_ingestion(workspace("seed", {"existing.swc": ordinary}))
        seed_id = seed["project_id"]
    point = ("73 1 1.25 2.5 3.75 0.5 -1 # point soma\n" if coordinate_unit == "um"
             else "73 1 1250 2500 3750 500 -1 # point soma\n")
    state_dir = workspace("import", {"alpha.swc": ordinary, "point.swc": point}, seed_id)
    original = materialization.materialize_and_verify_batch

    def fail_after_singleton_materialization(**kwargs):
        result = original(**kwargs)
        if kwargs["skeletons"][0]["node_count"] == 1:
            raise RetryableError("injected failure after singleton summary creation")
        return result

    monkeypatch.setattr(materialization, "materialize_and_verify_batch",
                        fail_after_singleton_materialization)
    with pytest.raises(RetryableError, match="singleton summary"):
        run_ingestion(state_dir)
    state = StateStore(state_dir).load()
    assert [batch["status"] for batch in state["batches"]] == ["committed", "retryable"]
    maps_before = [batch["artifacts"]["node_map"] for batch in state["batches"]]
    assert [item["count"] for item in maps_before] == [2, 1]
    assert all((state_dir / item["path"]).is_file() for item in maps_before)
    project_id = state["project"]["project_id"]
    with isolated_database.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM class_instance WHERE project_id=%s AND name=%s",
                       [project_id, "source-fragment:point"])
        assert cursor.fetchone()[0] == 0
    monkeypatch.setattr(materialization, "materialize_and_verify_batch", original)
    result = run_ingestion(state_dir)
    assert [batch["artifacts"]["node_map"] for batch in StateStore(state_dir).load()["batches"]] == maps_before
    assert result["status"] == ("complete" if existing_project else "ready_for_publication")
    assert result["skeletons"] == 2
    assert result["nodes"] == 3
    point_row = next(row for row in read_manifest(Path(result["manifest"]))
                     if row["member_path"] == "point.swc")
    with isolated_database.cursor() as cursor:
        cursor.execute("""
            SELECT t.id, t.parent_id, t.location_x, t.location_y, t.location_z, t.radius,
                   s.num_nodes, s.cable_length, e.parent_id, ST_3DLength(e.edge),
                   s.original_creation_time=t.creation_time,
                   s.last_edition_time=t.edition_time, s.last_editor_id=t.editor_id
            FROM treenode t JOIN catmaid_skeleton_summary s ON s.skeleton_id=t.skeleton_id
            JOIN treenode_edge e ON e.id=t.id WHERE t.skeleton_id=%s
        """, [int(point_row["skeleton_id"])])
        rows = cursor.fetchall()
        assert len(rows) == 1
        node_id, *values = rows[0]
        assert values == [None, 1250, 2500, 3750, 500, 1, 0, None, 0, True, True, True]
        with gzip.open(state_dir / maps_before[1]["path"], "rt", encoding="utf-8", newline="") as source:
            assert list(csv.DictReader(source)) == [{
                "member_path": "point.swc", "source_node_id": "73", "treenode_id": str(node_id),
            }]
        cursor.execute("""
            SELECT c.msgpack_data FROM node_grid_cache_cell c
            JOIN node_grid_cache g ON g.id=c.grid_id WHERE g.project_id=%s
        """, [project_id])
        cached_ids = {row[0] for (buckets,) in cursor.fetchall() for bucket in buckets
                      if bucket is not None
                      for row in msgpack.unpackb(bytes(bucket), strict_map_key=False)[0]}
        assert node_id in cached_ids
    assert run_ingestion(state_dir) == result


def test_known_rollback_preserves_prior_batch_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_database,
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
    maps_before = [batch["artifacts"]["node_map"] for batch in state["batches"]]
    saved_rows = []
    for descriptor in maps_before:
        with gzip.open(state_dir / descriptor["path"], "rt", encoding="utf-8", newline="") as source:
            saved_rows.append(list(csv.DictReader(source)))
    with isolated_database.cursor() as cursor:
        cursor.execute("SELECT id FROM treenode WHERE id=ANY(%s::bigint[])",
                       [[int(row["treenode_id"]) for row in saved_rows[1]]])
        assert cursor.fetchall() == []  # A saved mapping alone does not mean COMMIT.

    # A workspace from the previous importer can already have committed batches
    # without node maps. Resume derives these from its original reserved IDs.
    del state["batches"][0]["artifacts"]["node_map"]
    (state_dir / maps_before[0]["path"]).unlink()
    (state_dir / "state.json").write_text(json.dumps(state))

    monkeypatch.setattr(materialization, "materialize_and_verify_batch", original)
    result = run_ingestion(state_dir)
    assert result["status"] == "ready_for_publication"
    assert result["nodes"] == 6
    assert result["skeletons"] == 2
    assert [batch["status"] for batch in StateStore(state_dir).load()["batches"]] == [
        "committed",
        "committed",
    ]
    assert [batch["artifacts"]["node_map"] for batch in StateStore(state_dir).load()["batches"]] == maps_before
    with isolated_database.cursor() as cursor:
        for rows in saved_rows:
            cursor.execute("SELECT id, parent_id FROM treenode WHERE id=ANY(%s::bigint[]) ORDER BY id",
                           [[int(row["treenode_id"]) for row in rows]])
            ids = [int(row["treenode_id"]) for row in rows]
            assert cursor.fetchall() == [(ids[0], None), (ids[1], ids[0]), (ids[2], ids[1])]


def test_append_to_populated_project_preserves_old_data_stacks_permissions_and_caches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_database,
) -> None:
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion, run_ingestion
    from guardian.models import UserObjectPermission, GroupObjectPermission
    from catmaid.models import Project
    from django.contrib.contenttypes.models import ContentType
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
        # Non-default definitions must survive the full payload rebuild.
        cursor.execute("""
            UPDATE node_grid_cache SET has_json_data=true, has_json_text_data=true,
                n_lod_levels=2, lod_strategy='linear', enabled=false WHERE id=%s
        """, [grid_id])
        # Obsolete payloads are removed, rather than treated as configuration.
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
    old_node_ids = [row[0] for row in before[3]]
    def old_derived_rows():
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM treenode_edge WHERE id=ANY(%s::bigint[]) ORDER BY id", [old_node_ids])
            edges = cursor.fetchall()
            cursor.execute("""
                SELECT * FROM catmaid_skeleton_summary WHERE skeleton_id IN (
                    SELECT skeleton_id FROM treenode WHERE id=ANY(%s::bigint[])) ORDER BY skeleton_id
            """, [old_node_ids])
            return edges, cursor.fetchall()
    old_derived = old_derived_rows()
    append_state = request(tmp_path / "append", "new-fragment", pid)
    result = run_ingestion(append_state)
    after = snapshot()
    assert result["status"] == "complete"
    assert result["project_id"] == pid
    assert result["stack_id"] is None
    assert after[:3] == before[:3]
    assert after[4:] == before[4:]
    assert old_derived_rows() == old_derived
    assert after[3][:len(before[3])] == before[3]
    assert len(after[3]) == len(before[3]) + 2
    cells_after = cache_cells()
    assert (99, 99, 99) not in cells_after
    assert cells_after[(0, 0, 0)][0] > cells_before[(0, 0, 0)][0]
    cached_ids = {
        row[0] for bucket in cells_after[(0, 0, 0)][1] if bucket is not None
        for row in msgpack.unpackb(bucket, strict_map_key=False)[0]
    }
    assert cached_ids == {row[0] for row in after[3]}
    report = json.loads(Path(result["verification"]).read_text())
    assert report["cache"]["verified"] is True
    assert report["cache"]["cell_count"] == 1
    assert report["cache"]["strategy"] == "offline_full_grid_rebuild"
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT cardinality(json_data), cardinality(json_text_data), cardinality(msgpack_data)
            FROM node_grid_cache_cell WHERE grid_id=%s AND x_index=0 AND y_index=0 AND z_index=0
        """, [grid_id])
        assert cursor.fetchone() == (2, 2, 2)
    assert run_ingestion(append_state) == result
    assert cache_cells() == cells_after  # Verification does not regenerate payloads.
    assert snapshot() == after
    assert run_ingestion(append_state, rebuild_cache=True) == result
    assert snapshot() == after

    # A fresh workspace must not duplicate an earlier import's source IDs.
    collision = request(tmp_path / "collision", "new-fragment", pid)
    from catmaid_skeleton_batch_import.errors import InvalidInputError
    with pytest.raises(InvalidInputError, match="already exist"):
        run_ingestion(collision)
    assert snapshot() == after

    # Complete neuron deletion releases its source ID for a fresh import.
    from catmaid.apps import get_system_user
    from catmaid.control.neuron import _delete_neuron_as_user
    from catmaid_skeleton_batch_import.artifacts import read_manifest

    imported = read_manifest(Path(result["manifest"]))[0]
    _delete_neuron_as_user(get_system_user(), pid, int(imported["neuron_id"]))
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM class_instance WHERE project_id=%s AND name=%s",
                       [pid, "source-fragment:new-fragment"])
        assert cursor.fetchone()[0] == 0
    reimported = run_ingestion(request(tmp_path / "reimport", "new-fragment", pid))
    assert reimported["status"] == "complete"
    new_row = read_manifest(Path(reimported["manifest"]))[0]
    assert new_row["fragment_id"] == imported["fragment_id"]
    assert new_row["skeleton_id"] != imported["skeleton_id"]
    assert old_derived_rows() == old_derived
