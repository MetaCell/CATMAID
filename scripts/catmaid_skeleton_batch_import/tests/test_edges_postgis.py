"""Database-result regression tests, restricted to the disposable Docker clone."""
from __future__ import annotations

import csv
import io
import json
import math
import os
from pathlib import Path
import zipfile

import pytest
from catmaid_skeleton_batch_import.errors import InvalidInputError

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("CATMAID_BATCH_IMPORT_E2E") != "1",
        reason="requires the guarded local CATMAID/PostGIS clone",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def local_database(isolated_database):
    return isolated_database


@pytest.fixture(scope="module")
def project(local_database):
    from catmaid_skeleton_batch_import.database import allocate_project_ids
    from catmaid_skeleton_batch_import.project import create_hidden_project
    from catmaid_skeleton_batch_import.loader import ProjectContext
    return ProjectContext.from_dict(create_hidden_project(
        **allocate_project_ids(), title="Disposable edge regression tests",
        dimension=(100000000, 100000000, 100000000), resolution_nm=(1, 1, 1),
    ))


@pytest.fixture
def prepared(project):
    from catmaid_skeleton_batch_import.database import allocate_batch_ids
    from catmaid_skeleton_batch_import.domain import IngestionRequest, PlannedBatch, PlannedMember, SwcNode
    from catmaid_skeleton_batch_import.loader import prepare_copy_rows
    # Unordered, noncontiguous source AND allocated IDs; two roots, branches,
    # coincident coordinates, and float32 boundaries at 1 and 2**24.
    nodes = (
        SwcNode(51, 3, 16777217.000001, 1.00000007, 3, .5, 10),
        SwcNode(10, 1, 16777217, 1.00000005, 3, -1, -1),
        SwcNode(902, 3, 16777217, 1.00000005, 3, .4, 10),
        SwcNode(80, 3, 16777219, 2, 4, .3, 51),
    )
    positions = {node.source_id: (node.x, node.y, node.z) for node in nodes}
    cable_length = math.dist(positions[51], positions[10]) + math.dist(positions[80], positions[51])
    members = tuple(PlannedMember(name, 4, 0, 0, name, cable_length, name[:-4])
                    for name in ("a.swc", "b.swc"))
    batch = PlannedBatch(1, members)
    ids = allocate_batch_ids(2, 16)
    locations = tuple(reversed(ids["location_ids"][::2]))
    request = IngestionRequest(1, Path("/unused.zip"), "nm",
                               (100000000,) * 3, (1, 1, 1))
    with prepare_copy_rows(batch, request, project, ids["concept_ids"], locations,
                           lambda _: nodes) as result:
        yield result


def edge_rows(connection, ids):
    with connection.cursor() as c:
        c.execute("SELECT id,parent_id,project_id,ST_AsEWKB(edge) FROM treenode_edge "
                  "WHERE id=ANY(%s::bigint[]) ORDER BY id", [list(ids)])
        return [(a, b, p, bytes(g)) for a, b, p, g in c.fetchall()]


def summaries(connection, ids):
    with connection.cursor() as c:
        c.execute("SELECT * FROM catmaid_skeleton_summary WHERE skeleton_id=ANY(%s::bigint[]) "
                  "ORDER BY skeleton_id", [list(ids)])
        return c.fetchall()


def connector_snapshot(connection):
    with connection.cursor() as c:
        result = {}
        for table in ("connector", "connector_geom", "treenode_connector", "treenode_connector_edge"):
            c.execute(f"SELECT row_to_json(t)::text FROM {table} t ORDER BY id")
            result[table] = c.fetchall()
        return result


def assert_no_temp(connection):
    with connection.cursor() as c:
        c.execute("SELECT to_regclass('pg_temp.catmaid_import_edge_nodes')")
        assert c.fetchone()[0] is None


def assert_absent(connection, prepared):
    with connection.cursor() as c:
        for table, column, ids in (
            ("class_instance", "id", [i for s in prepared.expected_skeletons for i in (s.neuron_id, s.skeleton_id, s.annotation_id)]),
            ("class_instance_class_instance", "id", [i for s in prepared.expected_skeletons for i in (s.link_id, s.annotation_link_id)]),
            ("treenode", "id", prepared.location_ids),
            ("treenode_edge", "id", prepared.location_ids),
            ("catmaid_skeleton_summary", "skeleton_id", prepared.skeleton_ids),
        ):
            c.execute(f"SELECT count(*) FROM {table} WHERE {column}=ANY(%s::bigint[])", [list(ids)])
            assert c.fetchone()[0] == 0, table
    assert_no_temp(connection)


class RollbackTrial(Exception):
    pass


def test_exact_canonical_geometry_summary_and_connectors(local_database, project, prepared, monkeypatch):
    from django.db import connection
    from catmaid.control.edge import rebuild_edges_selectively
    from catmaid_skeleton_batch_import.config import load_settings
    from catmaid_skeleton_batch_import.loader import execute_batch_transaction

    before = connector_snapshot(connection)
    observed = {}
    # Seed real connector rows after core COPY but before edge creation, and
    # vary timestamps so parity checks include summary metadata semantics.
    from catmaid_skeleton_batch_import import materialization
    original_materialize = materialization.materialize_and_verify_batch
    connector_before = {}
    def seed_then_materialize(**kwargs):
        with connection.cursor() as c:
            c.execute("SET LOCAL session_replication_role=replica")
            c.execute("""
                UPDATE treenode
                SET creation_time=TIMESTAMPTZ '2020-01-01 00:00:00+00'
                                  + (id %% 3) * interval '1 day',
                    edition_time=TIMESTAMPTZ '2021-01-01 00:00:00+00'
                                  + (id %% 4) * interval '1 day'
                WHERE id=ANY(%s::bigint[])
            """, [list(prepared.location_ids)])
            c.execute("SET LOCAL session_replication_role=origin")
            c.execute("SELECT id FROM relation WHERE project_id=%s AND relation_name='presynaptic_to'",
                      [project.project_id])
            relation_id = c.fetchone()[0]
            c.execute("""INSERT INTO connector
                (project_id,user_id,editor_id,location_x,location_y,location_z)
                VALUES (%s,%s,%s,5,6,7) RETURNING id""",
                [project.project_id, project.user_id, project.user_id])
            connector_id = c.fetchone()[0]
            c.execute("""INSERT INTO treenode_connector
                (project_id,user_id,relation_id,treenode_id,connector_id,skeleton_id)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                [project.project_id, project.user_id, relation_id, prepared.location_ids[0],
                 connector_id, prepared.skeleton_ids[0]])
        connector_before.update(connector_snapshot(connection))
        assert all(connector_before.values())
        return original_materialize(**kwargs)
    monkeypatch.setattr(materialization, "materialize_and_verify_batch", seed_then_materialize)
    def compare():

        direct = edge_rows(connection, prepared.location_ids)
        direct_summary = summaries(connection, prepared.skeleton_ids)
        assert connector_snapshot(connection) == connector_before
        assert len(direct) == 8
        with connection.cursor() as c:
            c.execute("SELECT count(*) FROM treenode_edge WHERE id=ANY(%s::bigint[]) "
                      "AND parent_id IS NULL AND ST_3DLength(edge)=0", [list(prepared.location_ids)])
            assert c.fetchone()[0] == 2
            # Modify only this rolled-back trial's edges to compare the same
            # core rows and transaction timestamps through CATMAID's helper.
            c.execute("DELETE FROM treenode_edge WHERE id=ANY(%s::bigint[])", [list(prepared.location_ids)])
            c.execute("DELETE FROM catmaid_skeleton_summary WHERE skeleton_id=ANY(%s::bigint[])",
                      [prepared.skeleton_ids])
            rebuild_edges_selectively(prepared.skeleton_ids)
            c.execute("SELECT public.refresh_skeleton_summary_table_selectively(%s::bigint[])",
                      [prepared.skeleton_ids])
        assert edge_rows(connection, prepared.location_ids) == direct
        assert summaries(connection, prepared.skeleton_ids) == direct_summary
        assert connector_snapshot(connection) == connector_before
        raise RollbackTrial

    def timing(name, duration, _counts):
        observed[name] = duration
        if name == "exact_treenode_validation":
            compare()
    with pytest.raises(RollbackTrial):
        execute_batch_transaction(prepared, project, load_settings(), timing)
    assert {"edge_staging", "edge_creation", "summary_refresh", "materialization"} <= observed.keys()
    assert_absent(connection, prepared)
    assert connector_snapshot(connection) == before


@pytest.mark.parametrize("corruption", [
    "missing_parent", "cross_skeleton", "duplicate", "count", "recorded_id", "project", "skeleton",
])
def test_rejects_bad_staged_rows(local_database, project, prepared, corruption):
    from django.db import transaction, IntegrityError
    from psycopg2 import IntegrityError as CopyIntegrityError
    from catmaid_skeleton_batch_import.edges import create_batch_edges
    from catmaid_skeleton_batch_import.errors import VerificationError
    rows = list(csv.reader(prepared.treenode_file))
    if corruption == "missing_parent":
        rows[0][9] = "9223372036854770000"
    elif corruption == "cross_skeleton":
        rows[0][9] = rows[5][0]
    elif corruption == "duplicate":
        rows[1][0] = rows[0][0]
    elif corruption == "count":
        rows.pop()
    elif corruption == "recorded_id":
        rows[0][0] = "9223372036854770000"
    elif corruption == "project":
        rows[0][1] = str(project.project_id + 1000000)
    else:
        rows[0][7] = str(int(rows[0][7]) + 1000000)
    source = io.StringIO()
    csv.writer(source).writerows(rows)
    with pytest.raises((VerificationError, IntegrityError, CopyIntegrityError)):
        with transaction.atomic():
            with local_database.cursor() as c:
                create_batch_edges(cursor=c, treenode_file=source,
                    location_ids=prepared.location_ids,
                    skeletons=[s.to_validation_dict() for s in prepared.expected_skeletons],
                    project_id=project.project_id, user_id=project.user_id)
    assert_absent(local_database, prepared)


def test_existing_edge_fails_without_deletion(local_database, project, prepared):
    from django.db import transaction, IntegrityError
    from catmaid_skeleton_batch_import.config import load_settings
    from catmaid_skeleton_batch_import.edges import create_batch_edges
    from catmaid_skeleton_batch_import.loader import execute_batch_transaction
    def conflict():
        before = edge_rows(local_database, prepared.location_ids)
        # Remove the first staging table only, in a test-owned savepoint.
        # Repeated edge creation must reject the existing PKs.
        with pytest.raises(IntegrityError):
            with transaction.atomic():
                with local_database.cursor() as c:
                    c.execute("DROP TABLE pg_temp.catmaid_import_edge_nodes")
                    create_batch_edges(cursor=c, treenode_file=prepared.treenode_file,
                        location_ids=prepared.location_ids,
                        skeletons=[s.to_validation_dict() for s in prepared.expected_skeletons],
                        project_id=project.project_id, user_id=project.user_id)
        assert edge_rows(local_database, prepared.location_ids) == before
        raise RollbackTrial
    with pytest.raises(RollbackTrial):
        execute_batch_transaction(prepared, project, load_settings(),
                                  lambda name, *_args: conflict() if name == "exact_treenode_validation" else None)
    assert_absent(local_database, prepared)


def make_request(tmp_path):
    archive = tmp_path / "small.zip"
    with zipfile.ZipFile(archive, "w") as z:
        for name in ("a.swc", "b.swc"):
            z.writestr(name, "10 1 1 1 1 -1 -1\n90 3 2 1 1 .5 10\n30 3 1 2 1 .3 10\n")
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"schema_version": 1,
        "source": {"archive_path": str(archive), "coordinate_unit": "nm"},
        "stack": {"dimension": [10, 10, 10], "resolution_nm": [1, 1, 1]}}))
    return request


@pytest.mark.parametrize("phase", ["edge_staging", "edge_creation", "summary_refresh"])
def test_phase_failure_rolls_back_second_batch_and_is_terminal(local_database, tmp_path, monkeypatch, phase):
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.errors import OperationalFailureError
    from catmaid_skeleton_batch_import.orchestrator import plan_ingestion, run_ingestion
    from catmaid_skeleton_batch_import.state import StateStore
    from catmaid_skeleton_batch_import.artifacts import read_id_artifact
    monkeypatch.setenv("CATMAID_BATCH_IMPORT_MAX_NODES_PER_BATCH", "3")
    request = make_request(tmp_path)
    state_dir = tmp_path / "state"
    plan_ingestion(request, state_dir)
    original = materialization.materialize_and_verify_batch
    calls = 0
    def injected(**kwargs):
        nonlocal calls
        calls += 1
        callback = kwargs["record_timing"]
        def timing(name, duration):
            callback(name, duration)
            if calls == 2 and name == phase:
                raise OperationalFailureError("injected after " + phase)
        kwargs["record_timing"] = timing
        return original(**kwargs)
    monkeypatch.setattr(materialization, "materialize_and_verify_batch", injected)
    with pytest.raises(OperationalFailureError, match="injected after"):
        run_ingestion(state_dir)
    state = StateStore(state_dir).load()
    assert [b["status"] for b in state["batches"]] == ["committed", "failed"]
    artifacts = [b["artifacts"] for b in state["batches"]]
    first_ids = list(read_id_artifact(state_dir, artifacts[0]["location_ids"], expected_count=3))
    second_ids = list(read_id_artifact(state_dir, artifacts[1]["location_ids"], expected_count=3))
    first = edge_rows(local_database, first_ids)
    assert len(first) == 3
    with local_database.cursor() as c:
        for table in ("treenode", "treenode_edge"):
            c.execute(f"SELECT count(*) FROM {table} WHERE id=ANY(%s::bigint[])", [second_ids])
            assert c.fetchone()[0] == 0
        concepts = list(read_id_artifact(state_dir, artifacts[1]["concept_ids"], expected_count=5))
        c.execute("SELECT count(*) FROM class_instance WHERE id=ANY(%s::bigint[])", [concepts])
        assert c.fetchone()[0] == 0
        c.execute("SELECT count(*) FROM class_instance_class_instance WHERE id=ANY(%s::bigint[])", [concepts])
        assert c.fetchone()[0] == 0
        c.execute("SELECT count(*) FROM catmaid_skeleton_summary WHERE skeleton_id=ANY(%s::bigint[])", [concepts])
        assert c.fetchone()[0] == 0
    assert_no_temp(local_database)
    with pytest.raises(InvalidInputError, match="terminal"):
        run_ingestion(state_dir)
    assert edge_rows(local_database, first_ids) == first
    assert len(edge_rows(local_database, second_ids)) == 0
    assert StateStore(state_dir).load()["status"] == "failed"
    assert_no_temp(local_database)


@pytest.mark.parametrize("corruption", ["annotation_name", "duplicate_annotation", "duplicate_link"])
def test_fragment_metadata_failure_rolls_back_entire_batch(local_database, project, prepared, monkeypatch, corruption):
    from catmaid_skeleton_batch_import import materialization
    from catmaid_skeleton_batch_import.config import load_settings
    from catmaid_skeleton_batch_import.errors import VerificationError
    from catmaid_skeleton_batch_import.loader import execute_batch_transaction
    original = materialization.materialize_and_verify_batch
    item = prepared.expected_skeletons[0]

    def corrupted(**kwargs):
        cursor = kwargs["cursor"]
        if corruption == "annotation_name":
            cursor.execute("UPDATE class_instance SET name='source-fragment:changed' WHERE id=%s", [item.annotation_id])
        elif corruption == "duplicate_annotation":
            cursor.execute("""INSERT INTO class_instance (user_id,project_id,class_id,name)
                SELECT user_id,project_id,class_id,name FROM class_instance WHERE id=%s""", [item.annotation_id])
        else:
            cursor.execute("""INSERT INTO class_instance_class_instance
                (user_id,project_id,relation_id,class_instance_a,class_instance_b)
                SELECT user_id,project_id,relation_id,class_instance_a,class_instance_b
                FROM class_instance_class_instance WHERE id=%s""", [item.annotation_link_id])
        return original(**kwargs)

    monkeypatch.setattr(materialization, "materialize_and_verify_batch", corrupted)
    with pytest.raises(VerificationError):
        execute_batch_transaction(prepared, project, load_settings())
    assert_absent(local_database, prepared)
