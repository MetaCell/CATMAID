from __future__ import annotations

import json
import io
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import import (
    edges,
    database,
    materialization,
    project,
    project_checks,
)
from catmaid_skeleton_batch_import.errors import VerificationError


class ScriptedCursor:
    def __init__(self, connection):
        self.connection = connection
        self.current = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.connection.executions.append((" ".join(sql.split()), params))
        self.current = self.connection.responses.pop(0) if self.connection.responses else []

    def fetchall(self):
        return list(self.current or [])

    def fetchone(self):
        if self.current is None:
            return None
        if isinstance(self.current, tuple):
            return self.current
        return self.current[0] if self.current else None


class ScriptedConnection:
    def __init__(self, responses=(), *, in_atomic_block=False, settings_dict=None):
        self.responses = list(responses)
        self.executions = []
        self.in_atomic_block = in_atomic_block
        self.settings_dict = settings_dict or {}

    def cursor(self):
        return ScriptedCursor(self)


def test_sequence_allocation_uses_allowlist_and_returns_exact_values():
    connection = ScriptedConnection([[(101,), (103,), (104,)]])

    result = database.allocate_sequence_ids("concept_id_seq", 3, connection)

    assert result == (101, 103, 104)
    assert "nextval('concept_id_seq'::regclass)" in connection.executions[0][0]
    with pytest.raises(ValueError, match="Unsupported"):
        database.allocate_sequence_ids("unsafe; DROP TABLE project", 1, connection)


def test_database_fingerprint_contains_no_password():
    connection = ScriptedConnection(
        [[("catmaid", "importer", "160004", "cluster-1")]],
        settings_dict={
            "ENGINE": "django.db.backends.postgresql",
            "HOST": "database.internal",
            "PORT": "5432",
            "PASSWORD": "do-not-persist",
        },
    )
    result = database.database_target_fingerprint(connection)

    assert result["identity"]["system_identifier"] == "cluster-1"
    assert "PASSWORD" not in result["identity"]
    assert "do-not-persist" not in str(result)
    assert len(result["sha256"]) == 64


def test_analyze_is_allowlisted_and_outside_atomic():
    connection = ScriptedConnection()
    result = database.analyze_tables(connection, tables=("treenode", "treenode_edge"))
    assert result == {"tables": ["treenode", "treenode_edge"], "count": 2}
    assert [sql for sql, _params in connection.executions] == [
        "ANALYZE treenode",
        "ANALYZE treenode_edge",
    ]
    with pytest.raises(ValueError, match="Unsupported"):
        database.analyze_tables(connection, tables=("project; DROP TABLE stack",))
    connection.in_atomic_block = True
    with pytest.raises(RuntimeError, match="outside"):
        database.analyze_tables(connection)


def test_stack_metadata_is_one_writable_profile():
    assert project.build_stack_metadata() == {
        "cache_provider": "cached_msgpack_grid",
        "read_only": False,
        "spatial": [{"chunk_size": [1_500_000] * 3, "limit": 0}],
    }


def test_create_hidden_project_is_create_only_and_returns_tracing_ids():
    created = {}

    class Manager:
        def __init__(self, label):
            self.label = label

        def create(self, **kwargs):
            created[self.label] = kwargs
            return SimpleNamespace(**kwargs)

    project_model = SimpleNamespace(objects=Manager("project"))
    stack_model = SimpleNamespace(objects=Manager("stack"))
    project_stack_model = SimpleNamespace(objects=Manager("project_stack"))
    system_user = SimpleNamespace(id=7)
    calls = []
    components = {
        "get_system_user": lambda: system_user,
        "get_class_to_id_map": lambda *_args: {"neuron": 31, "skeleton": 32, "annotation": 33},
        "get_relation_to_id_map": lambda *_args: {"model_of": 41, "annotated_with": 42},
        "validate_project_setup": lambda *args, **kwargs: calls.append(
            ("validate", args, kwargs)
        ),
        "check_tracing_setup": lambda _project_id: True,
        "setup_tracing": lambda *args: calls.append(("tracing", args)),
        "Project": project_model,
        "ProjectStack": project_stack_model,
        "Stack": stack_model,
        "atomic": lambda **kwargs: nullcontext(),
        "assign_perm": lambda permission, user, target: calls.append(
            ("permission", permission, user.id, target.id)
        ),
    }

    result = project.create_hidden_project(
        project_id=1,
        stack_id=2,
        project_stack_id=3,
        title="example",
        dimension=(100, 200, 300),
        resolution_nm=(4, 5, 6),
        before_commit=lambda: calls.append(("before_commit",)),
        components=components,
    )

    assert created["project"]["id"] == 1
    assert created["stack"]["metadata"]["read_only"] is False
    assert created["stack"]["canary_location"] == (50, 100, 150)
    assert created["project_stack"]["translation"] == (0.0, 0.0, 0.0)
    assert result["system_user_id"] == 7
    assert result["neuron_class_id"] == 31
    assert sum(call[0] == "permission" for call in calls) == len(
        project.SYSTEM_PROJECT_PERMISSIONS
    )
    assert calls[-1] == ("before_commit",)


def _valid_batch_responses():
    return [
        [("origin",)],
        [],
        [
            (10, 7, 1, 31, "a"),
            (11, 7, 1, 32, "a"),
            (13, 7, 1, 33, "source-fragment:a"),
        ],
        [(12, 7, 1, 41, 11, 10), (14, 7, 1, 42, 10, 13)],
        [
            (20, 1, 7, 7, 11, None),
            (21, 1, 7, 7, 11, 20),
        ],
        [(20, None, 1), (21, 20, 1)],
        [(11, 1, 7, 2, 5.0)],
        [(13, 7, 1, 33, "source-fragment:a")],
        [(14, 7, 1, 42, 10, 13)],
    ]


def test_materialize_and_verify_batch_uses_batch_edges_and_exact_checks(monkeypatch):
    connection = ScriptedConnection(_valid_batch_responses(), in_atomic_block=True)
    created = []
    monkeypatch.setattr(edges, "create_batch_edges", lambda **kwargs: created.append(kwargs))
    timings = []
    expected = [
        {
            "neuron_id": 10,
            "skeleton_id": 11,
            "link_id": 12,
            "annotation_id": 13,
            "annotation_link_id": 14,
            "fragment_id": "a",
            "name": "a",
            "node_count": 2,
            "cable_length_nm": 5.0,
        }
    ]

    result = materialization.materialize_and_verify_batch(
        project_id=1,
        user_id=7,
        neuron_class_id=31,
        skeleton_class_id=32,
        model_of_relation_id=41,
        annotation_class_id=33,
        annotated_with_relation_id=42,
        skeletons=expected,
        location_ids=[20, 21],
        connection=connection,
        treenode_file=io.StringIO(),
        record_timing=lambda name, duration: timings.append((name, duration)),
    )

    assert created[0]["location_ids"] == [20, 21]
    assert created[0]["skeletons"][0]["skeleton_id"] == 11
    assert any(
        "refresh_skeleton_summary_table_selectively" in sql
        for sql, _params in connection.executions
    )
    assert result["treenode_count"] == 2
    assert result["summary_count"] == 1
    assert [name for name, _duration in timings] == [
        "summary_refresh",
        "materialization",
        "validation",
    ]
    assert all(duration >= 0 for _name, duration in timings)


def test_materialization_requires_origin_and_rejects_singletons():
    expected = [
        {
            "neuron_id": 10,
            "skeleton_id": 11,
            "link_id": 12,
            "annotation_id": 13,
            "annotation_link_id": 14,
            "fragment_id": "a",
            "name": "a",
            "node_count": 1,
            "cable_length_nm": 0,
        }
    ]
    with pytest.raises(ValueError, match="one-node"):
        materialization.materialize_and_verify_batch(
            project_id=1,
            user_id=7,
            neuron_class_id=31,
            skeleton_class_id=32,
            model_of_relation_id=41,
        annotation_class_id=33,
        annotated_with_relation_id=42,
            skeletons=expected,
            location_ids=[20],
            connection=ScriptedConnection(in_atomic_block=True),
            treenode_file=io.StringIO(),
        )


def test_batch_validation_detects_edge_parent_mismatch():
    responses = _valid_batch_responses()[2:]
    responses[3] = [(20, None, 1), (21, None, 1)]
    connection = ScriptedConnection(responses)
    expected = [
        {
            "neuron_id": 10,
            "skeleton_id": 11,
            "link_id": 12,
            "annotation_id": 13,
            "annotation_link_id": 14,
            "fragment_id": "a",
            "name": "a",
            "node_count": 2,
            "cable_length_nm": 5.0,
        }
    ]
    with pytest.raises(VerificationError, match="edge parent"):
        materialization.validate_batch(
            project_id=1,
            user_id=7,
            neuron_class_id=31,
            skeleton_class_id=32,
            model_of_relation_id=41,
        annotation_class_id=33,
        annotated_with_relation_id=42,
            skeletons=expected,
            location_ids=[20, 21],
            connection=connection,
        )


def test_final_project_stack_verification_checks_hidden_one_profile_project():
    metadata = project.build_stack_metadata()
    connection = ScriptedConnection(
        [
            [
                (
                    1,
                    "example",
                    2,
                    "example",
                    (10, 20, 30),
                    (4.0, 5.0, 6.0),
                    None,
                    metadata,
                    (5, 10, 15),
                    3,
                    (0.0, 0.0, 0.0),
                    0,
                )
            ],
            (1, 1),
        ]
    )
    result = project_checks.verify_project_stack(
        project_id=1,
        stack_id=2,
        project_stack_id=3,
        system_user_id=7,
        title="example",
        dimension=(10, 20, 30),
        resolution_nm=(4, 5, 6),
        connection=connection,
        permission_loader=lambda _project_id: {
            "users": {7: set(project.SYSTEM_PROJECT_PERMISSIONS)},
            "groups": {},
        },
        tracing_check=lambda _project_id: True,
    )
    assert result["hidden"] is True
    assert result["tracing_setup"] is True


def test_project_stack_verification_accepts_raw_json_driver_value():
    metadata = project.build_stack_metadata()
    connection = ScriptedConnection(
        [
            [
                (
                    1,
                    "example",
                    2,
                    "example",
                    (10, 20, 30),
                    (4.0, 5.0, 6.0),
                    None,
                    json.dumps(metadata),
                    (5, 10, 15),
                    3,
                    (0.0, 0.0, 0.0),
                    0,
                )
            ],
            (1, 1),
        ]
    )

    result = project_checks.verify_project_stack(
        project_id=1,
        stack_id=2,
        project_stack_id=3,
        system_user_id=7,
        title="example",
        dimension=(10, 20, 30),
        resolution_nm=(4, 5, 6),
        connection=connection,
        permission_loader=lambda _project_id: {
            "users": {7: set(project.SYSTEM_PROJECT_PERMISSIONS)},
            "groups": {},
        },
        tracing_check=lambda _project_id: True,
    )

    assert result["metadata"] == metadata


def test_project_aggregate_counts_use_one_constant_size_result():
    connection = ScriptedConnection([(3, 3, 3, 10, 10, 3, 3, 3)])

    result = project_checks.verify_project_aggregate_counts(
        project_id=1,
        neuron_class_id=31,
        skeleton_class_id=32,
        model_of_relation_id=41,
        annotation_class_id=33,
        annotated_with_relation_id=42,
        expected_skeleton_count=3,
        expected_node_count=10,
        connection=connection,
    )

    assert result == {
        "class_instance_count": 9,
        "annotation_count": 3,
        "annotation_link_count": 3,
        "model_of_count": 3,
        "neuron_count": 3,
        "skeleton_count": 3,
        "relationship_count": 6,
        "treenode_count": 10,
        "edge_count": 10,
        "summary_count": 3,
    }
    assert len(connection.executions) == 1
    assert "SELECT count(*) FROM treenode" in connection.executions[0][0]


def test_project_aggregate_counts_reject_extra_project_rows():
    connection = ScriptedConnection([(3, 3, 4, 10, 10, 3, 3, 3)])
    with pytest.raises(VerificationError, match="project-wide database counts"):
        project_checks.verify_project_aggregate_counts(
            project_id=1,
            neuron_class_id=31,
            skeleton_class_id=32,
            model_of_relation_id=41,
        annotation_class_id=33,
        annotated_with_relation_id=42,
            expected_skeleton_count=3,
            expected_node_count=10,
            connection=connection,
        )
