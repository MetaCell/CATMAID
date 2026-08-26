from __future__ import annotations

import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import import (
    cache,
    database,
    materialization,
    project,
    verification,
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


def test_replica_load_restores_origin_only_after_success():
    connection = ScriptedConnection(in_atomic_block=True)
    with database.replica_load(connection):
        pass
    assert [execution[0] for execution in connection.executions] == [
        "SET LOCAL session_replication_role = replica",
        "SET LOCAL session_replication_role = origin",
    ]

    failed = ScriptedConnection(in_atomic_block=True)
    with pytest.raises(RuntimeError, match="copy failed"):
        with database.replica_load(failed):
            raise RuntimeError("copy failed")
    assert len(failed.executions) == 1


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
        "get_class_to_id_map": lambda *_args: {"neuron": 31, "skeleton": 32},
        "get_relation_to_id_map": lambda *_args: {"model_of": 41},
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
        ],
        [(12, 7, 1, 41, 11, 10)],
        [
            (20, 1, 7, 7, 11, None),
            (21, 1, 7, 7, 11, 20),
        ],
        [(20, None, 1), (21, 20, 1)],
        [(11, 1, 7, 2, 5.0)],
    ]


def test_materialize_and_verify_batch_uses_catmaid_functions_and_exact_checks():
    connection = ScriptedConnection(_valid_batch_responses(), in_atomic_block=True)
    rebuilt = []
    timings = []
    expected = [
        {
            "neuron_id": 10,
            "skeleton_id": 11,
            "link_id": 12,
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
        skeletons=expected,
        location_ids=[20, 21],
        connection=connection,
        rebuild_edges=lambda skeleton_ids, connector_ids: rebuilt.append(
            (skeleton_ids, connector_ids)
        ),
        record_timing=lambda name, duration: timings.append((name, duration)),
    )

    assert rebuilt == [([11], [])]
    assert any(
        "refresh_skeleton_summary_table_selectively" in sql
        for sql, _params in connection.executions
    )
    assert result["treenode_count"] == 2
    assert result["summary_count"] == 1
    assert [name for name, _duration in timings] == [
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
            skeletons=expected,
            location_ids=[20],
            connection=ScriptedConnection(in_atomic_block=True),
            rebuild_edges=lambda *_args, **_kwargs: None,
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
            skeletons=expected,
            location_ids=[20, 21],
            connection=connection,
        )


def test_grid_cache_is_clean_single_xy_msgpack_and_outside_atomic():
    calls = []
    connection = ScriptedConnection(in_atomic_block=False)
    result = cache.build_grid_cache(
        project_id=1,
        dimension=(10, 20, 30),
        resolution_nm=(4, 5, 6),
        connection=connection,
        update_grid=lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    args, kwargs = calls[0]
    assert args == (1, "msgpack", ["xy"])
    assert kwargs["delete"] is True
    assert kwargs["node_limit"] is None
    assert kwargs["bb_limits"] == [[0.0, 0.0, 0.0], [40.0, 100.0, 180.0]]
    assert result["node_limit"] == 0

    connection.in_atomic_block = True
    with pytest.raises(RuntimeError, match="outside"):
        cache.build_grid_cache(
            project_id=1,
            dimension=(10, 20, 30),
            resolution_nm=(4, 5, 6),
            connection=connection,
            update_grid=lambda *_args, **_kwargs: None,
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
    result = verification.verify_project_stack(
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

    result = verification.verify_project_stack(
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


def test_cache_verification_checks_exact_grid_and_no_dirty_cells():
    connection = ScriptedConnection(
        [
            [
                (
                    99,
                    0,
                    1_500_000,
                    1_500_000,
                    1_500_000,
                    7,
                    "quadratic",
                    500,
                    None,
                    None,
                    None,
                    False,
                    False,
                    False,
                    True,
                    True,
                    None,
                )
            ],
            (2, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (0,),
        ]
    )
    result = verification.verify_cache(
        project_id=1,
        dimension=(10, 10, 10),
        resolution_nm=(1000, 1000, 1000),
        expected_node_count=2,
        connection=connection,
    )
    assert result == {
        "grid_id": 99,
        "grid_count": 1,
        "cell_count": 2,
        "dirty_cell_count": 0,
        "msgpack_only": True,
    }


def test_project_aggregate_counts_use_one_constant_size_result():
    connection = ScriptedConnection([(3, 3, 3, 10, 10, 3)])

    result = verification.verify_project_aggregate_counts(
        project_id=1,
        neuron_class_id=31,
        skeleton_class_id=32,
        model_of_relation_id=41,
        expected_skeleton_count=3,
        expected_node_count=10,
        connection=connection,
    )

    assert result == {
        "class_instance_count": 6,
        "neuron_count": 3,
        "skeleton_count": 3,
        "relationship_count": 3,
        "treenode_count": 10,
        "edge_count": 10,
        "summary_count": 3,
    }
    assert len(connection.executions) == 1
    assert "SELECT count(*) FROM treenode" in connection.executions[0][0]


def test_project_aggregate_counts_reject_extra_project_rows():
    connection = ScriptedConnection([(3, 3, 4, 10, 10, 3)])
    with pytest.raises(VerificationError, match="project-wide database counts"):
        verification.verify_project_aggregate_counts(
            project_id=1,
            neuron_class_id=31,
            skeleton_class_id=32,
            model_of_relation_id=41,
            expected_skeleton_count=3,
            expected_node_count=10,
            connection=connection,
        )
