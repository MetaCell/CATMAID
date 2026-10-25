"""Status enums preserve the files consumed by deployment jobs and clients."""

import json

import pytest

from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.metrics import MetricsStore
from catmaid_skeleton_batch_import.statuses import (
    DatabaseStatus, ImportStatus, MetricsStatus, OperationStatus,
)
from test_state import make_store


@pytest.mark.parametrize("enum_type", [DatabaseStatus, ImportStatus, MetricsStatus, OperationStatus])
def test_status_enums_use_plain_json_strings(enum_type):
    for status in enum_type:
        assert str(status) == status.value
        assert json.loads(json.dumps({"status": status})) == {"status": status.value}


def test_database_transition_rejects_a_phase_only_status(tmp_path):
    store, _ = make_store(tmp_path)
    before = store.state_path.read_bytes()
    with pytest.raises(InvalidInputError, match="Unknown project state"):
        store.transition_project(OperationStatus.COMPLETE)
    assert store.state_path.read_bytes() == before


def test_metrics_rejects_a_database_status(tmp_path):
    metrics = MetricsStore(tmp_path)
    metrics.initialize()
    before = metrics.path.read_bytes()
    with pytest.raises(InvalidInputError, match="Unknown metrics status"):
        metrics.set_status(DatabaseStatus.COMMITTED)
    assert metrics.path.read_bytes() == before
