"""Only batches, operations, and project locks have status vocabularies."""

import json

import pytest

from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.metrics import MetricsStore
from catmaid_skeleton_batch_import.statuses import BatchStatus, OperationStatus, ProjectLockStatus
from test_state import make_store


@pytest.mark.parametrize("enum_type", [BatchStatus, OperationStatus, ProjectLockStatus])
def test_status_enums_use_plain_json_strings(enum_type):
    for status in enum_type:
        assert str(status) == status.value
        assert json.loads(json.dumps({"status": status})) == {"status": status.value}


def test_batch_transition_rejects_a_phase_only_status(tmp_path):
    store, _ = make_store(tmp_path)
    before = store.state_path.read_bytes()
    with pytest.raises(InvalidInputError, match="Unknown batch state"):
        store.transition_batch(1, OperationStatus.COMPLETE)
    assert store.state_path.read_bytes() == before


def test_metrics_rejects_a_batch_status(tmp_path):
    metrics = MetricsStore(tmp_path)
    metrics.initialize()
    before = metrics.path.read_bytes()
    with pytest.raises(InvalidInputError, match="Unknown metrics status"):
        metrics.set_status(BatchStatus.COMMITTED)
    assert metrics.path.read_bytes() == before
