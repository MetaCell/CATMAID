import json
import sys
import pytest
from catmaid_skeleton_batch_import.provenance import publish_node_map
from catmaid_skeleton_batch_import.errors import RetryableError, OperatorAttentionError


def test_optional_transport_and_exact_argv(tmp_path, monkeypatch):
    monkeypatch.delenv("CATMAID_BATCH_IMPORT_NODE_MAP_COMMAND", raising=False)
    publish_node_map(tmp_path, 1)
    hook = tmp_path / "hook.py"
    result = tmp_path / "argv.json"
    hook.write_text(
        "import json,sys,pathlib;pathlib.Path("
        + repr(str(result))
        + ").write_text(json.dumps(sys.argv[1:]))"
    )
    monkeypatch.setenv(
        "CATMAID_BATCH_IMPORT_NODE_MAP_COMMAND", json.dumps([sys.executable, str(hook)])
    )
    publish_node_map(tmp_path, 2)
    assert json.loads(result.read_text()) == ["stage", str(tmp_path), "2"]
    publish_node_map(tmp_path, 2, committed=True)
    assert json.loads(result.read_text()) == ["committed", str(tmp_path), "2"]


def test_transport_failures_distinguish_before_and_after_commit(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "CATMAID_BATCH_IMPORT_NODE_MAP_COMMAND",
        json.dumps([sys.executable, "-c", "raise SystemExit(1)"]),
    )
    with pytest.raises(RetryableError, match="before the batch transaction"):
        publish_node_map(tmp_path, 1)
    with pytest.raises(OperatorAttentionError, match="do not reimport"):
        publish_node_map(tmp_path, 1, committed=True)
