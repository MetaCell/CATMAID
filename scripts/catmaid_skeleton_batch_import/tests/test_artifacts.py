from __future__ import annotations

from pathlib import Path
import gzip
import io

import pytest

from catmaid_skeleton_batch_import.artifacts import (
    artifact_descriptor,
    read_id_artifact,
    write_id_artifact,
    verify_node_map,
    write_node_map,
)
from catmaid_skeleton_batch_import.errors import ImmutableStateError


def test_reserved_id_artifact_is_deterministic_and_verified(tmp_path: Path) -> None:
    first = tmp_path / "first.txt.gz"
    second = tmp_path / "second.txt.gz"
    ids = [10, 15, 19]

    write_id_artifact(first, ids)
    write_id_artifact(second, ids)

    assert first.read_bytes() == second.read_bytes()
    descriptor = artifact_descriptor(
        tmp_path,
        first,
        count=len(ids),
        minimum=min(ids),
        maximum=max(ids),
    )
    assert read_id_artifact(tmp_path, descriptor, expected_count=3) == ids


def test_node_map_is_streamed_deterministically_and_verified(tmp_path):
    class BoundedReader(io.StringIO):
        def read(self, size=-1):
            assert 0 < size <= 64 * 1024
            return super().read(size)

    content = "member_path,source_node_id,treenode_id\n" + "细胞.swc,42,9007199254740993\n" * 10000
    source = BoundedReader(content)
    first, second = tmp_path / "first.csv.gz", tmp_path / "second.csv.gz"
    write_node_map(first, source)
    assert source.tell() == 0
    write_node_map(second, source)
    assert first.read_bytes() == second.read_bytes()
    assert gzip.decompress(first.read_bytes()).decode() == content
    verify_node_map(first, source)
    assert source.tell() == 0
    with pytest.raises(ImmutableStateError, match="does not match reserved IDs"):
        verify_node_map(first, BoundedReader(content.replace("42,", "43,")))


@pytest.mark.parametrize("data", [b"not gzip", b"\x1f\x8b", gzip.compress(b"\xff")])
def test_corrupt_node_map_is_rejected(tmp_path, data):
    path = tmp_path / "node-map.csv.gz"
    path.write_bytes(data)
    source = io.StringIO("member_path,source_node_id,treenode_id\n")
    with pytest.raises(ImmutableStateError, match="Could not read node mapping"):
        verify_node_map(path, source)
    assert source.tell() == 0
