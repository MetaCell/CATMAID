"""Hash-bound validated source nodes, grouped by the same whole-SWC batches as planning.

Coordinates remain source-unit Python doubles. Stored float4 rounding still happens
in the loader, exactly as for the legacy ZIP reader. No database rows are cached.
"""
from __future__ import annotations

import gzip
import json
import os
from pathlib import Path

from .domain import SwcNode
from .errors import ImmutableStateError
from .util import sha256_file


class PreparedSourceWriter:
    def __init__(self, root: Path, maximum_nodes: int):
        self.root = root
        self.maximum_nodes = maximum_nodes
        self.descriptors = []
        self.output = None
        self.nodes = 0

    def add(self, member, nodes):
        if self.output is None or self.nodes + member.node_count > self.maximum_nodes:
            self.close_batch()
            path = self.root / f"prepared-{len(self.descriptors) + 1:06d}.jsonl.gz"
            self.root.mkdir(parents=True, exist_ok=True)
            self.raw = path.open("wb")
            self.output = gzip.GzipFile(filename="", mode="wb", fileobj=self.raw, mtime=0, compresslevel=1)
            self.path = path
            self.nodes = 0
        record = [member.member_path, [[n.source_id, n.node_type, n.x, n.y, n.z, n.radius, n.parent_source_id] for n in nodes]]
        self.output.write((json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n").encode())
        self.nodes += member.node_count

    def close_batch(self):
        if self.output is None:
            return
        self.output.close()
        self.raw.flush()
        os.fsync(self.raw.fileno())
        self.raw.close()
        self.output = None
        self.descriptors.append({"path": self.path.name, "sha256": sha256_file(self.path),
                                 "size_bytes": self.path.stat().st_size, "node_count": self.nodes})

    def close(self):
        self.close_batch()


class PreparedSourceReader:
    """Keep only one bounded batch in memory; verify its bytes before decoding."""
    def __init__(self, root, plan):
        self.root = root
        self.plan = plan
        self.batch_index = None
        self.members = {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.members.clear()

    def load(self, member):
        if self.batch_index != member.batch_index:
            descriptor = self.plan.prepared_batches[member.batch_index - 1]
            path = self.root / descriptor["path"]
            try:
                valid = path.stat().st_size == descriptor["size_bytes"] and sha256_file(path) == descriptor["sha256"]
            except OSError as exc:
                raise ImmutableStateError("Prepared source is missing; restore it from the hash-pinned archive") from exc
            if not valid:
                raise ImmutableStateError("Prepared source bytes differ from the immutable plan")
            expected = {m.member_path: m for m in self.plan.batches[member.batch_index - 1].members}
            decoded = {}
            try:
                with gzip.open(path, "rt", encoding="utf-8") as source:
                    for line in source:
                        name, rows = json.loads(line)
                        if name not in expected or name in decoded or len(rows) != expected[name].node_count:
                            raise ValueError("member order/count mismatch")
                        decoded[name] = tuple(SwcNode(*row) for row in rows)
                if set(decoded) != set(expected):
                    raise ValueError("missing members")
            except (ValueError, TypeError, OSError, EOFError) as exc:
                raise ImmutableStateError(f"Invalid prepared source: {exc}") from exc
            self.members = decoded
            self.batch_index = member.batch_index
        return self.members[member.member_path]
