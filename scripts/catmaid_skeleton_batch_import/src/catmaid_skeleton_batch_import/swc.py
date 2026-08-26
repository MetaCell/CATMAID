"""Strict, bounded SWC parsing used by planning and row generation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import BinaryIO

from .domain import SwcNode, SwcSummary
from .errors import InvalidInputError


@dataclass(frozen=True)
class ParsedSwc:
    """One validated SWC and the facts needed by later planning phases."""

    nodes: tuple[SwcNode, ...]
    summary: SwcSummary
    bytes_read: int


def _line_error(label: str, line_number: int, message: str) -> InvalidInputError:
    return InvalidInputError(f"{label}:{line_number}: {message}")


def _parse_integer(value: str, label: str, line_number: int, column: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise _line_error(
            label, line_number, f"{column} must be an integer, got {value!r}"
        ) from exc


def _parse_number(value: str, label: str, line_number: int, column: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise _line_error(
            label, line_number, f"{column} must be numeric, got {value!r}"
        ) from exc
    if not math.isfinite(result):
        raise _line_error(label, line_number, f"{column} must be finite")
    return result


def _read_binary_line(
    source: BinaryIO,
    *,
    bytes_read: int,
    max_bytes: int | None,
    label: str,
) -> bytes:
    """Read one complete line without allowing an over-limit line allocation."""

    if max_bytes is None:
        raw = source.readline()
    else:
        remaining = max_bytes - bytes_read
        if remaining < 0:
            raise InvalidInputError(f"{label}: exceeds {max_bytes} uncompressed bytes")
        raw = source.readline(remaining + 1)
        if len(raw) > remaining:
            raise InvalidInputError(f"{label}: exceeds {max_bytes} uncompressed bytes")
    if not isinstance(raw, bytes):
        raise TypeError("parse_swc requires a binary input stream")
    return raw


def parse_swc(
    source: BinaryIO,
    label: str,
    *,
    max_bytes: int | None = None,
    max_nodes: int | None = None,
    scale_nm: float = 1.0,
) -> ParsedSwc:
    """Parse and validate an SWC from a binary stream.

    Parsing is strict UTF-8 and bounded by ``max_bytes`` and ``max_nodes`` when
    supplied. Geometry bounds are deliberately not checked: the ingestion
    request's stack geometry is authoritative in the first public profile.
    ``scale_nm`` is used only to reject values that would become non-finite
    during conversion; returned node values remain in source units.
    """

    if max_bytes is not None and max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if max_nodes is not None and max_nodes <= 0:
        raise ValueError("max_nodes must be positive")
    if not math.isfinite(scale_nm) or scale_nm <= 0:
        raise ValueError("scale_nm must be positive and finite")

    nodes: list[SwcNode] = []
    seen_ids: set[int] = set()
    bytes_read = 0
    physical_line_number = 0

    while True:
        raw_line = _read_binary_line(
            source,
            bytes_read=bytes_read,
            max_bytes=max_bytes,
            label=label,
        )
        if not raw_line:
            break
        bytes_read += len(raw_line)
        physical_line_number += 1

        try:
            decoded = raw_line.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise _line_error(label, physical_line_number, "invalid UTF-8") from exc

        line = decoded.strip()
        if not line or line.startswith("#"):
            continue
        columns = line.split()
        if len(columns) != 7:
            raise _line_error(
                label,
                physical_line_number,
                f"expected 7 columns, got {len(columns)}",
            )

        source_id = _parse_integer(columns[0], label, physical_line_number, "id")
        if source_id in seen_ids:
            raise _line_error(
                label, physical_line_number, f"duplicate SWC node id {source_id}"
            )
        node_type = _parse_integer(columns[1], label, physical_line_number, "type")
        x = _parse_number(columns[2], label, physical_line_number, "x")
        y = _parse_number(columns[3], label, physical_line_number, "y")
        z = _parse_number(columns[4], label, physical_line_number, "z")
        radius = _parse_number(columns[5], label, physical_line_number, "radius")
        parent_source_id = _parse_integer(
            columns[6], label, physical_line_number, "parent"
        )

        for column, value in (("x", x), ("y", y), ("z", z)):
            if not math.isfinite(value * scale_nm):
                raise _line_error(
                    label,
                    physical_line_number,
                    f"{column} is non-finite after conversion to nanometres",
                )
        if radius != -1.0:
            if radius < 0:
                raise _line_error(
                    label,
                    physical_line_number,
                    "radius must be -1 (unknown) or non-negative",
                )
            if not math.isfinite(radius * scale_nm):
                raise _line_error(
                    label,
                    physical_line_number,
                    "radius is non-finite after conversion to nanometres",
                )

        seen_ids.add(source_id)
        nodes.append(
            SwcNode(
                source_id=source_id,
                node_type=node_type,
                x=x,
                y=y,
                z=z,
                radius=radius,
                parent_source_id=parent_source_id,
            )
        )
        if max_nodes is not None and len(nodes) > max_nodes:
            raise InvalidInputError(f"{label}: exceeds {max_nodes} nodes")

    if not nodes:
        raise InvalidInputError(f"{label}: no SWC nodes found")
    if len(nodes) < 2:
        raise InvalidInputError(
            f"{label}: one-node SWCs are not supported by CATMAID selective summaries"
        )

    nodes_by_id = {node.source_id: node for node in nodes}
    roots = [node.source_id for node in nodes if node.parent_source_id == -1]
    if len(roots) != 1:
        raise InvalidInputError(
            f"{label}: expected exactly one root, found {len(roots)}"
        )

    for node in nodes:
        if node.parent_source_id == -1:
            continue
        if node.parent_source_id not in nodes_by_id:
            raise InvalidInputError(
                f"{label}: node {node.source_id} references missing parent "
                f"{node.parent_source_id}"
            )

    # Follow every parent chain so cycles receive a precise error rather than a
    # generic disconnected-graph error.
    resolved: set[int] = set()
    for start in nodes_by_id:
        current = start
        path: list[int] = []
        in_path: set[int] = set()
        while current != -1 and current not in resolved:
            if current in in_path:
                raise InvalidInputError(
                    f"{label}: cycle detected involving node {current}"
                )
            in_path.add(current)
            path.append(current)
            current = nodes_by_id[current].parent_source_id
        resolved.update(path)

    children: dict[int, list[int]] = {}
    for node in nodes:
        if node.parent_source_id != -1:
            children.setdefault(node.parent_source_id, []).append(node.source_id)
    visited: set[int] = set()
    stack = [roots[0]]
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        stack.extend(children.get(node_id, ()))
    if len(visited) != len(nodes):
        raise InvalidInputError(
            f"{label}: disconnected graph; reached {len(visited)} of {len(nodes)} nodes"
        )

    lengths: list[float] = []
    for node in nodes:
        if node.parent_source_id == -1:
            continue
        parent = nodes_by_id[node.parent_source_id]
        length = math.dist((node.x, node.y, node.z), (parent.x, parent.y, parent.z))
        if not math.isfinite(length) or not math.isfinite(length * scale_nm):
            raise InvalidInputError(
                f"{label}: edge from node {node.source_id} to parent "
                f"{node.parent_source_id} has non-finite length"
            )
        lengths.append(length)
    try:
        cable_length = math.fsum(lengths)
    except OverflowError as exc:
        raise InvalidInputError(f"{label}: cable length is non-finite") from exc
    if not math.isfinite(cable_length) or not math.isfinite(cable_length * scale_nm):
        raise InvalidInputError(f"{label}: cable length is non-finite")

    return ParsedSwc(
        nodes=tuple(nodes),
        summary=SwcSummary(
            node_count=len(nodes), cable_length_source_units=cable_length
        ),
        bytes_read=bytes_read,
    )
