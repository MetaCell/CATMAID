"""Strict, bounded SWC parsing used by planning and row generation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import BinaryIO

from .domain import SwcNode, SwcSummary
from .errors import InvalidInputError
from .stack_bounds import StackBounds, contains_point


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
    bounds_nm: tuple[StackBounds, ...] | None = None,
) -> ParsedSwc:
    """Parse and validate an SWC from a binary stream.

    Parsing is strict UTF-8 and bounded by max_bytes and max_nodes when supplied.
    scale_nm validates conversion while returned nodes remain in source units.
    Associated stack bounds are checked in project space without transforming
    coordinates; negative values are allowed inside those bounds.
    """

    if max_bytes is not None and max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if max_nodes is not None and max_nodes <= 0:
        raise ValueError("max_nodes must be positive")
    if not math.isfinite(scale_nm) or scale_nm <= 0:
        raise ValueError("scale_nm must be positive and finite")
    if bounds_nm is not None and not bounds_nm:
        raise ValueError("bounds_nm must contain at least one stack")

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

        # Exporters also put comments after the seven numeric fields. Strip
        # them before counting columns, retaining physical line numbers below.
        line = decoded.partition("#")[0].strip()
        if not line:
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
            coordinate_nm = value * scale_nm
            if not math.isfinite(coordinate_nm):
                raise _line_error(
                    label,
                    physical_line_number,
                    f"{column} is non-finite after conversion to nanometres",
                )
        if bounds_nm is not None and not contains_point((x * scale_nm, y * scale_nm, z * scale_nm), bounds_nm):
            raise _line_error(
                label, physical_line_number,
                f"point x={x * scale_nm!r}, y={y * scale_nm!r}, z={z * scale_nm!r} nm "
                f"is outside all associated target stack bounds {bounds_nm!r}",
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
