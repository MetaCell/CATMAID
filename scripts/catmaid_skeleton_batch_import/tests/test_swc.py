from __future__ import annotations

import io
import math

import pytest

from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.swc import parse_swc


def parse(text: bytes, **kwargs):
    return parse_swc(io.BytesIO(text), "archive.zip:cell.swc", **kwargs)


def test_parse_valid_swc_strictly_and_preserve_unknown_radius() -> None:
    parsed = parse(
        b"# standard SWC\n"
        b"2 3 3 4 0 2 1\n"
        b"1 1 0 0 0 -1 -1\n",
        scale_nm=1000,
    )

    assert [node.source_id for node in parsed.nodes] == [2, 1]
    assert parsed.nodes[0].node_type == 3
    assert parsed.nodes[1].radius == -1
    assert parsed.summary.node_count == 2
    assert parsed.summary.cable_length_source_units == 5
    assert parsed.bytes_read == len(
        b"# standard SWC\n2 3 3 4 0 2 1\n1 1 0 0 0 -1 -1\n"
    )


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"1 1 0 0 0 1 -1\n", "one-node SWCs"),
        (b"1 1 0 0 0 1 -1\n2 nope 1 0 0 1 1\n", "type must be an integer"),
        (b"1 1 0 0 0 1 -1\n2 3 1 0 0 1\n", "expected 7 columns"),
        (b"1 1 0 0 0 1 -1\n1 3 1 0 0 1 1\n", "duplicate SWC node id"),
        (b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 99\n", "missing parent"),
        (b"1 1 0 0 0 1 -1\n2 3 1 0 0 -2 1\n", "radius must be -1"),
        (b"1 1 nan 0 0 1 -1\n2 3 1 0 0 1 1\n", "x must be finite"),
        (b"1 1 0 0 0 1 -1\n2 3 1 0 0 inf 1\n", "radius must be finite"),
        (b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 -1\n", "exactly one root"),
        (
            b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 3\n3 3 2 0 0 1 2\n",
            "cycle detected",
        ),
    ],
)
def test_rejects_invalid_swc(content: bytes, message: str) -> None:
    with pytest.raises(InvalidInputError, match=message):
        parse(content)


def test_rejects_invalid_utf8_with_member_and_line_context() -> None:
    with pytest.raises(
        InvalidInputError, match=r"archive\.zip:cell\.swc:2: invalid UTF-8"
    ):
        parse(b"1 1 0 0 0 1 -1\n\xff\n")


def test_enforces_stream_byte_and_node_limits() -> None:
    content = b"1 1 0 0 0 1 -1\n2 3 1 0 0 1 1\n"
    with pytest.raises(InvalidInputError, match="exceeds 10 uncompressed bytes"):
        parse(content, max_bytes=10)
    with pytest.raises(InvalidInputError, match="exceeds 1 nodes"):
        parse(content, max_nodes=1)


def test_rejects_values_that_overflow_after_unit_conversion() -> None:
    content = b"1 1 1e308 0 0 1 -1\n2 3 1 0 0 1 1\n"
    with pytest.raises(InvalidInputError, match="after conversion"):
        parse(content, scale_nm=1000)


def test_cable_length_is_finite() -> None:
    content = b"1 1 -1e308 0 0 1 -1\n2 3 1e308 0 0 1 1\n"
    with pytest.raises(InvalidInputError, match="non-finite length"):
        parse(content)


def test_geometry_is_not_checked_against_stack_bounds() -> None:
    parsed = parse(b"1 1 -100 0 0 1 -1\n2 3 1000000 0 0 1 1\n")
    assert math.isclose(parsed.summary.cable_length_source_units, 1_000_100)
