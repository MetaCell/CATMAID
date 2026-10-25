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
        (b"# comments only\n", "no SWC nodes"),
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
    content = b"1 1 0 0 0 1 -1\n2 3 1.1e308 1.1e308 1.1e308 1 1\n"
    with pytest.raises(InvalidInputError, match="non-finite length"):
        parse(content)


def test_geometry_without_declared_stack_has_no_upper_bound() -> None:
    parsed = parse(b"1 1 100 0 0 1 -1\n2 3 1000000 0 0 1 1\n")
    assert math.isclose(parsed.summary.cable_length_source_units, 999_900)


@pytest.mark.parametrize("axis", ["x", "y", "z"])
@pytest.mark.parametrize("scale_nm", [1, 1000])
def test_rejects_coordinates_outside_associated_bounds_after_unit_conversion(axis, scale_nm):
    extents = (1000.0, 2000.0, 3000.0)
    index = ["x", "y", "z"].index(axis)
    coordinates = ["0", "0", "0"]
    coordinates[index] = str((extents[index] + 1) / scale_nm)
    content = ("# header\n1 1 " + " ".join(coordinates) + " -1 -1\n").encode()
    with pytest.raises(InvalidInputError, match=r"cell\.swc:2: point .* outside all associated target stack bounds"):
        parse(content, scale_nm=scale_nm, bounds_nm=(((0, 0, 0), extents),))


@pytest.mark.parametrize("scale_nm", [1, 1000])
def test_origin_and_exact_associated_extent_are_accepted_without_scaling_source_nodes(scale_nm):
    extents = (1000.0, 2000.0, 3000.0)
    coordinates = tuple(value / scale_nm for value in extents)
    content = ("1 1 0 0 0 -1 -1\n2 3 " +
               " ".join(map(str, coordinates)) + " -1 1\n").encode()
    parsed = parse(content, scale_nm=scale_nm, bounds_nm=(((0, 0, 0), extents),))
    assert (parsed.nodes[1].x, parsed.nodes[1].y, parsed.nodes[1].z) == coordinates


@pytest.mark.parametrize("axis", ["x", "y", "z"])
@pytest.mark.parametrize("scale_nm", [1, 1000])
def test_rejects_negative_coordinates_outside_associated_bounds_with_axis_member_and_line_context(axis, scale_nm):
    coordinates = ["0", "0", "0"]
    coordinates[["x", "y", "z"].index(axis)] = "-0.001"
    content = ("# header\n1 1 " + " ".join(coordinates) + " -1 -1\n").encode()
    with pytest.raises(InvalidInputError, match=r"cell\.swc:2: point .* outside all associated target stack bounds"):
        parse(content, scale_nm=scale_nm, bounds_nm=(((0, 0, 0), (1000, 2000, 3000)),))


def test_zero_coordinates_and_unknown_radius_and_root_sentinels_remain_valid():
    parsed = parse(b"1 1 -0.0 0 0 -1 -1\n", scale_nm=1000)
    assert parsed.nodes[0].radius == -1
    assert parsed.nodes[0].parent_source_id == -1


def test_single_node_has_zero_cable_length() -> None:
    parsed = parse(b"42 1 3 4 5 -1 -1 # point soma\n", scale_nm=1000)
    assert parsed.summary.node_count == 1
    assert parsed.summary.cable_length_source_units == 0
    assert parsed.nodes[0].source_id == 42
    assert parsed.nodes[0].parent_source_id == -1
    assert parsed.nodes[0].radius == -1


def test_inline_comments_leave_seven_fields_and_preserve_byte_counts() -> None:
    content = (b"  # header\n\n1 1 0 0 0 1 -1# root\n"
               b"2 3 3 4 0 1 1 # bifurcation / exporter note\n")
    parsed = parse(content)
    assert parsed.summary.node_count == 2
    assert parsed.summary.cable_length_source_units == 5
    assert parsed.bytes_read == len(content)


@pytest.mark.parametrize("row", [b"2 3 3 4 0 1", b"2 3 3 4 0 1 1 99"])
def test_inline_comments_do_not_hide_invalid_field_counts_or_line_numbers(row) -> None:
    with pytest.raises(InvalidInputError, match=r"cell\.swc:4: expected 7 columns"):
        parse(b"# header\n\n1 1 0 0 0 1 -1\n" + row + b" # note\n")
