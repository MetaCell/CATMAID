"""Validate physical stack bounds used by new-project verification."""

from __future__ import annotations

import math
from collections.abc import Sequence


def _positive_triple(
    values: Sequence[int | float], label: str
) -> tuple[float, float, float]:
    if len(values) != 3:
        raise ValueError(f"{label} must contain exactly three values")
    if any(isinstance(value, bool) for value in values):
        raise ValueError(f"{label} must contain positive finite values")
    normalized = tuple(float(value) for value in values)
    if any(not math.isfinite(value) or value <= 0 for value in normalized):
        raise ValueError(f"{label} must contain positive finite values")
    return normalized  # type: ignore[return-value]


def cache_bounds_nm(
    dimension: Sequence[int | float], resolution_nm: Sequence[int | float]
) -> tuple[tuple[float, float, float], tuple[float, float, float]]:
    """Return the trusted stack extent used to bound cache generation."""

    normalized_dimension = _positive_triple(dimension, "dimension")
    normalized_resolution = _positive_triple(resolution_nm, "resolution_nm")
    maximum = tuple(
        dimension_value * resolution_value
        for dimension_value, resolution_value in zip(
            normalized_dimension, normalized_resolution
        )
    )
    if any(not math.isfinite(value) for value in maximum):
        raise ValueError("stack cache extent is not finite")
    return (0.0, 0.0, 0.0), maximum  # type: ignore[return-value]
