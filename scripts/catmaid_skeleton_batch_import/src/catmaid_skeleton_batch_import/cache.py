"""Build the single importer-owned CATMAID MessagePack grid cache."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from typing import Any


def _default_connection(connection: Any | None) -> Any:
    if connection is not None:
        return connection
    from django.db import connection as django_connection

    return django_connection


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


def build_grid_cache(
    *,
    project_id: int,
    dimension: Sequence[int | float],
    resolution_nm: Sequence[int | float],
    cache_cell_size_nm: int = 1_500_000,
    lod_levels: int = 7,
    lod_bucket_size: int = 500,
    lod_strategy: str = "quadratic",
    jobs: int = 1,
    chunk_size: int = 10,
    connection: Any | None = None,
    update_grid: Callable[..., None] | None = None,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Cleanly rebuild one XY MessagePack grid outside database transactions.

    ``node_limit=None`` is CATMAID's Python equivalent of the management
    command's ``--node-limit 0`` (unlimited).  The stack spatial-profile limit
    remains the literal integer ``0`` in stack metadata.
    """

    if isinstance(project_id, bool) or not isinstance(project_id, int) or project_id <= 0:
        raise ValueError("project_id must be a positive integer")
    for value, label in (
        (cache_cell_size_nm, "cache_cell_size_nm"),
        (lod_levels, "lod_levels"),
        (lod_bucket_size, "lod_bucket_size"),
        (jobs, "jobs"),
        (chunk_size, "chunk_size"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} must be a positive integer")
    if lod_strategy not in {"linear", "quadratic", "exponential"}:
        raise ValueError("lod_strategy must be linear, quadratic, or exponential")

    lower, upper = cache_bounds_nm(dimension, resolution_nm)
    connection = _default_connection(connection)
    if getattr(connection, "in_atomic_block", False):
        raise RuntimeError("grid cache generation must run outside transaction.atomic()")

    if update_grid is None:
        from catmaid.control.node import update_grid_cache

        update_grid = update_grid_cache
    cache_log = log or (lambda _message: None)
    update_grid(
        project_id,
        "msgpack",
        ["xy"],
        cell_width=cache_cell_size_nm,
        cell_height=cache_cell_size_nm,
        cell_depth=cache_cell_size_nm,
        node_limit=None,
        n_largest_skeletons_limit=None,
        n_last_edited_skeletons_limit=None,
        hidden_last_editor_id=None,
        delete=True,
        bb_limits=[list(lower), list(upper)],
        log=cache_log,
        progress=False,
        allow_empty=False,
        lod_levels=lod_levels,
        lod_bucket_size=lod_bucket_size,
        lod_strategy=lod_strategy,
        jobs=jobs,
        depth_steps=1,
        chunksize=chunk_size,
        ordering=None,
    )
    return {
        "project_id": project_id,
        "orientation": "xy",
        "data_type": "msgpack",
        "cell_size_nm": cache_cell_size_nm,
        "node_limit": 0,
        "bounds_nm": [list(lower), list(upper)],
        "lod_levels": lod_levels,
        "lod_bucket_size": lod_bucket_size,
        "lod_strategy": lod_strategy,
    }
