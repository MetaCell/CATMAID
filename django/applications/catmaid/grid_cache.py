"""Size, provision and identify sparse tracing grids."""

import math
from dataclasses import dataclass

from django.conf import settings


# Physical query widths do not bound nodes per cell or guarantee RAM usage.
FINEST_SIDE_DIVISOR = 32
CACHE_WIDTH_MULTIPLIER = 2
TARGET_COARSEST_CHUNKS = 32
MAX_TARGET_COARSEST_CHUNKS = 2048
MAX_FRONTEND_LEVELS = 8
MAX_CACHE_WIDTH_RATIO = 8
NATIVE_LOD_LEVELS = 8
LOD_MIN_BUCKET_SIZE = 500
MAX_CHUNK_SIZE_NM = 2**32 - 1  # Neuroglancer stores chunk widths as uint32.
MAX_NATIVE_GRID_INTEGER = 2**31 - 1  # PostgreSQL cache widths and cell indices.
GRID_ORIENTATION = 0  # CATMAID's canonical XY orientation.
CACHE_PROVIDER = "cached_msgpack_grid"


class GridSizingError(ValueError):
    """The requested tracing space cannot be represented within the grid limits."""


@dataclass(frozen=True)
class GridPolicy:
    """Effective initialization choices and their generated query/cache widths."""

    finest_cell_size_nm: int
    max_cache_cell_size_nm: int
    target_coarsest_chunks: int
    chunk_sizes: tuple[int, ...]
    cache_sizes: tuple[int, ...]

    @property
    def initialization(self):
        return {
            "finest_cell_size_nm": self.finest_cell_size_nm,
            "max_cache_cell_size_nm": self.max_cache_cell_size_nm,
            "target_coarsest_chunks": self.target_coarsest_chunks,
        }


def _positive_integer(value, label, maximum):
    if (
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        or value > maximum
    ):
        raise GridSizingError(f"The {label} must be a positive integer no greater than {maximum}")
    return value


def _geometry_bounds(geometry):
    """Validate the stack and physical bounds before any project rows are created."""
    try:
        dimensions = tuple(geometry["dimension"])
        resolutions = tuple(geometry["resolution"])
        offsets = tuple(geometry.get("translation", (0, 0, 0)))
        if not all(len(values) == 3 for values in (dimensions, resolutions, offsets)):
            raise GridSizingError("The tracing space must have three XYZ axes")
        if not all(
            isinstance(value, int) and not isinstance(value, bool)
            and 0 < value <= MAX_NATIVE_GRID_INTEGER for value in dimensions
        ):
            raise GridSizingError("Tracing dimensions must fit positive PostgreSQL integers")
        if not all(
            isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) for value in (*resolutions, *offsets)
        ) or not all(value > 0 for value in resolutions):
            raise GridSizingError("The tracing space must have finite positive resolution and finite origins")
        extents = tuple(dimension * resolution for dimension, resolution in zip(dimensions, resolutions))
        upper = tuple(offset + extent for offset, extent in zip(offsets, extents))
        if not all(
            math.isfinite(extent) and extent > 0 and math.isfinite(end) and end > offset
            for extent, offset, end in zip(extents, offsets, upper)
        ):
            raise GridSizingError("The tracing space must have finite representable positive physical extents")
    except (KeyError, TypeError, OverflowError) as exc:
        raise GridSizingError("The tracing space has invalid or unrepresentable geometry") from exc
    return offsets, upper


def cached_cell_sizes(chunk_sizes, max_cache_cell_size_nm):
    """Select only the finest and largest eligible widths, deduplicated."""
    eligible = tuple(size for size in chunk_sizes if size <= max_cache_cell_size_nm)
    return tuple(dict.fromkeys((eligible[0], eligible[-1])))


def _closest_cache(size, cache_sizes):
    # Match CATMAID's native provider: compare cubic volumes, not edge lengths.
    return min(cache_sizes, key=lambda cache: abs(cache**3 - size**3))


def resolve_grid_policy(
    geometry, *, finest_cell_size_nm=None, max_cache_cell_size_nm=None,
    target_coarsest_chunks=None,
):
    """Resolve a volume-relative policy once, before any project rows are created."""
    offsets, upper = _geometry_bounds(geometry)
    longest = max(
        dimension * resolution
        for dimension, resolution in zip(geometry["dimension"], geometry["resolution"])
    )
    nominal_finest = max(1, math.ceil(longest / FINEST_SIDE_DIVISOR))
    # Derive both defaults together: independently rounding L/32 and L/16 can
    # exclude the largest nested cache by a few nanometres.
    relative_ceiling = _positive_integer(
        nominal_finest * CACHE_WIDTH_MULTIPLIER,
        "volume-relative cache ceiling in nanometres", MAX_NATIVE_GRID_INTEGER,
    )
    maximum = _positive_integer(
        relative_ceiling if max_cache_cell_size_nm is None else max_cache_cell_size_nm,
        "maximum cache cell size in nanometres", relative_ceiling,
    )
    finest = _positive_integer(
        nominal_finest if finest_cell_size_nm is None else finest_cell_size_nm,
        "finest grid cell size in nanometres", maximum,
    )
    target = _positive_integer(
        getattr(settings, "NEUROGLASS_CATMAID_TARGET_COARSEST_CHUNKS", TARGET_COARSEST_CHUNKS)
        if target_coarsest_chunks is None else target_coarsest_chunks,
        "target coarsest chunk count", MAX_TARGET_COARSEST_CHUNKS,
    )
    minimum_coverage = 2 ** sum(offset < 0 < end for offset, end in zip(offsets, upper))
    if target < minimum_coverage:
        raise GridSizingError(
            f"The overview target cannot be less than {minimum_coverage} for bounds crossing zero"
        )

    widths = [finest]
    while math.prod(
        math.ceil(end / widths[-1]) - math.floor(offset / widths[-1])
        for offset, end in zip(offsets, upper)
    ) > target:
        if len(widths) >= MAX_FRONTEND_LEVELS:
            raise GridSizingError(f"The overview requires more than {MAX_FRONTEND_LEVELS} frontend levels")
        if widths[-1] > MAX_CHUNK_SIZE_NM // 2:
            raise GridSizingError("The overview target cannot be met with uint32 frontend chunk widths")
        widths.append(widths[-1] * 2)

    caches = cached_cell_sizes(widths, maximum)
    for size in widths:
        cache = _closest_cache(size, caches)
        if size // cache > MAX_CACHE_WIDTH_RATIO:
            raise GridSizingError(
                f"A frontend chunk cannot exceed {MAX_CACHE_WIDTH_RATIO} times its selected cache width"
            )
    # Closed-edge membership and the XY query halo may reach neighbouring cells.
    for size in caches:
        for offset, end in zip(offsets, upper):
            if (
                math.floor(offset / size) - 1 < -MAX_NATIVE_GRID_INTEGER - 1
                or math.floor(end / size) + 1 > MAX_NATIVE_GRID_INTEGER
            ):
                raise GridSizingError("The tracing bounds exceed PostgreSQL cache cell index limits")
    return GridPolicy(finest, maximum, target, tuple(widths), caches)


def chunk_cell_sizes(geometry, **overrides):
    """The frontend widths produced by the volume-relative initialization policy."""
    return resolve_grid_policy(geometry, **overrides).chunk_sizes


def _chunk_density_hint(size, level, cached_sizes, level_count):
    if level == 0:
        return 0  # The finest request reads every LOD bucket, hence every node.
    prefix = max(1, int(NATIVE_LOD_LEVELS * (level_count - 1 - level) / (level_count - 1)))
    cache_size = _closest_cache(size, cached_sizes)
    ratio = size // cache_size
    # Native queries include upper X/Y boundary cells, but exclude upper Z.
    cells = (ratio + 1) ** 2 * ratio
    # A conservative hint for the bounded quadratic node/connector buckets.
    # The last, unbounded bucket is used only at the finest frontend level.
    return cells * LOD_MIN_BUCKET_SIZE * sum(bucket**2 for bucket in range(1, prefix + 1))


def stack_cache_metadata(policy):
    return {
        "spatial": [
            {"limit": _chunk_density_hint(size, level, policy.cache_sizes, len(policy.chunk_sizes)),
             "chunk_size": [size] * 3}
            for level, size in enumerate(policy.chunk_sizes)
        ],
        "grid_policy": policy.initialization,
        "read_only": False,
        "cache_provider": CACHE_PROVIDER,
    }


def create_grid_caches(project, policy):
    from catmaid.models import NodeGridCache

    grids = []
    for size in policy.cache_sizes:
        grid, _ = NodeGridCache.objects.get_or_create(
            project=project,
            orientation=GRID_ORIENTATION,
            cell_width=size,
            cell_height=size,
            cell_depth=size,
            defaults={
                "enabled": True,
                "has_msgpack_data": True,
                "has_json_data": False,
                "has_json_text_data": False,
                "n_lod_levels": NATIVE_LOD_LEVELS,
                "lod_strategy": "quadratic",
                "lod_min_bucket_size": LOD_MIN_BUCKET_SIZE,
                "allow_empty": False,
                "n_largest_skeletons_limit": None,
                "n_last_edited_skeletons_limit": None,
                "hidden_last_editor_id": None,
                "ordering": None,
            },
        )
        grids.append(grid)
    return grids


def has_managed_grid(project_id, cache_id):
    """Check for an enabled sparse MsgPack grid declared by stack metadata."""
    from catmaid.models import NodeGridCache, ProjectStack

    grids = NodeGridCache.objects.filter(
        project_id=project_id,
        orientation=GRID_ORIENTATION,
        enabled=True,
        has_msgpack_data=True,
        n_lod_levels__in=(1, NATIVE_LOD_LEVELS),
    )
    if cache_id is not None:
        grids = grids.filter(pk=cache_id)
    sizes = {(grid.cell_width, grid.cell_height, grid.cell_depth) for grid in grids}
    if not sizes:
        return False
    return any(
        isinstance(metadata, dict)
        and metadata.get("cache_provider") == CACHE_PROVIDER
        and isinstance(metadata.get("spatial"), list)
        and any(
            isinstance(level, dict)
            and isinstance(level.get("chunk_size"), list)
            and len(level["chunk_size"]) == 3
            and all(isinstance(size, (int, float)) and not isinstance(size, bool) for size in level["chunk_size"])
            and tuple(level["chunk_size"]) in sizes
            for level in metadata["spatial"]
        )
        for metadata in ProjectStack.objects.filter(project_id=project_id).values_list("stack__metadata", flat=True)
    )
