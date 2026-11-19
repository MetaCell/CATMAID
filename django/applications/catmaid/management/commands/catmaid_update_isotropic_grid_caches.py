import json
import math
import re

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from catmaid.models import Project, ProjectStack, Stack


_LENGTH_RE = re.compile(r'^(?P<value>[0-9]*\.?[0-9]+)\s*(?P<unit>nm|um|mm|px)?$')
_UNIT_TO_NM = {
    'nm': 1.0,
    'um': 1.0e3,
    'mm': 1.0e6,
}

_GRID_CACHE_PROVIDER_BY_TYPE = {
    'json': 'cached_json_grid',
    'json_text': 'cached_json_text_grid',
    'msgpack': 'cached_msgpack_grid',
}


def _parse_length(value, grid_size_units):
    if value is None:
        raise CommandError('Grid cell size must be provided')

    if isinstance(value, (int, float)):
        return int(math.ceil(float(value)))

    text = str(value).strip().lower()
    match = _LENGTH_RE.match(text)
    if not match:
        raise CommandError(
            f'Invalid grid cell size "{value}". Use a number with optional unit suffix: nm, um, mm.'
        )

    number = float(match.group('value'))
    unit = match.group('unit')

    if grid_size_units == 'px':
        if unit in (None, 'px'):
            return int(math.ceil(number))
        raise CommandError(
            f'Grid size units are set to pixels, but got "{unit}". Use a number or "px".'
        )

    unit = unit or 'nm'
    if unit == 'px':
        raise CommandError(
            'Grid size units are set to nanometers, but got "px". Use nm, um, or mm.'
        )
    return int(math.ceil(number * _UNIT_TO_NM[unit]))


def _stack_dimensions(stack, grid_size_units):
    if grid_size_units == 'px':
        return (stack.dimension.x, stack.dimension.y, stack.dimension.z)

    return (
        stack.dimension.x * stack.resolution.x,
        stack.dimension.y * stack.resolution.y,
        stack.dimension.z * stack.resolution.z,
    )


def _project_dimensions(stacks, grid_size_units):
    max_dims = [None, None, None]
    for stack in stacks:
        dims = _stack_dimensions(stack, grid_size_units)
        for i, dim in enumerate(dims):
            if max_dims[i] is None or dim > max_dims[i]:
                max_dims[i] = dim

    if any(dim is None for dim in max_dims):
        return None

    return tuple(max_dims)


def _grid_cell_count(dimensions, cell_size):
    return math.prod(
        max(1, int(math.ceil(dimension / cell_size)))
        for dimension in dimensions
    )


def _min_cell_size_for_total_cells_cap(dimensions, max_cells):
    """Return the smallest cell size whose total grid cells stay within max_cells.

    Total cell count decreases monotonically as cell size grows, so this binary
    search finds the threshold used for the coarsest generated cache size.
    """
    if max_cells <= 0:
        raise CommandError('--max-grid-cells must be greater than 0')

    if not dimensions or any(dimension <= 0 for dimension in dimensions):
        raise CommandError('Project dimensions must be greater than 0')

    low = 1
    high = max(1, int(math.ceil(max(dimensions))))

    while low < high:
        cell_size = (low + high) // 2
        if _grid_cell_count(dimensions, cell_size) <= max_cells:
            high = cell_size
        else:
            low = cell_size + 1

    return low


def _compute_cache_sizes(max_cell_size, min_cell_size, cache_count, strategy):
    if cache_count <= 0:
        raise CommandError('--cache-count must be greater than 0')

    if max_cell_size < min_cell_size:
        max_cell_size = min_cell_size

    if cache_count == 1 or max_cell_size == min_cell_size:
        return [max_cell_size]

    if strategy == 'linear':
        step = (max_cell_size - min_cell_size) / (cache_count - 1)
        raw_sizes = [max_cell_size - i * step for i in range(cache_count)]
    elif strategy == 'exponential':
        ratio = (min_cell_size / max_cell_size) ** (1.0 / (cache_count - 1))
        raw_sizes = [max_cell_size * (ratio ** i) for i in range(cache_count)]
    else:
        raise CommandError(f'Unknown grid size strategy: {strategy}')

    sizes: list[int] = []
    for size in raw_sizes:
        cell_size = int(round(size))
        if cell_size < min_cell_size:
            cell_size = min_cell_size
        if cell_size > max_cell_size:
            cell_size = max_cell_size
        if not sizes or cell_size != sizes[-1]:
            sizes.append(cell_size)

    return sizes


class Command(BaseCommand):
    help = 'Sequentially create isotropic grid caches based on stack dimensions.'

    def add_arguments(self, parser):
        parser.add_argument('--clean', action='store_true', dest='clean',
            default=False, help='Remove all existing cache data before update.')
        parser.add_argument('--project_id', dest='project_id', nargs='+',
            default=False, help='Compute only statistics for these projects only (otherwise all)')
        parser.add_argument('--type', dest='data_type', default='msgpack',
            help='Which type of cache to populate: json, json_text, msgpack')
        parser.add_argument('--orientation', dest='orientations', nargs='+',
            default='xy', help='Which orientations should be generated: xy, xz, zy.')
        parser.add_argument('--step', dest='steps', nargs='+',
            help='Map section thickness (depth resultion) for each orientation (in nm)')
        parser.add_argument('--min-x', dest='min_x', default='-inf',
            help='Optional minimum X project space coordinate for cache update')
        parser.add_argument('--max-x', dest='max_x', default='inf',
            help='Optional maximum X project space coordinate for cache update')
        parser.add_argument('--min-y', dest='min_y', default='-inf',
            help='Optional minimum Y project space coordinate for cache update')
        parser.add_argument('--max-y', dest='max_y', default='inf',
            help='Optional mayimum Y project space coordinate for cache update')
        parser.add_argument('--min-z', dest='min_z', default='-inf',
            help='Optional minimum Z project space coordinate for cache update')
        parser.add_argument('--max-z', dest='max_z', default='inf',
            help='Optional maximum Z project space coordinate for cache update')
        parser.add_argument('--allow-empty', dest='allow_empty', action='store_true', default=False,
            required=False, help='Optional, used with grid cache type: whether empty cells are created')
        parser.add_argument('--node-limit', dest='node_limit',
            default=settings.NODE_LIST_MAXIMUM_COUNT, help='Override node limit from settings. 0 means no limit')
        parser.add_argument('--n-largest-skeletons-limit', dest='n_largest_skeletons_limit',
            default=None, help='Only show treenodes of the N largest skeletons in the field of view')
        parser.add_argument('--n-last-edited-skeletons-limit', dest='n_last_edited_skeletons_limit',
            default=None, help='Only show treenodes of the N most recently edited skeletons in the field of view')
        parser.add_argument('--hidden-last-editor', dest='hidden_last_editor',
            default=None, help='Only show treenodes that have not been edited last by this user (username)'),
        parser.add_argument('--lod-levels', dest='lod_levels', default=7,
                type=int, help='Optional, number of level-of-detail levels'),
        parser.add_argument('--lod-bucket-size', dest='lod_bucket_size', default=500,
                type=int, help='Optional, number of (smallest) LOD bucket.'),
        parser.add_argument('--lod-strategy', dest='lod_strategy', default='quadratic',
                type=str, help='Optional, the strategy of LOD bucket size change with LOD. Can be "linear" or "quadratic".'),
        parser.add_argument('--from-config', action='store_true', dest='from_config',
            default=False, help='Not supported for isotropic cache generation.'),
        parser.add_argument('--progress', dest='progress', default=True,
            const=True, type=lambda x: (str(x).lower() == 'true'), nargs='?',
            help='Whether to show progress information')
        parser.add_argument('--jobs', dest='jobs', default=4, help='The number of processes to use', type=int)
        parser.add_argument('--depth-steps', dest='depth_steps', default=1, type=int,
                help='The number of steps in which the source bounding box is re-evaluated')
        parser.add_argument('--chunk-size', dest='chunk_size', default=10, type=int,
                help='The number of cache cells evaluated per process')
        parser.add_argument('--order', dest='order', default=None, type=str,
                help='The order of data in the cache, can be either "cable-asc" or "cable-desc". By default no ordering is applied.')

        parser.add_argument('--max-grid-cells', dest='max_grid_cells', default=10, type=int,
            help='Maximum total number of isotropic grid cells allowed in the coarsest cache across the project bounding volume.')
        parser.add_argument('--min-grid-cell-size', dest='min_grid_cell_size', default='1.5mm',
            help='Minimum isotropic grid cell size, accepts nm, um, or mm (default: 1.5mm).')
        parser.add_argument('--cache-count', dest='cache_count', default=4, type=int,
            help='Number of isotropic grid caches to generate per project.')
        parser.add_argument('--grid-size-strategy', dest='grid_size_strategy', default='linear',
            choices=('linear', 'exponential'),
            help='Spacing strategy for grid sizes between max and min: linear or exponential.')
        parser.add_argument('--grid-size-units', dest='grid_size_units', default='nm',
            choices=('nm', 'px'),
            help='Units for grid sizes. "nm" uses stack dimension scaled by resolution '
                 '(project space). "px" uses raw stack pixel dimensions.')

    def handle(self, *args, **options):
        if options['from_config']:
            raise CommandError('--from-config is not supported for isotropic grid cache generation')

        project_ids = options['project_id']
        if project_ids:
            projects = Project.objects.filter(id__in=project_ids)
        else:
            projects = Project.objects.all()

        project_stacks = ProjectStack.objects.select_related('stack').filter(project__in=projects)
        stacks_by_project: dict[int, list[Stack]] = {}
        for ps in project_stacks:
            stacks_by_project.setdefault(ps.project_id, []).append(ps.stack)

        grid_size_units = options['grid_size_units']
        min_cell_size = _parse_length(options['min_grid_cell_size'], grid_size_units)
        if min_cell_size <= 0:
            raise CommandError('--min-grid-cell-size must be greater than 0')

        max_cells = options['max_grid_cells']
        if max_cells <= 0:
            raise CommandError('--max-grid-cells must be greater than 0')

        cache_count = options['cache_count']
        strategy = options['grid_size_strategy']
        data_type = options['data_type']
        cache_provider = _GRID_CACHE_PROVIDER_BY_TYPE.get(data_type)
        if not cache_provider:
            raise CommandError(f'Unknown cache provider for data type: {data_type}')

        for project in projects:
            stacks = stacks_by_project.get(project.id, [])
            if not stacks:
                self.stdout.write(f'Skipping project {project.id}: no stacks found')
                continue

            project_dimensions = _project_dimensions(stacks, grid_size_units)
            if not project_dimensions or any(dimension <= 0 for dimension in project_dimensions):
                self.stdout.write(f'Skipping project {project.id}: invalid stack dimensions')
                continue

            coarsest_cell_size = _min_cell_size_for_total_cells_cap(project_dimensions, max_cells)
            if coarsest_cell_size < min_cell_size:
                self.stdout.write(
                    f'Project {project.id}: computed coarsest cell size {coarsest_cell_size} {grid_size_units} '
                    f'is smaller than min cell size {min_cell_size} {grid_size_units}. '
                    'Using min cell size for all caches.'
                )
                coarsest_cell_size = min_cell_size

            sizes = _compute_cache_sizes(coarsest_cell_size, min_cell_size, cache_count, strategy)
            self.stdout.write(
                f'Project {project.id}: generating {len(sizes)} isotropic grid caches '
                f'from {coarsest_cell_size} {grid_size_units} to {min_cell_size} {grid_size_units} '
                f'using {strategy} spacing.'
            )

            clean = options['clean']
            for cell_size in sizes:
                self.stdout.write(
                    f'Project {project.id}: updating grid cache with cell size {cell_size} {grid_size_units}'
                )
                call_command(
                    'catmaid_update_cache_tables',
                    clean=clean,
                    project_id=[str(project.id)],
                    cache_type='grid',
                    data_type=data_type,
                    orientations=options['orientations'],
                    steps=options['steps'],
                    min_x=options['min_x'],
                    max_x=options['max_x'],
                    min_y=options['min_y'],
                    max_y=options['max_y'],
                    min_z=options['min_z'],
                    max_z=options['max_z'],
                    cell_width=cell_size,
                    cell_height=cell_size,
                    cell_depth=cell_size,
                    allow_empty=options['allow_empty'],
                    node_limit=options['node_limit'],
                    n_largest_skeletons_limit=options['n_largest_skeletons_limit'],
                    n_last_edited_skeletons_limit=options['n_last_edited_skeletons_limit'],
                    hidden_last_editor=options['hidden_last_editor'],
                    lod_levels=options['lod_levels'],
                    lod_bucket_size=options['lod_bucket_size'],
                    lod_strategy=options['lod_strategy'],
                    from_config=False,
                    progress=options['progress'],
                    jobs=options['jobs'],
                    depth_steps=options['depth_steps'],
                    chunk_size=options['chunk_size'],
                    order=options['order'],
                )
                clean = False

            cache_configurations = [
                {
                    'cache_type': 'grid',
                    'cell_width': cell_size,
                    'cell_height': cell_size,
                    'cell_depth': cell_size,
                }
                for cell_size in sizes
            ]
            self.stdout.write(json.dumps({
                'cache_provider': cache_provider,
                'cache_configurations': cache_configurations,
            }))

        self.stdout.write('Done')
