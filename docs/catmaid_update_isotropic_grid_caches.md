# catmaid_update_isotropic_grid_caches

Sequentially create isotropic (equal X/Y/Z) grid caches for node queries. This command derives grid cell sizes from stack dimensions and then calls `catmaid_update_cache_tables` once per size.

## How sizes are computed

1. For each project, compute the physical X/Y/Z dimensions for each linked stack:
   - `dimension.x * resolution.x`, `dimension.y * resolution.y`, `dimension.z * resolution.z`
2. Build the project-sized bounding dimensions by taking the maximum X, Y, and Z extent seen across linked stacks.
3. Compute the coarsest generated cache size by finding the smallest isotropic cell size whose total cell count across that X/Y/Z volume is no more than `--max-grid-cells`.
   - Total cell count is `ceil(x / size) * ceil(y / size) * ceil(z / size)`.
   - Because total cell count decreases monotonically as cell size grows, the command uses a binary search to find this threshold.
   - Because the grid is isotropic and cell counts are integers, the coarsest cache may contain fewer than `--max-grid-cells` cells. For example, a cubic volume with `--max-grid-cells 10` typically resolves to `2 x 2 x 2 = 8` cells, not `10`.
4. Min grid cell size defaults to `1.5mm` (1,500,000 nm).
5. Generate `--cache-count` sizes between max and min using `--grid-size-strategy`.
6. For each size, call `catmaid_update_cache_tables --cache-type grid` with `cell_width=cell_height=cell_depth`.

If the computed coarsest cell size is smaller than the min cell size, the command uses the min size for all caches.

## Output

After caches are created for each project, the command prints a single-line JSON object that can be used as a cache provider configuration. Example:

```json
{"cache_provider":"cached_msgpack_grid","cache_configurations":[{"cache_type":"grid","cell_width":20000,"cell_height":20000,"cell_depth":20000},{"cache_type":"grid","cell_width":10000,"cell_height":10000,"cell_depth":10000}]}
```

## Usage

```bash
./manage.py catmaid_update_isotropic_grid_caches --project_id 1
```

```bash
./manage.py catmaid_update_isotropic_grid_caches \
  --project_id 1 2 \
  --cache-count 6 \
  --grid-size-strategy exponential
```

```bash
./manage.py catmaid_update_isotropic_grid_caches \
  --min-grid-cell-size 2mm \
  --max-grid-cells 12 \
  --jobs 4
```

## New options

- `--max-grid-cells` (default: `10`): Maximum total number of isotropic grid cells allowed in the coarsest cache across the project bounding volume.
- `--min-grid-cell-size` (default: `1.5mm`): Minimum isotropic grid cell size. Accepts `nm`, `um`, or `mm` suffixes.
- `--cache-count` (default: `4`): Number of cache sizes to create per project.
- `--grid-size-strategy` (default: `linear`): How sizes are spaced between max and min. Options: `linear`, `exponential`.
- `--grid-size-units` (default: `nm`): Units for grid sizes. `nm` uses `stack.dimension * stack.resolution` (project space). `px` uses raw `stack.dimension` values.

## Notable defaults

These defaults are set on the command itself:

- `--lod-levels 7`
- `--lod-bucket-size 500`
- `--lod-strategy quadratic`
- `--type msgpack`
- `--progress`
- `--jobs 4`

## Notes

- `--from-config` is not supported because this command always derives grid sizes from stack dimensions.
- `--clean` is applied only to the first cache size per project to avoid deleting caches created earlier in the sequence.
- If your project coordinates are in pixels (not nanometers), use `--grid-size-units px` and pass `--min-grid-cell-size` in pixels (e.g. `1250` or `1250px`).
