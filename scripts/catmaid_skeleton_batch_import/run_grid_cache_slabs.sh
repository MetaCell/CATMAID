#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${IMPORT_CONFIG_JSON:-}" || -n "${IMPORT_CONFIG:-}" ]]; then
  if [[ -z "${IMPORT_CONFIG_JSON:-}" && ! -f "${IMPORT_CONFIG:-}" ]]; then
    echo "Missing import config file: ${IMPORT_CONFIG:-<unset>}" >&2
    exit 2
  fi

  eval "$(
    IMPORT_CONFIG_JSON="${IMPORT_CONFIG_JSON:-}" python3 - "${IMPORT_CONFIG:-}" <<'PY'
import json
import os
import shlex
import sys

inline = os.environ.get("IMPORT_CONFIG_JSON")
if inline:
    config = json.loads(inline)
else:
    with open(sys.argv[1]) as f:
        config = json.load(f)

grid = config.get("grid", {})
min_nm = grid.get("min_nm")
max_nm = grid.get("max_nm")

def emit(name, values, index):
    if values is None:
        return
    value = values[index]
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    print(f"export CONFIG_{name}={shlex.quote(str(value))}")

emit("MIN_X", min_nm, 0)
emit("MIN_Y", min_nm, 1)
emit("MIN_Z", min_nm, 2)
emit("MAX_X", max_nm, 0)
emit("MAX_Y", max_nm, 1)
emit("MAX_Z", max_nm, 2)
PY
  )"
fi

if [[ -z "${PROJECT_ID:-}" ]]; then
  echo "Missing required environment variable: PROJECT_ID" >&2
  exit 2
fi

CELL_SIZE="${CELL_SIZE:-1500000}"
ORIENTATION="${ORIENTATION:-xy}"
LOD_LEVELS="${LOD_LEVELS:-7}"
LOD_BUCKET_SIZE="${LOD_BUCKET_SIZE:-500}"
LOD_STRATEGY="${LOD_STRATEGY:-quadratic}"
JOBS="${JOBS:-2}"
CHUNK_SIZE="${CHUNK_SIZE:-5}"
START_SLAB="${START_SLAB:-0}"
CLEAN_FIRST="${CLEAN_FIRST:-false}"

MIN_X="${MIN_X:-${CONFIG_MIN_X:-0}}"
MIN_Y="${MIN_Y:-${CONFIG_MIN_Y:-0}}"
MIN_Z="${MIN_Z:-${CONFIG_MIN_Z:-0}}"
MAX_X="${MAX_X:-${CONFIG_MAX_X:-}}"
MAX_Y="${MAX_Y:-${CONFIG_MAX_Y:-}}"
MAX_Z="${MAX_Z:-${CONFIG_MAX_Z:-}}"
SLAB_SIZE="${SLAB_SIZE:-$CELL_SIZE}"

if [[ -z "$MAX_X" || -z "$MAX_Y" || -z "$MAX_Z" ]]; then
  echo "Missing grid bounds: set IMPORT_CONFIG with grid.max_nm or set MAX_X, MAX_Y, and MAX_Z" >&2
  exit 2
fi

SLAB_MINS=()
SLAB_MAXS=()
for ((z = MIN_Z; z < MAX_Z; z += SLAB_SIZE)); do
  next_z=$((z + SLAB_SIZE))
  if ((next_z > MAX_Z)); then
    next_z="$MAX_Z"
  fi
  SLAB_MINS+=("$z")
  SLAB_MAXS+=("$next_z")
done
TOTAL_SLABS="${#SLAB_MINS[@]}"

cd /home/django/projects

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

format_duration() {
  local seconds="$1"
  local hours=$((seconds / 3600))
  local minutes=$(((seconds % 3600) / 60))
  local secs=$((seconds % 60))
  printf "%02d:%02d:%02d" "$hours" "$minutes" "$secs"
}

grid_counts() {
  PROJECT_ID="$PROJECT_ID" CELL_SIZE="$CELL_SIZE" /home/env/bin/python manage.py shell -c '
import os
from django.db import connection

project_id = int(os.environ["PROJECT_ID"])
cell_size = int(os.environ["CELL_SIZE"])

with connection.cursor() as cursor:
    cursor.execute("""
        WITH target_grid AS (
            SELECT id
            FROM node_grid_cache
            WHERE project_id = %s
              AND orientation = 0
              AND cell_width = %s
              AND cell_height = %s
              AND cell_depth = %s
            ORDER BY id DESC
            LIMIT 1
        )
        SELECT
            COALESCE((SELECT id FROM target_grid), 0) AS grid_id,
            COALESCE((SELECT count(*) FROM node_grid_cache_cell WHERE grid_id = (SELECT id FROM target_grid)), 0) AS cache_cells,
            COALESCE((SELECT count(*) FROM dirty_node_grid_cache_cell WHERE grid_id = (SELECT id FROM target_grid)), 0) AS dirty_cells;
    """, [project_id, cell_size, cell_size, cell_size])
    print("\t".join(str(v) for v in cursor.fetchone()))
'
}

overall_start_epoch="$(date +%s)"
echo "[$(timestamp)] Starting project ${PROJECT_ID} ${CELL_SIZE}nm ${ORIENTATION} grid cache build"
echo "[$(timestamp)] Slabs: ${TOTAL_SLABS}; slab size: ${SLAB_SIZE}; start slab: ${START_SLAB}; jobs: ${JOBS}; chunk size: ${CHUNK_SIZE}; clean first: ${CLEAN_FIRST}"

completed_in_this_run=0
total_slab_seconds=0

for ((slab = START_SLAB; slab < TOTAL_SLABS; slab++)); do
  min_z="${SLAB_MINS[$slab]}"
  max_z="${SLAB_MAXS[$slab]}"
  # CATMAID's grid builder computes the maximum grid index inclusively. For
  # slab boundaries exactly on cell edges, use one nm less on non-final slabs
  # so adjacent slab runs don't rebuild the same Z grid layer.
  command_max_z="$max_z"
  if ((slab < TOTAL_SLABS - 1)); then
    command_max_z=$((max_z - 1))
  fi
  slab_start_epoch="$(date +%s)"

  clean_arg=()
  if [[ "$CLEAN_FIRST" == "true" && "$slab" == "$START_SLAB" ]]; then
    clean_arg=(--clean)
  fi

  echo "[$(timestamp)] Slab $((slab + 1))/${TOTAL_SLABS}: z ${min_z} <= z < ${max_z} (command max-z ${command_max_z})"

  /home/env/bin/python manage.py catmaid_update_cache_tables \
    --project_id "$PROJECT_ID" \
    --cache grid \
    --type msgpack \
    --orientation "$ORIENTATION" \
    --cell-width "$CELL_SIZE" \
    --cell-height "$CELL_SIZE" \
    --cell-depth "$CELL_SIZE" \
    --node-limit 0 \
    --lod-levels "$LOD_LEVELS" \
    --lod-bucket-size "$LOD_BUCKET_SIZE" \
    --lod-strategy "$LOD_STRATEGY" \
    --jobs "$JOBS" \
    --chunk-size "$CHUNK_SIZE" \
    --min-x "$MIN_X" \
    --max-x "$MAX_X" \
    --min-y "$MIN_Y" \
    --max-y "$MAX_Y" \
    --min-z "$min_z" \
    --max-z "$command_max_z" \
    "${clean_arg[@]}"

  slab_end_epoch="$(date +%s)"
  slab_seconds=$((slab_end_epoch - slab_start_epoch))
  total_slab_seconds=$((total_slab_seconds + slab_seconds))
  completed_in_this_run=$((completed_in_this_run + 1))
  completed_slabs=$((slab + 1))
  remaining_slabs=$((TOTAL_SLABS - completed_slabs))
  avg_seconds=$((total_slab_seconds / completed_in_this_run))
  eta_seconds=$((avg_seconds * remaining_slabs))
  total_elapsed=$((slab_end_epoch - overall_start_epoch))

  counts="$(grid_counts | awk -F '\t' 'NF == 3 && $1 ~ /^[0-9]+$/ {line = $0} END {print line}')"
  grid_id="$(printf "%s" "$counts" | cut -f1)"
  cache_cells="$(printf "%s" "$counts" | cut -f2)"
  dirty_cells="$(printf "%s" "$counts" | cut -f3)"

  echo "[$(timestamp)] Completed slab $((slab + 1))/${TOTAL_SLABS}: slab_elapsed=$(format_duration "$slab_seconds") total_elapsed=$(format_duration "$total_elapsed") eta=$(format_duration "$eta_seconds") grid_id=${grid_id} cache_cells=${cache_cells} dirty_cells=${dirty_cells}"
done

echo "[$(timestamp)] Project ${PROJECT_ID} ${CELL_SIZE}nm ${ORIENTATION} grid cache build complete"
