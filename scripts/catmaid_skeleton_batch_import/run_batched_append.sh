#!/usr/bin/env bash
# Generate and load SWCs in online-safe CSV batches.
#
# This wrapper keeps CATMAID running. Each batch is scanned first to compute
# exact counts, then reserve_ids.sh advances the PostgreSQL sequences by those
# counts before CSV generation. The generated load.sql never repairs sequences.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

set_db_defaults
require_env PROJECT_ID STACK_ID IMPORT_USER_ID IMPORT_CONFIG DB_USER DB_NAME DB_PASS

BATCH_MAX_ZIPS="${BATCH_MAX_ZIPS:-1}"
STAGING_BASE="${STAGING_BASE:-/tmp/catmaid_skeleton_import_${PROJECT_ID}_batches}"
EXCLUDE_ZIPS="${EXCLUDE_ZIPS:-}"
AFTER_ZIP="${AFTER_ZIP:-}"

if [[ ! -f "$IMPORT_CONFIG" ]]; then
  echo "Missing import config file: $IMPORT_CONFIG" >&2
  exit 2
fi

mkdir -p "$STAGING_BASE/completed"

echo "Starting online batched import for PROJECT_ID=$PROJECT_ID STACK_ID=$STACK_ID"
echo "CATMAID app deployment is not scaled down; keep PROJECT_ID=$PROJECT_ID hidden until publish."

batch_number=1
while true; do
  batch_start_epoch="$(date +%s)"
  batch_name="$(printf 'batch_%05d' "$batch_number")"
  export STAGING="$STAGING_BASE/$batch_name"
  rm -rf "$STAGING"
  mkdir -p "$STAGING"

  echo "Starting $batch_name after zip '${AFTER_ZIP:-<start>}'"

  export EXCLUDE_ZIPS AFTER_ZIP IMPORT_CONFIG BATCH_MAX_ZIPS
  scan_args=(--scan-only --batch-plan "$STAGING/batch_plan.tsv")
  if [[ -n "$BATCH_MAX_ZIPS" ]]; then
    scan_args+=(--max-zip-files "$BATCH_MAX_ZIPS")
  fi
  python3 "$SCRIPT_DIR/generate_bulk_csv.py" "${scan_args[@]}"
  scan_end_epoch="$(date +%s)"

  if [[ -f "$STAGING/skipped.json" ]]; then
    echo "Scan skipped one or more SWCs; inspect $STAGING/skipped.json" >&2
    exit 1
  fi

  # shellcheck source=/dev/null
  source "$STAGING/batch.env"
  if [[ "$BATCH_SKELETONS" == "0" || "$BATCH_TREENODES" == "0" ]]; then
    echo "Batch scan produced no skeletons or no treenodes" >&2
    exit 1
  fi

  reservation_exports="$("$SCRIPT_DIR/reserve_ids.sh")"
  eval "$reservation_exports"
  if [[ "$RESERVED_CONCEPT_IDS" != "$((BATCH_SKELETONS * 3))" ]]; then
    echo "Reserved concept ID count does not match scan count" >&2
    exit 1
  fi
  if [[ "$RESERVED_LOCATION_IDS" != "$BATCH_TREENODES" ]]; then
    echo "Reserved location ID count does not match scan count" >&2
    exit 1
  fi

  export BATCH_PLAN BATCH_COMPLETED_ALL_INPUT
  python3 "$SCRIPT_DIR/generate_bulk_csv.py"
  generation_end_epoch="$(date +%s)"

  "$SCRIPT_DIR/load_csv.sh"
  load_end_epoch="$(date +%s)"

  completed_dir="$STAGING_BASE/completed/$batch_name"
  rm -rf "$completed_dir"
  mkdir -p "$completed_dir"
  gzip -f "$STAGING/manifest.tsv"
  gzip -f "$STAGING/reserved_concept_ids.txt" "$STAGING/reserved_location_ids.txt"
  cp \
    "$STAGING/summary.json" \
    "$STAGING/scan_summary.json" \
    "$STAGING/batch.env" \
    "$STAGING/batch_plan.tsv" \
    "$STAGING/reserved_concept_ids.txt.gz" \
    "$STAGING/reserved_location_ids.txt.gz" \
    "$STAGING/manifest.tsv.gz" \
    "$completed_dir/"

  scan_seconds=$((scan_end_epoch - batch_start_epoch))
  generation_seconds=$((generation_end_epoch - scan_end_epoch))
  load_seconds=$((load_end_epoch - generation_end_epoch))
  batch_seconds=$((load_end_epoch - batch_start_epoch))
  cat > "$completed_dir/timing.env" <<EOF
export BATCH_SCAN_SECONDS=$scan_seconds
export BATCH_GENERATION_SECONDS=$generation_seconds
export BATCH_LOAD_SECONDS=$load_seconds
export BATCH_TOTAL_SECONDS=$batch_seconds
EOF
  echo "Loaded $batch_name: zips=${BATCH_FIRST_ZIP}..${BATCH_LAST_ZIP}, skeletons=${BATCH_SKELETONS}, treenodes=${BATCH_TREENODES}, scan_seconds=${scan_seconds}, generation_seconds=${generation_seconds}, load_seconds=${load_seconds}, total_seconds=${batch_seconds}"

  rm -rf "$STAGING"

  if [[ "$BATCH_COMPLETED_ALL_INPUT" == "true" ]]; then
    break
  fi

  AFTER_ZIP="$BATCH_NEXT_AFTER_ZIP"
  batch_number=$((batch_number + 1))
done

echo "Online batched import complete for PROJECT_ID=$PROJECT_ID STACK_ID=$STACK_ID"
echo "Next: rebuild treenode_edge, refresh summary/stat tables, build grid caches, verify, then publish permissions."
