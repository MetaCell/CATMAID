#!/usr/bin/env bash
# Reserve CATMAID IDs for one online bulk-import batch.
#
# PostgreSQL sequences cannot be locked with LOCK TABLE, and concurrent CATMAID
# writes may call nextval() at any time. To stay online-safe, this script
# consumes the exact IDs the importer will use with nextval() and writes them
# to local staging files. The CSV generator reads those files directly and does
# not assume IDs are contiguous.
#
# Required inputs:
#
#   PROJECT_ID
#   STAGING
#   BATCH_SKELETONS
#   BATCH_TREENODES
#
# Output:
#
#   export RESERVED_CONCEPT_IDS_FILE=...
#   export RESERVED_LOCATION_IDS_FILE=...
#   export RESERVED_CONCEPT_IDS=...
#   export RESERVED_LOCATION_IDS=...
#   export NEURON_CLASS_ID=...
#   export SKELETON_CLASS_ID=...
#   export MODEL_OF_RELATION_ID=...
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

set_db_defaults
require_env PROJECT_ID STAGING DB_USER DB_NAME DB_PASS

if ! [[ "$PROJECT_ID" =~ ^[0-9]+$ ]] || ((PROJECT_ID <= 0)); then
  echo "PROJECT_ID must be a positive integer" >&2
  exit 2
fi

batch_skeletons="${BATCH_SKELETONS:-${SKELETON_COUNT:-}}"
batch_treenodes="${BATCH_TREENODES:-${TREENODE_COUNT:-}}"

if [[ -z "$batch_skeletons" || -z "$batch_treenodes" ]]; then
  echo "Missing required batch counts: set BATCH_SKELETONS and BATCH_TREENODES" >&2
  exit 2
fi
if ! [[ "$batch_skeletons" =~ ^[0-9]+$ && "$batch_treenodes" =~ ^[0-9]+$ ]]; then
  echo "Batch counts must be non-negative integers" >&2
  exit 2
fi
if ((batch_skeletons <= 0 || batch_treenodes <= 0)); then
  echo "Batch counts must be greater than zero" >&2
  exit 2
fi

concept_ids_needed=$((batch_skeletons * 3))
location_ids_needed="$batch_treenodes"
concept_ids_file="$STAGING/reserved_concept_ids.txt"
location_ids_file="$STAGING/reserved_location_ids.txt"
concept_ids_tmp="$concept_ids_file.tmp"
location_ids_tmp="$location_ids_file.tmp"

db_psql() {
  kubectl -n "$NS" exec "$DB_POD" -- env PGPASSWORD="$DB_PASS" \
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" "$@"
}

metadata_output="$(
  db_psql -At -F $'\t' -c "
SELECT 'NEURON_CLASS_ID', id FROM class WHERE project_id = ${PROJECT_ID} AND class_name = 'neuron'
UNION ALL
SELECT 'SKELETON_CLASS_ID', id FROM class WHERE project_id = ${PROJECT_ID} AND class_name = 'skeleton'
UNION ALL
SELECT 'MODEL_OF_RELATION_ID', id FROM relation WHERE project_id = ${PROJECT_ID} AND relation_name = 'model_of';"
)"

for required_name in NEURON_CLASS_ID SKELETON_CLASS_ID MODEL_OF_RELATION_ID; do
  if ! awk -F $'\t' -v name="$required_name" '$1 == name { found = 1 } END { exit !found }' <<<"$metadata_output"; then
    echo "Metadata query did not return ${required_name}; check project tracing setup" >&2
    exit 1
  fi
done

rm -f "$concept_ids_tmp" "$location_ids_tmp" "$concept_ids_file" "$location_ids_file"

db_psql -At -c "SELECT nextval('concept_id_seq') FROM generate_series(1, ${concept_ids_needed});" \
  > "$concept_ids_tmp"
db_psql -At -c "SELECT nextval('location_id_seq') FROM generate_series(1, ${location_ids_needed});" \
  > "$location_ids_tmp"

concept_count="$(wc -l < "$concept_ids_tmp" | tr -d ' ')"
location_count="$(wc -l < "$location_ids_tmp" | tr -d ' ')"

if [[ "$concept_count" != "$concept_ids_needed" ]]; then
  echo "Reserved concept ID count mismatch: expected ${concept_ids_needed}, got ${concept_count}" >&2
  exit 1
fi
if [[ "$location_count" != "$location_ids_needed" ]]; then
  echo "Reserved location ID count mismatch: expected ${location_ids_needed}, got ${location_count}" >&2
  exit 1
fi

mv "$concept_ids_tmp" "$concept_ids_file"
mv "$location_ids_tmp" "$location_ids_file"

id_min_max() {
  awk '
    NR == 1 { min = $1; max = $1 }
    $1 < min { min = $1 }
    $1 > max { max = $1 }
    END { print min "\t" max }
  ' "$1"
}

concept_min_max="$(id_min_max "$concept_ids_file")"
location_min_max="$(id_min_max "$location_ids_file")"
concept_min="$(cut -f1 <<<"$concept_min_max")"
concept_max="$(cut -f2 <<<"$concept_min_max")"
location_min="$(cut -f1 <<<"$location_min_max")"
location_max="$(cut -f2 <<<"$location_min_max")"

printf '%s\n' "$metadata_output" >&2
printf 'RESERVED_CONCEPT_IDS\t%s\tmin=%s\tmax=%s\tfile=%s\n' "$concept_count" "$concept_min" "$concept_max" "$concept_ids_file" >&2
printf 'RESERVED_LOCATION_IDS\t%s\tmin=%s\tmax=%s\tfile=%s\n' "$location_count" "$location_min" "$location_max" "$location_ids_file" >&2

awk -F $'\t' '
  $1 == "NEURON_CLASS_ID" { print "export NEURON_CLASS_ID=" $2 }
  $1 == "SKELETON_CLASS_ID" { print "export SKELETON_CLASS_ID=" $2 }
  $1 == "MODEL_OF_RELATION_ID" { print "export MODEL_OF_RELATION_ID=" $2 }
' <<<"$metadata_output"

cat <<EOF
export RESERVED_CONCEPT_IDS_FILE=$(printf '%q' "$concept_ids_file")
export RESERVED_LOCATION_IDS_FILE=$(printf '%q' "$location_ids_file")
export RESERVED_CONCEPT_IDS=$concept_count
export RESERVED_LOCATION_IDS=$location_count
export RESERVED_CONCEPT_ID_MIN=$concept_min
export RESERVED_CONCEPT_ID_MAX=$concept_max
export RESERVED_LOCATION_ID_MIN=$location_min
export RESERVED_LOCATION_ID_MAX=$location_max
EOF
