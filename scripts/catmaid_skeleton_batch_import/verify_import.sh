#!/usr/bin/env bash
# Print lightweight post-import checks for an Allen Dense Skeleton CATMAID
# project.
#
# Usage:
#
#   export NS=<namespace>
#   export PROJECT_ID=<project-id>
#   export DB_USER=<db-user>
#   export DB_NAME=<db-name>
#   export DB_PASS=<db-password>
#   scripts/catmaid_skeleton_batch_import/verify_import.sh
#
# The checks intentionally rely on materialized/project-level tables where
# possible instead of running raw count(*) scans over the 171M-row treenode and
# treenode_edge tables. It reports skeleton count, skeleton summary totals,
# project statistics totals, grid cache state, dirty grid cache cells, and the
# custom batched rebuild/refresh progress views.
#
# This is a sanity-check script, not a full validator. It is meant to confirm
# that the expected materializations exist and are complete before users browse
# the project.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

set_db_defaults
require_env PROJECT_ID DB_USER DB_NAME DB_PASS

db_query() {
  kubectl -n "$NS" exec "$DB_POD" -- env PGPASSWORD="$DB_PASS" \
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -x -c "$1"
}

db_query "
SELECT count(*) AS skeletons
FROM class_instance ci
JOIN class c ON c.id = ci.class_id
WHERE ci.project_id = $PROJECT_ID AND c.class_name = 'skeleton';"

db_query "
SELECT
  count(*) AS skeleton_summary_rows,
  sum(num_nodes)::bigint AS summary_treenodes,
  sum(cable_length) AS summary_cable_length_nm
FROM catmaid_skeleton_summary
WHERE project_id = $PROJECT_ID;"

db_query "
SELECT
  count(*) AS stats_summary_rows,
  sum(n_treenodes)::bigint AS stats_treenodes,
  sum(cable_length) AS stats_cable_length_nm
FROM catmaid_stats_summary
WHERE project_id = $PROJECT_ID;"

db_query "
SELECT
  id AS grid_id,
  cell_width,
  cell_height,
  cell_depth,
  enabled,
  has_msgpack_data,
  (SELECT count(*) FROM node_grid_cache_cell c WHERE c.grid_id = g.id) AS cache_cells,
  (SELECT count(*) FROM dirty_node_grid_cache_cell d WHERE d.grid_id = g.id) AS dirty_cells
FROM node_grid_cache g
WHERE project_id = $PROJECT_ID
ORDER BY cell_width;"

db_query "
SELECT
  (SELECT last_value FROM concept_id_seq) AS concept_sequence_last_value,
  (SELECT max(id) FROM concept) AS concept_max_id,
  (SELECT last_value FROM location_id_seq) AS location_sequence_last_value,
  (SELECT max(id) FROM location) AS location_max_id;"

db_query "
SELECT
  'treenode_edge' AS materialization,
  status,
  rows_done,
  target_rows,
  percent_done,
  completed_at
FROM public.catmaid_skeleton_treenode_edge_rebuild_status
WHERE project_id = $PROJECT_ID
UNION ALL
SELECT
  'summary_stats' AS materialization,
  status,
  rows_done,
  target_rows,
  percent_done,
  completed_at
FROM public.catmaid_skeleton_summary_stats_refresh_status
WHERE project_id = $PROJECT_ID;"
