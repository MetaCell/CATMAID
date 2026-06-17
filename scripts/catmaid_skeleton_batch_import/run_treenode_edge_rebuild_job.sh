#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Missing required environment variable: PROJECT_ID}"
: "${BATCH_SIZE:=250000}"
: "${TARGET_ROWS:?Missing required environment variable: TARGET_ROWS}"
: "${DB_HOST:?Missing required environment variable: DB_HOST}"
: "${DB_PORT:=5432}"
: "${DB_NAME:?Missing required environment variable: DB_NAME}"
: "${DB_USER:?Missing required environment variable: DB_USER}"

export PGHOST="$DB_HOST"
export PGPORT="$DB_PORT"
export PGDATABASE="$DB_NAME"
export PGUSER="$DB_USER"
export PGAPPNAME="catmaid_skeleton_edge_rebuild_project_${PROJECT_ID}"

echo "Installing batched treenode_edge rebuild procedure"
psql -v ON_ERROR_STOP=1 -f /scripts/rebuild_treenode_edges_batched.sql

echo "Starting batched treenode_edge rebuild: project=${PROJECT_ID} batch_size=${BATCH_SIZE} target_rows=${TARGET_ROWS}"
psql -v ON_ERROR_STOP=1 -c "CALL public.catmaid_skeleton_rebuild_treenode_edges_batched(${PROJECT_ID}, ${BATCH_SIZE}, ${TARGET_ROWS});"
