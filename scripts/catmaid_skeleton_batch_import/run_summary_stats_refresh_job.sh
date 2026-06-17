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
export PGAPPNAME="catmaid_skeleton_summary_stats_refresh_project_${PROJECT_ID}"

echo "Installing batched summary/stat refresh procedure"
psql -v ON_ERROR_STOP=1 -f /scripts/refresh_summary_stats_by_node_batched.sql

echo "Starting batched summary/stat refresh: project=${PROJECT_ID} batch_size=${BATCH_SIZE} target_rows=${TARGET_ROWS}"
psql -v ON_ERROR_STOP=1 -c "CALL public.catmaid_skeleton_refresh_summary_stats_by_node_batched(${PROJECT_ID}, ${BATCH_SIZE}, ${TARGET_ROWS});"
