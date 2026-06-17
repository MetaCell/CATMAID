#!/usr/bin/env bash
# Shared shell helpers for the Allen Dense Skeleton import scripts.
#
# Source this file from other scripts in scripts/catmaid_skeleton_batch_import.
# It provides small, consistent helpers for:
#
# - failing early when required environment variables are missing;
# - discovering the current catmaid-db pod from NS;
# - discovering the current catmaid app pod from NS;
# - producing default psql arguments for the CATMAID database.
#
# Most scripts can be run with just NS plus their task-specific environment
# variables. The helpers fill DB_POD and APP_POD when the caller
# has not pinned them explicitly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

require_env() {
  local missing=0
  for name in "$@"; do
    if [[ -z "${!name:-}" ]]; then
      echo "Missing required environment variable: ${name}" >&2
      missing=1
    fi
  done
  if [[ "$missing" -ne 0 ]]; then
    exit 2
  fi
}

set_db_defaults() {
  require_env NS

  if [[ -z "${DB_POD:-}" ]]; then
    DB_POD="$(kubectl -n "$NS" get pod -l app=catmaid-db -o jsonpath='{.items[0].metadata.name}')"
    export DB_POD
  fi
}

set_app_defaults() {
  require_env NS

  if [[ -z "${APP_POD:-}" ]]; then
    APP_POD="$(kubectl -n "$NS" get pod -l app=catmaid -o jsonpath='{.items[0].metadata.name}')"
    export APP_POD
  fi
}

set_kube_defaults() {
  set_db_defaults
  set_app_defaults
}

db_psql_args() {
  require_env DB_USER DB_NAME
  printf '%s\n' -U "$DB_USER" -d "$DB_NAME"
}
