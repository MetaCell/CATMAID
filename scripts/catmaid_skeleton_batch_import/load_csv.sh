#!/usr/bin/env bash
# Load one generated CATMAID CSV batch into the remote database.
#
# The loader runs psql inside the database pod and streams local CSV data over
# kubectl exec as COPY ... FROM STDIN. This avoids a long-lived kubectl
# port-forward during large COPY operations.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

set_db_defaults
require_env STAGING DB_USER DB_NAME DB_PASS
LOAD_DISABLE_TRIGGERS="${LOAD_DISABLE_TRIGGERS:-false}"

class_path="${STAGING}/class_instance.csv"
link_path="${STAGING}/class_instance_class_instance.csv"
treenode_path="${STAGING}/treenode.csv"

for path in "$class_path" "$link_path" "$treenode_path"; do
  if [[ ! -f "$path" ]]; then
    echo "Missing generated CSV: $path" >&2
    exit 2
  fi
done

stream_copy_sql() {
  cat <<'SQL'
\set ON_ERROR_STOP on
SET statement_timeout = 0;
SET lock_timeout = 0;
SQL
  if [[ "$LOAD_DISABLE_TRIGGERS" == "true" ]]; then
    cat <<'SQL'
SET session_replication_role = replica;
SQL
  fi
  cat <<'SQL'
BEGIN;
COPY class_instance (id, user_id, project_id, class_id, name) FROM STDIN WITH (FORMAT csv, NULL '\N');
SQL
  cat "$class_path"
  printf '\\.\n'

  cat <<'SQL'
COPY class_instance_class_instance (id, user_id, project_id, relation_id, class_instance_a, class_instance_b) FROM STDIN WITH (FORMAT csv, NULL '\N');
SQL
  cat "$link_path"
  printf '\\.\n'

  cat <<'SQL'
COPY treenode (id, project_id, location_x, location_y, location_z, editor_id, user_id, skeleton_id, radius, parent_id) FROM STDIN WITH (FORMAT csv, NULL '\N');
SQL
  cat "$treenode_path"
  printf '\\.\n'

  cat <<'SQL'
COMMIT;
SQL
  if [[ "$LOAD_DISABLE_TRIGGERS" == "true" ]]; then
    cat <<'SQL'
SET session_replication_role = origin;
SQL
  fi
  cat <<'SQL'
ANALYZE class_instance;
ANALYZE class_instance_class_instance;
ANALYZE treenode;
SQL
}

stream_copy_sql | kubectl -n "$NS" exec -i "$DB_POD" -- env PGPASSWORD="$DB_PASS" \
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1
