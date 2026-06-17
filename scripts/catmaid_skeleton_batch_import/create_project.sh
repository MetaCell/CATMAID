#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

set_app_defaults
require_env IMPORT_CONFIG

if [[ ! -f "$IMPORT_CONFIG" ]]; then
  echo "Missing import config file: $IMPORT_CONFIG" >&2
  exit 2
fi

import_config_json="$(<"$IMPORT_CONFIG")"

output="$(
  kubectl -n "$NS" exec -i "$APP_POD" -- env \
    IMPORT_CONFIG_JSON="$import_config_json" \
    PROJECT_TITLE="${PROJECT_TITLE:-}" \
    STACK_TITLE="${STACK_TITLE:-}" \
    STACK_DIMENSION="${STACK_DIMENSION:-}" \
    STACK_RESOLUTION="${STACK_RESOLUTION:-}" \
    STACK_TRANSLATION="${STACK_TRANSLATION:-}" \
    STACK_ORIENTATION="${STACK_ORIENTATION:-}" \
    STACK_CANARY_LOCATION="${STACK_CANARY_LOCATION:-}" \
    bash -lc 'cd /home/django/projects && /home/env/bin/python manage.py shell' \
    < "$SCRIPT_DIR/create_project.py"
)"

printf '%s\n' "$output" >&2

project_id="$(awk -F= '/^PROJECT_ID=/ {print $2}' <<<"$output" | tail -1)"
stack_id="$(awk -F= '/^STACK_ID=/ {print $2}' <<<"$output" | tail -1)"
import_user_id="$(awk -F= '/^IMPORT_USER_ID=/ {print $2}' <<<"$output" | tail -1)"

if [[ -z "$project_id" || -z "$stack_id" || -z "$import_user_id" ]]; then
  echo "Failed to parse project setup output" >&2
  exit 1
fi

cat <<EOF
export PROJECT_ID=$project_id
export STACK_ID=$stack_id
export IMPORT_USER_ID=$import_user_id
EOF
