#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

set_app_defaults
require_env PROJECT_ID TEMPLATE_PROJECT_ID

kubectl -n "$NS" exec -i "$APP_POD" -- env \
  PROJECT_ID="$PROJECT_ID" \
  TEMPLATE_PROJECT_ID="$TEMPLATE_PROJECT_ID" \
  bash -lc 'cd /home/django/projects && /home/env/bin/python manage.py shell' \
  < "$SCRIPT_DIR/publish_project_permissions.py"
