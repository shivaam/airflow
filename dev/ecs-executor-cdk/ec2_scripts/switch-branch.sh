#!/bin/bash
# Switch to a different airflow branch, reinstall, rebuild, restart.
#
# Usage: bash /opt/airflow-scripts/switch-branch.sh <branch-name>
#        bash /opt/airflow-scripts/switch-branch.sh main
#        bash /opt/airflow-scripts/switch-branch.sh my-feature-branch
set -e

BRANCH="${1:?Usage: switch-branch.sh <branch-name>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

log_step "Stopping services"
bash "${SCRIPT_DIR}/airflow-ctl.sh" stop

log_step "Switching to branch: ${BRANCH}"
cd "${AIRFLOW_SRC}"
git fetch --all
git checkout "${BRANCH}"
git pull origin "${BRANCH}" 2>/dev/null || true

log_step "Reinstalling Airflow"
source "${AIRFLOW_VENV}/bin/activate"
uv pip install ./airflow-core ./task-sdk ./providers/amazon asyncpg psycopg2-binary

log_step "Rebuilding React UI"
cd "${AIRFLOW_UI_DIR}"
npm install --legacy-peer-deps 2>&1 | tail -3
npm run build 2>&1 | tail -3

log_step "Running DB migrations"
cd "${AIRFLOW_SRC}"
airflow db migrate 2>&1 | tail -5

log_step "Rebuilding worker image"
bash "${SCRIPT_DIR}/rebuild-worker-image.sh"

log_step "Starting services"
bash "${SCRIPT_DIR}/airflow-ctl.sh" start

log_info "Switched to ${BRANCH} and all services restarted."
