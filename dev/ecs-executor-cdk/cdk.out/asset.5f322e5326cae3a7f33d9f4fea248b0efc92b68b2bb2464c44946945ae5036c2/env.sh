#!/bin/bash
# Source-able environment config for all Airflow EC2 scripts.
# Every other script sources this. Change paths/config here only.
#
# Usage: source /opt/airflow-scripts/env.sh

# ── Paths ──────────────────────────────────────────────────────────────
export AIRFLOW_USER="ec2-user"
export AIRFLOW_HOME="/home/${AIRFLOW_USER}/airflow-home"
export AIRFLOW_SRC="/home/${AIRFLOW_USER}/airflow"
export AIRFLOW_VENV="/home/${AIRFLOW_USER}/airflow-venv"
export AIRFLOW_UI_DIR="${AIRFLOW_SRC}/airflow-core/src/airflow/ui"
export DAG_STAGING_DIR="/tmp/dags"
export SCRIPTS_DIR="/opt/airflow-scripts"
export LOG_DIR="/tmp"

# ── Activate venv if it exists ─────────────────────────────────────────
if [ -f "${AIRFLOW_VENV}/bin/activate" ]; then
    source "${AIRFLOW_VENV}/bin/activate"
fi

# ── Read SSM parameters (cached for the shell session) ─────────────────
_ssm_get() {
    aws ssm get-parameter --name "$1" --query Parameter.Value --output text 2>/dev/null
}

if [ -z "$AIRFLOW_ENV_LOADED" ]; then
    export REGION=$(_ssm_get /airflow-test/region)
    export DB_ENDPOINT=$(_ssm_get /airflow-test/db-endpoint)
    export DB_SECRET_ARN=$(_ssm_get /airflow-test/db-secret-arn)
    export DB_NAME=$(_ssm_get /airflow-test/db-name)
    export ECR_REPO=$(_ssm_get /airflow-test/ecr-repo)
    export LOG_BUCKET=$(_ssm_get /airflow-test/log-bucket)
    export DAG_BUCKET=$(_ssm_get /airflow-test/dag-bucket)
    export NLB_DNS=$(_ssm_get /airflow-test/nlb-dns)
    export ALPHA_TASK_DEF=$(_ssm_get /airflow-test/alpha-task-def)
    export BETA_TASK_DEF=$(_ssm_get /airflow-test/beta-task-def)
    export PRIVATE_SUBNETS=$(_ssm_get /airflow-test/private-subnets)
    export WORKER_SG=$(_ssm_get /airflow-test/worker-sg)

    # DB credentials from Secrets Manager
    _DB_SECRET=$(aws secretsmanager get-secret-value --secret-id "$DB_SECRET_ARN" --query SecretString --output text 2>/dev/null)
    export DB_USER=$(echo "$_DB_SECRET" | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])" 2>/dev/null)
    export DB_PASS=$(echo "$_DB_SECRET" | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])" 2>/dev/null)
    unset _DB_SECRET

    export AIRFLOW_ENV_LOADED=1
fi

# ── Helper functions ───────────────────────────────────────────────────
log_info()  { echo -e "\033[0;32m[INFO]\033[0m  $*"; }
log_warn()  { echo -e "\033[0;33m[WARN]\033[0m  $*"; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $*"; }
log_step()  { echo -e "\n\033[1;36m── $* ──\033[0m"; }
