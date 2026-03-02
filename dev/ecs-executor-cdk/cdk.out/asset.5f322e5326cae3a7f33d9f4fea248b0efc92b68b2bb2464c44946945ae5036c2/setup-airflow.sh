#!/bin/bash
# One-shot Airflow setup on EC2. Run once after CDK deploy + first SSM login.
#
# What this does:
#   1. Clone airflow repo from GitHub
#   2. Create Python venv + install Airflow, task-sdk, amazon provider
#   3. Build the React UI
#   4. Write airflow.cfg (multi-team, S3 DAG bundles, ECS executor)
#   5. Initialize DB + create teams
#   6. Create test DAGs + upload to S3
#   7. Start all services
#
# Usage: sudo su - ec2-user && bash /opt/airflow-scripts/setup-airflow.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

echo "============================================"
echo "  Airflow Multi-Team ECS Executor Setup"
echo "============================================"

# ── 1. Clone repo ─────────────────────────────────────────────────────
log_step "1/7 Cloning airflow repo"
if [ ! -d "${AIRFLOW_SRC}/.git" ]; then
    git clone https://github.com/apache/airflow.git "${AIRFLOW_SRC}"
else
    log_info "Repo exists, pulling latest..."
    cd "${AIRFLOW_SRC}" && git pull
fi

# ── 2. Python venv + install ──────────────────────────────────────────
log_step "2/7 Installing Airflow"
cd "${AIRFLOW_SRC}"
uv venv "${AIRFLOW_VENV}" --python 3.12 2>/dev/null || true
source "${AIRFLOW_VENV}/bin/activate"
uv pip install ./airflow-core ./task-sdk ./providers/amazon asyncpg psycopg2-binary

# ── 3. Build React UI ─────────────────────────────────────────────────
log_step "3/7 Building React UI"
cd "${AIRFLOW_UI_DIR}"
npm install --legacy-peer-deps 2>&1 | tail -3
npm run build 2>&1 | tail -3
cd "${AIRFLOW_SRC}"

# ── 4. Write airflow.cfg ──────────────────────────────────────────────
log_step "4/7 Writing airflow.cfg"
mkdir -p "${AIRFLOW_HOME}"

JWT_SECRET=$(python3 -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(64)).decode())")

cat > "${AIRFLOW_HOME}/airflow.cfg" << EOF
[database]
sql_alchemy_conn = postgresql+psycopg2://${DB_USER}:${DB_PASS}@${DB_ENDPOINT}:5432/${DB_NAME}

[core]
executor = LocalExecutor;team_alpha=airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor;team_beta=airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor
multi_team = True
execution_api_server_url = http://localhost:8080/execution/
auth_manager = airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
simple_auth_manager_all_admins = true

[api]
expose_config = True

[api_auth]
jwt_secret = ${JWT_SECRET}

[logging]
remote_logging = True
remote_base_log_folder = s3://${LOG_BUCKET}/logs
remote_log_conn_id = aws_default

[dag_processor]
dag_bundle_config_list = [{"name": "team_alpha_dags", "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle", "kwargs": {"bucket_name": "${DAG_BUCKET}", "prefix": "team_alpha"}, "team_name": "team_alpha"}, {"name": "team_beta_dags", "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle", "kwargs": {"bucket_name": "${DAG_BUCKET}", "prefix": "team_beta"}, "team_name": "team_beta"}, {"name": "shared_dags", "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle", "kwargs": {"bucket_name": "${DAG_BUCKET}", "prefix": "shared"}}]

[team_alpha=aws_ecs_executor]
cluster = alpha-cluster
container_name = airflow-worker
task_definition = ${ALPHA_TASK_DEF}
subnets = ${PRIVATE_SUBNETS}
security_groups = ${WORKER_SG}
launch_type = FARGATE
assign_public_ip = False
region_name = ${REGION}

[team_beta=aws_ecs_executor]
cluster = beta-cluster
container_name = airflow-worker
task_definition = ${BETA_TASK_DEF}
subnets = ${PRIVATE_SUBNETS}
security_groups = ${WORKER_SG}
launch_type = FARGATE
assign_public_ip = False
region_name = ${REGION}
EOF
log_info "Written to ${AIRFLOW_HOME}/airflow.cfg"

# ── 5. Init DB + create teams ─────────────────────────────────────────
log_step "5/7 Initializing DB and creating teams"
PGPASSWORD="${DB_PASS}" psql -h "${DB_ENDPOINT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT 1;" > /dev/null
airflow db migrate 2>&1 | tail -5

AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_alpha 2>/dev/null || log_info "team_alpha already exists"
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_beta 2>/dev/null || log_info "team_beta already exists"
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams list

# ── 6. Create test DAGs + upload to S3 ────────────────────────────────
log_step "6/7 Deploying test DAGs to S3"
bash "${SCRIPT_DIR}/deploy-dags.sh"

# ── 7. Start services ─────────────────────────────────────────────────
log_step "7/7 Starting Airflow services"
bash "${SCRIPT_DIR}/airflow-ctl.sh" start

echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "Quick commands (available in any shell):"
echo "  af status     - check service health"
echo "  af restart    - restart all services"
echo "  af logs       - tail all logs"
echo "  af db         - psql into metadata DB"
echo ""
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
echo "UI access (from your Mac):"
echo "  aws ssm start-session --target ${INSTANCE_ID} \\"
echo "    --document-name AWS-StartPortForwardingSession \\"
echo "    --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
echo ""
echo "Then open http://localhost:8080"
