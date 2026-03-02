#!/bin/bash
# Complete Airflow setup on EC2 for multi-team ECS executor testing.
# Run once after CDK deploy and first SSM login.
#
# What this does:
#   1. Reads all config from SSM Parameter Store
#   2. Clones the airflow repo from GitHub
#   3. Installs Airflow + amazon provider via uv
#   4. Builds the React UI
#   5. Writes airflow.cfg (multi-team, S3 DAG bundles, ECS executor)
#   6. Initializes the DB and creates teams
#   7. Creates test DAGs and uploads them to S3
#   8. Builds and pushes the worker Docker image to ECR
#   9. Starts all Airflow services
#
# Usage: sudo su - ec2-user && bash /opt/airflow-scripts/setup-airflow.sh
set -e

echo "============================================"
echo "  Airflow Multi-Team ECS Executor Setup"
echo "============================================"
echo ""

# ── Step 1: Read config from SSM ──────────────────────────────────────
echo "[1/9] Reading config from SSM..."
REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
DB_ENDPOINT=$(aws ssm get-parameter --name /airflow-test/db-endpoint --query Parameter.Value --output text)
DB_SECRET_ARN=$(aws ssm get-parameter --name /airflow-test/db-secret-arn --query Parameter.Value --output text)
DB_NAME=$(aws ssm get-parameter --name /airflow-test/db-name --query Parameter.Value --output text)
ECR_REPO=$(aws ssm get-parameter --name /airflow-test/ecr-repo --query Parameter.Value --output text)
LOG_BUCKET=$(aws ssm get-parameter --name /airflow-test/log-bucket --query Parameter.Value --output text)
DAG_BUCKET=$(aws ssm get-parameter --name /airflow-test/dag-bucket --query Parameter.Value --output text)
NLB_DNS=$(aws ssm get-parameter --name /airflow-test/nlb-dns --query Parameter.Value --output text)
ALPHA_TASK_DEF=$(aws ssm get-parameter --name /airflow-test/alpha-task-def --query Parameter.Value --output text)
BETA_TASK_DEF=$(aws ssm get-parameter --name /airflow-test/beta-task-def --query Parameter.Value --output text)
PRIVATE_SUBNETS=$(aws ssm get-parameter --name /airflow-test/private-subnets --query Parameter.Value --output text)
WORKER_SG=$(aws ssm get-parameter --name /airflow-test/worker-sg --query Parameter.Value --output text)

DB_SECRET=$(aws secretsmanager get-secret-value --secret-id "$DB_SECRET_ARN" --query SecretString --output text)
DB_USER=$(echo "$DB_SECRET" | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
DB_PASS=$(echo "$DB_SECRET" | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")

echo "  Region: $REGION"
echo "  DB: $DB_ENDPOINT"
echo "  DAG bucket: $DAG_BUCKET"
echo "  Log bucket: $LOG_BUCKET"
echo ""

# ── Step 2: Clone repo and install Airflow ─────────────────────────────
echo "[2/9] Cloning airflow repo and installing..."
if [ ! -d /home/ec2-user/airflow/.git ]; then
    git clone https://github.com/apache/airflow.git /home/ec2-user/airflow
else
    echo "  Repo already exists, pulling latest..."
    cd /home/ec2-user/airflow && git pull
fi

cd /home/ec2-user/airflow
uv venv /home/ec2-user/airflow-venv --python 3.12 2>/dev/null || true
source /home/ec2-user/airflow-venv/bin/activate
uv pip install ./airflow-core ./task-sdk ./providers/amazon asyncpg psycopg2-binary
echo ""

# ── Step 3: Build React UI ────────────────────────────────────────────
echo "[3/9] Building React UI..."
cd /home/ec2-user/airflow/airflow-core/src/airflow/ui
npm install --legacy-peer-deps 2>&1 | tail -3
npm run build 2>&1 | tail -3
cd /home/ec2-user/airflow
echo ""

# ── Step 4: Write airflow.cfg ─────────────────────────────────────────
echo "[4/9] Writing airflow.cfg..."
export AIRFLOW_HOME=/home/ec2-user/airflow-home
mkdir -p $AIRFLOW_HOME

JWT_SECRET=$(python3 -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(64)).decode())")

cat > $AIRFLOW_HOME/airflow.cfg << EOF
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
echo "  Written to $AIRFLOW_HOME/airflow.cfg"
echo ""

# ── Step 5: Initialize DB ─────────────────────────────────────────────
echo "[5/9] Verifying DB connectivity and running migrations..."
PGPASSWORD=$DB_PASS psql -h "$DB_ENDPOINT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" > /dev/null
airflow db migrate 2>&1 | tail -5
echo ""

# ── Step 6: Create teams ──────────────────────────────────────────────
echo "[6/9] Creating teams..."
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_alpha 2>/dev/null || echo "  team_alpha already exists"
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_beta 2>/dev/null || echo "  team_beta already exists"
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams list
echo ""

# ── Step 7: Create test DAGs and upload to S3 ─────────────────────────
echo "[7/9] Creating test DAGs and uploading to S3..."
mkdir -p /tmp/dags/{team_alpha,team_beta,shared}

cat > /tmp/dags/team_alpha/alpha_dag.py << 'PYEOF'
"""Test DAG for team_alpha - verifies ECS executor routing."""
from __future__ import annotations
from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["multi-team", "team_alpha", "ecs-test"])
def alpha_simple_dag():
    @task
    def alpha_hello():
        import socket
        print(f"Hello from team_alpha on host {socket.gethostname()}")
        return "alpha_done"
    alpha_hello()

alpha_simple_dag()
PYEOF

cat > /tmp/dags/team_beta/beta_dag.py << 'PYEOF'
"""Test DAG for team_beta - verifies ECS executor routing."""
from __future__ import annotations
from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["multi-team", "team_beta", "ecs-test"])
def beta_simple_dag():
    @task
    def beta_hello():
        import socket
        print(f"Hello from team_beta on host {socket.gethostname()}")
        return "beta_done"
    beta_hello()

beta_simple_dag()
PYEOF

cat > /tmp/dags/shared/shared_dag.py << 'PYEOF'
"""Shared DAG (no team) - should use global LocalExecutor, not ECS."""
from __future__ import annotations
from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["multi-team", "shared", "ecs-test"])
def shared_simple_dag():
    @task
    def shared_hello():
        import socket
        print(f"Hello from shared DAG on host {socket.gethostname()} (should be EC2)")
        return "shared_done"
    shared_hello()

shared_simple_dag()
PYEOF

aws s3 sync /tmp/dags/ s3://$DAG_BUCKET/ --delete
echo "  DAGs uploaded to s3://$DAG_BUCKET/"
echo ""

# ── Step 8: Build and push worker image ───────────────────────────────
echo "[8/9] Building and pushing worker Docker image..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_REPO"

cat > /tmp/Dockerfile.worker << 'DEOF'
FROM apache/airflow:latest
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon asyncpg psycopg2-binary
DEOF

docker build -f /tmp/Dockerfile.worker -t "${ECR_REPO}:latest" /tmp 2>&1 | tail -5
docker push "${ECR_REPO}:latest" 2>&1 | tail -3
echo "  Image pushed to ${ECR_REPO}:latest"
echo ""

# ── Step 9: Start services ────────────────────────────────────────────
echo "[9/9] Starting Airflow services..."
pkill -9 -f "airflow api-server" 2>/dev/null || true
pkill -9 -f "airflow scheduler" 2>/dev/null || true
pkill -9 -f "airflow dag-processor" 2>/dev/null || true
pkill -9 -f "gunicorn.*airflow" 2>/dev/null || true
fuser -k 8080/tcp 2>/dev/null || true
sleep 3

rm -f /tmp/api-server.log /tmp/scheduler.log /tmp/dag-processor.log

nohup airflow api-server --port 8080 > /tmp/api-server.log 2>&1 &
sleep 5
nohup airflow scheduler > /tmp/scheduler.log 2>&1 &
nohup airflow dag-processor > /tmp/dag-processor.log 2>&1 &
sleep 2

echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "Services:"
pgrep -a -f "airflow api-server" || echo "  api-server: NOT RUNNING"
pgrep -a -f "airflow scheduler" || echo "  scheduler: NOT RUNNING"
pgrep -a -f "airflow dag-processor" || echo "  dag-processor: NOT RUNNING"
echo ""
echo "Logs:"
echo "  tail -f /tmp/api-server.log"
echo "  tail -f /tmp/scheduler.log"
echo "  tail -f /tmp/dag-processor.log"
echo ""
echo "UI access (from your Mac):"
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
echo "  aws ssm start-session --target $INSTANCE_ID \\"
echo "    --document-name AWS-StartPortForwardingSession \\"
echo "    --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
echo ""
echo "Then open http://localhost:8080"
