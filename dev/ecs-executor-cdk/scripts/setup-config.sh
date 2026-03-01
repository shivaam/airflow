#!/bin/bash
# Generate airflow.cfg from SSM parameters.
# Run once after first SSM login, or after CDK redeploy.
# Usage: bash /opt/airflow-scripts/setup-config.sh
set -e

source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home
mkdir -p $AIRFLOW_HOME

echo "Reading config from SSM..."
REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
DB_ENDPOINT=$(aws ssm get-parameter --name /airflow-test/db-endpoint --query Parameter.Value --output text)
DB_SECRET_ARN=$(aws ssm get-parameter --name /airflow-test/db-secret-arn --query Parameter.Value --output text)
DB_NAME=$(aws ssm get-parameter --name /airflow-test/db-name --query Parameter.Value --output text)
LOG_BUCKET=$(aws ssm get-parameter --name /airflow-test/log-bucket --query Parameter.Value --output text)
NLB_DNS=$(aws ssm get-parameter --name /airflow-test/nlb-dns --query Parameter.Value --output text)
ALPHA_TASK_DEF=$(aws ssm get-parameter --name /airflow-test/alpha-task-def --query Parameter.Value --output text)
BETA_TASK_DEF=$(aws ssm get-parameter --name /airflow-test/beta-task-def --query Parameter.Value --output text)
PRIVATE_SUBNETS=$(aws ssm get-parameter --name /airflow-test/private-subnets --query Parameter.Value --output text)
WORKER_SG=$(aws ssm get-parameter --name /airflow-test/worker-sg --query Parameter.Value --output text)

DB_SECRET=$(aws secretsmanager get-secret-value --secret-id $DB_SECRET_ARN --query SecretString --output text)
DB_USER=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
DB_PASS=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")

# Generate a stable JWT secret
JWT_SECRET=$(python -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(64)).decode())")

echo "Writing airflow.cfg..."
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

[dag_processor]
dag_bundle_config_list = [{"name": "team_alpha_dags", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "/home/ec2-user/airflow-dags/team_alpha"}, "team_name": "team_alpha"}, {"name": "team_beta_dags", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "/home/ec2-user/airflow-dags/team_beta"}, "team_name": "team_beta"}, {"name": "shared_dags", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "/home/ec2-user/airflow-dags/shared"}}]
EOF

echo "Config written to $AIRFLOW_HOME/airflow.cfg"
echo ""
echo "Next steps:"
echo "  1. airflow db migrate"
echo "  2. airflow teams create team_alpha"
echo "  3. airflow teams create team_beta"
echo "  4. bash /opt/airflow-scripts/restart-airflow.sh"
