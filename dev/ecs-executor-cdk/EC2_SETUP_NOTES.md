# EC2 Setup Notes

Captured during first manual setup. Use this to build automation scripts later.

## Prerequisites (installed by UserData on boot)

- Python 3.12 + pip
- uv (pip3.12 install uv)
- Docker (systemctl enabled)
- Git
- AWS CLI v2
- psql (postgresql15)

### Not installed by UserData (install manually)

```bash
sudo dnf install -y nodejs tmux
```

- `nodejs` — needed to build the Airflow React UI
- `tmux` — run api-server, scheduler, dag-processor in separate panes

## Step 1 — Read config from SSM

```bash
sudo su - ec2-user

REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
DB_ENDPOINT=$(aws ssm get-parameter --name /airflow-test/db-endpoint --query Parameter.Value --output text)
DB_SECRET_ARN=$(aws ssm get-parameter --name /airflow-test/db-secret-arn --query Parameter.Value --output text)
DB_NAME=$(aws ssm get-parameter --name /airflow-test/db-name --query Parameter.Value --output text)
ECR_REPO=$(aws ssm get-parameter --name /airflow-test/ecr-repo --query Parameter.Value --output text)
LOG_BUCKET=$(aws ssm get-parameter --name /airflow-test/log-bucket --query Parameter.Value --output text)
NLB_DNS=$(aws ssm get-parameter --name /airflow-test/nlb-dns --query Parameter.Value --output text)
ALPHA_TASK_DEF=$(aws ssm get-parameter --name /airflow-test/alpha-task-def --query Parameter.Value --output text)
BETA_TASK_DEF=$(aws ssm get-parameter --name /airflow-test/beta-task-def --query Parameter.Value --output text)
PRIVATE_SUBNETS=$(aws ssm get-parameter --name /airflow-test/private-subnets --query Parameter.Value --output text)
WORKER_SG=$(aws ssm get-parameter --name /airflow-test/worker-sg --query Parameter.Value --output text)

DB_SECRET=$(aws secretsmanager get-secret-value --secret-id $DB_SECRET_ARN --query SecretString --output text)
DB_USER=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
DB_PASS=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
```

## Step 2 — Verify DB connectivity

```bash
PGPASSWORD=$DB_PASS psql -h $DB_ENDPOINT -U $DB_USER -d $DB_NAME -c "SELECT 1;"
```

## Step 3 — Clone repo and install Airflow

```bash
git clone https://github.com/apache/airflow.git /home/ec2-user/airflow
cd /home/ec2-user/airflow

uv venv /home/ec2-user/airflow-venv --python 3.12
source /home/ec2-user/airflow-venv/bin/activate
uv pip install ./airflow-core ./task-sdk ./providers/amazon asyncpg psycopg2-binary
```

- `asyncpg` — Airflow 3.x uses async PostgreSQL (SQLAlchemy asyncpg dialect)
- `psycopg2-binary` — sync PostgreSQL driver for migrations and CLI

## Step 4 — Build the React UI

```bash
cd /home/ec2-user/airflow/airflow-core/src/airflow/ui
npm install --legacy-peer-deps
npm run build
cd /home/ec2-user/airflow
```

Without this, the api-server will crash with `TemplateNotFound: '/index.html'`.
`--legacy-peer-deps` is needed because `@visx` hasn't updated peer deps for React 19.

## Step 5 — Configure Airflow (multi-team ECS executor)

Or use `setup-config.sh` from `dev/ecs-executor-cdk/scripts/` which reads SSM and writes the full config.

```bash
export AIRFLOW_HOME=/home/ec2-user/airflow-home
mkdir -p $AIRFLOW_HOME

JWT_SECRET=$(python -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(64)).decode())")

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
EOF
```

Key config notes:
- `LocalExecutor` is the global default — DAGs without a team run locally on EC2
- `team_alpha` and `team_beta` use `AwsEcsExecutor` — DAGs assigned to those teams run on ECS Fargate
- `multi_team = True` enables team-based executor routing and team selectors in the UI
- `execution_api_server_url = http://localhost:8080/execution/` — LocalExecutor tasks run on EC2, so they talk to the local api-server. ECS tasks get the NLB URL from their task definition env vars.
- `expose_config = True` makes the Config page visible in the UI (disabled by default)
- `simple_auth_manager_all_admins = true` gives all users admin access (no passwords.json needed)
- `[api_auth] jwt_secret` — shared JWT signing key. Without this, each process generates its own random key and JWT signature verification fails.
- `[team_alpha=aws_ecs_executor]` — per-team ECS config. The `[team_name=section]` format is how Airflow resolves team-scoped config.
- ECS executor import path must include `.aws.`: `airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor`

## Step 5a — Create teams in DB

Teams must exist in the database before the scheduler can start with team-based executor config.

```bash
source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home

# Use env var override to bypass chicken-and-egg team validation
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_alpha
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_beta
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams list
```

## Step 6 — Init DB

```bash
source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home
airflow db migrate
```

## Step 7 — Start services

Use `restart-airflow.sh` from `dev/ecs-executor-cdk/scripts/`:

```bash
bash /opt/airflow-scripts/restart-airflow.sh
```

Or start manually with tmux:

```bash
tmux new -s airflow

# Pane 1 — api-server
source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home
airflow api-server --port 8080

# Ctrl+B then " to split

# Pane 2 — scheduler
source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home
airflow scheduler

# Ctrl+B then " to split

# Pane 3 — dag-processor
source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home
airflow dag-processor
```

Detach with `Ctrl+B d`. Reattach later with `tmux attach -t airflow`.

## Step 8 — Access UI (from cloud desktop)

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name AirflowEcsExecutorTest \
  --query "Stacks[0].Outputs[?OutputKey=='Ec2InstanceId'].OutputValue" --output text)

aws ssm start-session \
  --target $INSTANCE_ID \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Open http://localhost:8080 — all users are admin (simple_auth_manager_all_admins).

Multi-team UI: no dedicated Teams page. Teams are managed via CLI (`airflow teams list/create/delete`).
The UI shows team dropdown selectors on Connections, Variables, and Pools pages when `multi_team = True`.

## Step 9 — Build and push worker image

```bash
source /home/ec2-user/airflow-venv/bin/activate
REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
ECR_REPO=$(aws ssm get-parameter --name /airflow-test/ecr-repo --query Parameter.Value --output text)

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_REPO

cat > /tmp/Dockerfile.worker << 'DEOF'
FROM python:3.12-slim
COPY . /opt/airflow-src/
RUN pip install --no-cache-dir \
    /opt/airflow-src/airflow-core \
    /opt/airflow-src/task-sdk \
    /opt/airflow-src/providers/amazon \
    asyncpg psycopg2-binary
DEOF

cd /home/ec2-user/airflow
docker build -f /tmp/Dockerfile.worker -t ${ECR_REPO}:latest .
docker push ${ECR_REPO}:latest
```

## Automation Scripts

Scripts in `dev/ecs-executor-cdk/scripts/`, copy to EC2 at `/opt/airflow-scripts/`:

```bash
sudo mkdir -p /opt/airflow-scripts
sudo cp /home/ec2-user/airflow/dev/ecs-executor-cdk/scripts/*.sh /opt/airflow-scripts/
sudo chmod +x /opt/airflow-scripts/*.sh
```

| Script | Purpose | When to run |
|--------|---------|-------------|
| `setup-config.sh` | Reads SSM params, writes complete airflow.cfg | Once after first SSM login, or after CDK redeploy |
| `create-teams.sh` | Creates team_alpha and team_beta in the DB | Once after `airflow db migrate` |
| `restart-airflow.sh` | Kills all processes, cleans logs, starts all services | Each time you need to restart |

## Issues encountered

| Issue | Fix |
|-------|-----|
| `No module named 'asyncpg'` | `uv pip install asyncpg psycopg2-binary` |
| SG description em dashes rejected by AWS | Replace all `—` with `-` in SG descriptions |
| Cloud desktop IP is AWS internal | Removed ALB, use SSM tunnel instead |
| RDS takes 10-20 min with Multi-AZ | Set `multiAz: false` |
| `airflow users create` not found in 3.x | Use simple auth manager — no user management needed |
| Wrong SimpleAuthManager import path | `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager` |
| `TemplateNotFound: '/index.html'` | Build React UI: `cd airflow-core/src/airflow/ui && npm install --legacy-peer-deps && npm run build` |
| `npm: command not found` | `sudo dnf install -y nodejs` |
| `npm install` peer dep conflict (React 19 vs @visx) | Use `npm install --legacy-peer-deps` |
| `pkill` not killing Airflow processes | Use `pkill -9` + `pkill -9 -f "gunicorn.*airflow"` + `fuser -k 8080/tcp` |
| Port 8080 already in use on restart | `fuser -k 8080/tcp` in restart script |
| Config page says "admin has disabled" | Add `[api] expose_config = True` to airflow.cfg |
| Scheduler fails: teams don't exist in DB | Create teams with `airflow teams create`. Use `AIRFLOW__CORE__EXECUTOR=LocalExecutor` override to bypass chicken-and-egg validation |
| Scheduler fails: module could not be loaded | Wrong import path. Correct: `airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor` (note `.aws.`). Also ensure provider is installed: `uv pip install ./providers/amazon` |
| Scheduler fails: `[aws_ecs_executor/cluster] not found` | Need per-team config sections: `[team_alpha=aws_ecs_executor]` with cluster, task_definition, subnets, etc. |
| LocalExecutor tasks timeout to Execution API | `execution_api_server_url` pointed to NLB (EC2-SG not allowed). Changed to `http://localhost:8080/execution/`. ECS tasks use NLB URL from task definition env vars. |
| JWT signature verification failed | Each process generated its own random JWT secret. Fix: add `[api_auth] jwt_secret = <shared-key>` to airflow.cfg |
| `pip: command not found` in venv | Use `uv pip` instead — the venv was created with uv |

## TODO — Automate into scripts

Per DESIGN.md, these should become:
- `setup-airflow.sh` — Steps 1-7 (first-time setup) — partially done via `setup-config.sh`
- `switch-branch.sh <branch>` — git checkout + reinstall + rebuild UI + rebuild image + db migrate + restart
- `rebuild-worker-image.sh` — Step 9 only
- Add nodejs and tmux to UserData
- Systemd units for Airflow services (replace nohup)
