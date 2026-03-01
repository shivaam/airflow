# Integration Test: Multi-Team AWS ECS Executor

**GitHub Issue:** [#62246](https://github.com/apache/airflow/issues/62246)
**Assignee:** @shivaam
**Date:** 2026-02-28

## Objective

Spin up a real Airflow environment with two teams (`team_alpha`, `team_beta`), both using
`AwsEcsExecutor` but pointing at different ECS clusters. Trigger DAGs for each team and
verify via AWS console and Airflow logs that tasks land in the correct cluster.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Breeze Container (your remote host)                    │
│                                                         │
│  ┌─────────┐  ┌───────────┐  ┌──────────────────────┐  │
│  │Scheduler│  │API Server │  │DAG Processor          │  │
│  │         │  │ (UI:28080)│  │                       │  │
│  └────┬────┘  └───────────┘  └──────────────────────┘  │
│       │                                                 │
│       ├── team_alpha DAG → ECS Executor (alpha config)  │
│       │                        │                        │
│       ├── team_beta DAG  → ECS Executor (beta config)   │
│       │                        │                        │
│       └── shared DAG     → LocalExecutor                │
└────────────────────────────────┼────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
          ┌─────────────────┐       ┌─────────────────┐
          │  AWS us-east-1  │       │  AWS us-west-2  │
          │                 │       │                 │
          │  alpha-cluster  │       │  beta-cluster   │
          │  alpha-task-def │       │  beta-task-def  │
          │  subnet-alpha   │       │  subnet-beta    │
          │  sg-alpha       │       │  sg-beta        │
          └─────────────────┘       └─────────────────┘
```

---

## Prerequisites

- An AWS account with permissions to create ECS, ECR, RDS, VPC resources
- AWS CLI configured (`aws configure` or IAM role on your remote host)
- Breeze working on your remote host
- Docker for building the Airflow worker image

---

## Phase 1: AWS Infrastructure Setup

You need to create resources in two regions. You can use the same region with two clusters
if you prefer — the key is that each team has distinct config.

### 1.1 Shared Database (RDS PostgreSQL)

Both teams' ECS tasks need to connect back to the same Airflow metadata DB. Breeze uses
SQLite by default, but ECS tasks can't reach it. You need an RDS instance that both Breeze
and ECS tasks can connect to.

1. Create an RDS PostgreSQL instance in one region (e.g., us-east-1)
2. DB name: `airflow_db`, note the endpoint, username, password
3. Security group: allow inbound PostgreSQL (5432) from:
   - Your remote host IP (for Breeze)
   - The CIDR of both ECS clusters' subnets
4. Make it publicly accessible (for dev/test — not for production)

Set the DB connection in `init.sh`:
```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://<user>:<pass>@<rds-endpoint>:5432/airflow_db"
```

### 1.2 ECR Repository (Container Image)

Build and push the Airflow worker image that ECS tasks will run.

```bash
# On your remote host (or Mac if same arch)
cd providers/amazon/src/airflow/providers/amazon/aws/executors/

# Build the image
docker build -t airflow-ecs-worker \
  --build-arg aws_default_region=us-east-1 .

# Create ECR repo (do this once)
aws ecr create-repository --repository-name airflow-ecs-worker --region us-east-1

# Login, tag, push
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
docker tag airflow-ecs-worker:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/airflow-ecs-worker:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/airflow-ecs-worker:latest
```

Note: The image needs the same Airflow version and Python version as your Breeze environment.
Since we're on `main`, you may need to build from source rather than using `apache/airflow:latest`.

### 1.3 ECS Cluster: alpha-cluster (us-east-1)

1. AWS Console → ECS → Create Cluster
2. Name: `alpha-cluster`
3. Infrastructure: AWS Fargate (Serverless)
4. Create

### 1.4 ECS Cluster: beta-cluster

Same steps, name: `beta-cluster`. Can be in the same region or a different one (e.g., us-west-2).
Using a different region makes the test more convincing but requires the ECR image to be
available in both regions (cross-region pull or replicate the repo).

### 1.5 Task Definitions

Create one per team. The key difference is the name — the container config is the same.

**alpha-task-def:**
1. ECS → Task Definitions → Create
2. Family: `alpha-task-def`
3. Launch type: Fargate
4. Task role: role with permissions your DAGs need
5. Task execution role: `AmazonECSTaskExecutionRolePolicy` + `CloudWatchLogsFullAccess`
6. Container name: `airflow-worker` (must match config)
7. Image: `<account-id>.dkr.ecr.us-east-1.amazonaws.com/airflow-ecs-worker:latest`
8. Environment variables:
   - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` = your RDS connection string
9. CPU/Memory: 0.5 vCPU / 1 GB (enough for test DAGs)

**beta-task-def:** Same steps, family name `beta-task-def`.

### 1.6 Networking

Note the VPC, subnets, and security groups for each cluster. You'll need:
- Subnet IDs (at least one per cluster)
- Security group IDs (must allow outbound to RDS and internet for ECR pulls)

---

## Phase 2: Configure Airflow (init.sh)

Update `files/airflow-breeze-config/init.sh` with your real AWS values:

```bash
# Replace the placeholder values with your real AWS resource IDs

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://user:pass@your-rds-endpoint:5432/airflow_db"

export AIRFLOW__TEAM_ALPHA___AWS_ECS_EXECUTOR__CLUSTER=alpha-cluster
export AIRFLOW__TEAM_ALPHA___AWS_ECS_EXECUTOR__REGION_NAME=us-east-1
export AIRFLOW__TEAM_ALPHA___AWS_ECS_EXECUTOR__SUBNETS=subnet-0abc123...
export AIRFLOW__TEAM_ALPHA___AWS_ECS_EXECUTOR__SECURITY_GROUPS=sg-0abc123...
export AIRFLOW__TEAM_ALPHA___AWS_ECS_EXECUTOR__TASK_DEFINITION=alpha-task-def

export AIRFLOW__TEAM_BETA___AWS_ECS_EXECUTOR__CLUSTER=beta-cluster
export AIRFLOW__TEAM_BETA___AWS_ECS_EXECUTOR__REGION_NAME=us-east-1  # or us-west-2
export AIRFLOW__TEAM_BETA___AWS_ECS_EXECUTOR__SUBNETS=subnet-0def456...
export AIRFLOW__TEAM_BETA___AWS_ECS_EXECUTOR__SECURITY_GROUPS=sg-0def456...
export AIRFLOW__TEAM_BETA___AWS_ECS_EXECUTOR__TASK_DEFINITION=beta-task-def
```

Also ensure AWS credentials are available inside Breeze. Options:
- Mount `~/.aws` into the container
- Export `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in init.sh
- Use an IAM role if running on EC2

---

## Phase 3: Setup & Run

```bash
# 1. Rsync from Mac to remote
rsync -avz --exclude='.venv' --exclude='build/' --exclude='__pycache__' \
  --exclude='*.pyc' --exclude='.pytest_cache' --exclude='*.egg-info' \
  --exclude='node_modules' --exclude='dist/' --exclude='.ruff_cache' \
  /Users/rastoshi/workplace/airflow/airflow/ \
  rastoshi-2-clouddesk.aka.corp.amazon.com:~/workplace/airflow/airflow/

# 2. SSH to remote
ssh rastoshi-2-clouddesk.aka.corp.amazon.com

# 3. Start Breeze (init.sh is auto-sourced)
cd ~/workplace/airflow/airflow
breeze --backend postgres  # use postgres since we need RDS anyway

# 4. Inside Breeze: point at RDS and migrate
airflow db migrate

# 5. Create teams
python /opt/airflow/dev/setup_multi_team.py

# 6. Verify
airflow teams list
airflow dags list   # should show alpha_simple_dag, beta_simple_dag, shared_simple_dag

# 7. Start Airflow (in a separate breeze shell or use start-airflow)
# Exit breeze, then:
breeze start-airflow --backend postgres --skip-assets-compilation
```

---

## Phase 4: Trigger & Verify

### Trigger DAGs

From the Airflow UI (http://localhost:28080) or CLI:

```bash
# Inside Breeze
airflow dags trigger alpha_simple_dag
airflow dags trigger beta_simple_dag
airflow dags trigger shared_simple_dag
```

### What to Check

| # | Check | Where to Look |
|---|-------|---------------|
| 1 | alpha_simple_dag task appears in `alpha-cluster` | AWS Console → ECS → alpha-cluster → Tasks |
| 2 | beta_simple_dag task appears in `beta-cluster` | AWS Console → ECS → beta-cluster → Tasks |
| 3 | shared_simple_dag runs locally (no ECS task) | Airflow UI → task logs (should be local) |
| 4 | Alpha task uses `alpha-task-def` | ECS Console → task details → Task Definition |
| 5 | Beta task uses `beta-task-def` | ECS Console → task details → Task Definition |
| 6 | Alpha task runs in alpha subnets | ECS Console → task details → Network |
| 7 | Beta task runs in beta subnets | ECS Console → task details → Network |
| 8 | Task completes successfully | Airflow UI → DAG run status = success |
| 9 | Logs are accessible | Airflow UI → task logs (remote logging if configured) |

### Scheduler Logs

Check the scheduler logs for executor selection messages:
```bash
# Inside Breeze
grep -i "executor" ~/airflow/logs/scheduler/*.log
```

You should see the scheduler picking the ECS executor for team DAGs and LocalExecutor for
the shared DAG.

---

## Phase 5: Report Results

Post a summary on the GitHub issue with:
- Screenshots of ECS console showing tasks in each cluster
- Airflow UI screenshots showing successful DAG runs
- Any log snippets showing executor selection
- Any bugs or issues encountered

---

## Cleanup

Don't forget to tear down AWS resources when done to avoid charges:
- Delete ECS clusters (or just the task definitions)
- Delete RDS instance
- Delete ECR repository
- Remove security group rules

---

## Quick Reference: File Locations

| File | Purpose |
|------|---------|
| `files/airflow-breeze-config/init.sh` | Env vars (auto-sourced by Breeze) |
| `dev/setup_multi_team.py` | Creates teams in DB |
| `dev/test_ecs_multi_team_config.py` | Quick config sanity check (mocked, optional) |
| `dags/team_alpha/alpha_dag.py` | Test DAG for team_alpha |
| `dags/team_beta/beta_dag.py` | Test DAG for team_beta |
| `dags/shared/shared_dag.py` | Test DAG for global scope |

---

## Troubleshooting

**ECS task fails to start:**
- Check task definition has correct image URI
- Check security group allows outbound to ECR (for image pull) and RDS (for DB)
- Check task execution role has `AmazonECSTaskExecutionRolePolicy`

**Task stuck in "queued":**
- Check scheduler logs for ECS API errors
- Verify AWS credentials are available inside Breeze
- Check `check_health_on_startup` — if the health check fails, the executor won't run tasks

**DAGs not appearing:**
- Run `airflow dags list-import-errors` to check for import issues
- Verify DAG bundle config in init.sh points to correct paths
- Verify teams exist: `airflow teams list`

**"Team does not exist" error on startup:**
- Run `setup_multi_team.py` before starting the scheduler
- Teams must exist in DB before DAG bundles sync
