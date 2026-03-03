# Airflow ECS/Batch Executor Test Infrastructure

4-stack CDK deployment for testing Airflow 3.x multi-team ECS and Batch executors on AWS.

## Stacks

| Stack | What | Deploy time |
|-------|------|-------------|
| AirflowInfra | VPC, RDS, S3, ECR, NLB, IAM, SSM params | ~15 min (RDS) |
| AirflowEc2 | EC2 instance, NLB target, scripts | ~2 min |
| AirflowEcs | 2 ECS clusters + Fargate task defs | ~30s |
| AirflowBatch | Batch compute env + job queue + job def | ~30s |

## Deploy (from your Mac)

```bash
cd dev/ecs-executor-cdk
npm install
npm run build
npm run deploy          # all 4 stacks
```

First deploy takes ~15 min (RDS). Subsequent deploys of individual stacks are fast:

```bash
npm run deploy:ecs      # ~30s
npm run deploy:batch    # ~30s
npm run deploy:ec2      # ~2 min (replaces instance)
```

## EC2 Setup (one-time, after deploy)

```bash
# Get instance ID
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name AirflowEc2 \
  --query "Stacks[0].Outputs[?OutputKey=='Ec2InstanceId'].OutputValue" --output text)

# SSM into the instance
aws ssm start-session --target $INSTANCE_ID

# Switch to ec2-user
sudo su - ec2-user

# If /opt/airflow-scripts/ is empty (cloud-init didn't run UserData), run it manually:
sudo bash -c "$(curl -s http://169.254.169.254/latest/user-data)"

# Run the one-shot setup (~10 min: clone, install, build UI, config, DB, teams, DAGs, start)
bash /opt/airflow-scripts/setup-airflow.sh
```

## Access Airflow UI (from your Mac)

```bash
aws ssm start-session --target $INSTANCE_ID \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Open http://localhost:8080 — all users are admin.

## Day-to-day commands (on EC2)

The `af` command is available in every shell after setup:

```
af status          Check service health
af restart         Restart all services
af stop            Stop all services
af logs            Tail all logs
af logs scheduler  Tail specific service

af deploy-dags     Create test DAGs + upload to S3
af rebuild         Build + push worker image to ECR
af switch <branch> Switch branch, rebuild everything, restart

af db              Open psql to metadata DB
af db-reset        Drop DB, recreate, migrate, recreate teams
af ssm             Show all SSM params
af config          Show airflow.cfg
af teams           List teams
af dags            List DAGs
af ecs-tasks       List running ECS tasks
af batch-jobs      List running Batch jobs
af tunnel          Show SSM tunnel command
```

## Destroy

```bash
npm run destroy:ecs       # just ECS
npm run destroy:batch     # just Batch
npm run destroy:ec2       # just EC2
npm run destroy:compute   # all 3 compute stacks, keep infra
npm run destroy           # everything (correct order)
```

## Building worker images from source

Since we run from Airflow `main` (3.2.0dev), there are no published Docker images that match.
Worker images must be built from source using Breeze.

```bash
# On EC2 — builds prod image from your local checkout, layers on Amazon provider, pushes to ECR
af rebuild
```

This runs `rebuild-worker-image.sh` which:
1. Runs `breeze prod-image build --python 3.12` from `~/airflow`
2. Layers on `apache-airflow-providers-amazon`, `asyncpg`, `psycopg2-binary`
3. Tags and pushes to ECR

### Prerequisites (installed automatically by `setup-airflow.sh`)

| Tool | Why | Install |
|------|-----|---------|
| pnpm | Build simple auth manager UI assets | `npm install -g pnpm` |
| docker-compose v2 | Required by Breeze | See below |
| breeze | Builds prod/CI images from source | `uv tool install -e ~/airflow/dev/breeze` |

docker-compose v2 install (if not present):
```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

### Troubleshooting: `dag_bundle_config_list` validation error

```
AirflowConfigException: Invalid config for section `dag_processor` key `dag_bundle_config_list`.
Expected keys {'kwargs', 'classpath', 'name'} but found {'kwargs', 'team_name', 'classpath', 'name'}
```

This means the worker image is running an older Airflow version that doesn't support `team_name`
in bundle configs. The `team_name` field was added in `main` and isn't in any released version yet.

Fix: rebuild the worker image from source so it matches the scheduler:
```bash
af rebuild
```

Then force a new ECS deployment to pick up the new image:
```bash
aws ecs update-service --cluster alpha-cluster --service <service-name> --force-new-deployment
# or just re-trigger the DAG — ECS executor launches new tasks with the latest image
```

### Troubleshooting: Breeze `UI Vite manifest file does not exist`

Breeze requires all UI assets to be compiled before building the prod image.
The main UI is built during `setup-airflow.sh`, but the simple auth manager UI
may be missing.

```bash
cd ~/airflow/airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui
pnpm install
pnpm build
```

Then retry `breeze prod-image build --python 3.12`.

## Known quirks

### cloud-init may skip UserData

After `npm run deploy:ec2`, cloud-init may skip UserData on the replacement instance.
If `/opt/airflow-scripts/` is empty after SSM-ing in, run UserData manually:

```bash
sudo bash -c "$(curl -s http://169.254.169.254/latest/user-data)"
```

This is a one-time thing per instance replacement.
