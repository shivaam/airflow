# Airflow ECS/Batch Executor Test Infrastructure

Two-stack CDK deployment for testing Airflow ECS and Batch executors on AWS.

- **AirflowInfra** — VPC, RDS, S3, ECR, NLB, IAM, security groups (deploy once)
- **AirflowCompute** — EC2, ECS clusters, Batch, NLB target (update frequently)

## Prerequisites

- AWS CLI configured with credentials (`aws sts get-caller-identity` should work)
- Node.js 18+
- CDK bootstrapped in your account/region:
  ```bash
  cd dev/ecs-executor-cdk
  npx cdk bootstrap
  ```

## Deploy

```bash
cd dev/ecs-executor-cdk
npm install
npm run build
npx cdk deploy --all --require-approval never
```

First deploy takes ~15 min (RDS is slow). Subsequent compute-only updates take ~2 min:

```bash
npx cdk deploy AirflowCompute --require-approval never
```

## Access EC2 (shell)

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name AirflowCompute \
  --query "Stacks[0].Outputs[?OutputKey=='Ec2InstanceId'].OutputValue" --output text)

aws ssm start-session --target $INSTANCE_ID
```

## Access Airflow UI (port forwarding)

```bash
aws ssm start-session \
  --target $INSTANCE_ID \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Then open http://localhost:8080 in your browser.

## First-time setup on EC2

After SSM-ing into the instance:

```bash
# Switch to ec2-user (SSM drops you in as ssm-user)
sudo su - ec2-user

# Read all config from SSM
REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
DB_ENDPOINT=$(aws ssm get-parameter --name /airflow-test/db-endpoint --query Parameter.Value --output text)
DB_SECRET_ARN=$(aws ssm get-parameter --name /airflow-test/db-secret-arn --query Parameter.Value --output text)
DB_NAME=$(aws ssm get-parameter --name /airflow-test/db-name --query Parameter.Value --output text)
ECR_REPO=$(aws ssm get-parameter --name /airflow-test/ecr-repo --query Parameter.Value --output text)
LOG_BUCKET=$(aws ssm get-parameter --name /airflow-test/log-bucket --query Parameter.Value --output text)
NLB_DNS=$(aws ssm get-parameter --name /airflow-test/nlb-dns --query Parameter.Value --output text)

# Get DB password from Secrets Manager
DB_SECRET=$(aws secretsmanager get-secret-value --secret-id $DB_SECRET_ARN --query SecretString --output text)
DB_USER=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
DB_PASS=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")

# Clone airflow repo
git clone https://github.com/apache/airflow.git /home/ec2-user/airflow
cd /home/ec2-user/airflow

# Install Airflow (using uv for speed)
uv venv /home/ec2-user/airflow-venv --python 3.12
source /home/ec2-user/airflow-venv/bin/activate
uv pip install ./airflow-core ./task-sdk ./providers/amazon

# Configure Airflow
export AIRFLOW_HOME=/home/ec2-user/airflow-home
mkdir -p $AIRFLOW_HOME

cat > $AIRFLOW_HOME/airflow.cfg << EOF
[database]
sql_alchemy_conn = postgresql+psycopg2://${DB_USER}:${DB_PASS}@${DB_ENDPOINT}:5432/${DB_NAME}

[core]
executor = airflow.providers.amazon.executors.ecs.ecs_executor.AwsEcsExecutor
execution_api_server_url = http://${NLB_DNS}:8080/execution/

[logging]
remote_logging = True
remote_base_log_folder = s3://${LOG_BUCKET}/logs
remote_log_conn_id = aws_default
EOF

# Initialize DB and create admin user
airflow db migrate
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Start services (in separate terminals or use tmux/screen)
airflow api-server --port 8080 &
airflow scheduler &
airflow dag-processor &
```

## Build and push worker image

```bash
cd /home/ec2-user/airflow
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO=$(aws ssm get-parameter --name /airflow-test/ecr-repo --query Parameter.Value --output text)
REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
GIT_SHA=$(git rev-parse --short HEAD)

# Login to ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_REPO

# Build worker image
cat > /tmp/Dockerfile.worker << 'EOF'
FROM python:3.12-slim
COPY . /opt/airflow-src/
RUN pip install --no-cache-dir \
    /opt/airflow-src/airflow-core \
    /opt/airflow-src/task-sdk \
    /opt/airflow-src/providers/amazon
EOF

docker build -f /tmp/Dockerfile.worker -t ${ECR_REPO}:${GIT_SHA} -t ${ECR_REPO}:latest .
docker push ${ECR_REPO}:${GIT_SHA}
docker push ${ECR_REPO}:latest
```

## Switch branches

```bash
cd /home/ec2-user/airflow

# Stop services
pkill -f "airflow api-server" || true
pkill -f "airflow scheduler" || true
pkill -f "airflow dag-processor" || true

# Switch branch and reinstall
git fetch && git checkout <branch>
source /home/ec2-user/airflow-venv/bin/activate
uv pip install ./airflow-core ./task-sdk ./providers/amazon

# Migrate DB (if schema changed)
airflow db migrate

# Rebuild and push worker image (see above)

# Restart services
airflow api-server --port 8080 &
airflow scheduler &
airflow dag-processor &
```

## Fresh DB reset

```bash
source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home

# Drop and recreate
DB_ENDPOINT=$(aws ssm get-parameter --name /airflow-test/db-endpoint --query Parameter.Value --output text)
DB_SECRET_ARN=$(aws ssm get-parameter --name /airflow-test/db-secret-arn --query Parameter.Value --output text)
DB_SECRET=$(aws secretsmanager get-secret-value --secret-id $DB_SECRET_ARN --query SecretString --output text)
DB_PASS=$(echo $DB_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")

PGPASSWORD=$DB_PASS psql -h $DB_ENDPOINT -U postgres -c "DROP DATABASE IF EXISTS airflow_db;"
PGPASSWORD=$DB_PASS psql -h $DB_ENDPOINT -U postgres -c "CREATE DATABASE airflow_db;"

airflow db migrate
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

## Teardown

```bash
cd dev/ecs-executor-cdk

# Destroy compute first, then infra
npx cdk destroy --all --force
```

## Stack architecture

```
AirflowInfra (deploy once, keep running)
  ├── VPC + 4 security groups
  ├── RDS PostgreSQL (single-AZ)
  ├── S3 log bucket
  ├── ECR repository
  ├── IAM roles (ec2, ecsExec, task)
  ├── Internal NLB (stable DNS)
  └── SSM params: db, ecr, nlb, s3, vpc

AirflowCompute (update frequently, ~2 min)
  ├── EC2 instance (API server + scheduler)
  ├── NLB target group + listener
  ├── ECS clusters (alpha, beta) + task defs
  ├── Batch compute env + job queue + job def
  └── SSM params: clusters, task defs, job queue
```

See [DESIGN.md](DESIGN.md) for full architecture details.
