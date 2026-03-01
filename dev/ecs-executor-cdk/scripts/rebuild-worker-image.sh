#!/bin/bash
# Build and push the Airflow worker image to ECR.
# Uses the official apache/airflow:latest image (3.x) + amazon provider.
#
# Usage: bash /opt/airflow-scripts/rebuild-worker-image.sh
set -e

echo "Reading config from SSM..."
REGION=$(aws ssm get-parameter --name /airflow-test/region --query Parameter.Value --output text)
ECR_REPO=$(aws ssm get-parameter --name /airflow-test/ecr-repo --query Parameter.Value --output text)

echo "Logging into ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_REPO"

echo "Writing Dockerfile..."
cat > /tmp/Dockerfile.worker << 'DEOF'
FROM apache/airflow:latest
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon asyncpg psycopg2-binary
DEOF

echo "Building image..."
docker build -f /tmp/Dockerfile.worker -t "${ECR_REPO}:latest" /tmp

echo "Pushing ${ECR_REPO}:latest..."
docker push "${ECR_REPO}:latest"

echo ""
echo "Done. Image pushed as ${ECR_REPO}:latest"
