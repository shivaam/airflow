#!/bin/bash
# Build and push the Airflow worker image to ECR.
# Uses apache/airflow:latest + amazon provider. No source build needed.
# Usage: bash /opt/airflow-scripts/rebuild-worker-image.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

log_step "Logging into ECR"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_REPO}"

log_step "Building worker image"
cat > /tmp/Dockerfile.worker << 'DEOF'
FROM apache/airflow:latest
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon asyncpg psycopg2-binary
DEOF

docker build -f /tmp/Dockerfile.worker -t "${ECR_REPO}:latest" /tmp

log_step "Pushing to ECR"
docker push "${ECR_REPO}:latest"

log_info "Done. Image pushed as ${ECR_REPO}:latest"
