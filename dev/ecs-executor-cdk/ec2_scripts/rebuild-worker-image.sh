#!/bin/bash
# Build and push the Airflow worker image to ECR.
# Builds full prod image from source using Breeze, then layers on Amazon provider.
# Usage: bash /opt/airflow-scripts/rebuild-worker-image.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

DOCKER_CMD="docker"
if ! docker info > /dev/null 2>&1; then
    DOCKER_CMD="sudo docker"
fi

BREEZE_TAG="ghcr.io/apache/airflow/main/prod/python3.12"
FINAL_TAG="${ECR_REPO}:latest"

log_step "Building prod image from source via Breeze"
cd "${AIRFLOW_SRC}"
breeze prod-image build --python 3.12

log_step "Adding Amazon provider + DB drivers"
cat > /tmp/Dockerfile.worker << DEOF
FROM ${BREEZE_TAG}
USER airflow
COPY providers/amazon /opt/airflow/providers/amazon
RUN pip install --no-cache-dir \
    /opt/airflow/providers/amazon \
    asyncpg \
    psycopg2-binary
DEOF

$DOCKER_CMD build -f /tmp/Dockerfile.worker -t "${FINAL_TAG}" "${AIRFLOW_SRC}"

log_step "Logging into ECR"
aws ecr get-login-password --region "${REGION}" | \
    $DOCKER_CMD login --username AWS --password-stdin "${ECR_REPO}"

log_step "Pushing to ECR"
$DOCKER_CMD push "${FINAL_TAG}"

log_info "Done. Image pushed as ${FINAL_TAG}"
