#!/bin/bash
# Create test DAGs and upload to S3.
# Usage: bash /opt/airflow-scripts/deploy-dags.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

log_step "Creating test DAGs"
mkdir -p "${DAG_STAGING_DIR}"/{team_alpha,team_beta,shared}

cat > "${DAG_STAGING_DIR}/team_alpha/alpha_dag.py" << 'PYEOF'
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

cat > "${DAG_STAGING_DIR}/team_beta/beta_dag.py" << 'PYEOF'
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

cat > "${DAG_STAGING_DIR}/shared/shared_dag.py" << 'PYEOF'
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

log_step "Uploading to S3"
aws s3 sync "${DAG_STAGING_DIR}/" "s3://${DAG_BUCKET}/" --delete
log_info "DAGs uploaded to s3://${DAG_BUCKET}/"
log_info "Dag-processor will pick them up on next refresh cycle."
