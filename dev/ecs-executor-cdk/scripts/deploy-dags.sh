#!/bin/bash
# Create test DAGs on EC2 for multi-team ECS executor testing.
# Run on EC2 after setup-config.sh and create-teams.sh.
#
# Usage: bash /opt/airflow-scripts/deploy-dags.sh
set -e

DAG_ROOT=/home/ec2-user/airflow-dags
mkdir -p $DAG_ROOT/team_alpha $DAG_ROOT/team_beta $DAG_ROOT/shared

echo "Writing team_alpha DAG..."
cat > $DAG_ROOT/team_alpha/alpha_dag.py << 'PYEOF'
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

echo "Writing team_beta DAG..."
cat > $DAG_ROOT/team_beta/beta_dag.py << 'PYEOF'
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

echo "Writing shared DAG..."
cat > $DAG_ROOT/shared/shared_dag.py << 'PYEOF'
"""Shared DAG (no team) - should use the global LocalExecutor, not ECS."""
from __future__ import annotations
from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["multi-team", "shared", "ecs-test"])
def shared_simple_dag():
    @task
    def shared_hello():
        import socket
        print(f"Hello from shared DAG on host {socket.gethostname()} (should be EC2, not ECS)")
        return "shared_done"
    shared_hello()

shared_simple_dag()
PYEOF

echo ""
echo "DAGs deployed to $DAG_ROOT:"
find $DAG_ROOT -name '*.py' -type f
echo ""
echo "Make sure airflow.cfg has [dag_processor] dag_bundle_config_list"
echo "pointing to these paths with team_name set. Then restart Airflow."
