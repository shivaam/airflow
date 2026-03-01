#!/bin/bash
# Create teams in the Airflow metadata DB.
# Run after airflow db migrate, before starting the scheduler.
# Usage: bash /opt/airflow-scripts/create-teams.sh
set -e

source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home

echo "Creating teams..."
# Use LocalExecutor override to avoid chicken-and-egg validation
# (scheduler config references teams that may not exist yet)
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_alpha 2>/dev/null || echo "  team_alpha already exists"
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_beta 2>/dev/null || echo "  team_beta already exists"

echo ""
echo "Teams:"
AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams list
