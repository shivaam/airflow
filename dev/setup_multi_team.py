"""
Setup script for multi-team ECS executor testing.

Creates teams in the Airflow metadata DB and ensures DAG directories exist.
Idempotent — safe to run multiple times.

Usage (inside Breeze, after `airflow db migrate`):
    python /opt/airflow/dev/setup_multi_team.py
"""

from __future__ import annotations

import os
import sys

from sqlalchemy import select

from airflow.configuration import conf
from airflow.models.team import Team
from airflow.settings import Session

TEAMS = ["team_alpha", "team_beta"]

DAG_DIRS = [
    "/opt/airflow/dags/team_alpha",
    "/opt/airflow/dags/team_beta",
    "/opt/airflow/dags/shared",
]


def create_teams(session):
    """Create teams if they don't already exist."""
    existing = set(session.scalars(select(Team.name)).all())
    for name in TEAMS:
        if name in existing:
            print(f"  Team '{name}' already exists, skipping.")
        else:
            session.add(Team(name=name))
            print(f"  Created team '{name}'.")
    session.commit()


def ensure_dag_dirs():
    """Create DAG directories if they don't exist."""
    for path in DAG_DIRS:
        os.makedirs(path, exist_ok=True)
        print(f"  DAG dir ready: {path}")


def verify_config():
    """Print current multi-team config for verification."""
    multi_team = conf.getboolean("core", "multi_team", fallback=False)
    executor = conf.get("core", "executor", fallback="?")
    print(f"  multi_team = {multi_team}")
    print(f"  executor   = {executor}")
    if not multi_team:
        print("  WARNING: multi_team is not enabled! Set AIRFLOW__CORE__MULTI_TEAM=True")


def main():
    print("\n=== Multi-Team ECS Executor Setup ===\n")

    print("[1/3] Verifying config...")
    verify_config()

    print("\n[2/3] Creating teams...")
    session = Session()
    try:
        create_teams(session)
    finally:
        session.close()

    print("\n[3/3] Ensuring DAG directories...")
    ensure_dag_dirs()

    print("\n=== Setup complete! ===")
    print("Next steps:")
    print("  1. Verify: airflow teams list")
    print("  2. Start airflow: airflow api-server / scheduler / etc.")
    print("  3. Run config test: python /opt/airflow/dev/test_ecs_multi_team_config.py")


if __name__ == "__main__":
    main()
