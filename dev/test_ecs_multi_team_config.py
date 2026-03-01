"""
Multi-team ECS Executor config isolation test.

Verifies that two teams using the same AwsEcsExecutor get independent
configurations — different clusters, subnets, regions, task definitions.
Uses mocking (no real AWS calls).

Usage (inside Breeze, after setup_multi_team.py):
    python /opt/airflow/dev/test_ecs_multi_team_config.py
"""

from __future__ import annotations

import json
import sys
from unittest import mock

from airflow.providers.amazon.aws.executors.ecs.ecs_executor import AwsEcsExecutor


def create_executor(team_name: str) -> AwsEcsExecutor:
    """Create an ECS executor for a team, mocking the boto3 ECS client."""
    with mock.patch(
        "airflow.providers.amazon.aws.executors.ecs.ecs_executor.EcsHook"
    ) as mock_hook:
        mock_hook.return_value.conn = mock.MagicMock()
        executor = AwsEcsExecutor(team_name=team_name)
    return executor


def test_config_isolation():
    """Verify each team's executor reads its own config."""
    print("\n=== Test: Config Isolation ===\n")
    failures = []

    alpha = create_executor("team_alpha")
    beta = create_executor("team_beta")

    # --- Cluster ---
    print(f"  team_alpha cluster: {alpha.cluster}")
    print(f"  team_beta  cluster: {beta.cluster}")
    if alpha.cluster != "alpha-cluster":
        failures.append(f"alpha cluster expected 'alpha-cluster', got '{alpha.cluster}'")
    if beta.cluster != "beta-cluster":
        failures.append(f"beta cluster expected 'beta-cluster', got '{beta.cluster}'")
    if alpha.cluster == beta.cluster:
        failures.append("CROSS-CONTAMINATION: both teams have the same cluster!")

    # --- Container name ---
    print(f"  team_alpha container: {alpha.container_name}")
    print(f"  team_beta  container: {beta.container_name}")
    if alpha.container_name != "airflow-worker":
        failures.append(f"alpha container expected 'airflow-worker', got '{alpha.container_name}'")
    if beta.container_name != "airflow-worker":
        failures.append(f"beta container expected 'airflow-worker', got '{beta.container_name}'")

    # --- run_task_kwargs ---
    alpha_kwargs = alpha.run_task_kwargs
    beta_kwargs = beta.run_task_kwargs

    print(f"\n  team_alpha run_task_kwargs:\n    {json.dumps(alpha_kwargs, indent=4, default=str)}")
    print(f"\n  team_beta  run_task_kwargs:\n    {json.dumps(beta_kwargs, indent=4, default=str)}")

    # Check cluster in kwargs
    if alpha_kwargs.get("cluster") != "alpha-cluster":
        failures.append(f"alpha kwargs cluster: {alpha_kwargs.get('cluster')}")
    if beta_kwargs.get("cluster") != "beta-cluster":
        failures.append(f"beta kwargs cluster: {beta_kwargs.get('cluster')}")

    # Check task definition
    if alpha_kwargs.get("taskDefinition") != "alpha-task-def:3":
        failures.append(f"alpha taskDefinition: {alpha_kwargs.get('taskDefinition')}")
    if beta_kwargs.get("taskDefinition") != "beta-task-def:1":
        failures.append(f"beta taskDefinition: {beta_kwargs.get('taskDefinition')}")

    # Check subnets
    alpha_subnets = (
        alpha_kwargs.get("networkConfiguration", {})
        .get("awsvpcConfiguration", {})
        .get("subnets", [])
    )
    beta_subnets = (
        beta_kwargs.get("networkConfiguration", {})
        .get("awsvpcConfiguration", {})
        .get("subnets", [])
    )
    print(f"\n  team_alpha subnets: {alpha_subnets}")
    print(f"  team_beta  subnets: {beta_subnets}")
    if alpha_subnets != ["subnet-alpha1", "subnet-alpha2"]:
        failures.append(f"alpha subnets: {alpha_subnets}")
    if beta_subnets != ["subnet-beta1"]:
        failures.append(f"beta subnets: {beta_subnets}")

    # Check security groups
    alpha_sgs = (
        alpha_kwargs.get("networkConfiguration", {})
        .get("awsvpcConfiguration", {})
        .get("securityGroups", [])
    )
    beta_sgs = (
        beta_kwargs.get("networkConfiguration", {})
        .get("awsvpcConfiguration", {})
        .get("securityGroups", [])
    )
    print(f"  team_alpha security groups: {alpha_sgs}")
    print(f"  team_beta  security groups: {beta_sgs}")
    if alpha_sgs != ["sg-alpha"]:
        failures.append(f"alpha security groups: {alpha_sgs}")
    if beta_sgs != ["sg-beta"]:
        failures.append(f"beta security groups: {beta_sgs}")

    # Cross-contamination check
    alpha_json = json.dumps(alpha_kwargs, default=str)
    beta_json = json.dumps(beta_kwargs, default=str)
    if "beta-cluster" in alpha_json or "subnet-beta" in alpha_json:
        failures.append("CROSS-CONTAMINATION: beta values found in alpha kwargs!")
    if "alpha-cluster" in beta_json or "subnet-alpha" in beta_json:
        failures.append("CROSS-CONTAMINATION: alpha values found in beta kwargs!")

    return failures


def test_run_task_call_args():
    """Verify that run_task is called with the correct team-specific kwargs."""
    print("\n=== Test: run_task Call Args ===\n")
    failures = []

    for team, expected_cluster, expected_task_def in [
        ("team_alpha", "alpha-cluster", "alpha-task-def:3"),
        ("team_beta", "beta-cluster", "beta-task-def:1"),
    ]:
        executor = create_executor(team)
        executor.IS_BOTO_CONNECTION_HEALTHY = True

        # Mock a task key and command
        mock_key = mock.MagicMock()
        mock_key.__iter__ = mock.MagicMock(return_value=iter(["dag", "task", "run", "try", 1]))
        mock_cmd = ["python", "-m", "airflow.sdk.execution_time.execute_workload", "--json-string", "{}"]

        # Call _run_task_kwargs to get what would be sent to ECS
        kwargs = executor._run_task_kwargs(mock_key, mock_cmd, "default", {})

        print(f"  {team} -> cluster={kwargs.get('cluster')}, taskDef={kwargs.get('taskDefinition')}")

        if kwargs.get("cluster") != expected_cluster:
            failures.append(f"{team} run_task cluster: {kwargs.get('cluster')}")
        if kwargs.get("taskDefinition") != expected_task_def:
            failures.append(f"{team} run_task taskDefinition: {kwargs.get('taskDefinition')}")

    return failures


def main():
    print("=" * 60)
    print("Multi-Team ECS Executor Config Isolation Test")
    print("=" * 60)

    all_failures = []
    all_failures.extend(test_config_isolation())
    all_failures.extend(test_run_task_call_args())

    print("\n" + "=" * 60)
    if all_failures:
        print(f"FAILED — {len(all_failures)} issue(s):")
        for f in all_failures:
            print(f"  ✗ {f}")
        sys.exit(1)
    else:
        print("ALL PASSED — config isolation verified!")
        sys.exit(0)


if __name__ == "__main__":
    main()
