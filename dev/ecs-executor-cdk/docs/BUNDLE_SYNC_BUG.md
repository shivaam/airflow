# Bug: DAG Bundle Models Not Persisted for Team-Scoped Bundles

## Summary

When using `dag_bundle_config_list` with `team_name` in multi-team mode, the dag-processor's
`sync_bundles_to_db` method logs "Added new DAG bundle" for team-scoped bundles but the records
are not persisted to the `dag_bundle` table. Non-team bundles (like `shared_dags` and `example_dags`)
persist correctly. The dag-processor then continuously logs "Bundle model not found" for the
team bundles, and their DAGs are never processed.

## Environment

- Airflow version: 3.2.0 (main branch, commit from 2026-03-01)
- Python: 3.12
- Database: PostgreSQL 16 (RDS)
- OS: Amazon Linux 2023 (EC2)
- Multi-team: enabled (`core.multi_team = True`)
- Teams: `team_alpha`, `team_beta` (created via `airflow teams create`)

## Configuration

```ini
[core]
executor = LocalExecutor;team_alpha=airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor;team_beta=airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor
multi_team = True

[dag_processor]
dag_bundle_config_list = [
  {"name": "team_alpha_dags", "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle", "kwargs": {"bucket_name": "airflow-ecs-dags-ACCOUNT-REGION", "prefix": "team_alpha"}, "team_name": "team_alpha"},
  {"name": "team_beta_dags", "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle", "kwargs": {"bucket_name": "airflow-ecs-dags-ACCOUNT-REGION", "prefix": "team_beta"}, "team_name": "team_beta"},
  {"name": "shared_dags", "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle", "kwargs": {"bucket_name": "airflow-ecs-dags-ACCOUNT-REGION", "prefix": "shared"}}
]
```

## Steps to Reproduce

1. Deploy Airflow with multi-team enabled and team-scoped S3 DAG bundles
2. Create teams: `airflow teams create team_alpha` and `airflow teams create team_beta`
3. Run `airflow db migrate`
4. Start dag-processor: `airflow dag-processor`
5. Check the `dag_bundle` table

## Expected Behavior

All four bundles (`team_alpha_dags`, `team_beta_dags`, `shared_dags`, `example_dags`) should
have rows in the `dag_bundle` table, and the `dag_bundle_team` association table should link
the team bundles to their respective teams.

## Actual Behavior

Only `shared_dags` and `example_dags` are in the `dag_bundle` table. `team_alpha_dags` and
`team_beta_dags` are missing despite being logged as "Added".

### Dag-processor logs (startup)

```
2026-03-02T00:26:05.798Z [info] DAG bundles loaded: team_alpha_dags, team_beta_dags, shared_dags, example_dags
2026-03-02T00:26:05.849Z [info] Added new DAG bundle team_alpha_dags to the database
2026-03-02T00:26:05.867Z [warning] Unable to find AWS Connection ID 'aws_default', switching to empty.
2026-03-02T00:26:05.868Z [info] Added new DAG bundle team_beta_dags to the database
2026-03-02T00:26:05.893Z [warning] Unable to find AWS Connection ID 'aws_default', switching to empty.
2026-03-02T00:26:05.893Z [warning] Removing ownership of team 'None' from Dag bundle 'shared_dags'
2026-03-02T00:26:05.893Z [warning] Removing ownership of team 'None' from Dag bundle 'example_dags'
2026-03-02T00:26:05.894Z [info] DAG bundles loaded: team_alpha_dags, team_beta_dags, shared_dags, example_dags
2026-03-02T00:26:05.895Z [info] Checking for new files in bundle team_alpha_dags every 300 seconds
2026-03-02T00:26:05.895Z [info] Checking for new files in bundle team_beta_dags every 300 seconds
```

### Dag-processor logs (refresh loop, repeating every 5s)

```
2026-03-02T00:26:07.017Z [warning] Bundle model not found for team_alpha_dags
2026-03-02T00:26:07.764Z [warning] Bundle model not found for team_beta_dags
2026-03-02T00:26:10.934Z [warning] Bundle model not found for team_alpha_dags
2026-03-02T00:26:10.937Z [warning] Bundle model not found for team_beta_dags
... (continues indefinitely)
```

### Database state after startup

```sql
airflow_db=> SELECT * FROM dag_bundle;
     name     | active | version |        last_refreshed         | signed_url_template | template_params
--------------+--------+---------+-------------------------------+---------------------+-----------------
 shared_dags  | t      |         | 2026-03-02 00:31:09.535605+00 | ...                 | {}
 example_dags | t      |         | 2026-03-02 00:31:09.535605+00 |                     | {}
(2 rows)

airflow_db=> SELECT * FROM dag_bundle_team;
(0 rows)

airflow_db=> SELECT * FROM team;
    name
------------
 team_alpha
 team_beta
(2 rows)
```

## Root Cause Analysis

The `sync_bundles_to_db` method in `airflow/dag_processing/bundles/manager.py` runs twice
at startup — once in the dag-processor parent process and once in the forked child. Key observations:

1. The first run adds all 4 bundles via `session.add()` and logs "Added new DAG bundle" for each
2. The method uses `@provide_session` which should auto-commit at the end
3. The second run (same second, `00:26:05.894`) sees only `shared_dags` and `example_dags` in the DB
4. This means the first run's transaction only partially committed — the team bundles were rolled back

The likely cause is that `bundle.teams = [team]` (line ~271 in manager.py) triggers a SQLAlchemy
relationship flush that fails or conflicts when the second sync runs concurrently. The non-team
bundles don't have this relationship assignment, so they commit successfully.

Relevant code path: `airflow/dag_processing/bundles/manager.py`, method `sync_bundles_to_db`,
around lines 233-290.

## Workaround

Manually insert the bundle records after startup:

```sql
INSERT INTO dag_bundle (name, active) VALUES ('team_alpha_dags', true) ON CONFLICT (name) DO UPDATE SET active = true;
INSERT INTO dag_bundle (name, active) VALUES ('team_beta_dags', true) ON CONFLICT (name) DO UPDATE SET active = true;
INSERT INTO dag_bundle_team (bundle_name, team_name) VALUES ('team_alpha_dags', 'team_alpha') ON CONFLICT DO NOTHING;
INSERT INTO dag_bundle_team (bundle_name, team_name) VALUES ('team_beta_dags', 'team_beta') ON CONFLICT DO NOTHING;
```

Then restart the dag-processor. The bundles will be found and DAGs will be processed.

## Impact

Team-scoped DAG bundles are completely non-functional without the manual workaround. The
dag-processor never processes DAGs from team bundles, so they never appear in the UI or CLI.
Non-team bundles work fine.
