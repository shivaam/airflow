# Issue #62244 — Test Plan

## Overview

This plan verifies that replacing `session.expunge_all()` with
`session.expunge(conn)` in MetastoreBackend fixes the team-scoped DAG bundle
persistence bug without regressing any other system that depends on
MetastoreBackend for connection/variable lookups.

## Prerequisites

- EC2 instance running Airflow with S3DagBundles
- Multi-team mode enabled (`multi_team = True` in `[core]`)
- At least 2 teams created in the DB
- At least 2 S3DagBundles with `team_name` configured
- At least 1 S3DagBundle without `team_name` (shared)
- `aws_default` connection configured in the metastore DB
- At least 1 variable set in the metastore DB
- Logging level set to DEBUG for `airflow.secrets.metastore` to see
  MetastoreBackend activity

### Example airflow.cfg

```ini
[core]
multi_team = True

[logging]
logging_level = INFO
# Override just for metastore to see the debug logs:
# In your log_config.py or via env var:
# AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG (if you want everything)

[dag_processor]
dag_bundle_config_list = [
    {
        "name": "team_alpha_dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "your-bucket", "prefix": "team_alpha/"},
        "team_name": "team_alpha"
    },
    {
        "name": "team_beta_dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "your-bucket", "prefix": "team_beta/"},
        "team_name": "team_beta"
    },
    {
        "name": "shared_dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "your-bucket", "prefix": "shared/"}
    }
]
```

### Setup commands

```bash
# Create teams (if not already present)
airflow teams create team_alpha
airflow teams create team_beta

# Ensure aws_default connection exists
airflow connections get aws_default

# Set a test variable
airflow variables set test_var "hello_world"
airflow variables set test_encrypted_var "s3cr3t_value"
```

---

## Test 1: Team-scoped DAG bundles persist (the bug fix)

**What it tests**: The core bug — `sync_bundles_to_db` should persist all
bundles including team-scoped ones.

**Steps**:
1. Stop the dag-processor
2. Clear existing bundle records:
   ```sql
   -- Connect to your metadata DB
   DELETE FROM dag_bundle WHERE name IN ('team_alpha_dags', 'team_beta_dags', 'shared_dags');
   ```
3. Start the dag-processor
4. Wait 30 seconds for `sync_bundles_to_db` to run
5. Check the DB:
   ```bash
   airflow dags list-bundles
   ```

**Expected result (BEFORE fix)**:
- Only `shared_dags` appears (and possibly `example_dags`)
- `team_alpha_dags` and `team_beta_dags` are missing
- Dag-processor logs show "Bundle model not found" for team bundles

**Expected result (AFTER fix)**:
- All three bundles appear: `team_alpha_dags`, `team_beta_dags`, `shared_dags`
- No "Bundle model not found" errors in dag-processor logs
- Dag-processor logs show "Added new DAG bundle team_alpha_dags to the database"
  and same for team_beta_dags

---

## Test 2: Connection lookup via CLI

**What it tests**: `airflow connections get` goes through
`Connection.get_connection_from_secrets` → MetastoreBackend. Verifies the
returned Connection object has all fields intact after `expunge(conn)`.

**Steps**:
```bash
airflow connections get aws_default --output json
```

**Expected result**:
- Connection is returned with all fields populated
- `conn_id`, `conn_type`, `host`, `login`, `password`, `port`, `schema`,
  `extra`, `description` are all present and correct
- No errors or warnings in output

**What to watch for**:
- If `description` is `None` when it shouldn't be → the fix might be
  returning a reconstructed object instead of the ORM object (wrong approach)
- If you get `DetachedInstanceError` → the expunge is breaking something

---

## Test 3: Variable lookup via CLI

**What it tests**: `airflow variables get` goes through
`Variable.get_variable_from_secrets` → MetastoreBackend. Verifies
`get_variable` works correctly after replacing `expunge_all` with
`expunge(var_value)`.

**Steps**:
```bash
airflow variables get test_var
airflow variables get test_encrypted_var
airflow variables get nonexistent_var
```

**Expected result**:
- `test_var` returns `hello_world`
- `test_encrypted_var` returns `s3cr3t_value` (decrypted)
- `nonexistent_var` returns an error/empty (variable not found)

---

## Test 4: Variable set + get round-trip

**What it tests**: `Variable.set` calls `check_for_write_conflict` which
iterates secrets backends (skipping MetastoreBackend). Then `Variable.get`
reads it back through MetastoreBackend.

**Steps**:
```bash
airflow variables set roundtrip_test "value_123"
airflow variables get roundtrip_test
airflow variables set roundtrip_test "updated_456"
airflow variables get roundtrip_test
```

**Expected result**:
- First get returns `value_123`
- Second get returns `updated_456`
- No errors

---

## Test 5: DAG execution with hook (connection lookup at runtime)

**What it tests**: When a DAG task runs, the hook calls
`BaseHook.get_connection` → secrets chain → MetastoreBackend. This verifies
the connection is usable for actual AWS operations after being detached.

**Steps**:
1. Create a simple test DAG in one of your S3 bundles:

```python
# test_connection_dag.py
from airflow.sdk import DAG, task
from datetime import datetime

with DAG(
    "test_connection_lookup",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def test_s3_connection():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id="aws_default")
        # Just verify the hook initializes and can talk to AWS
        region = hook.conn_config.region_name
        print(f"Connected to AWS region: {region}")
        return region

    test_s3_connection()
```

2. Upload to your S3 bucket (shared or team prefix)
3. Wait for dag-processor to pick it up
4. Trigger the DAG manually from the UI or CLI:
   ```bash
   airflow dags trigger test_connection_lookup
   ```
5. Check the task log

**Expected result**:
- Task succeeds
- Log shows the AWS region
- No `DetachedInstanceError` or session-related errors

---

## Test 6: Dag-processor bundle refresh with S3 connections

**What it tests**: During periodic bundle refresh, S3DagBundle calls
`S3Hook.sync_to_local_dir()` which needs the `aws_default` connection.
This is a different code path from `view_url_template()` but still goes
through MetastoreBackend.

**Steps**:
1. Add a new DAG file to one of the S3 bundle prefixes
2. Wait for the dag-processor to refresh (check `dag_bundle.last_refreshed`)
3. Verify the new DAG appears in `airflow dags list`

**Expected result**:
- New DAG appears after refresh
- No errors in dag-processor logs related to connection lookup or sessions

---

## Test 7: Multiple concurrent connection lookups (scheduler)

**What it tests**: The scheduler may look up connections for email alerts
or other purposes while also doing session work. This verifies no session
interference under concurrent access.

**Steps**:
1. Configure email alerts on a DAG (set `email_on_failure = True` with
   an SMTP connection)
2. Create a DAG that intentionally fails
3. Trigger it and let it fail
4. Check if the email alert fires

**Expected result**:
- Email alert is sent (if SMTP is configured)
- Or at minimum: no session-related errors in scheduler logs during the
  email sending attempt

---

## Test 8: Execution API connection lookup

**What it tests**: The Execution API serves connections to workers via
`/execution/connections/{connection_id}`. This goes through
`Connection.get_connection_from_secrets` → MetastoreBackend.

**Steps**:
1. Run a DAG task that accesses a connection (same as Test 5)
2. Check the API server logs for the connection lookup

**Expected result**:
- Task gets the connection successfully
- API server logs show no errors
- If DEBUG logging is enabled, you should see:
  ```
  MetastoreBackend retrieving connection: aws_default
  MetastoreBackend found and detached connection: aws_default
  ```

---

## Test 9: Verify no session corruption (the key regression check)

**What it tests**: The whole point of the fix — MetastoreBackend should not
corrupt the shared scoped session.

**Steps**:
1. With all 3 S3 bundles configured (2 team + 1 shared), restart the
   dag-processor
2. Monitor the dag-processor logs for 5 minutes
3. Check the `dag_bundle` table

**Expected result**:
- All 3 bundles have rows in `dag_bundle` table with `active = True`
- Team bundles have correct team associations in `dag_bundle_team` table
- No "Bundle model not found" messages in logs
- No repeated "Added new DAG bundle" messages (which would indicate bundles
  keep getting re-added because they weren't persisted)

**Verification SQL**:
```sql
SELECT b.name, b.active, b.version, b.last_refreshed, t.team_name
FROM dag_bundle b
LEFT JOIN dag_bundle_team t ON b.name = t.dag_bundle_name
ORDER BY b.name;
```

Expected output:
```
name              | active | version | last_refreshed      | team_name
------------------+--------+---------+---------------------+-----------
example_dags      | true   | ...     | ...                 | NULL
shared_dags       | true   | ...     | ...                 | NULL
team_alpha_dags   | true   | ...     | ...                 | team_alpha
team_beta_dags    | true   | ...     | ...                 | team_beta
```

---

## Test 10: Existing unit tests

**What it tests**: All existing MetastoreBackend and secrets tests still pass.

**Steps**:
```bash
# Bug reproduction tests (should all PASS now)
.venv/bin/python -m pytest dev/test_expunge_all_bug.py -xvs

# Existing MetastoreBackend tests
.venv/bin/python -m pytest airflow-core/tests/unit/always/test_secrets_backends.py -xvs

# Secrets routing tests
.venv/bin/python -m pytest airflow-core/tests/unit/always/test_secrets.py -xvs

# Connection model tests (mock MetastoreBackend)
.venv/bin/python -m pytest airflow-core/tests/unit/models/test_connection.py -xvs

# Variable model tests (mock MetastoreBackend)
.venv/bin/python -m pytest airflow-core/tests/unit/models/test_variable.py -xvs
```

**Expected result**: All tests pass.

---

## Smoke test checklist

Quick checklist for a fast verification pass:

- [ ] `airflow connections get aws_default` returns correct data
- [ ] `airflow variables get test_var` returns correct value
- [ ] `airflow dags list-bundles` shows all bundles (team + shared)
- [ ] DAG from team bundle appears in `airflow dags list`
- [ ] Trigger a DAG that uses S3Hook — task succeeds
- [ ] No `DetachedInstanceError` in any logs
- [ ] No "Bundle model not found" in dag-processor logs
- [ ] No repeated "Added new DAG bundle" messages after initial sync

---

## Files changed

| File | Change |
|---|---|
| `airflow-core/src/airflow/secrets/metastore.py` | `expunge_all()` → `expunge(conn)` / `expunge(var_value)`, removed debug logging, added clean debug logs |
| `airflow-core/src/airflow/dag_processing/bundles/manager.py` | Removed `[DEBUG-SYNC]` investigation logging |
| `dev/test_expunge_all_bug.py` | Bug reproduction tests (3 tests) |
