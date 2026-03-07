# Issue #62244 — Fix Verification Results

## Summary

Replaced `session.expunge_all()` with `session.expunge(conn)` /
`session.expunge(var_value)` in `MetastoreBackend.get_connection()` and
`MetastoreBackend.get_variable()`. This fixes the root cause of team-scoped
DAG bundles not persisting to the database when S3DagBundles are configured.

The fix was verified with unit tests locally and end-to-end on an EC2 instance
running Airflow with multi-team mode, S3DagBundles, and a PostgreSQL metadata
database.

---

## Root Cause Recap

`MetastoreBackend.get_connection()` called `session.expunge_all()` which
removed ALL objects from the shared scoped session — including pending
`DagBundleModel` objects that `sync_bundles_to_db` had added via
`session.add()` but not yet committed. This caused team-scoped bundles to
silently disappear before the commit.

## Fix Applied

Two files changed:

### `airflow-core/src/airflow/secrets/metastore.py`

- `get_connection`: replaced `session.expunge_all()` with
  `if conn: session.expunge(conn)` — only detaches the queried Connection
- `get_variable`: replaced `session.expunge_all()` with
  `if var_value: session.expunge(var_value)` — only detaches the queried
  Variable
- Removed `[DEBUG-METASTORE]` investigation logging
- Added `[METASTORE]` observability logging at INFO level showing caller
  chain, session state before/after, and whether pending objects were preserved

### `airflow-core/src/airflow/dag_processing/bundles/manager.py`

- Removed all `[DEBUG-SYNC]` investigation logging from `sync_bundles_to_db`
- Removed unused `from sqlalchemy import inspect as sa_inspect` import
- No logic changes — only logging cleanup

---

## Unit Test Results (Local)

### Bug reproduction tests (`dev/test_expunge_all_bug.py`)

Three tests that directly prove the bug and verify the fix:

| Test | What it checks | Result |
|---|---|---|
| `test_get_connection_expunges_pending_objects` | Pending object in `session.new` survives `get_connection` call on same session | PASS |
| `test_get_variable_expunges_pending_objects` | Pending object in `session.new` survives `get_variable` call on same session | PASS |
| `test_get_connection_preserves_persistent_objects` | Identity map objects survive `get_connection` call | PASS |

Before the fix, the first two tests FAILED with:
```
AssertionError: BUG: expunge_all() removed the pending object from session.new!
assert pending_bundle_conn in IdentitySet([])
```

### Existing MetastoreBackend tests (`test_secrets_backends.py`)

| Test | Result |
|---|---|
| `test_build_path[default]` | PASS |
| `test_build_path[with_sep]` | PASS |
| `test_connection_env_secrets_backend` | PASS |
| `test_connection_metastore_secrets_backend` | PASS |
| `test_variable_env_secrets_backend` | PASS |
| `test_variable_metastore_secrets_backend` | PASS |

All 6 existing tests pass. No regressions.

---

## End-to-End Test Results (EC2)

### Infrastructure

- EC2 instance in us-west-2
- PostgreSQL 16.10 on RDS (`airflowinfra-db...rds.amazonaws.com`)
- Airflow installed from source (branch with fix)
- Python 3.12, venv at `/home/ec2-user/airflow-venv`

### Airflow Configuration

```ini
[core]
multi_team = True
executor = LocalExecutor;team_alpha=...AwsEcsExecutor;team_beta=...AwsEcsExecutor

[dag_processor]
dag_bundle_config_list = [
    {
        "name": "team_alpha_dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "<dag-bucket>", "prefix": "team_alpha"},
        "team_name": "team_alpha"
    },
    {
        "name": "team_beta_dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "<dag-bucket>", "prefix": "team_beta"},
        "team_name": "team_beta"
    },
    {
        "name": "shared_dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "<dag-bucket>", "prefix": "shared"}
    }
]

[logging]
remote_logging = True
remote_base_log_folder = s3://<log-bucket>/logs
remote_log_conn_id = aws_default
```

### Setup Steps

1. `airflow-ctl.sh db-reset` — dropped and recreated database, ran migrations,
   created teams (`team_alpha`, `team_beta`)
2. `airflow connections add aws_default --conn-type aws` — created the AWS
   connection needed by S3DagBundle
3. `airflow-ctl.sh start` — started api-server, scheduler, dag-processor

### Test 1: Team-scoped DAG bundles persist ✅

**Before fix**: Only `shared_dags` and `example_dags` persisted. Team bundles
were silently dropped by `expunge_all()`.

**After fix**: All 4 bundles persisted:

```sql
airflow_db=> select * from dag_bundle;
      name       | active | version |        last_refreshed         | signed_url_template | template_params
-----------------+--------+---------+-------------------------------+---------------------+-----------------
 team_alpha_dags | t      |         | 2026-03-07 22:05:55.062735+00 | .eJxFzE...          | {}
 team_beta_dags  | t      |         | 2026-03-07 22:05:55.062735+00 | .eJw9zN...          | {}
 shared_dags     | t      |         | 2026-03-07 22:05:55.062735+00 | .eJwlzE...          | {}
 example_dags    | t      |         | 2026-03-07 22:05:55.062735+00 |                     | {}
```

### Test 2: Session state preserved during bundle sync ✅

Dag-processor logs show pending objects survived MetastoreBackend calls:

```
[METASTORE] get_connection called — conn_id=aws_default, team=None,
  session_id=140039618793536, session.new=1, session.dirty=1
  | caller: execution_time/context.py:163(_get_connection) <- ...hook.py:61(get_connection)

[METASTORE] get_connection found and detached — conn_id=aws_default, conn_type=aws,
  session.new=1 (preserved), session.dirty=1 (preserved)
```

Key evidence: `session.new=1` before the call, `session.new=1 (preserved)`
after. The pending DagBundleModel survived. With the old `expunge_all()`,
this would have been `session.new=0`.

### Test 3: DAGs from team bundles parsed ✅

```
airflow dags list-bundles
```

Shows `alpha_simple_dag` from `team_alpha_dags` bundle — confirming the
dag-processor found the bundle in the DB and parsed its DAGs.

### Test 4: Connection lookup via CLI ✅

```bash
$ airflow connections get aws_default
id | conn_id     | conn_type | description | host | schema | login | password | port | is_encrypted | ...
 1 | aws_default | aws       | None        | None | None   | None  | None     | None | False        | ...
```

All fields returned correctly. No `DetachedInstanceError`.

### Test 5: Variable set/get via CLI ✅

```bash
$ airflow variables set test_var "hello_world"
Variable test_var created

$ airflow variables get test_var
hello_world

$ airflow variables get nonexistent_var
Variable nonexistent_var does not exist.
```

### Test 6: No errors in service logs ✅

No `DetachedInstanceError`, no "Bundle model not found", no session-related
errors in any of:
- `/tmp/dag-processor.log`
- `/tmp/scheduler.log`
- `/tmp/api-server.log`

---

## Approaches Considered

| # | Approach | Verdict |
|---|---|---|
| 1 | `expunge_all()` → `expunge(conn)` | **Selected** — minimal, safe, no data loss |
| 2 | Override `get_conn_value`, return URI string | Rejected — URI round-trip loses `description`, `id`; breaks CLI output |
| 3 | Copy fields into fresh Connection object | Rejected — decrypt/re-encrypt cycle for passwords; maintenance burden |
| 4 | Remove expunge entirely | Rejected — risk of accidental DB modifications via dirty tracking |
| 5 | Use non-scoped session | Rejected — extra DB connection per lookup |
| 6 | Thread session through callers | Rejected — impractical, deep call chain |
| 7 | Restructure `sync_bundles_to_db` | Rejected — doesn't fix root cause, other callers still vulnerable |

Full analysis in `solutions.md`.

---

## Files Changed

| File | Change |
|---|---|
| `airflow-core/src/airflow/secrets/metastore.py` | `expunge_all()` → `expunge(obj)`, removed debug logs, added observability logs |
| `airflow-core/src/airflow/dag_processing/bundles/manager.py` | Removed `[DEBUG-SYNC]` investigation logging |
| `dev/test_expunge_all_bug.py` | Bug reproduction tests (3 tests) |

---

## Before PR Submission

- [ ] Strip `[METASTORE]` INFO logs back to DEBUG level (too noisy for production)
- [ ] Remove `_get_caller_info()` helper and `traceback` import (investigation only)
- [ ] Move bug reproduction tests from `dev/` to `airflow-core/tests/unit/always/test_secrets_backends.py`
- [ ] Run full existing test suite via breeze
- [ ] Run `prek run --ref-from main` for lint/format checks
