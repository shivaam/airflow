# Bug: Team-Scoped DAG Bundles Not Persisted — `expunge_all()` Session Corruption

## Status: Root Cause Confirmed

## Summary

When using `dag_bundle_config_list` with `team_name` in multi-team mode, team-scoped DAG
bundles (`team_alpha_dags`, `team_beta_dags`) are never persisted to the `dag_bundle` table.
The dag-processor logs "Added new DAG bundle" for them, but they silently vanish before commit.
The processor then loops forever logging "Bundle model not found" every ~5 seconds, and those
bundles' DAGs are never discovered or processed. Non-team bundles (`shared_dags`, `example_dags`)
work fine because they already exist in the DB from a prior run.

## Environment

- Airflow version: 3.2.0 (main branch)
- Python: 3.12
- Database: PostgreSQL 16 (RDS)
- OS: Amazon Linux 2023 (EC2)
- Multi-team: enabled (`core.multi_team = True`)
- Teams: `team_alpha`, `team_beta`
- Bundle type: `S3DagBundle` (airflow.providers.amazon.aws.bundles.s3.S3DagBundle)
- No `aws_default` Airflow connection configured (uses IAM role)

## Root Cause (Confirmed via instrumented logs)

`MetastoreBackend.get_connection()` calls `session.expunge_all()` on the shared
`scoped_session` singleton, destroying pending objects that `sync_bundles_to_db` had added.

### The chain of events

1. `sync_bundles_to_db` gets a session via `@provide_session` → scoped session `140126030839936`
2. For each bundle, it calls `_extract_and_sign_template()` which instantiates an `S3DagBundle`,
   which creates an `S3Hook`, which accesses `self.conn_config`, which calls
   `self.get_connection('aws_default')`
3. `get_connection` → `Connection.get_connection_from_secrets` → `MetastoreBackend.get_connection`
4. `MetastoreBackend.get_connection` is also `@provide_session` — but because Airflow uses
   `scoped_session` (thread-local singleton), it gets the **same session** `140126030839936`
5. `MetastoreBackend.get_connection` calls `session.expunge_all()` at the end, which detaches
   **every object** from the session — including pending `DagBundleModel` and `Team` objects
6. When `sync_bundles_to_db` returns, `session.new` is empty — nothing gets committed

### Why non-team bundles survive

`shared_dags` and `example_dags` already exist in the DB from a previous run. They're loaded
into `stored` as persistent objects. Even though `expunge_all()` detaches them, they were
already committed — the session just updates them in place. New bundles (the team ones) are
in `session.new` and get destroyed.

### Why it doesn't happen with local dev (SQLite)

- Local dev typically doesn't use S3 bundles, so `S3Hook` / `MetastoreBackend.get_connection`
  is never triggered during `sync_bundles_to_db`
- If `aws_default` connection exists in the DB, the `SecretCache` may return it before
  `MetastoreBackend` is reached
- SQLite's in-process behavior may also mask timing-dependent session state issues

## Proof (Instrumented Log Output)

```
[DEBUG-SYNC] sync_bundles_to_db START — session id=140126030839936, session.new=0
[DEBUG-SYNC] Processing bundle 'team_alpha_dags' (team_name=team_alpha)
[DEBUG-SYNC] Loaded team 'team_alpha' — persistent=True, detached=False
[DEBUG-SYNC] BEFORE _extract_and_sign_template('team_alpha_dags') — session.new=[]
[DEBUG-SYNC] AFTER _extract_and_sign_template('team_alpha_dags') — session.new=[]
[DEBUG-SYNC] Team 'team_alpha' state after hook init — persistent=False, detached=True
[DEBUG-SYNC] After session.add('team_alpha_dags') — session.new=['team_alpha_dags']

[DEBUG-SYNC] Processing bundle 'team_beta_dags' (team_name=team_beta)
[DEBUG-SYNC] BEFORE _extract_and_sign_template('team_beta_dags') — session.new=['team_alpha_dags']
[DEBUG-METASTORE] expunge_all() about to nuke session id=140126030839936 — new=1, dirty=1
[DEBUG-METASTORE] Objects being expunged from session.new: ['team_alpha_dags']    <-- DESTROYED
[DEBUG-SYNC] AFTER _extract_and_sign_template('team_beta_dags') — session.new=[]
[DEBUG-SYNC] After session.add('team_beta_dags') — session.new=['team_beta_dags']

[DEBUG-SYNC] Processing bundle 'shared_dags' (team_name=None)
[DEBUG-SYNC] BEFORE _extract_and_sign_template('shared_dags') — session.new=['team_beta_dags']
[DEBUG-METASTORE] expunge_all() about to nuke session id=140126030839936 — new=1, dirty=1
[DEBUG-METASTORE] Objects being expunged from session.new: ['team_beta_dags']     <-- DESTROYED
[DEBUG-SYNC] AFTER _extract_and_sign_template('shared_dags') — session.new=[]

[DEBUG-SYNC] sync_bundles_to_db END — session.new=[], session.dirty=[]            <-- NOTHING TO COMMIT
```

## Affected Code

- `airflow/dag_processing/bundles/manager.py` — `sync_bundles_to_db()` (victim)
- `airflow/secrets/metastore.py` — `MetastoreBackend.get_connection()` (culprit: `expunge_all()`)
- `airflow/utils/session.py` — `scoped_session` singleton (enabler)

## Possible Fixes

### Option A: Flush before the dangerous call (minimal, targeted)

In `sync_bundles_to_db`, call `session.flush()` after `session.add(bundle)` so the row is
persisted before `_extract_and_sign_template` can nuke it:

```python
session.add(bundle)
session.flush()  # persist to DB before S3Hook triggers expunge_all()
```

Downside: doesn't fix the root cause. Other callers of `MetastoreBackend` could hit the same issue.

### Option B: Move `_extract_and_sign_template` before `session.add` (reorder)

Call `_extract_and_sign_template` at the top of the loop, before any session mutations.
The `expunge_all()` would fire on a clean session with no pending objects.

Downside: requires restructuring the loop. Still doesn't fix the root cause.

### Option C: Fix `MetastoreBackend.get_connection` to not nuke the session (root cause fix)

Replace `session.expunge_all()` with targeted expunge of just the connection object:

```python
if conn:
    session.expunge(conn)
```

Or use a separate non-scoped session for the connection lookup.

This is the proper fix — `expunge_all()` is a sledgehammer that shouldn't be used on a shared session.

## Workaround (Manual SQL)

```sql
INSERT INTO dag_bundle (name, active) VALUES ('team_alpha_dags', true) ON CONFLICT (name) DO UPDATE SET active = true;
INSERT INTO dag_bundle (name, active) VALUES ('team_beta_dags', true) ON CONFLICT (name) DO UPDATE SET active = true;
INSERT INTO dag_bundle_team (bundle_name, team_name) VALUES ('team_alpha_dags', 'team_alpha') ON CONFLICT DO NOTHING;
INSERT INTO dag_bundle_team (bundle_name, team_name) VALUES ('team_beta_dags', 'team_beta') ON CONFLICT DO NOTHING;
```

Then restart the dag-processor.

## Impact

Team-scoped DAG bundles are completely non-functional without the manual workaround when
using S3DagBundle (or any bundle whose initialization triggers a connection lookup via
`MetastoreBackend`). The dag-processor never processes DAGs from team bundles.
