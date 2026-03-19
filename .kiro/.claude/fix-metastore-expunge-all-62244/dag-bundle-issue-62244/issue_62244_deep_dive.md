# Issue #62244 Deep Dive: DAG Bundles with team_name Not Persisted to Database

## Summary

When `dag_bundle_config_list` includes bundles with `team_name` in multi-team mode,
`sync_bundles_to_db` logs "Added new DAG bundle" for team-scoped bundles, but the records
are never committed to the `dag_bundle` table. Non-team bundles (like `shared_dags` and
`example_dags`) persist correctly. The dag-processor then continuously logs
"Bundle model not found" for the team bundles, and their DAGs are never processed.

## Root Cause

`MetastoreBackend.get_connection()` calls `session.expunge_all()` on the shared
scoped session. This gets triggered during `sync_bundles_to_db` when
`_extract_and_sign_template` instantiates an `S3DagBundle` → `S3Hook` →
`get_connection('aws_default')`. Since both methods share the same scoped session,
`expunge_all()` destroys the pending `DagBundleModel` objects that were just added
via `session.add()`.

## Detailed Call Chain

Here is the exact sequence of calls that leads to the bug:

### Step 1: sync_bundles_to_db gets a scoped session

```
DagBundlesManager.sync_bundles_to_db()          # manager.py:212
  └─ @provide_session decorator                  # session.py:93
       └─ create_session(scoped=True)            # session.py:34
            └─ settings.Session()                # settings.py:431
                 # Session = scoped_session(NonScopedSession)
                 # Returns thread-local session, let's call it SESSION_A
```

`sync_bundles_to_db` is decorated with `@provide_session`. Since no session is passed
explicitly, the decorator calls `create_session()` which calls `settings.Session()`.
This is a `scoped_session` — a thread-local session factory. It returns the same
session object for all calls within the same thread. Let's call it `SESSION_A`.

### Step 2: First bundle (team_alpha_dags) is added to session

```
sync_bundles_to_db:
  for name, config in self._bundle_config.items():
    # name = "team_alpha_dags"
    ...
    bundle = DagBundleModel(name="team_alpha_dags")
    session.add(bundle)                          # manager.py:308
    # SESSION_A.new = {DagBundleModel(name="team_alpha_dags")}
```

At this point, `SESSION_A.new` contains the `team_alpha_dags` `DagBundleModel`. It has
not been flushed or committed yet — it's a pending insert.

### Step 3: Next iteration processes team_beta_dags, calls _extract_and_sign_template

```
sync_bundles_to_db:
  for name, config in self._bundle_config.items():
    # name = "team_beta_dags"
    ...
    new_template, new_params = _extract_and_sign_template("team_beta_dags")
```

### Step 4: _extract_and_sign_template instantiates S3DagBundle

```
_extract_and_sign_template("team_beta_dags"):    # manager.py:224
  bundle_instance = self.get_bundle(name)         # manager.py:225
    └─ cfg_bundle.bundle_class(name=name, ...)    # manager.py:345
         # bundle_class = S3DagBundle
```

### Step 5: S3DagBundle.view_url_template() accesses self.s3_hook

```
_extract_and_sign_template("team_beta_dags"):
  new_template_ = bundle_instance.view_url_template()  # manager.py:226
    └─ S3DagBundle.view_url_template()                  # s3.py:155
         └─ self.s3_hook.region_name                    # s3.py:163
              └─ S3DagBundle.s3_hook (property)          # s3.py:101
                   └─ S3Hook(aws_conn_id="aws_default")  # s3.py:103
```

`view_url_template()` accesses `self.s3_hook.region_name`. The `s3_hook` property
lazily creates an `S3Hook` instance.

### Step 6: S3Hook.__init__ → AwsGenericHook.__init__ → conn_config → get_connection

```
S3Hook(aws_conn_id="aws_default")
  └─ AwsGenericHook.__init__()                    # base_aws.py:495
       └─ self.conn_config (cached_property)       # base_aws.py:612
            └─ self.get_connection("aws_default")  # base_aws.py:618
```

`AwsGenericHook.conn_config` is a cached property. On first access during `__init__`,
it calls `self.get_connection(self.aws_conn_id)`.

### Step 7: get_connection resolves through secrets backends → MetastoreBackend

```
self.get_connection("aws_default")
  └─ Connection.get_connection_from_secrets("aws_default")  # connection.py:492
       └─ for secrets_backend in ensure_secrets_loaded():    # connection.py:551
            └─ MetastoreBackend.get_connection("aws_default")  # metastore.py:40
```

The hook's `get_connection` ultimately calls `Connection.get_connection_from_secrets`,
which iterates through configured secrets backends. `MetastoreBackend` is the default
backend for database-stored connections.

### Step 8: MetastoreBackend.get_connection calls session.expunge_all() — THE BUG

```
MetastoreBackend.get_connection("aws_default"):   # metastore.py:40
  @provide_session                                 # metastore.py:39
    └─ create_session(scoped=True)                 # session.py:34
         └─ settings.Session()                     # settings.py:431
              # RETURNS THE SAME SESSION_A (scoped = thread-local)

  conn = session.scalar(select(Connection)...)     # metastore.py:58
  session.expunge_all()                            # metastore.py:80
  # SESSION_A.new is now EMPTY
  # DagBundleModel(name="team_alpha_dags") has been expunged!
  return conn
```

`MetastoreBackend.get_connection` is also decorated with `@provide_session`. Since no
session is passed explicitly, the decorator calls `create_session()` → `settings.Session()`.
Because `Session` is a `scoped_session`, it returns **the exact same `SESSION_A`** that
`sync_bundles_to_db` is using.

Then `session.expunge_all()` is called on line 80 of `metastore.py`. This removes **all**
objects from the session's identity map and pending collections — including the
`DagBundleModel(name="team_alpha_dags")` that was added in Step 2.

### Step 9: sync_bundles_to_db continues, unaware objects are gone

```
sync_bundles_to_db:
  # Back from _extract_and_sign_template
  # SESSION_A.new is now empty — team_alpha_dags was expunged
  bundle = DagBundleModel(name="team_beta_dags")
  session.add(bundle)
  # SESSION_A.new = {DagBundleModel(name="team_beta_dags")}
  # But on the NEXT iteration, this will also be expunged...
```

### Step 10: Pattern repeats for every subsequent bundle

Each time `_extract_and_sign_template` is called for the next bundle, the S3Hook
initialization triggers `get_connection` → `expunge_all()`, destroying whatever was
added in the previous iteration.

### Step 11: At commit time, nothing is left

```
sync_bundles_to_db returns
  └─ @provide_session wrapper
       └─ create_session context manager
            └─ session.commit()                    # session.py:44
                 # SESSION_A.new = [] — nothing to commit
```

The `create_session` context manager calls `session.commit()` on exit. But by this point,
`SESSION_A.new` is empty — all `DagBundleModel` objects have been expunged. The commit
is a no-op for the bundle records.

## Why Non-Team Bundles (shared_dags) Work

Non-team bundles like `shared_dags` happen to be processed **last** in the iteration.
When `shared_dags` is processed:

1. `_extract_and_sign_template("shared_dags")` triggers `expunge_all()`, destroying
   `team_beta_dags` from the session.
2. `shared_dags` is then added to the session via `session.add(bundle)`.
3. There is **no subsequent iteration** that would trigger another `expunge_all()`.
4. So `shared_dags` survives until commit time and gets persisted.

This is purely an ordering artifact. If `shared_dags` were processed before the team
bundles, it would also be expunged.

## Why This Only Affects Bundles That Trigger get_connection

Local bundles (like `LocalDagBundle`) don't access any hooks or connections during
`view_url_template()`. Their `_extract_and_sign_template` call doesn't trigger the
`MetastoreBackend` → `expunge_all()` chain. So they're safe.

The bug specifically manifests with provider bundles (like `S3DagBundle`, and likely
`GCSDagBundle` or any future bundle) that need a connection/hook to construct their
view URL template.

## Affected Files

| File | Role |
|------|------|
| `airflow-core/src/airflow/secrets/metastore.py:80` | **The bug**: `session.expunge_all()` nukes the shared scoped session |
| `airflow-core/src/airflow/secrets/metastore.py:99` | Same issue in `get_variable()` |
| `airflow-core/src/airflow/dag_processing/bundles/manager.py:212-350` | `sync_bundles_to_db` — the victim |
| `airflow-core/src/airflow/utils/session.py:34,93` | `create_session` / `provide_session` — scoped session plumbing |
| `airflow-core/src/airflow/settings.py:431` | `Session = scoped_session(NonScopedSession)` — the shared session |
| `providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py:101-103` | `s3_hook` property triggers hook init |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py:612-618` | `conn_config` calls `get_connection` |
| `airflow-core/src/airflow/models/connection.py:492-561` | `get_connection_from_secrets` iterates backends |

## Why expunge_all Exists in MetastoreBackend

The original intent of `expunge_all()` was to detach the returned `Connection` (or
`Variable`) object from the session so that the caller could use it freely without
worrying about the session lifecycle. Without expunging, accessing lazy-loaded attributes
on the returned object after the session is closed would raise a `DetachedInstanceError`.

The problem is that `expunge_all()` is a sledgehammer — it removes **every** object from
the session, not just the one that was queried. This is fine when `MetastoreBackend` is
the only user of the session, but breaks when it shares a scoped session with other code
that has pending work.

## Scope of Impact

This bug affects **any** code path where:
1. A function decorated with `@provide_session` adds objects to the session, AND
2. Within that same function (or a nested call), `MetastoreBackend.get_connection()` or
   `MetastoreBackend.get_variable()` is called without an explicit session argument.

The `sync_bundles_to_db` + `S3DagBundle` combination is the known trigger, but the
pattern could theoretically affect other code paths too.

## Note on Debug Logging

The current codebase contains extensive `[DEBUG-SYNC]` and `[DEBUG-METASTORE]` warning-level
log statements that were added during investigation of this bug. These should be removed
as part of the fix.
