# DAG Bundle System — Component Guide

A walkthrough of every piece involved in the DAG bundle system, how they connect,
and where to find them in the codebase. Written for someone new to this area.

---

## The Big Picture

Airflow loads DAG files from "bundles" — pluggable sources like local directories,
S3 buckets, or Git repos. The DAG Processor discovers files in each bundle, parses
them in subprocesses, and stores the results in the metadata database.

```
airflow.cfg                    DagProcessorJobRunner
  dag_bundle_config_list ──►  DagFileProcessorManager
                                │
                                ├─ DagBundlesManager.sync_bundles_to_db()
                                │    └─ Writes DagBundleModel rows to DB
                                │
                                ├─ For each bundle:
                                │    ├─ bundle.initialize()   (first time)
                                │    ├─ bundle.refresh()      (periodic)
                                │    └─ Scan bundle.path for .py files
                                │
                                └─ Spawn DagFileProcessorProcess per file
                                     └─ Parse DAGs, write to DB
```

---

## 1. Bundle Abstraction

### BaseDagBundle
`airflow-core/src/airflow/dag_processing/bundles/base.py`

Abstract base class. Every bundle type implements this.

| Method / Property | What it does |
|---|---|
| `path` (abstract property) | Local filesystem path where DAG files live |
| `refresh()` (abstract) | Pull latest files from the source |
| `get_current_version()` (abstract) | Return a version string (or None) |
| `initialize()` | One-time setup (create dirs, validate source). Called before first use |
| `view_url_template()` | URL template for linking to the bundle in the UI (no init required) |
| `lock()` | Context manager for exclusive file-level locking |
| `supports_versioning` | Class-level bool. S3 and Local are both `False` |

Key design choice: `view_url_template()` must work without `initialize()` being called.
This matters because `sync_bundles_to_db` calls it to store URL metadata, and that
happens before bundles are initialized for actual DAG parsing.

### LocalDagBundle
`airflow-core/src/airflow/dag_processing/bundles/local.py`

The simplest bundle. Points to a local directory.

- `path` → the configured directory (defaults to `settings.DAGS_FOLDER`)
- `refresh()` → no-op (it's already local)
- `view_url_template()` → returns `None` (no external URL)
- No hooks, no connections, no side effects during construction

### S3DagBundle
`providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py`

Loads DAGs from an S3 bucket. This is the bundle type involved in issue #62244.

- Constructor takes `aws_conn_id`, `bucket_name`, `prefix`
- `path` → local directory where S3 files are synced to
- `refresh()` → calls `S3Hook.sync_to_local_dir()` to download from S3
- `view_url_template()` → builds an S3 console URL. Accesses `self.s3_hook.region_name`,
  which triggers hook creation and connection lookup (this is the trigger for the bug)
- `s3_hook` → lazy property that creates `S3Hook(aws_conn_id=...)` on first access

---

## 2. Bundle Manager

### DagBundlesManager
`airflow-core/src/airflow/dag_processing/bundles/manager.py`

Central orchestrator for all bundle operations.

**Config parsing** (`parse_config`):
- Reads `[dag_processor] dag_bundle_config_list` from airflow.cfg (JSON list)
- Each entry: `{"name": "...", "classpath": "...", "kwargs": {...}, "team_name": "..."}`
- Dynamically imports the bundle class via `import_string(classpath)`
- Validates: no duplicate names, `example_dags` is reserved, team_name requires multi_team mode
- Stores as `_InternalBundleConfig` (bundle_class + kwargs + team_name)

**Database sync** (`sync_bundles_to_db`):
- Decorated with `@provide_session` — gets a scoped (thread-local) SQLAlchemy session
- For each configured bundle:
  1. Look up the Team if `team_name` is set
    2. Call `_extract_and_sign_template()` → instantiates the bundle, calls `view_url_template()`
  3. Create or update `DagBundleModel` row
  4. Set `bundle.teams = [team]` if team association changed
- Deactivates bundles no longer in config
- Cleans up `ParseImportError` rows for deactivated bundles

**Bundle instantiation** (`get_bundle`):
- Creates a fresh bundle instance: `bundle_class(name=name, version=version, **kwargs)`
- Used by `sync_bundles_to_db` (for URL templates) and by `DagFileProcessorManager` (for parsing)

---

## 3. Database Models

### DagBundleModel
`airflow-core/src/airflow/models/dagbundle.py`

Table: `dag_bundle`

| Column | Type | Purpose |
|---|---|---|
| `name` (PK) | String(250) | Bundle identifier |
| `active` | Boolean | Whether bundle is currently in config |
| `version` | String(200) | Latest version seen |
| `last_refreshed` | DateTime | When last refreshed |
| `signed_url_template` | Text | Signed URL for UI links |
| `template_params` | JSON | Parameters for URL template rendering |

Relationships:
- `teams` → many-to-many with `Team` via `dag_bundle_team` association table

Methods:
- `render_url(version)` → unsigns the template and formats it with version + params

### Team
`airflow-core/src/airflow/models/team.py`

Table: `team`

| Column | Type | Purpose |
|---|---|---|
| `name` (PK) | String(50) | Team identifier |

Relationships:
- `dag_bundles` → many-to-many with `DagBundleModel`

Association table `dag_bundle_team`:
- Composite PK: `(dag_bundle_name, team_name)`
- Has a unique index on `dag_bundle_name` — enforces one team per bundle

---

## 4. DAG Processing Pipeline

### DagProcessorJobRunner
`airflow-core/src/airflow/jobs/dag_processor_job_runner.py`

Thin wrapper. Creates a `DagFileProcessorManager` and calls `manager.run()`.
Provides heartbeat callbacks to keep the job alive in the DB.

### DagFileProcessorManager
`airflow-core/src/airflow/dag_processing/manager.py`

The main processing loop. This is where bundles are actually used.

1. **Startup**: `DagBundlesManager().sync_bundles_to_db()` — register bundles in DB
2. **Bundle refresh loop**:
   - For each bundle, check if refresh is needed (time-based or version change)
   - Call `bundle.initialize()` on first use
   - Call `bundle.refresh()` periodically
   - Scan `bundle.path` for Python files
3. **File processing**:
   - Queue `DagFileInfo` objects (rel_path + bundle_name + bundle_path)
   - Spawn `DagFileProcessorProcess` subprocesses to parse each file
   - Collect results and update DB (DAG models, import errors, etc.)

---

## 5. AWS Hook Chain (the bug trigger path)

This is the chain that causes issue #62244. Understanding it is key.

### S3Hook
`providers/amazon/src/airflow/providers/amazon/aws/hooks/s3.py`

Extends `AwsBaseHook`. Provides S3-specific operations like `sync_to_local_dir()`,
`check_for_bucket()`, `list_keys()`, etc.

### AwsBaseHook / AwsGenericHook
`providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py`

`AwsBaseHook` extends `AwsGenericHook`. The important piece is the `conn_config`
cached property:

```python
@cached_property
def conn_config(self) -> AwsConnectionWrapper:
    connection = None
    if self.aws_conn_id:
        connection = self.get_connection(self.aws_conn_id)  # ← triggers DB lookup
    return AwsConnectionWrapper(conn=connection, ...)
```

On first access, `conn_config` calls `self.get_connection(aws_conn_id)`. This goes
through the secrets backend chain and eventually hits `MetastoreBackend`.

---

## 6. Secrets & Connection Resolution

### Connection.get_connection_from_secrets
`airflow-core/src/airflow/models/connection.py`

Static method. Iterates through configured secrets backends in order:
1. Check `SecretCache` first
2. For each backend: call `backend.get_connection(conn_id, team_name)`
3. First non-None result wins
4. Raises `AirflowNotFoundException` if nothing found

### MetastoreBackend
`airflow-core/src/airflow/secrets/metastore.py`

The default secrets backend. Queries the `connection` table directly.

```python
@provide_session
def get_connection(self, conn_id, team_name=None, session=NEW_SESSION):
    conn = session.scalar(select(Connection).where(...))
    session.expunge_all()  # ← THE BUG: nukes ALL objects from the session
    return conn
```

`expunge_all()` was intended to detach the returned Connection from the session
so it could be used freely. But it removes everything — including unrelated
pending objects from other code sharing the same scoped session.

---

## 7. Session Management

### Scoped Session
`airflow-core/src/airflow/settings.py`

```python
Session = scoped_session(NonScopedSession)
```

`scoped_session` is a SQLAlchemy pattern that returns the same session object for
all calls within the same thread. This means:

- `sync_bundles_to_db` gets `SESSION_A`
- `MetastoreBackend.get_connection` (called from within sync) also gets `SESSION_A`
- `expunge_all()` on `SESSION_A` affects both callers

### provide_session decorator
`airflow-core/src/airflow/utils/session.py`

```python
def provide_session(func):
    def wrapper(*args, **kwargs):
        if "session" in kwargs or session_args_idx < len(args):
            return func(*args, **kwargs)  # use provided session
        with create_session() as session:
            return func(*args, session=session, **kwargs)  # create new scoped session
    return wrapper
```

If no session is passed, it creates one via `create_session()` → `settings.Session()`.
Since `Session` is scoped, nested `@provide_session` calls without explicit session
arguments all share the same session.

---

## 8. How It All Connects (Issue #62244 Flow)

```
DagFileProcessorManager.run()
  └─ DagBundlesManager().sync_bundles_to_db(session=SESSION_A)
       │
       ├─ session.add(DagBundleModel("team_alpha"))  ← added to SESSION_A.new
       │
       ├─ _extract_and_sign_template("team_beta")
       │    └─ S3DagBundle("team_beta").view_url_template()
       │         └─ self.s3_hook.region_name
       │              └─ S3Hook("aws_default")
       │                   └─ conn_config → get_connection("aws_default")
       │                        └─ MetastoreBackend.get_connection(session=SESSION_A)
       │                             └─ SESSION_A.expunge_all()  ← BOOM
       │                                  team_alpha DagBundleModel is gone
       │
       ├─ session.add(DagBundleModel("team_beta"))  ← added, will be expunged next iteration
       │
       └─ session.commit()  ← nothing left to commit for team bundles
```

---

## File Index

| File | What it is |
|---|---|
| `airflow-core/src/airflow/dag_processing/bundles/base.py` | BaseDagBundle ABC, locking, storage paths |
| `airflow-core/src/airflow/dag_processing/bundles/local.py` | LocalDagBundle (simple local dir) |
| `airflow-core/src/airflow/dag_processing/bundles/manager.py` | DagBundlesManager (config, sync, instantiation) |
| `airflow-core/src/airflow/models/dagbundle.py` | DagBundleModel (DB table) |
| `airflow-core/src/airflow/models/team.py` | Team model + association table |
| `airflow-core/src/airflow/dag_processing/manager.py` | DagFileProcessorManager (parsing loop) |
| `airflow-core/src/airflow/jobs/dag_processor_job_runner.py` | Job runner entry point |
| `providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py` | S3DagBundle |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/s3.py` | S3Hook |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py` | AwsGenericHook (conn_config) |
| `airflow-core/src/airflow/secrets/metastore.py` | MetastoreBackend (expunge_all bug) |
| `airflow-core/src/airflow/models/connection.py` | Connection.get_connection_from_secrets |
| `airflow-core/src/airflow/utils/session.py` | provide_session, create_session |
| `airflow-core/src/airflow/settings.py` | Session = scoped_session(...) |
