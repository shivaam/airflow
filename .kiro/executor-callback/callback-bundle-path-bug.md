# Bug: Callback Execution Fails to Set Up DAG Bundle Python Path

## Summary

When a deadline callback is dispatched to a containerized executor (ECS, Batch, Kubernetes),
the callback container cannot import callback functions defined in DAG bundle files. The
container fails with `ModuleNotFoundError` because the DAG bundle directory is never added
to `sys.path` during callback execution.

This is a bug in the common callback execution path, not specific to any executor.

## How We Found It

1. Created a test DAG (`test_deadline_callback`) with a 30-second deadline and a `SyncCallback`
2. Uploaded to S3 DAG bundle (`team_alpha_dags`)
3. Triggered the DAG — ECS executor launched the task container successfully
4. After 30s the scheduler detected the missed deadline, created an `ExecuteCallback` workload,
   and dispatched it to ECS
5. ECS launched a second container for the callback
6. The callback container failed

## Error Progression

### Attempt 1: Inline function in DAG file

```python
def deadline_missed_alert(**kwargs):
    print("DEADLINE MISSED")

SyncCallback(deadline_missed_alert)
```

Error:
```
ModuleNotFoundError: No module named
'unusual_prefix_6538b1c2f34b1c4cff42468481908b4ed3350019_test_deadline_callback'
```

Airflow mangles DAG module names with `unusual_prefix_*` during import to avoid collisions.
The callback container can't resolve that mangled name — it only exists in the DAG
processor's memory, not as a real file on disk.

### Attempt 2: Separate module with string path

```python
SyncCallback("deadline_callback_fn.deadline_missed_alert")
```

With `deadline_callback_fn.py` uploaded alongside the DAG to the same S3 bundle.

Error:
```
ModuleNotFoundError: No module named 'deadline_callback_fn'
```

The file exists in the bundle directory on disk, but the bundle directory isn't on `sys.path`.

---

## Deep Dive: How Tasks vs Callbacks Execute

### Task Execution (works correctly)

When the ECS executor runs a task, the full chain is:

```
ECS container starts
  → execute_workload.py receives ExecuteTask workload (JSON with bundle_info, dag_rel_path, etc.)
  → calls supervise()                                    [task-sdk/supervisor.py]
    → calls parse()                                      [task-sdk/task_runner.py]
      → DagBundlesManager().get_bundle(name, version)    [airflow-core/bundles/manager.py]
        → S3DagBundle(name="team_alpha_dags", ...)       [providers/amazon/bundles/s3.py]
        → bundle.initialize()
          → Downloads DAGs from S3 to local dir           (/tmp/airflow/dag_bundles/team_alpha_dags/)
        → bundle.path returns /tmp/airflow/dag_bundles/team_alpha_dags/
      → BundleDagBag(bundle_path=bundle.path)            [airflow-core/dagbag.py]
        → sys.path.append("/tmp/airflow/dag_bundles/team_alpha_dags/")  ← THIS IS THE KEY STEP
      → DAG file is imported, task is found
    → Task subprocess runs with bundle dir on sys.path
    → Task can import any module from the bundle ✓
```

Key classes involved:
- `BundleDagBag` (airflow-core/src/airflow/dag_processing/dagbag.py:537) — adds bundle path to sys.path
- `DagBundlesManager` (airflow-core/src/airflow/dag_processing/bundles/manager.py:164) — resolves bundle config
- `S3DagBundle` (providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py:30) — syncs S3 to local dir
- `BaseDagBundle.base_dir` → `get_bundle_base_folder()` → `/tmp/airflow/dag_bundles/{bundle_name}/`

### Callback Execution (broken)

When the ECS executor runs a callback, the chain is much shorter:

```
ECS container starts
  → execute_workload.py receives ExecuteCallback workload (JSON with bundle_info, callback data, etc.)
  → Sees it's an ExecuteCallback
  → calls execute_callback_workload(workload.callback, log)   [airflow-core/workloads/callback.py]
    → Reads callback_path from callback.data["path"]           ("deadline_callback_fn.deadline_missed_alert")
    → Calls import_module("deadline_callback_fn")
    → Python searches sys.path for "deadline_callback_fn"
    → Bundle directory is NOT on sys.path
    → ModuleNotFoundError ✗
```

The `ExecuteCallback` workload carries `bundle_info` (name="team_alpha_dags", version=None)
and `dag_rel_path` — the exact same info the task path uses. But the callback code path
**never reads these fields**. It jumps straight to `import_module()`.

### The Gap in execute_workload.py

```python
# task-sdk/src/airflow/sdk/execution_time/execute_workload.py

def execute_workload(workload):
    if isinstance(workload, workloads.ExecuteCallback):
        # ❌ No bundle setup — goes straight to import
        from airflow.executors.workloads.callback import execute_callback_workload
        log.info("Executing callback workload", callback_id=workload.callback.id)
        success, error_msg = execute_callback_workload(workload.callback, log)
        # ...
        return

    # For tasks, this eventually calls supervise() → parse() → BundleDagBag → sys.path.append()
    supervise(
        ti=workload.ti,
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,  # ← Used by task path, ignored by callback path
        ...
    )
```

### What execute_callback_workload Does

```python
# airflow-core/src/airflow/executors/workloads/callback.py

def execute_callback_workload(callback, log):
    callback_path = callback.data.get("path")       # "deadline_callback_fn.deadline_missed_alert"
    module_path, function_name = callback_path.rsplit(".", 1)
    module = import_module(module_path)              # ← Fails here: can't find "deadline_callback_fn"
    callback_callable = getattr(module, function_name)
    # ... call the function
```

It receives only the `CallbackDTO` (id, fetch_method, data dict) — not the bundle_info.
Even if it wanted to set up the bundle path, it doesn't have the information to do so.

---

## Which Executors Are Affected?

### LocalExecutor — works by accident

LocalExecutor spawns workers using `multiprocessing.Process()` which **forks** the
scheduler process. On Linux (default fork start method), the child process inherits
the parent's entire memory space, including `sys.path`. Since the scheduler/DAG processor
already loaded the bundle paths, the forked worker inherits them.

The LocalExecutor `_execute_callback()` function (local_executor.py:155) calls
`execute_callback_workload(workload.callback, log)` directly — same broken code path,
no bundle setup. It just happens to work because `sys.path` was inherited from the parent.

If Python's multiprocessing start method were changed to `spawn` (which creates a fresh
process), LocalExecutor callbacks would break too.

### CeleryExecutor — broken (same bug)

Celery workers are separate processes, typically on different machines. The Celery
worker-side `execute_workload()` function (celery_executor_utils.py:191) does:

```python
elif isinstance(workload, workloads.ExecuteCallback):
    success, error_msg = execute_callback_workload(workload.callback, log)
```

Same pattern — no bundle path setup. Celery workers might work if the DAG files happen
to be on the worker's Python path (e.g., if `DAGS_FOLDER` is configured and on `sys.path`),
but with S3/GCS/remote bundles, the bundle directory won't be on `sys.path` and callbacks
will fail with the same `ModuleNotFoundError`.

### ECS / Batch / Kubernetes — broken (confirmed)

These launch fresh containers. The container runs `execute_workload.py` which has the
same missing bundle setup for callbacks. Confirmed broken on ECS.

### Summary

| Executor | Callback Works? | Why |
|----------|----------------|-----|
| LocalExecutor | Yes (by accident) | Forked process inherits parent's sys.path |
| CeleryExecutor | Depends | Works if DAGs on worker's Python path; fails with remote bundles |
| ECS | No | Fresh container, no bundle path setup |
| Batch | No | Fresh container, no bundle path setup |
| Kubernetes | No | Fresh container, no bundle path setup |

---

## Fix Options

### Option A: Set up bundle path in execute_workload.py (before calling execute_callback_workload)

The most straightforward fix. The workload already carries `bundle_info`. Use it to resolve
the bundle, initialize it (which syncs from S3), and add its path to `sys.path`.

Where to change: `task-sdk/src/airflow/sdk/execution_time/execute_workload.py`

```python
if isinstance(workload, workloads.ExecuteCallback):
    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.executors.workloads.callback import execute_callback_workload

    # Set up bundle path so callback can import modules from the DAG bundle
    bundle = DagBundlesManager().get_bundle(
        name=workload.bundle_info.name,
        version=workload.bundle_info.version,
    )
    bundle.initialize()
    if str(bundle.path) not in sys.path:
        sys.path.append(str(bundle.path))

    log.info("Executing callback workload", callback_id=workload.callback.id)
    success, error_msg = execute_callback_workload(workload.callback, log)
    ...
```

Pros:
- Minimal change, localized to one file
- Mirrors exactly what the task path does (via BundleDagBag)
- The workload already has all the info needed

Cons:
- `DagBundlesManager` reads config from airflow.cfg / env vars — the container needs
  `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` available (but it already needs this
  for task execution too, so this should already work)
- `bundle.initialize()` for S3 bundles triggers a full S3 sync, which adds latency to
  callback startup

### Option B: Pass bundle_info into execute_callback_workload and let it handle the setup

Instead of doing the setup in `execute_workload.py`, pass the bundle_info down to
`execute_callback_workload` and let it own the responsibility.

Where to change:
- `task-sdk/src/airflow/sdk/execution_time/execute_workload.py` (pass bundle_info)
- `airflow-core/src/airflow/executors/workloads/callback.py` (accept and use bundle_info)

```python
# execute_workload.py
success, error_msg = execute_callback_workload(workload.callback, log, bundle_info=workload.bundle_info)

# callback.py
def execute_callback_workload(callback, log, bundle_info=None):
    if bundle_info:
        _setup_bundle_path(bundle_info)
    # ... existing import logic
```

Pros:
- Cleaner separation of concerns
- `execute_callback_workload` becomes self-contained

Cons:
- Changes the function signature (could affect other callers)
- More files to modify

### Option C: Use the existing dag_rel_path to compute the bundle path without DagBundlesManager

The workload has `dag_rel_path` and `bundle_info.name`. The bundle base folder is
deterministic: `/tmp/airflow/dag_bundles/{bundle_name}/`. We could compute the path
directly without going through `DagBundlesManager` and `bundle.initialize()`.

Where to change: `task-sdk/src/airflow/sdk/execution_time/execute_workload.py`

```python
if isinstance(workload, workloads.ExecuteCallback):
    from airflow.dag_processing.bundles.base import get_bundle_base_folder

    bundle_path = str(get_bundle_base_folder(workload.bundle_info.name))
    if bundle_path not in sys.path:
        sys.path.append(bundle_path)
    ...
```

Pros:
- No S3 sync overhead — assumes the bundle is already on disk (which it should be if the
  task container already ran and synced it, or if the DAG processor already synced it)
- Very lightweight, no DagBundlesManager instantiation
- Doesn't need bundle config to be available in the container

Cons:
- Assumes the bundle directory already exists and is populated — if the callback container
  starts before the bundle is synced, the directory might be empty
- Bypasses the bundle initialization logic (no refresh/sync)
- Fragile if the path convention changes

### Option D: Have the callback container go through the full supervise/parse path

Instead of the shortcut in `execute_workload.py`, route callbacks through `supervise()` too,
which would set up the bundle, parse the DAG, and then execute the callback.

Pros:
- Fully consistent with task execution
- Gets all the same setup (logging, bundle path, DAG parsing)

Cons:
- Massive overkill — callbacks don't need DAG parsing, task instance setup, heartbeating, etc.
- Would require significant refactoring of `supervise()` to support callback workloads
- Much more complex change

---

## Recommendation

**Option A** is the best balance of correctness and simplicity. It's a small, localized
change in `execute_workload.py` that mirrors the proven task path. The S3 sync latency
is acceptable since callbacks aren't latency-critical, and the container already needs
the bundle config for task execution anyway.

If the S3 sync latency becomes a concern, **Option C** could work as an optimization,
but only if we can guarantee the bundle directory is already populated (which is true
on the EC2 host but may not be true in a fresh container).

---

## Related PR: callback_supervisor.py Refactor

There is an in-progress PR that refactors callback execution by introducing
`task-sdk/src/airflow/sdk/execution_time/callback_supervisor.py`. Key changes:

- Creates `supervise_callback()` and `CallbackSubprocess` (extends `WatchedSubprocess`)
- Callbacks now run in a forked subprocess instead of being called directly
- Callback path/kwargs passed via env vars (`_AIRFLOW_CALLBACK_PATH`, `_AIRFLOW_CALLBACK_KWARGS`)
- LocalExecutor and CeleryExecutor updated to call `supervise_callback()` instead of
  `execute_callback_workload()`
- Adds `key`, `display_name`, `success_state`, `failure_state` abstract properties to
  `BaseDagBundleWorkload`
- Introduces `ExecutorWorkload` type alias for `ExecuteTask | ExecuteCallback`

### Does this PR fix the bundle path bug? NO.

The new `_callback_subprocess_main()` still does a raw `import_module()` without any
bundle path setup:

```python
def _callback_subprocess_main():
    callback_path = os.environ.get("_AIRFLOW_CALLBACK_PATH", "")
    # ...
    success, error_msg = execute_callback(callback_path, callback_kwargs, log)

def execute_callback(callback_path, callback_kwargs, log):
    module_path, function_name = callback_path.rsplit(".", 1)
    module = import_module(module_path)  # ← No bundle path on sys.path!
```

The callers pass `callback_path` and `callback_kwargs` but NOT `bundle_info`:

```python
# local_executor.py
supervise_callback(
    id=workload.callback.id,
    callback_path=workload.callback.data.get("path", ""),
    callback_kwargs=workload.callback.data.get("kwargs", {}),
    log_path=workload.log_path,
)
```

And critically, `execute_workload.py` (the entry point for containerized executors like
ECS/Batch) is NOT modified in this PR — so the containerized path still goes through the
old `execute_callback_workload()`.

### What this means for our fix

The bundle path fix needs to be added to this PR (or as a follow-up). Two places need it:

1. `callback_supervisor.py` — `supervise_callback()` should accept `bundle_info`, resolve
   the bundle, and pass the bundle path to the subprocess (e.g., via another env var like
   `_AIRFLOW_BUNDLE_PATH`). The subprocess main should add it to `sys.path` before importing.

2. `execute_workload.py` — the containerized executor entry point needs to be updated to
   call `supervise_callback()` (like LocalExecutor and CeleryExecutor do in this PR) instead
   of the old `execute_callback_workload()`. And it needs to pass `bundle_info`.

---

## Test Environment

- Airflow running on EC2 (single node: scheduler, api-server, dag-processor)
- ECS executor for `team_alpha` team (cluster: `alpha-cluster`)
- S3 DAG bundles (`s3://airflow-ecs-dags-741443349243-us-west-2/`)
- S3 remote logging (`s3://airflow-ecs-logs-741443349243-us-west-2/logs/`)
- PostgreSQL on RDS
- Bundle storage: `/tmp/airflow/dag_bundles/team_alpha_dags/`

## Evidence

Scheduler logs showing successful deadline detection and callback dispatch:
```
01:10:36 Received executor event with state running for callback 019cef0b-0712-...
01:10:36 Callback 019cef0b-0712-... is currently running
```

ECS container logs showing the import failure (attempt 1 — inline function):
```
ModuleNotFoundError: No module named
'unusual_prefix_6538b1c2f34b1c4cff42468481908b4ed3350019_test_deadline_callback'
```

ECS container logs showing the import failure (attempt 2 — separate module):
```
Callback execution failed: ModuleNotFoundError: No module named 'deadline_callback_fn'
```

Task execution on the same container image works fine — `slow_task` ran successfully on
ECS, proving the container can import and execute DAG code when the bundle path is set up.
