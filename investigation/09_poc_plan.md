# Proof of Concept Plan

## Goal

Validate the backwards-compatible migration approach with a minimal end-to-end prototype:
a DAG with `on_failure_callback=my_func` where `my_func` is automatically resolved to an
import path and executed via the executor instead of the DAG Processor.

## Scope

- DAG-level `on_failure_callback` only (simplest case)
- LocalExecutor only
- Module-level function only (no lambdas, no closures)
- Simplified context (dag_id, run_id, state — not full template context)
- No fallback to DAG Processor (POC only handles the happy path)

## Steps

### Step 1: Import Path Resolution

**File**: `task-sdk/src/airflow/sdk/definitions/callback.py`

Add a utility function:

```python
def resolve_import_path(callback: Callable) -> str | None:
    """
    Attempt to resolve a callable to a dotted import path.
    Returns None if the callable cannot be imported by path (lambda, closure, etc.)
    """
    module = getattr(callback, "__module__", None)
    qualname = getattr(callback, "__qualname__", None)
    if not module or not qualname:
        return None
    if "<lambda>" in qualname or "<locals>" in qualname:
        return None
    return f"{module}.{qualname}"
```

Write unit tests for this function with various callable types.

### Step 2: Store Resolved Path in Serialized DAG

**File**: `airflow-core/src/airflow/serialization/definitions/dag.py`

Add optional fields:

```python
class SerializedDAG:
    ...
    on_failure_callback_path: str | None = None
    on_success_callback_path: str | None = None
```

**File**: `airflow-core/src/airflow/serialization/serialized_objects.py`

During serialization, resolve and store the path:

```python
if dag.on_failure_callback:
    path = resolve_import_path(dag.on_failure_callback)
    if path:
        serialized_dag["on_failure_callback_path"] = path
```

### Step 3: Scheduler Creates ExecutorCallback

**File**: `airflow-core/src/airflow/jobs/scheduler_job_runner.py`

In the callback handling path, check for a resolved path:

```python
if serialized_dag.on_failure_callback_path:
    # Create ExecutorCallback with the resolved path
    callback = ExecutorCallback(
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
        data={
            "path": serialized_dag.on_failure_callback_path,
            "kwargs": {"context": {"dag_id": dag_id, "run_id": run_id}},
        },
    )
    session.add(callback)
    workload = ExecuteCallback.make(callback=callback, dag_run=dag_run, ...)
    executor.queue_workload(workload, session)
else:
    # Legacy path
    executor.send_callback(dag_callback_request)
```

### Step 4: Test DAG

Create a test DAG:

```python
# dags/test_callback_migration.py
from airflow.sdk import DAG, task
from datetime import datetime

def my_failure_alert(context):
    print(f"POC CALLBACK FIRED: DAG {context.get('dag_id')} failed!")
    # Write to a file to verify execution
    with open("/tmp/callback_poc_result.txt", "w") as f:
        f.write(f"Callback executed at {datetime.now()}")

with DAG(
    "test_callback_migration",
    schedule=None,
    on_failure_callback=my_failure_alert,
) as dag:

    @task
    def always_fail():
        raise Exception("Intentional failure for POC")

    always_fail()
```

### Step 5: Run and Verify

```bash
# Start Airflow with LocalExecutor
breeze run airflow dags trigger test_callback_migration

# Check that callback ran via executor (not DAG Processor)
# Look for "POC CALLBACK FIRED" in executor worker logs, NOT DAG Processor logs
# Check /tmp/callback_poc_result.txt exists
```

## Success Criteria

1. The callback function is resolved to an import path during DAG parsing
2. When the DAG fails, the scheduler creates an `ExecutorCallback` (not `DagCallbackRequest`)
3. The `ExecuteCallback` workload is queued to the LocalExecutor
4. The LocalExecutor worker imports and executes the callback
5. The callback receives at least minimal context (dag_id, run_id)
6. The callback execution is logged in executor worker logs
7. The `callback` table shows the ExecutorCallback with state=SUCCESS

## What This POC Does NOT Validate

- Lambda/closure fallback to DAG Processor
- Full template context compatibility
- Task-level callbacks
- Other executors (Celery, ECS, K8s)
- List of callbacks
- Notifier instances
- Error handling and retry
- Scheduling priority

## Estimated POC Effort

- **Import path resolution**: 2-3 hours (with tests)
- **Serialization changes**: 2-3 hours
- **Scheduler changes**: 4-6 hours (most complex part)
- **Test DAG + validation**: 2-3 hours
- **Total**: ~1-2 days of focused work

## Follow-Up After POC

If POC succeeds:
1. Share results with ferruzzi and community
2. Draft dev list email with concrete proposal
3. Address context compatibility (Phase 3)
4. Expand to task-level callbacks
5. Write AIP if community discussion warrants it
