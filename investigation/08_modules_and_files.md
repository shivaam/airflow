# Modules and Files to Touch

## Phase 1: Core Migration (DAG-Level Callbacks)

### SDK Layer (task-sdk/)

| File | Change |
|------|--------|
| `task-sdk/src/airflow/sdk/definitions/callback.py` | Add `from_callable()` factory that converts a Python callable to SyncCallback by resolving import path |
| `task-sdk/src/airflow/sdk/definitions/dag.py` | Modify `on_success_callback` / `on_failure_callback` to accept `SyncCallback | AsyncCallback | Callable`. Auto-wrap callables. Store resolved path. |
| `task-sdk/src/airflow/sdk/execution_time/callback_supervisor.py` | Extend to support context building for on_foo callbacks (fetch context from Execution API) |

### Core Layer (airflow-core/)

| File | Change |
|------|--------|
| `airflow-core/src/airflow/callbacks/callback_requests.py` | Possibly add `import_path` field to `DagCallbackRequest` for resolved callbacks. Or deprecate in favor of `ExecutorCallback`. |
| `airflow-core/src/airflow/callbacks/database_callback_sink.py` | Modify to create `ExecutorCallback` instead of `DagProcessorCallback` when import path is available |
| `airflow-core/src/airflow/models/callback.py` | May need new fields on `ExecutorCallback` to carry on_foo context (dag_id, run_id, callback_type) |
| `airflow-core/src/airflow/models/dagrun.py` | Modify `_execute_dagrun_state_callbacks()` — if callback is importable, create `ExecutorCallback` instead of `DagCallbackRequest` |
| `airflow-core/src/airflow/jobs/scheduler_job_runner.py` | Modify callback handling in `_schedule_dag_run()` and `_process_executor_events()` to queue `ExecuteCallback` workloads for resolved callbacks |
| `airflow-core/src/airflow/executors/workloads/callback.py` | Extend `ExecuteCallback.make()` to support on_foo callback context. Add callback_type field. |
| `airflow-core/src/airflow/executors/base_executor.py` | May need to handle new callback workload origin (on_foo vs deadline) |

### Serialization Layer

| File | Change |
|------|--------|
| `airflow-core/src/airflow/serialization/definitions/dag.py` | Add `on_success_callback_path: str | None` and `on_failure_callback_path: str | None` alongside boolean flags |
| `airflow-core/src/airflow/serialization/serialized_objects.py` | Serialize/deserialize the callback import paths |

### DAG Processing Layer (legacy fallback)

| File | Change |
|------|--------|
| `airflow-core/src/airflow/dag_processing/manager.py` | Add deprecation logging when processing legacy callbacks |
| `airflow-core/src/airflow/dag_processing/processor.py` | Add deprecation logging in `_execute_dag_callbacks()` |

## Phase 2: Task-Level Callbacks

### SDK Layer

| File | Change |
|------|--------|
| `task-sdk/src/airflow/sdk/bases/operator.py` | Modify `on_failure_callback`, `on_retry_callback`, etc. to accept `SyncCallback | Callable`. Auto-wrap. |

### Core Layer

| File | Change |
|------|--------|
| `airflow-core/src/airflow/callbacks/callback_requests.py` | Modify or deprecate `TaskCallbackRequest` |
| `airflow-core/src/airflow/jobs/scheduler_job_runner.py` | Modify task callback handling (`_purge_task_instances_without_heartbeats`) |
| `airflow-core/src/airflow/executors/workloads/callback.py` | Extend for task-level callback context |
| `airflow-core/src/airflow/models/taskinstance.py` | Modify callback triggering to create `ExecutorCallback` |

### Serialization Layer

| File | Change |
|------|--------|
| `airflow-core/src/airflow/serialization/definitions/baseoperator.py` | Add callback path fields |
| `airflow-core/src/airflow/serialization/definitions/mappedoperator.py` | Same for mapped operators |

## Phase 3: Context Building in Workers

### Execution API

| File | Change |
|------|--------|
| `airflow-core/src/airflow/api_fastapi/execution_api/` | New endpoint: `GET /callback-context/{dag_id}/{run_id}/{task_id}` to provide context data for callbacks |

### SDK Layer

| File | Change |
|------|--------|
| `task-sdk/src/airflow/sdk/execution_time/callback_supervisor.py` | Build full template context by calling Execution API |

## Phase 4: Email Callbacks

| File | Change |
|------|--------|
| `airflow-core/src/airflow/callbacks/callback_requests.py` | Deprecate `EmailRequest` |
| `airflow-core/src/airflow/dag_processing/processor.py` | Deprecate `_execute_email_callbacks()` |

## Phase 5: Cleanup (Airflow 4.0)

### Files to Remove

| File | Reason |
|------|--------|
| `airflow-core/src/airflow/callbacks/database_callback_sink.py` | No longer needed — callbacks go through executor |
| `airflow-core/src/airflow/callbacks/base_callback_sink.py` | Base class for removed sink |
| `airflow-core/src/airflow/models/db_callback_request.py` | Alias for removed `DagProcessorCallback` |

### Files to Simplify

| File | Change |
|------|--------|
| `airflow-core/src/airflow/callbacks/callback_requests.py` | Remove `DagCallbackRequest`, `TaskCallbackRequest`, `EmailRequest` |
| `airflow-core/src/airflow/models/callback.py` | Remove `DagProcessorCallback` subclass |
| `airflow-core/src/airflow/dag_processing/manager.py` | Remove `_fetch_callbacks()`, `_add_callback_to_queue()` |
| `airflow-core/src/airflow/dag_processing/processor.py` | Remove `_execute_callbacks()` and all sub-functions |

## Tests

### New Tests Needed

| File | Coverage |
|------|----------|
| `airflow-core/tests/unit/callbacks/test_callback_resolution.py` | Import path resolution from callables |
| `airflow-core/tests/unit/callbacks/test_callback_migration.py` | Auto-wrapping of callables to SyncCallback |
| `airflow-core/tests/unit/executors/test_callback_workloads.py` | ExecuteCallback for on_foo callbacks |
| `task-sdk/tests/task_sdk/execution_time/test_callback_context.py` | Context building in workers |

### Existing Tests to Modify

| File | Change |
|------|--------|
| `airflow-core/tests/unit/jobs/test_scheduler_job.py` | Update callback handling tests |
| `airflow-core/tests/unit/models/test_dagrun.py` | Update callback execution tests |
| `airflow-core/tests/unit/dag_processing/test_processor.py` | Add deprecation warning tests |
| `airflow-core/tests/unit/executors/test_base_executor.py` | Test on_foo callback queueing |
| `airflow-core/tests/unit/executors/test_local_executor.py` | Test on_foo callback execution |

## Dependency Graph

```
Phase 1 (DAG callbacks)
  └── Phase 3 (Context building)  ← needed for correct context in workers
        └── Phase 2 (Task callbacks)  ← more complex context requirements
              └── Phase 4 (Email)  ← lowest priority
                    └── Phase 5 (Cleanup)  ← major version only
```

Note: Phase 3 (Context building) could be started in parallel with Phase 1 if context
compatibility is critical from the start. Otherwise, Phase 1 could ship with a simplified
context and Phase 3 adds full compatibility.
