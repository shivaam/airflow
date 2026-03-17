# New Callback System (SyncCallback / AsyncCallback / ExecuteCallback)

## User-Facing API

Introduced for DeadlineAlerts in Airflow 3.2:

```python
from airflow.sdk.definitions.callback import SyncCallback, AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DagRunLogicalDateDeadline

def my_sync_alert(context, **kwargs):
    slack.send(f"Deadline missed for {context['dag_run'].dag_id}")

async def my_async_alert(context, **kwargs):
    await aiohttp.post("https://webhook.site/...", json=context)

with DAG(
    "my_dag",
    deadline_alerts=[
        DeadlineAlert(
            reference=DagRunLogicalDateDeadline(),
            interval=timedelta(hours=2),
            callback=SyncCallback(path="my_module.my_sync_alert"),
        ),
        DeadlineAlert(
            reference=DagRunQueuedAtDeadline(),
            interval=timedelta(minutes=30),
            callback=AsyncCallback(path="my_module.my_async_alert"),
        ),
    ],
):
    ...
```

Key difference from legacy: callbacks are identified by **import path strings**, not
raw Python callables.

## SDK Callback Definitions

**File**: `task-sdk/src/airflow/sdk/definitions/callback.py`

```
Callback (abstract base)
  ├── path: str          # dotted import path (e.g., "my_module.alert_func")
  ├── kwargs: dict       # additional arguments to pass
  ├── serialize() → dict
  └── deserialize(data, version) → Callback

SyncCallback(Callback)
  ├── executor: str | None   # target specific executor (optional)
  └── Runs in executor worker (sync)

AsyncCallback(Callback)
  └── Runs in triggerer (async/await)
```

The `Callback` base validates that `path` resolves to an importable callable.
Lambda functions are explicitly not supported (noted as a TODO).

## ORM Models

**File**: `airflow-core/src/airflow/models/callback.py`

Polymorphic inheritance from base `Callback` ORM model:

```
Callback (Base, BaseWorkload)
  ├── id: UUID
  ├── type: CallbackType       # EXECUTOR, TRIGGERER, DAG_PROCESSOR
  ├── fetch_method: CallbackFetchMethod  # IMPORT_PATH or DAG_ATTRIBUTE
  ├── state: CallbackState     # SCHEDULED → PENDING → QUEUED → RUNNING → SUCCESS/FAILED
  ├── data: dict               # path, kwargs, executor, context
  ├── priority_weight: int
  ├── output: str | None
  └── created_at / updated_at

ExecutorCallback(Callback)
  ├── type = EXECUTOR
  ├── fetch_method = IMPORT_PATH
  └── data contains: path, kwargs, executor name

TriggererCallback(Callback)
  ├── type = TRIGGERER
  ├── fetch_method = IMPORT_PATH
  ├── trigger: Trigger (relationship)
  └── queue() creates CallbackTrigger

DagProcessorCallback(Callback)
  ├── type = DAG_PROCESSOR
  ├── fetch_method = DAG_ATTRIBUTE
  └── data contains: req_class, req_data (legacy format)
```

Factory: `Callback.create_from_sdk_def(callback_def)` converts SDK definitions to ORM models.

## Executor Workload

**File**: `airflow-core/src/airflow/executors/workloads/callback.py`

```python
class ExecuteCallback(BaseDagBundleWorkload):
    callback: CallbackDTO       # id, fetch_method, data
    type: Literal["ExecuteCallback"]

    @classmethod
    def make(cls, callback, dag_run, generator, ...) -> ExecuteCallback
```

`CallbackDTO` is a lightweight Pydantic model containing:
- `id`: UUID
- `fetch_method`: IMPORT_PATH or DAG_ATTRIBUTE
- `data`: dict with `path`, `kwargs`, `executor`, metadata

## Complete Flow (SyncCallback via Executor)

```
USER CODE
│  DeadlineAlert(callback=SyncCallback(path="alerts.slack_alert"))
│
▼
DAG PARSING
│  DeadlineAlert serialized → Deadline ORM created when DAG run starts
│  Callback ORM (ExecutorCallback) created with state=SCHEDULED
│
▼
SCHEDULER
│  Detects missed deadline: deadline_time < now() and missed=False
│  Calls deadline.handle_miss(session)
│    → Enriches callback with context (dag_run info, deadline metadata)
│    → Sets callback state to PENDING
│
▼
SCHEDULER (callback queuing)
│  Queries ExecutorCallback with state=PENDING
│  Creates ExecuteCallback workload via ExecuteCallback.make()
│  Calls executor.queue_workload(workload, session)
│  Sets callback state to QUEUED
│
▼
EXECUTOR (e.g., LocalExecutor, ECS, Celery)
│  Worker receives ExecuteCallback workload
│  Dispatches to supervise_callback()
│
▼
CALLBACK SUPERVISOR (subprocess)
│  File: task-sdk/src/airflow/sdk/execution_time/callback_supervisor.py
│  Forks subprocess
│  Sets up bundle sys.path
│  Imports callback: import_string("alerts.slack_alert")
│  Calls: slack_alert(context=enriched_context)
│
▼
RESULT
│  Executor updates ExecutorCallback state to SUCCESS or FAILED
│  Records output/error
│  Emits metrics
```

## Complete Flow (AsyncCallback via Triggerer)

```
SCHEDULER
│  Detects missed deadline
│  deadline.handle_miss() → TriggererCallback state = PENDING
│
▼
SCHEDULER (callback queuing)
│  TriggererCallback.queue() creates CallbackTrigger
│  Associates trigger with callback
│
▼
TRIGGERER
│  CallbackTrigger.run():
│    callback = import_string("alerts.async_alert")
│    result = await callback(**kwargs, context=context)
│  Yields TriggerEvent(SUCCESS/FAILED)
│
▼
TRIGGERER (event handling)
│  TriggererCallback.handle_event() → state = SUCCESS/FAILED
```

## Callback Context (New System)

When `Deadline.handle_miss()` fires:

```python
context = {
    "dag_run": {DAGRunResponse},      # serialized DAGRun data
    "deadline": {
        "id": UUID,
        "deadline_time": datetime,
    },
}
```

Note: This is a **much simpler context** than the legacy system. Legacy callbacks get
the full Airflow template context (macros, XCom, params, etc.). The new system passes
only what's explicitly included in kwargs.

## Key Files

| File | Role |
|------|------|
| `task-sdk/src/airflow/sdk/definitions/callback.py` | SyncCallback, AsyncCallback definitions |
| `task-sdk/src/airflow/sdk/definitions/deadline.py` | DeadlineAlert integration |
| `task-sdk/src/airflow/sdk/execution_time/callback_supervisor.py` | Worker-side subprocess execution |
| `airflow-core/src/airflow/models/callback.py` | ORM models (ExecutorCallback, TriggererCallback) |
| `airflow-core/src/airflow/models/deadline.py` | Deadline ORM, handle_miss() |
| `airflow-core/src/airflow/executors/workloads/callback.py` | ExecuteCallback workload DTO |
| `airflow-core/src/airflow/executors/base_executor.py` | Base executor callback queueing |
| `airflow-core/src/airflow/triggers/callback.py` | CallbackTrigger for async path |

## Executor Support Matrix

| Executor | supports_callbacks | Status |
|----------|--------------------|--------|
| LocalExecutor | Yes | Implemented |
| CeleryExecutor | Yes | Implemented (3.2+) |
| ECS Executor | Yes | In progress (our branch) |
| Batch Executor | Planned | Not yet |
| KubernetesExecutor | Planned | Not yet |
