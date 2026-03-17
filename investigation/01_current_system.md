# Current (Legacy) Callback System

## User-Facing API

Users define callbacks as Python callables on DAGs and tasks:

```python
# DAG-level callbacks
def on_dag_failure(context):
    slack.send(f"DAG {context['dag_id']} failed!")

with DAG(
    "my_dag",
    on_failure_callback=on_dag_failure,
    on_success_callback=lambda ctx: print("done"),
):
    ...

# Task-level callbacks
def on_task_failure(context):
    pagerduty.alert(context)

my_task = PythonOperator(
    task_id="etl",
    on_failure_callback=on_task_failure,
    on_retry_callback=on_task_failure,
    on_success_callback=[notify_slack, update_dashboard],  # lists supported
)
```

Supported callback types:
- **DAG**: `on_success_callback`, `on_failure_callback`
- **Task**: `on_success_callback`, `on_failure_callback`, `on_retry_callback`,
  `on_execute_callback`, `on_skipped_callback`
- **Email**: `email_on_failure`, `email_on_retry` (deprecated, use Notifiers)

Callbacks can be:
- Module-level functions
- Lambda expressions
- Notifier class instances (e.g., `SlackWebhookNotifier(...)`)
- Lists of any of the above

## Serialization

**Key insight**: The actual callable is **never serialized**. Only boolean flags are stored.

**File**: `airflow-core/src/airflow/serialization/definitions/dag.py`
```python
class SerializedDAG:
    has_on_success_callback: bool = False
    has_on_failure_callback: bool = False
```

**File**: `airflow-core/src/airflow/serialization/definitions/baseoperator.py`
```python
class SerializedBaseOperator:
    has_on_success_callback: bool = False
    has_on_failure_callback: bool = False
    has_on_retry_callback: bool = False
    has_on_execute_callback: bool = False
    has_on_skipped_callback: bool = False
```

The scheduler reads these flags to know *whether* a callback exists, but it never has
access to the actual callable. Only the DAG Processor, which re-parses the DAG file,
can resolve the callable.

## Callback Request Classes

**File**: `airflow-core/src/airflow/callbacks/callback_requests.py`

```
BaseCallbackRequest
  ├── filepath: str            # path to DAG file
  ├── bundle_name: str         # which DAG bundle
  ├── bundle_version: str      # bundle version
  └── msg: str                 # logging message

DagCallbackRequest(BaseCallbackRequest)
  ├── dag_id: str
  ├── run_id: str
  ├── context_from_server: DagRunContext   # dag_run model, last_ti
  └── is_failure_callback: bool

TaskCallbackRequest(BaseCallbackRequest)
  ├── ti: TaskInstance          # simplified TI data model
  ├── task_callback_type: TaskInstanceState  # FAILED, UP_FOR_RETRY, UPSTREAM_FAILED
  └── context_from_server: TIRunContext     # task execution context

EmailRequest(BaseCallbackRequest)
  ├── ti: TaskInstance
  ├── email_type: "failure" | "retry"
  └── context_from_server: TIRunContext
```

## Complete Flow

```
USER CODE (my_dag.py)
│  Define: dag.on_failure_callback = my_alert
│
▼
DAG PARSING (DagFileProcessorProcess)
│  Parse DAG file → serialize → store in metadata DB
│  SerializedDAG has_on_failure_callback = True
│  Actual callable stays in memory (not serialized)
│
▼
SCHEDULER
│  Reads serialized DAG (boolean flags only)
│  DagRun.update_state() detects failure
│  Calls produce_dag_callback(execute=False)
│  Creates DagCallbackRequest(dag_id, run_id, filepath, context, is_failure=True)
│
▼
CALLBACK SINK (DatabaseCallbackSink)
│  executor.send_callback(request)
│  executor.callback_sink.send(request)
│  Creates DagProcessorCallback ORM record
│  Stores JSON-serialized request in `callback` table
│
▼
DAG FILE PROCESSOR MANAGER
│  _fetch_callbacks(): queries callback table, ordered by priority_weight
│  Fetches up to max_callbacks_per_loop (~50)
│  Deserializes each request
│  Adds to _callback_to_execute[file_info] queue
│  Deletes from DB
│
▼
DAG FILE PROCESSOR PROCESS (subprocess)
│  Receives callback requests list
│  _execute_callbacks() dispatcher:
│    ├── TaskCallbackRequest → _execute_task_callbacks()
│    ├── DagCallbackRequest → _execute_dag_callbacks()
│    └── EmailRequest → _execute_email_callbacks()
│
▼
CALLBACK EXECUTION
│  1. Load DAG from bundle (re-imports the DAG file)
│  2. Resolve actual callable from DAG/task attribute
│  3. Build RuntimeTaskInstance context
│  4. Call: my_alert(context)
│  5. Log result, emit metrics (dag.callback_exceptions)
```

## Context Building

When a callback is executed, the DAG Processor builds a context dict containing:

- **DAG Run info**: dag_run, run_id, logical_date, data_interval_start/end
- **Task Instance info**: task_id, try_number, max_tries, state, start_date, end_date
- **DAG instance**: The actual DAG object
- **Macros**: Airflow template macros (ds, ds_nodash, etc.)
- **Params**: DAG run conf + task params
- **XCom**: Access to cross-communication values
- **Variables**: Airflow variables

For DAG callbacks, a "representative" task instance is selected:
- On failure: the latest failed task
- On timeout: the latest started-but-not-finished task
- On success: the latest succeeded task

## Key Files

| File | Role |
|------|------|
| `airflow-core/src/airflow/callbacks/callback_requests.py` | Callback request Pydantic models |
| `airflow-core/src/airflow/callbacks/database_callback_sink.py` | Writes requests to DB |
| `airflow-core/src/airflow/models/db_callback_request.py` | ORM alias for DagProcessorCallback |
| `airflow-core/src/airflow/models/callback.py` | Callback ORM (DagProcessorCallback subclass) |
| `airflow-core/src/airflow/dag_processing/manager.py` | Fetches callbacks from DB, dispatches to processors |
| `airflow-core/src/airflow/dag_processing/processor.py` | Executes callbacks in subprocess |
| `airflow-core/src/airflow/jobs/scheduler_job_runner.py` | Creates callback requests |
| `airflow-core/src/airflow/models/dagrun.py` | Triggers DAG callbacks via update_state() |
| `airflow-core/src/airflow/serialization/definitions/dag.py` | Serialized DAG with boolean flags |
| `airflow-core/src/airflow/serialization/definitions/baseoperator.py` | Serialized operator with boolean flags |
