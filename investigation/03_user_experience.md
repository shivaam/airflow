# User Experience: Current vs. Desired

## Current UX (Legacy on_foo_callback)

### How Users Define Callbacks Today

```python
# Pattern 1: Module-level function (most common)
def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    slack.send(f"Task {task_id} in {dag_id} failed!")

# Pattern 2: Lambda (common for simple cases)
dag = DAG("my_dag", on_failure_callback=lambda ctx: print(ctx["dag"].dag_id))

# Pattern 3: Notifier class instance (recommended modern pattern)
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
dag = DAG("my_dag", on_failure_callback=SlackNotifier(text="DAG failed"))

# Pattern 4: List of callbacks
dag = DAG(
    "my_dag",
    on_failure_callback=[alert_on_failure, SlackNotifier(text="backup alert")],
)

# Pattern 5: Inline function defined in DAG file
with DAG("my_dag") as dag:
    def my_inline_alert(context):
        print("inline alert")
    dag.on_failure_callback = my_inline_alert
```

### What Users Expect

1. Callback fires when the state changes (success, failure, retry)
2. Callback receives a `context` dict with task/DAG info, XCom, params, macros
3. Callback exceptions don't crash the DAG — they're logged and metrics are emitted
4. Callbacks work regardless of executor type (Local, Celery, ECS, K8s)
5. No special imports or framework knowledge needed — just pass a callable

### Current Pain Points Users May Not Be Aware Of

- Callbacks run in the DAG Processor, not in a worker — resource contention with DAG parsing
- A slow callback blocks DAG parsing for that file
- On container executors (ECS, K8s), callbacks still run on the DAG Processor host,
  not in the container — breaking isolation assumptions
- No retry mechanism for failed callbacks
- No visibility into callback execution (no UI, no logs separate from DAG processor logs)

## Desired UX (After Migration)

### Backwards-Compatible Path (Must Work)

Existing DAGs must continue to work unchanged:

```python
# This MUST still work in 3.3+
def my_alert(context):
    print(f"Failed: {context['task_instance'].task_id}")

with DAG("my_dag", on_failure_callback=my_alert):
    ...
```

Behind the scenes, Airflow would:
1. Detect that `my_alert` is an importable function
2. Resolve its import path: `my_dag_file.my_alert`
3. Wrap it in a `SyncCallback(path="my_dag_file.my_alert")`
4. Execute via executor (not DAG Processor)

### New Recommended API (Optional Enhancement)

```python
from airflow.sdk.definitions.callback import SyncCallback, AsyncCallback

# Explicit import path — clean, no ambiguity
with DAG(
    "my_dag",
    on_failure_callback=SyncCallback(path="my_alerts.slack_alert"),
    on_success_callback=AsyncCallback(path="my_alerts.async_webhook"),
):
    ...

# Or with the proposed new API (if AIP approved):
with DAG(
    "my_dag",
    state_callbacks=[
        StateCallback(state="failure", callback=SyncCallback(path="my_alerts.slack_alert")),
        StateCallback(state="success", callback=AsyncCallback(path="my_alerts.async_webhook")),
    ],
):
    ...
```

### What Must NOT Break

| Feature | Status |
|---------|--------|
| `on_success_callback=callable` on DAGs | Must work |
| `on_failure_callback=callable` on DAGs | Must work |
| `on_failure_callback=callable` on tasks | Must work |
| `on_retry_callback=callable` on tasks | Must work |
| `on_execute_callback=callable` on tasks | Must work |
| `on_skipped_callback=callable` on tasks | Must work |
| Lambda callbacks | Must work (may fall back to DAG Processor) |
| List of callbacks | Must work |
| Notifier instances | Must work (already importable) |
| Context dict contents | Must be identical |
| Callback exceptions don't crash DAGs | Must preserve |

### UX Improvements After Migration

1. **Callbacks run in workers** — proper resource isolation
2. **Visible in UI** — callback execution tracked in callback table with state
3. **Retry capability** — executor can retry failed callbacks
4. **Container-native** — callbacks run in the same container environment as tasks
5. **Consistent with DeadlineAlerts** — one callback framework, not two
6. **Priority control** — callbacks can be prioritized relative to tasks
