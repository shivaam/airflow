# Non-Backwards-Compatible Approach (Clean Slate)

## Motivation

If backwards compatibility proves too complex or fragile (especially around context
building and DAG module name mangling), a cleaner path is to deprecate `on_foo_callback`
entirely and introduce a new API.

## Proposed New API

### Option A: `state_callback()` (ferruzzi's suggestion)

```python
from airflow.sdk.definitions.callback import SyncCallback, AsyncCallback, StateCallback

with DAG(
    "my_dag",
    state_callbacks=[
        StateCallback(
            state="failure",
            callback=SyncCallback(path="my_alerts.slack_alert"),
        ),
        StateCallback(
            state="success",
            callback=AsyncCallback(path="my_alerts.webhook_notify"),
        ),
    ],
):
    ...

# Task-level
PythonOperator(
    task_id="etl",
    state_callbacks=[
        StateCallback(state="failure", callback=SyncCallback(path="my_alerts.pagerduty")),
        StateCallback(state="retry", callback=SyncCallback(path="my_alerts.retry_log")),
    ],
)
```

**Pros:**
- Unified API for all callback types (success, failure, retry, execute, skip)
- Consistent with DeadlineAlert pattern
- Import path is explicit — no ambiguity
- Supports both sync and async callbacks

**Cons:**
- Breaking change — every DAG with callbacks needs updating
- More verbose than `on_failure_callback=my_func`
- Learning curve for users accustomed to the simple pattern

### Option B: Accept both callables and Callback objects

```python
# New way (recommended)
with DAG(
    "my_dag",
    on_failure_callback=SyncCallback(path="my_alerts.slack_alert"),
):
    ...

# Old way (deprecated but still works — auto-wrapped)
with DAG(
    "my_dag",
    on_failure_callback=my_alert,  # DeprecationWarning emitted
):
    ...
```

**Pros:**
- Least disruptive — existing parameter names preserved
- Users can migrate at their own pace
- No new API surface to learn beyond SyncCallback/AsyncCallback

**Cons:**
- Dual code paths remain during deprecation period
- Type hints become messy: `Callable | SyncCallback | AsyncCallback | list[...]`

### Option C: Unified `callbacks` parameter

```python
from airflow.sdk.definitions.callback import OnFailure, OnSuccess, OnRetry

with DAG(
    "my_dag",
    callbacks=[
        OnFailure(SyncCallback(path="my_alerts.slack_alert")),
        OnSuccess(AsyncCallback(path="my_alerts.webhook")),
    ],
):
    ...
```

## Deprecation Plan

If going non-backwards-compatible:

### Phase 1: Airflow 3.3
- Introduce `SyncCallback` / `AsyncCallback` as accepted types for `on_foo_callback`
- Emit `DeprecationWarning` when raw callables are passed
- Document migration guide
- Both paths work: new callbacks go through executor, old go through DAG Processor

### Phase 2: Airflow 3.4
- `on_foo_callback` with raw callables emits `FutureWarning`
- New `state_callbacks` parameter available (if Option A chosen)
- DAG Processor callback path marked as "legacy, will be removed in 4.0"

### Phase 3: Airflow 4.0
- Remove support for raw callables in `on_foo_callback`
- Remove `DagCallbackRequest`, `TaskCallbackRequest`, `EmailRequest`
- Remove DAG Processor callback execution code
- Remove `DatabaseCallbackSink`
- Remove `DagProcessorCallback` ORM model
- Only `SyncCallback` / `AsyncCallback` accepted

## Migration Guide Template

```markdown
# Migrating Callbacks to Airflow 4.0

## Before (Airflow 3.x)
```python
def my_alert(context):
    slack.send(f"Failed: {context['task_instance'].task_id}")

with DAG("my_dag", on_failure_callback=my_alert):
    ...
```

## After (Airflow 4.0)

1. Move your callback function to an importable module:

```python
# my_alerts.py (in your DAGs directory or installed package)
def slack_alert(context):
    slack.send(f"Failed: {context['task_instance'].task_id}")
```

2. Reference it by import path:

```python
from airflow.sdk.definitions.callback import SyncCallback

with DAG(
    "my_dag",
    on_failure_callback=SyncCallback(path="my_alerts.slack_alert"),
):
    ...
```
```

## AIP Scope

If an AIP is needed, it should cover:

1. **Motivation**: Why callbacks should not run in the DAG Processor
2. **Proposal**: The specific API change (Option A, B, or C above)
3. **Migration path**: How users transition from old to new
4. **Scheduling priority**: How callbacks are prioritized relative to tasks
5. **Context compatibility**: What happens to the `context` dict
6. **Rejected alternatives**: Other approaches considered and why they were discarded
7. **Timeline**: Which Airflow versions introduce/deprecate/remove
