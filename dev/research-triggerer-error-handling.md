# Research: Triggerer Framework Error Handling

## Overview

When a trigger's `run()` method raises an uncaught exception, the triggerer framework handles it very differently from when `run()` yields a `TriggerEvent`. This difference is the root cause of the GlueJobOperator deferred status issue.

---

## 1. Two Paths: Yield TriggerEvent vs Raise Exception

### Path A: trigger.run() yields a TriggerEvent (GOOD)
```
trigger.run() yields TriggerEvent({"status": "error", "message": "..."})
  → Triggerer sends event to scheduler
    → Scheduler calls Trigger.submit_event(trigger_id, event)
      → TaskInstance.next_method = "execute_complete"
      → TaskInstance.next_kwargs = {"event": {"status": "error", "message": "..."}}
      → TaskInstance rescheduled → Worker runs execute_complete(event=...)
        → Operator can inspect event["message"] and raise with details
```

### Path B: trigger.run() raises an exception (BAD — generic error)
```
trigger.run() raises AirflowException("Glue job FAILED...")
  → Triggerer catches it in cleanup_finished_triggers()
    → trigger exited with 0 events → added to failed_triggers
      → Trigger.submit_failure(trigger_id, exc)
        → TaskInstance.next_method = "__fail__"  (TRIGGER_FAIL_REPR)
        → TaskInstance.next_kwargs = {
              "error": TriggerFailureReason.TRIGGER_FAILURE,
              "traceback": formatted_traceback
          }
        → TaskInstance rescheduled → Worker runs resume_execution()
          → Sees next_method == "__fail__" → raises TaskDeferralError("Trigger failure")
```

**Key difference:** In Path B, `execute_complete` is NEVER called. The task fails with a generic `TaskDeferralError("Trigger failure")` message, and the original exception details are only in the traceback logs — not in the error message visible to `on_failure_callback`.

---

## 2. TriggerFailureReason Enum

**File:** `airflow-core/src/airflow/models/trigger.py` (line 62)

```python
class TriggerFailureReason(str, Enum):
    TRIGGER_TIMEOUT = "Trigger timeout"
    TRIGGER_FAILURE = "Trigger failure"
```

Also mirrored in `task-sdk/src/airflow/sdk/bases/operator.py` (line 114).

Two failure reasons:
- `TRIGGER_TIMEOUT` — Task exceeded its `execution_timeout` while deferred
- `TRIGGER_FAILURE` — Trigger's `run()` raised an exception without yielding an event

---

## 3. Triggerer Job Runner — Exception Handling

**File:** `airflow-core/src/airflow/jobs/triggerer_job_runner.py`

### run_trigger() (line ~1171)
```python
async def run_trigger(trigger_id, trigger):
    """Run a trigger and push events to the trigger's event list."""
    async for event in trigger.run():
        # sends event to trigger_events list
        ...
```

No try/except here — exceptions from `trigger.run()` propagate up as task failures.

### cleanup_finished_triggers() (line ~1024)
```python
for trigger_id, details in finished:
    try:
        details["task"].result()
    except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
        ...  # expected exceptions
    except BaseException as exc:
        saved_exc = exc  # saved for later
        self.log.error("Trigger failed", ...)

    if details["events"] == 0:
        # Trigger exited without sending an event → FAILURE
        self.failed_triggers.append((trigger_id, saved_exc))
```

### process_trigger_events() (line ~1072)
Failed triggers get formatted into `TriggerStateChanges.failures`:
```python
failures = [
    (trigger_id, format_exception(type(exc), exc, exc.__traceback__) if exc else None)
    for trigger_id, exc in self.failed_triggers
]
```

---

## 4. Trigger.submit_failure() — The Failure Path

**File:** `airflow-core/src/airflow/models/trigger.py` (line 291)

```python
@classmethod
def submit_failure(cls, trigger_id, exc=None, session=...):
    for task_instance in session.scalars(
        select(TaskInstance).where(
            TaskInstance.trigger_id == trigger_id,
            TaskInstance.state == TaskInstanceState.DEFERRED
        )
    ):
        if isinstance(exc, BaseException):
            traceback = format_exception(type(exc), exc, exc.__traceback__)
        else:
            traceback = exc
        task_instance.next_method = TRIGGER_FAIL_REPR  # "__fail__"
        task_instance.next_kwargs = {
            "error": TriggerFailureReason.TRIGGER_FAILURE,
            "traceback": traceback,
        }
        task_instance.trigger_id = None
        task_instance.state = TaskInstanceState.SCHEDULED
```

---

## 5. Worker-Side: resume_execution()

**File:** `task-sdk/src/airflow/sdk/bases/operator.py` (line ~1651)

```python
def resume_execution(self, next_method, next_kwargs, context):
    if next_method == TRIGGER_FAIL_REPR:
        next_kwargs = next_kwargs or {}
        traceback = next_kwargs.get("traceback")
        if traceback is not None:
            self.log.error("Trigger failed:\n%s", "\n".join(traceback))
        if (error := next_kwargs.get("error", "Unknown")) == TriggerFailureReason.TRIGGER_TIMEOUT:
            raise TaskDeferralTimeout(error)
        raise TaskDeferralError(error)  # "Trigger failure" — generic!
```

The traceback IS logged, but the error raised is just `TaskDeferralError("Trigger failure")` — no details about what actually went wrong.

---

## 6. Why This Matters for on_failure_callback

When a task fails via Path B:
- `context["exception"]` = `TaskDeferralError("Trigger failure")`
- The actual Glue error (e.g., "FAILED - Script failed with exit code 1") is buried in task logs
- Any alerting/monitoring built on `on_failure_callback` gets a useless message

When a task fails via Path A:
- `execute_complete` receives `event["message"]` with the actual error
- Operator can raise a specific exception: `AirflowException(f"Glue job failed: {event['message']}")`
- `context["exception"]` contains the meaningful error message

---

## Conclusion

The fix in `AwsBaseWaiterTrigger.run()` converts Path B failures into Path A by catching `AirflowException` from `async_wait()` and yielding a `TriggerEvent` with the error details. This ensures `execute_complete` is always called and the operator can surface meaningful error messages.

## Files Referenced

1. `airflow-core/src/airflow/models/trigger.py` — `TriggerFailureReason`, `submit_failure()`, `submit_event()`
2. `airflow-core/src/airflow/jobs/triggerer_job_runner.py` — `run_trigger()`, `cleanup_finished_triggers()`
3. `task-sdk/src/airflow/sdk/bases/operator.py` — `resume_execution()`, `TRIGGER_FAIL_REPR`
