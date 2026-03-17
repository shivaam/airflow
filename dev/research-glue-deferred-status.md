# Fix: GlueJobOperator in deferred mode loses detailed failure status

## Context

**Problem:** When `GlueJobOperator` runs with `deferrable=True` and the Glue job fails, users see only a generic "Trigger failure" message instead of the actual Glue error (e.g., "FAILED - Script failed with exit code 1"). The `on_failure_callback` lacks actionable error details.

**Root cause:** `AwsBaseWaiterTrigger.run()` (line 143-161 in `triggers/base.py`) does NOT catch exceptions from `async_wait()`. When `async_wait()` detects a terminal failure (FAILED/STOPPED/ERROR/TIMEOUT), it raises `AirflowException`. This exception propagates **uncaught** from `run()` — the triggerer framework catches it generically and marks the task failed with `TriggerFailureReason.TRIGGER_FAILURE` ("Trigger failure"). **`execute_complete` is never called**, so the operator cannot extract or surface error details.

**Precedent:** Several AWS triggers already fix this individually by overriding `run()`:
- `EksCreateClusterTrigger` (`triggers/eks.py:72-89`) — catches `AirflowException`, yields `{"status": "failed"}`
- `EmrServerlessStartJobTrigger` (`triggers/emr.py:446-490`) — catches `Exception`, yields `{"status": "failure", "message": ...}`
- `S3KeyTrigger` (`triggers/s3.py:149-150`) — catches `Exception`, yields `{"status": "error", "message": ...}`

The fix belongs in the **base class** so all `AwsBaseWaiterTrigger` subclasses benefit automatically.

---

## Changes

### 1. `providers/amazon/src/airflow/providers/amazon/aws/triggers/base.py` — `AwsBaseWaiterTrigger.run()`

Wrap `async_wait()` in try/except to catch `AirflowException` and yield an error `TriggerEvent` instead of propagating:

```python
async def run(self) -> AsyncIterator[TriggerEvent]:
    hook = self.hook()
    async with await hook.get_async_conn() as client:
        waiter = hook.get_waiter(
            self.waiter_name,
            deferrable=True,
            client=client,
            config_overrides=self.waiter_config_overrides,
        )
        try:
            await async_wait(
                waiter,
                self.waiter_delay,
                self.attempts,
                self.waiter_args,
                self.failure_message,
                self.status_message,
                self.status_queries,
            )
        except AirflowException as e:
            yield TriggerEvent({"status": "error", "message": str(e), self.return_key: self.return_value})
        else:
            yield TriggerEvent({"status": "success", self.return_key: self.return_value})
```

**Impact:** Affects ALL `AwsBaseWaiterTrigger` subclasses. This is safe because every `execute_complete` already checks `status != "success"` and raises — they just never received non-success events before. Now they will, with the detailed error message from `async_wait()`.

### 2. `providers/amazon/src/airflow/providers/amazon/aws/triggers/glue.py` — `GlueJobCompleteTrigger`

Add `JobRun.ErrorMessage` to `status_queries` so the Glue error message is captured in `async_wait()`'s exception message:

```python
status_queries=["JobRun.JobRunState", "JobRun.ErrorMessage"],
```

Currently only `["JobRun.JobRunState"]` is queried — the actual error text is lost.

### 3. Tests

#### `providers/amazon/tests/unit/amazon/aws/triggers/test_base.py`
- Add test: when `async_wait` raises `AirflowException`, trigger yields `TriggerEvent` with `status="error"`, `message` containing error text, and `return_key`/`return_value`

#### `providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py`
- Verify `GlueJobCompleteTrigger` includes `JobRun.ErrorMessage` in `status_queries`
- Update the existing `test_wait_job_failed` to verify it now yields an error event instead of raising

---

## Files to modify

1. `providers/amazon/src/airflow/providers/amazon/aws/triggers/base.py` (lines 143-161)
2. `providers/amazon/src/airflow/providers/amazon/aws/triggers/glue.py` (line 68)
3. `providers/amazon/tests/unit/amazon/aws/triggers/test_base.py`
4. `providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py`

## Verification

1. Format & lint: `uv run ruff format <files> && uv run ruff check --fix <files>`
2. Run base trigger tests: `uv run --project providers/amazon pytest providers/amazon/tests/unit/amazon/aws/triggers/test_base.py -xvs`
3. Run Glue trigger tests: `uv run --project providers/amazon pytest providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py -xvs`
4. Run Glue operator tests: `uv run --project providers/amazon pytest providers/amazon/tests/unit/amazon/aws/operators/test_glue.py -xvs`
5. Run static checks: `prek run --from-ref main --stage pre-commit`
