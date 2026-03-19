# Implementation Plan: Glue Deferrable Verbose Logging

**Approach:** Option A — ECS-style async `get_log_events` in the trigger

---

## Step 1: Fix the Glue test script for better log visibility

**File:** `glue_test/test_glue_script.py`

The current script uses `print()` which Python buffers when not connected to a TTY.
Glue pythonshell jobs don't have a TTY, so all output gets buffered and flushed at the
end (or when the buffer fills at ~4KB). This makes it look like logs only appear at the end.

**Change:** Use `sys.stderr` or `flush=True` so logs appear incrementally in CloudWatch.

```python
import sys
import time

print("Glue job started", flush=True)
for i in range(20):
    print(f"Processing step {i + 1}/20...", flush=True)
    # Also write to stderr (Glue flushes stderr more aggressively)
    print(f"[stderr] Step {i + 1}/20 debug info", file=sys.stderr, flush=True)
    time.sleep(15)
print("Glue job completed successfully", flush=True)
```

---

## Step 2: Override `run()` in `GlueJobCompleteTrigger`

**File:** `providers/amazon/src/airflow/providers/amazon/aws/triggers/glue.py`

Add two methods to `GlueJobCompleteTrigger`:

### `run()` — override the inherited waiter-only loop

When `verbose=False`: delegate to `super().run()` (zero behavior change).
When `verbose=True`: custom poll loop that checks state + fetches logs.

```python
async def run(self) -> AsyncIterator[TriggerEvent]:
    if not self.verbose:
        async for event in super().run():
            yield event
        return

    hook = self.hook()
    async with (
        await hook.get_async_conn() as glue_client,
        await AwsLogsHook(
            aws_conn_id=self.aws_conn_id, region_name=self.region_name
        ).get_async_conn() as logs_client,
    ):
        # One-time: get log group name from job run metadata
        job_run_resp = await glue_client.get_job_run(
            JobName=self.job_name, RunId=self.run_id
        )
        log_group_prefix = job_run_resp["JobRun"].get("LogGroupName", "/aws-glue/jobs")
        log_group_output = f"{log_group_prefix}/{DEFAULT_LOG_SUFFIX}"
        log_group_error = f"{log_group_prefix}/{ERROR_LOG_SUFFIX}"

        output_token: str | None = None
        error_token: str | None = None

        for _attempt in range(self.attempts):
            # Check job state
            resp = await glue_client.get_job_run(
                JobName=self.job_name, RunId=self.run_id
            )
            job_run_state = resp["JobRun"]["JobRunState"]

            # Fetch and print logs from both streams
            output_token = await self._forward_logs(
                logs_client, log_group_output, self.run_id, output_token
            )
            error_token = await self._forward_logs(
                logs_client, log_group_error, self.run_id, error_token
            )

            if job_run_state in ("FAILED", "TIMEOUT"):
                raise AirflowException(
                    f"Exiting Job {self.run_id} Run State: {job_run_state}"
                )
            if job_run_state in ("SUCCEEDED", "STOPPED"):
                self.log.info(
                    "Exiting Job %s Run State: %s", self.run_id, job_run_state
                )
                yield TriggerEvent({"status": "success", self.return_key: self.return_value})
                return

            self.log.info(
                "Polling for AWS Glue Job %s current run state: %s",
                self.job_name, job_run_state,
            )
            await asyncio.sleep(self.waiter_delay)

        raise AirflowException("Waiter exceeded max attempts")
```

### `_forward_logs()` — fetch new events from one CloudWatch stream

Directly modeled on ECS `TaskDoneTrigger._forward_logs()`.

```python
async def _forward_logs(
    self,
    logs_client,
    log_group: str,
    log_stream: str,
    next_token: str | None,
) -> str | None:
    """Read new CloudWatch log events and print them. Returns updated token."""
    while True:
        token_arg = {"nextToken": next_token} if next_token else {}
        try:
            response = await logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                startFromHead=True,
                **token_arg,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                self.log.info("No logs yet in %s/%s", log_group, log_stream)
                return None
            raise

        events = response["events"]
        for event in events:
            self.log.info(event["message"])

        if not events or next_token == response["nextForwardToken"]:
            return response["nextForwardToken"]
        next_token = response["nextForwardToken"]
```

### New imports needed at top of file

```python
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.hooks.glue import DEFAULT_LOG_SUFFIX, ERROR_LOG_SUFFIX
```

---

## Step 3: Add unit tests

**File:** `providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py`

Add these tests to `TestGlueJobTrigger`:

### Test 1: `test_verbose_false_delegates_to_super`
Verify that `verbose=False` still uses the waiter path (same as existing `test_wait_job`).
This is already covered by the existing test — just confirm it still passes.

### Test 2: `test_verbose_run_success`
Mock both `GlueJobHook.get_async_conn` and `AwsLogsHook.get_async_conn`.
Mock `get_job_run` to return RUNNING once then SUCCEEDED.
Mock `get_log_events` to return some events.
Assert: TriggerEvent with success, log.info called with event messages.

```python
@pytest.mark.asyncio
@mock.patch.object(AwsLogsHook, "get_async_conn")
@mock.patch.object(GlueJobHook, "get_async_conn")
async def test_verbose_run_success(self, mock_glue_conn, mock_logs_conn):
    # Setup Glue client mock
    glue_client = AsyncMock()
    glue_client.get_job_run = AsyncMock(side_effect=[
        {"JobRun": {"JobRunState": "RUNNING", "LogGroupName": "/aws-glue/python-jobs"}},
        {"JobRun": {"JobRunState": "SUCCEEDED", "LogGroupName": "/aws-glue/python-jobs"}},
    ])
    mock_glue_conn.return_value.__aenter__.return_value = glue_client

    # Setup CloudWatch Logs client mock
    logs_client = AsyncMock()
    logs_client.get_log_events = AsyncMock(return_value={
        "events": [{"timestamp": 1234, "message": "Processing step 1"}],
        "nextForwardToken": "token_1",
    })
    mock_logs_conn.return_value.__aenter__.return_value = logs_client

    trigger = GlueJobCompleteTrigger(
        job_name="job_name", run_id="jr_123", verbose=True,
        aws_conn_id="aws_conn_id", waiter_delay=0, waiter_max_attempts=5,
    )
    generator = trigger.run()
    event = await generator.asend(None)

    assert event.payload["status"] == "success"
    # Logs client was called (both output and error streams)
    assert logs_client.get_log_events.call_count >= 2
```

### Test 3: `test_verbose_run_job_failed`
Mock `get_job_run` to return FAILED.
Assert: raises AirflowException.

```python
@pytest.mark.asyncio
@mock.patch.object(AwsLogsHook, "get_async_conn")
@mock.patch.object(GlueJobHook, "get_async_conn")
async def test_verbose_run_job_failed(self, mock_glue_conn, mock_logs_conn):
    glue_client = AsyncMock()
    glue_client.get_job_run = AsyncMock(return_value={
        "JobRun": {"JobRunState": "FAILED", "LogGroupName": "/aws-glue/python-jobs"}
    })
    mock_glue_conn.return_value.__aenter__.return_value = glue_client

    logs_client = AsyncMock()
    logs_client.get_log_events = AsyncMock(return_value={
        "events": [], "nextForwardToken": "token_1",
    })
    mock_logs_conn.return_value.__aenter__.return_value = logs_client

    trigger = GlueJobCompleteTrigger(
        job_name="job_name", run_id="jr_123", verbose=True,
        aws_conn_id="aws_conn_id", waiter_delay=0, waiter_max_attempts=5,
    )
    with pytest.raises(AirflowException, match="FAILED"):
        await trigger.run().__anext__()
```

### Test 4: `test_verbose_run_max_attempts_exceeded`
Mock `get_job_run` to always return RUNNING.
Set `waiter_max_attempts=2`.
Assert: raises AirflowException about max attempts.

```python
@pytest.mark.asyncio
@mock.patch.object(AwsLogsHook, "get_async_conn")
@mock.patch.object(GlueJobHook, "get_async_conn")
async def test_verbose_run_max_attempts(self, mock_glue_conn, mock_logs_conn):
    glue_client = AsyncMock()
    glue_client.get_job_run = AsyncMock(return_value={
        "JobRun": {"JobRunState": "RUNNING", "LogGroupName": "/aws-glue/python-jobs"}
    })
    mock_glue_conn.return_value.__aenter__.return_value = glue_client

    logs_client = AsyncMock()
    logs_client.get_log_events = AsyncMock(return_value={
        "events": [], "nextForwardToken": "token_1",
    })
    mock_logs_conn.return_value.__aenter__.return_value = logs_client

    trigger = GlueJobCompleteTrigger(
        job_name="job_name", run_id="jr_123", verbose=True,
        aws_conn_id="aws_conn_id", waiter_delay=0, waiter_max_attempts=2,
    )
    with pytest.raises(AirflowException, match="max attempts"):
        await trigger.run().__anext__()
```

### Test 5: `test_forward_logs_resource_not_found`
Test that `_forward_logs` handles `ResourceNotFoundException` gracefully
(returns None, logs a message, doesn't crash).

```python
@pytest.mark.asyncio
async def test_forward_logs_resource_not_found(self):
    logs_client = AsyncMock()
    logs_client.get_log_events = AsyncMock(side_effect=ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetLogEvents",
    ))

    trigger = GlueJobCompleteTrigger(
        job_name="job_name", run_id="jr_123", verbose=True,
        aws_conn_id="aws_conn_id", waiter_delay=0, waiter_max_attempts=5,
    )
    result = await trigger._forward_logs(
        logs_client, "/aws-glue/python-jobs/output", "jr_123", None
    )
    assert result is None
```

### Test 6: `test_forward_logs_pagination`
Test that `_forward_logs` follows `nextForwardToken` until events are exhausted.

```python
@pytest.mark.asyncio
async def test_forward_logs_pagination(self):
    logs_client = AsyncMock()
    logs_client.get_log_events = AsyncMock(side_effect=[
        {
            "events": [{"timestamp": 1, "message": "line 1"}],
            "nextForwardToken": "token_2",
        },
        {
            "events": [{"timestamp": 2, "message": "line 2"}],
            "nextForwardToken": "token_3",
        },
        {
            "events": [],
            "nextForwardToken": "token_3",  # same token = no more events
        },
    ])

    trigger = GlueJobCompleteTrigger(
        job_name="job_name", run_id="jr_123", verbose=True,
        aws_conn_id="aws_conn_id", waiter_delay=0, waiter_max_attempts=5,
    )
    result = await trigger._forward_logs(
        logs_client, "/aws-glue/python-jobs/output", "jr_123", None
    )
    assert result == "token_3"
    assert logs_client.get_log_events.call_count == 3
```

---

## Step 4: Update the Glue test script

**File:** `glue_test/test_glue_script.py`

```python
"""Simple Glue pythonshell job for testing verbose mode with incremental logs."""
import sys
import time

print("Glue job started", flush=True)
print("[stderr] Job initialized", file=sys.stderr, flush=True)
for i in range(20):
    print(f"Processing step {i + 1}/20...", flush=True)
    print(f"[stderr] Step {i + 1}/20 debug info", file=sys.stderr, flush=True)
    time.sleep(15)
print("Glue job completed successfully", flush=True)
```

---

## Execution Order

1. Update `glue_test/test_glue_script.py` (flush=True for incremental logs)
2. Implement `run()` and `_forward_logs()` in `GlueJobCompleteTrigger`
3. Add unit tests
4. Run tests: `breeze run pytest providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py -xvs`
5. Run lint: `prek run ruff --from-ref main`
6. Manual test with real Glue job (deferrable=True, verbose=True)

---

## Files Changed Summary

| File | Change |
|------|--------|
| `providers/.../triggers/glue.py` | Add `run()` override + `_forward_logs()` + imports |
| `providers/.../tests/.../triggers/test_glue.py` | Add 5 new tests for verbose path |
| `glue_test/test_glue_script.py` | Add `flush=True` + stderr output |
