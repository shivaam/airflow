# Research: Existing AWS Trigger Error Handling Precedents

## Overview

Several AWS triggers already override `AwsBaseWaiterTrigger.run()` to catch exceptions and yield error events.
Our fix in `base.py` consolidates this pattern so all subclasses benefit automatically.

---

## 1. EksCreateClusterTrigger

**File:** `providers/amazon/src/airflow/providers/amazon/aws/triggers/eks.py` (line ~72)

```python
async def run(self):
    async with await self.hook().get_async_conn() as client:
        waiter = client.get_waiter(self.waiter_name)
        try:
            await async_wait(waiter, ...)
        except AirflowException as exception:
            self.log.error("Error creating cluster: %s", exception)
            yield TriggerEvent({"status": "failed"})
        else:
            yield TriggerEvent({"status": "success"})
```

**Notes:**
- Catches `AirflowException` specifically
- Does NOT include error message in event payload — operator gets no details
- Status value: `"failed"` (not `"error"`)

**Operator:** `EksCreateClusterOperator.execute_complete()` checks `event["status"] != "success"` and raises.

---

## 2. EmrServerlessStartJobTrigger

**File:** `providers/amazon/src/airflow/providers/amazon/aws/triggers/emr.py` (line ~446)

```python
async def run(self):
    hook = self.hook()
    try:
        async with await hook.get_async_conn() as client:
            waiter = hook.get_waiter(self.waiter_name, deferrable=True, client=client,
                                     config_overrides=self.waiter_config_overrides)
            await async_wait(waiter, ...)
        yield TriggerEvent({"status": "success", self.return_key: self.return_value})
    except asyncio.CancelledError:
        if self.job_id and self.cancel_on_kill and await self.safe_to_cancel():
            self.log.info("Cancelling EMR Serverless job %s", self.job_id)
            hook.conn.cancel_job_run(applicationId=self.application_id, jobRunId=self.job_id)
        raise  # re-raise CancelledError
    except Exception as e:
        yield TriggerEvent({"status": "failure", "message": str(e)})
```

**Notes:**
- Catches `asyncio.CancelledError` separately for job cancellation logic
- Catches generic `Exception` — includes message in event
- Status value: `"failure"` (not `"error"`)
- Also handles cancel-on-kill pattern

**Operator:** `EmrServerlessStartJobOperator.execute_complete()` uses `validate_execute_complete_event()`.

---

## 3. S3KeyTrigger

**File:** `providers/amazon/src/airflow/providers/amazon/aws/triggers/s3.py` (line ~100)

```python
async def run(self):
    try:
        async with await self.hook.get_async_conn() as client:
            while True:
                if await self.hook.check_key_async(client, ...):
                    if self.should_check_fn:
                        # ... metadata collection ...
                        yield TriggerEvent({"status": "running", "files": files})
                    else:
                        yield TriggerEvent({"status": "success"})
                    return
                await asyncio.sleep(self.poke_interval)
    except Exception as e:
        yield TriggerEvent({"status": "error", "message": str(e)})
```

**Notes:**
- Does NOT use `async_wait` — custom polling loop
- Can yield intermediate `"running"` events
- Catches generic `Exception` with message
- Status value: `"error"`

**Sensor:** `S3KeySensor.execute_complete()` checks `event["status"] == "error"` and raises `AirflowException(event["message"])`.

---

## 4. RedshiftDataTrigger

**File:** `providers/amazon/src/airflow/providers/amazon/aws/triggers/redshift_data.py` (line ~90)

```python
async def run(self):
    try:
        while await self.hook.is_still_running(self.statement_id):
            await asyncio.sleep(self.poll_interval)
        is_finished = await self.hook.check_query_is_finished_async(self.statement_id)
        if is_finished:
            response = {"status": "success", "statement_id": self.statement_id}
        else:
            response = {"status": "error", "statement_id": self.statement_id,
                        "message": f"{self.task_id} failed"}
        yield TriggerEvent(response)
    except (RedshiftDataQueryFailedError, RedshiftDataQueryAbortedError) as error:
        response = {"status": "error", "statement_id": self.statement_id,
                    "message": str(error),
                    "type": FAILED_STATE if isinstance(error, RedshiftDataQueryFailedError) else ABORTED_STATE}
        yield TriggerEvent(response)
    except Exception as error:
        yield TriggerEvent({"status": "error", "statement_id": self.statement_id, "message": str(error)})
```

**Notes:**
- Catches specific Redshift exception types AND generic Exception
- Includes `type` field for error classification (FAILED vs ABORTED)
- Custom polling (not `async_wait`)

---

## 5. SageMakerPipelineExecutionTrigger

**File:** `providers/amazon/src/airflow/providers/amazon/aws/triggers/sagemaker.py` (line ~167)

```python
async def run(self):
    hook = SageMakerHook(aws_conn_id=self.aws_conn_id)
    async with await hook.get_async_conn() as conn:
        waiter = hook.get_waiter(self._waiter_name[self.waiter_type], deferrable=True, client=conn)
        for _ in range(self.waiter_max_attempts):
            try:
                await waiter.wait(PipelineExecutionArn=..., WaiterConfig={"MaxAttempts": 1})
                yield TriggerEvent({"status": "success", "value": self.pipeline_execution_arn})
                return
            except WaiterError as error:
                if "terminal failure" in str(error):
                    raise  # <-- propagates as uncaught! Still has the old bug
                self.log.info("Status: %s", error.last_response["PipelineExecutionStatus"])
                await asyncio.sleep(int(self.waiter_delay))
        raise AirflowException("Waiter error: max attempts reached")
```

**Notes:**
- Reimplements its own polling loop instead of using `async_wait`
- Re-raises terminal failures — still has the "generic failure" bug!
- Could benefit from the base class fix if refactored to use `AwsBaseWaiterTrigger`

---

## Summary: Status Value Inconsistency

| Trigger | Error Status Value | Message Included |
|---------|--------------------|-----------------|
| EksCreateClusterTrigger | `"failed"` | No |
| EmrServerlessStartJobTrigger | `"failure"` | Yes |
| S3KeyTrigger | `"error"` | Yes |
| RedshiftDataTrigger | `"error"` | Yes |
| AwsBaseWaiterTrigger (our fix) | `"error"` | Yes |

The inconsistency in status values (`"failed"` vs `"failure"` vs `"error"`) is pre-existing.
All operators check `status != "success"` so any non-success value works.
Our fix uses `"error"` which is the most common convention.

---

## Why the Base Class Fix Is Better

Instead of each trigger individually overriding `run()`:

1. **Consistency** — All `AwsBaseWaiterTrigger` subclasses get the same behavior
2. **No duplicate code** — EKS/EMR overrides become unnecessary (though kept for backward compat)
3. **Error details preserved** — The `message` field contains the full `async_wait` exception text
4. **return_key/return_value preserved** — Operators can still extract the job ID etc. from error events

## Files Referenced

1. `providers/amazon/src/airflow/providers/amazon/aws/triggers/eks.py`
2. `providers/amazon/src/airflow/providers/amazon/aws/triggers/emr.py`
3. `providers/amazon/src/airflow/providers/amazon/aws/triggers/s3.py`
4. `providers/amazon/src/airflow/providers/amazon/aws/triggers/redshift_data.py`
5. `providers/amazon/src/airflow/providers/amazon/aws/triggers/sagemaker.py`
6. `providers/amazon/src/airflow/providers/amazon/aws/triggers/base.py`
