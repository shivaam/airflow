# Learning Guide: Glue Logs, Async Patterns, and CloudWatch in Airflow

This doc explains the concepts behind the Glue verbose logging bug we're fixing.
It starts from the basics and builds up to the async patterns used in Airflow triggers.

---

## 1. How AWS Glue Jobs Produce Logs

When a Glue job runs, AWS automatically sends its stdout/stderr to CloudWatch Logs.
Glue creates two log streams per job run under a log group:

```
Log Group:   /aws-glue/python-jobs       (for pythonshell jobs)
             /aws-glue/jobs              (for spark jobs)

Log Streams: {log_group}/output/{run_id}  ← stdout (print statements, normal output)
             {log_group}/error/{run_id}   ← stderr (errors, warnings, Python logging)
```

The log group name can also be customized per job. Glue stores it in the job run metadata,
accessible via `get_job_run()["JobRun"]["LogGroupName"]`.

Key thing: **pythonshell jobs buffer stdout**. Python buffers stdout when it's not connected
to a TTY (terminal). So your `print()` statements sit in a buffer and only get flushed to
CloudWatch when the buffer fills (~4KB) or the job ends. That's why you might see all logs
appear at once at the end. Spark jobs flush more aggressively because the Spark runtime
manages its own log shipping.

---

## 2. CloudWatch Logs APIs That Matter

There are two main APIs for reading logs:

### `get_log_events`
- Reads from a **single** log stream
- You specify the exact log group + stream name
- Returns events in order with a `nextForwardToken` for pagination
- Simple, predictable, works well with async

```python
response = client.get_log_events(
    logGroupName="/aws-glue/python-jobs/output",
    logStreamName="jr_abc123",
    startFromHead=True,
    nextToken=previous_token,  # omit on first call
)
events = response["events"]           # list of {timestamp, message, ingestionTime}
next_token = response["nextForwardToken"]  # pass this to the next call
```

### `filter_log_events`
- Can query **across multiple streams** in a log group
- Supports filtering by stream names, time range, and filter patterns
- More powerful but more complex pagination
- This is what Glue's sync `print_job_logs()` uses

```python
paginator = client.get_paginator("filter_log_events")
for response in paginator.paginate(
    logGroupName="/aws-glue/python-jobs/output",
    logStreamNames=["jr_abc123"],
    startTime=1234567890000,  # epoch milliseconds
    PaginationConfig={"StartingToken": continuation_token},
):
    for event in response["events"]:
        print(event["message"])
```

### Why does this matter?

The sync Glue hook uses `filter_log_events` via a paginator. Paginators are a boto3
convenience that auto-handles the `nextToken` loop. But **aiobotocore (the async boto3
library) doesn't support paginators the same way**. So you can't just slap `await` on
the existing paginator code.

For async log fetching, `get_log_events` is simpler because you manage the token yourself
and each call is a single awaitable request.

---

## 3. Airflow's Two Execution Models: Workers vs Triggerer

This is the core concept that explains why the bug exists.

### Workers (sync path)
- Each task runs in its own **process** (or Kubernetes pod, etc.)
- The process has its own thread, its own memory, its own boto3 clients
- Blocking calls (like `time.sleep()` or sync boto3 API calls) are fine — they only
  block that one task's process
- This is where `GlueJobHook.job_completion()` runs

### Triggerer (async path / deferrable)
- A **single process** running an asyncio event loop
- Manages **hundreds of triggers concurrently** using cooperative multitasking
- Each trigger is a coroutine that must `await` and yield control back to the event loop
- **If any trigger makes a blocking call, ALL triggers freeze** until it returns
- This is where `GlueJobCompleteTrigger.run()` runs

```
┌─────────────────────────────────────────────────┐
│                 TRIGGERER PROCESS                │
│                                                  │
│   asyncio event loop                             │
│   ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│   │ Trigger 1│ │ Trigger 2│ │ Trigger 3│  ...   │
│   │ (Glue)   │ │ (S3)     │ │ (ECS)    │       │
│   └──────────┘ └──────────┘ └──────────┘       │
│                                                  │
│   All share the SAME thread.                     │
│   They take turns running.                       │
│   A blocking call in one freezes all others.     │
└─────────────────────────────────────────────────┘
```

### What "deferrable" means

When an operator sets `deferrable=True`, instead of sitting in a worker process polling
for completion, it:

1. Starts the job
2. Creates a Trigger object with the job details
3. Calls `self.defer(trigger=..., method_name="execute_complete")`
4. The worker process **exits** (freeing resources)
5. The triggerer picks up the trigger and runs it in the event loop
6. When the trigger fires (job done), the task is re-scheduled on a worker
7. `execute_complete()` runs to handle the result

This is great for resource efficiency — instead of 100 workers sitting idle waiting for
100 Glue jobs, you have 1 triggerer process handling all 100.

---

## 4. sync boto3 vs async boto3 (aiobotocore)

### sync boto3 (regular)
```python
client = boto3.client("glue")
response = client.get_job_run(JobName="my-job", RunId="jr_123")
# This blocks the current thread until AWS responds (~50-200ms)
```

### async boto3 (aiobotocore)
```python
session = aiobotocore.get_session()
async with session.create_client("glue") as client:
    response = await client.get_job_run(JobName="my-job", RunId="jr_123")
    # This yields control to the event loop while waiting for AWS to respond
    # Other coroutines can run during this time
```

The `await` is the key difference. When you `await` an async API call:
- The event loop says "ok, this coroutine is waiting for I/O, let me run something else"
- Other triggers get to run their code
- When the AWS response arrives, the event loop resumes this coroutine

When you make a sync call without `await`:
- The entire thread blocks
- No other coroutine can run
- The event loop is frozen

In Airflow's AWS provider, hooks use `get_conn()` for sync and `get_async_conn()` for async:
```python
# Sync (in workers)
client = self.get_conn()
response = client.get_job_run(...)

# Async (in triggerer)
async with await self.get_async_conn() as client:
    response = await client.get_job_run(...)
```

---

## 5. The Continuation Token Pattern

CloudWatch logs are append-only streams. When you're tailing logs (reading new entries as
they appear), you need to remember where you left off between polls. That's what
continuation tokens do.


### How Glue's sync path does it

```python
class LogContinuationTokens:
    """Tracks position in both CloudWatch streams Glue writes to."""
    def __init__(self):
        self.output_stream_continuation: str | None = None  # for /output stream
        self.error_stream_continuation: str | None = None   # for /error stream
```

Each poll cycle:
1. Call `filter_log_events` with `StartingToken=continuation_token`
2. Print any new events
3. Save the `nextToken` from the response as the new continuation token
4. Next poll, pass that token so you only get events *after* where you left off

```
Poll 1: token=None     → gets events 1-5,   saves token "AAA"
Poll 2: token="AAA"    → gets events 6-8,   saves token "BBB"
Poll 3: token="BBB"    → gets nothing (no new logs yet)
Poll 4: token="BBB"    → gets events 9-12,  saves token "CCC"
...
```

### How ECS's async path does it (the pattern we want to follow)

ECS uses `get_log_events` with `nextForwardToken`:

```python
async def _forward_logs(self, logs_client, next_token=None):
    while True:
        response = await logs_client.get_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            startFromHead=True,
            **({"nextToken": next_token} if next_token else {}),
        )
        events = response["events"]
        for event in events:
            self.log.info(event["message"])

        # If no new events or token didn't change, we've caught up
        if len(events) == 0 or next_token == response["nextForwardToken"]:
            return response["nextForwardToken"]
        next_token = response["nextForwardToken"]
```

Same concept, different API. The token tracks your position in the stream.

---

## 6. The Three Async Patterns for I/O in Triggers

When a trigger needs to do something that involves I/O (like calling AWS APIs), there
are three approaches:

### Pattern 1: Native async (best)
Use the async client directly. Non-blocking, efficient.

```python
async def run(self):
    async with await hook.get_async_conn() as client:
        response = await client.some_api_call(...)
```

Used by: ECS trigger (for both ECS waiter and CloudWatch log fetching)

### Pattern 2: run_in_executor (pragmatic)
Wrap a sync function in `run_in_executor` to run it in a thread pool.
The event loop isn't blocked because the sync code runs on a separate thread.

```python
async def run(self):
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,                    # use default thread pool
        some_sync_function,      # the blocking function
        arg1, arg2,              # its arguments
    )
```

Used by: This is what we'd use for `print_job_logs()` — it's sync code that we
don't want to rewrite, but we need to call from an async context without blocking.

### Pattern 3: Just call sync code directly (bad in triggers)
Call sync boto3 directly from the async context. Blocks the event loop.

```python
async def run(self):
    response = hook.get_conn().some_api_call(...)  # BLOCKS EVERYTHING
```

Used by: `GlueJobHook.async_job_completion()` when it calls `_handle_state()` →
`print_job_logs()`. This is the problematic pattern.

---

## 7. How the Glue Trigger Currently Works (and Why It's Broken)

### The class hierarchy

```
BaseTrigger                          (airflow core)
  └── AwsBaseWaiterTrigger           (aws provider base)
        └── GlueJobCompleteTrigger   (glue-specific)
```

`AwsBaseWaiterTrigger` provides a generic `run()` that:
1. Gets an async boto3 client
2. Creates a boto3 waiter (e.g., "wait until job_complete")
3. Calls `async_wait()` which polls the waiter in a loop
4. On each poll, logs the job status via JMESPath queries on the response
5. When the waiter succeeds, yields a `TriggerEvent`

```python
# AwsBaseWaiterTrigger.run() — simplified
async def run(self):
    hook = self.hook()
    async with await hook.get_async_conn() as client:
        waiter = hook.get_waiter("job_complete", client=client)
        await async_wait(waiter, delay=60, max_attempts=75, ...)
        yield TriggerEvent({"status": "success", "run_id": self.run_id})
```

`GlueJobCompleteTrigger` does NOT override `run()`. It just configures the waiter
params in `__init__` and provides a `hook()` method. The `verbose` flag is stored
but the inherited `run()` never looks at it.

### What async_wait does

```python
# waiter_with_logging.py — simplified
async def async_wait(waiter, delay, max_attempts, args, ...):
    for attempt in range(max_attempts):
        try:
            await waiter.wait(**args, WaiterConfig={"MaxAttempts": 1})
            return  # success!
        except WaiterError as e:
            # Extract status from the response using JMESPath
            status = jmespath.search("JobRun.JobRunState", e.last_response)
            log.info("Status of AWS Glue job is: %s", status)
            await asyncio.sleep(delay)
    raise AirflowException("Waiter exceeded max attempts")
```

This is a clean, generic polling loop. But it has no concept of "also fetch CloudWatch
logs on each iteration." It only knows about the waiter response.

### The gap

The sync path has a tight loop that does TWO things per iteration:
1. Check job state
2. If verbose, fetch and print CloudWatch logs

The async/trigger path only does #1. There's no mechanism for #2.

---

## 8. How Other AWS Services Handle This

### ECS (TaskDoneTrigger) — Custom run() with log forwarding

ECS overrides `run()` completely (doesn't use `AwsBaseWaiterTrigger`). It opens both
an ECS client and a CloudWatch Logs client, then in each poll iteration:

```python
async def run(self):
    async with (
        await EcsHook(...).get_async_conn() as ecs_client,
        await AwsLogsHook(...).get_async_conn() as logs_client,
    ):
        waiter = ecs_client.get_waiter("tasks_stopped")
        logs_token = None
        while self.waiter_max_attempts:
            self.waiter_max_attempts -= 1
            try:
                await waiter.wait(cluster=..., tasks=[...], WaiterConfig={"MaxAttempts": 1})
                yield TriggerEvent({"status": "success", ...})
                return
            except WaiterError:
                self.log.info("Status: %s", ...)
                await asyncio.sleep(self.waiter_delay)
            finally:
                # ALWAYS fetch logs, even if the waiter succeeded or failed
                if self.log_group and self.log_stream:
                    logs_token = await self._forward_logs(logs_client, logs_token)
```

Key points:
- Uses `get_log_events` (single stream), not `filter_log_events`
- Manages its own `nextForwardToken`
- Log fetching is in `finally` so it runs every iteration
- Fully async — `await logs_client.get_log_events(...)`

### SageMaker — Async methods on the hook, but trigger doesn't use them

SageMaker has elaborate async log methods on `SageMakerHook`:
- `describe_log_streams_async()` — lists streams
- `get_log_events_async()` — async generator for a single stream
- `get_multi_stream()` — interleaves events from multiple streams
- `describe_training_job_with_log_async()` — full async log tailing

But `SageMakerTrigger.run()` just uses a waiter — none of the async log methods
are called from the trigger. They exist but aren't wired up.

### Batch, EMR, Glue (current) — No log tailing at all

These all use `AwsBaseWaiterTrigger` as-is. Just poll status, no logs.

---

## 9. The Fix We're Going With (Option A: ECS-style)

Override `run()` in `GlueJobCompleteTrigger`. When `verbose=True`, implement a custom
poll loop that fetches CloudWatch logs on each iteration using the async logs client.

The approach:
1. Open both a Glue async client and a CloudWatch Logs async client
2. Each iteration: check job state via the Glue client
3. Each iteration: call `get_log_events` on both output and error streams
4. Track `nextForwardToken` for each stream between iterations
5. When job completes, yield the trigger event

This follows the ECS pattern exactly. The only Glue-specific detail is that Glue has
TWO log streams (output + error) while ECS has one.

When `verbose=False`, we just call `super().run()` — zero behavior change from today.

---

## 10. Experimenting with the APIs

Here are some scripts you can run inside Breeze to play with these APIs directly.
Place them in `dev/` and run with `breeze run python dev/script_name.py`.

### Script 1: List Glue job runs and their log groups

```python
"""dev/explore_glue_runs.py — List recent Glue job runs and their log config."""
import boto3

glue = boto3.client("glue")

# Replace with your job name
JOB_NAME = "your-glue-job-name"

response = glue.get_job_runs(JobName=JOB_NAME, MaxResults=5)
for run in response["JobRuns"]:
    print(f"Run ID:     {run['Id']}")
    print(f"  State:    {run['JobRunState']}")
    print(f"  Started:  {run.get('StartedOn', 'N/A')}")
    print(f"  LogGroup: {run.get('LogGroupName', '/aws-glue/jobs')}")
    print()
```

### Script 2: Read CloudWatch logs for a Glue run (sync)

```python
"""dev/read_glue_logs_sync.py — Read logs from a Glue job run using sync boto3."""
import boto3

logs = boto3.client("logs")

# Replace these with real values from Script 1
LOG_GROUP = "/aws-glue/python-jobs/output"
RUN_ID = "jr_abc123"

next_token = None
while True:
    kwargs = {
        "logGroupName": LOG_GROUP,
        "logStreamName": RUN_ID,
        "startFromHead": True,
    }
    if next_token:
        kwargs["nextToken"] = next_token

    response = logs.get_log_events(**kwargs)
    for event in response["events"]:
        print(f"  [{event['timestamp']}] {event['message']}")

    # Stop when token doesn't change (no more events)
    if next_token == response["nextForwardToken"]:
        break
    next_token = response["nextForwardToken"]

print(f"\nFinal token: {next_token}")
```

### Script 3: Read logs async (simulating what the trigger will do)

```python
"""dev/read_glue_logs_async.py — Read logs using aiobotocore (async), like a trigger would."""
import asyncio
import aiobotocore.session

LOG_GROUP = "/aws-glue/python-jobs/output"
RUN_ID = "jr_abc123"

async def read_logs():
    session = aiobotocore.session.get_session()
    async with session.create_client("logs") as client:
        next_token = None
        while True:
            kwargs = {
                "logGroupName": LOG_GROUP,
                "logStreamName": RUN_ID,
                "startFromHead": True,
            }
            if next_token:
                kwargs["nextToken"] = next_token

            response = await client.get_log_events(**kwargs)
            for event in response["events"]:
                print(f"  [{event['timestamp']}] {event['message']}")

            if next_token == response["nextForwardToken"]:
                break
            next_token = response["nextForwardToken"]

        print(f"\nFinal token: {next_token}")

asyncio.run(read_logs())
```

### Script 4: Compare filter_log_events vs get_log_events

```python
"""dev/compare_log_apis.py — Show the difference between the two CloudWatch APIs."""
import boto3

logs = boto3.client("logs")

LOG_GROUP = "/aws-glue/python-jobs/output"
RUN_ID = "jr_abc123"

print("=== get_log_events (single stream, simple) ===")
resp = logs.get_log_events(
    logGroupName=LOG_GROUP,
    logStreamName=RUN_ID,
    startFromHead=True,
    limit=5,
)
for e in resp["events"]:
    print(f"  {e['message'].strip()}")

print("\n=== filter_log_events (multi-stream capable, paginated) ===")
resp = logs.filter_log_events(
    logGroupName=LOG_GROUP,
    logStreamNames=[RUN_ID],
    limit=5,
)
for e in resp["events"]:
    print(f"  {e['message'].strip()}")

print("\nBoth return the same data for a single stream.")
print("filter_log_events is more powerful but harder to use async.")
```

---

## 11. Glossary

| Term | Meaning |
|------|---------|
| **Trigger** | An async coroutine that runs in the triggerer process, polling for some condition |
| **Triggerer** | The Airflow process that runs all triggers in a single asyncio event loop |
| **Deferrable** | An operator that can hand off its "waiting" to a trigger, freeing the worker |
| **Waiter** | A boto3 concept — polls an API until a condition is met (e.g., job state = SUCCEEDED) |
| **aiobotocore** | Async version of boto3's core — used by Airflow for async AWS API calls |
| **Continuation token** | A string that marks your position in a log stream, so you can resume reading |
| **`get_log_events`** | CloudWatch API to read from one specific log stream |
| **`filter_log_events`** | CloudWatch API to search/filter across multiple streams in a log group |
| **`run_in_executor`** | asyncio method to run sync/blocking code in a thread pool without blocking the event loop |
| **Log group** | CloudWatch container for log streams (e.g., `/aws-glue/python-jobs`) |
| **Log stream** | A sequence of log events within a group (e.g., the run ID `jr_abc123`) |
| **Event loop** | The core of asyncio — a single-threaded loop that runs coroutines cooperatively |
| **Coroutine** | An async function — pauses at `await` points, letting other coroutines run |
