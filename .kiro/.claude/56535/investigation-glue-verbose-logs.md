# Investigation: GlueJobOperator verbose mode does not pull logs in deferrable mode

**Issue:** https://github.com/apache/airflow/issues/56535

## Problem Statement

When `GlueJobOperator` runs with `deferrable=True`, `wait_for_completion=True`, and `verbose=True`,
CloudWatch logs from the Glue job are never fetched. The user only sees generic waiter status messages:

```
INFO - Status of AWS Glue job is: RUNNING
INFO - Status of AWS Glue job is: RUNNING
INFO - Status of AWS Glue job is: RUNNING
...
INFO - Trigger fired ... result=TriggerEvent<{'status': 'success', ...}>
```

Instead of the actual Glue driver output/error logs from CloudWatch that appear in the sync path:

```
INFO - Glue Job Run /aws-glue/python-jobs/output Logs:
    Glue job started
    Processing step 1/20...
    Processing step 2/20...
    ...
```

## Confirmed via Live Testing

Both paths were tested against a real Glue pythonshell job (`test_glue_script.py`) that prints
20 processing steps over ~5 minutes.

**Sync path** (`deferrable=False, verbose=True`): CloudWatch logs appeared in the Airflow task
log via `print_job_logs()`. All 20 steps were visible.

**Deferrable path** (`deferrable=True, verbose=True`): Only `Status of AWS Glue job is: RUNNING`
repeated every 30 seconds. Zero CloudWatch logs. The `verbose` flag was stored but never read.

---

## Root Cause Analysis

Two completely separate code paths exist for waiting on Glue job completion. Only the sync path
supports verbose logging.

### Sync path (works)

```
GlueJobOperator.execute()
  → GlueJobHook.job_completion()
    → loop:
        get_job_state()
        _handle_state(verbose=True)  ← checks verbose flag
          → print_job_logs()         ← fetches CloudWatch logs via filter_log_events
        sleep(poll_interval)
```

The key code in `hooks/glue.py`:

```python
# GlueJobHook.job_completion() — line 374
def job_completion(self, job_name, run_id, verbose=False, sleep_before_return=0):
    next_log_tokens = self.LogContinuationTokens()
    while True:
        job_run_state = self.get_job_state(job_name, run_id)
        ret = self._handle_state(job_run_state, job_name, run_id, verbose, next_log_tokens)
        if ret:
            time.sleep(sleep_before_return)
            return ret
        time.sleep(self.job_poll_interval)

# GlueJobHook._handle_state() — line 412
def _handle_state(self, state, job_name, run_id, verbose, next_log_tokens):
    if verbose:
        self.print_job_logs(job_name=job_name, run_id=run_id, continuation_tokens=next_log_tokens)
    # ... check finished/failed states ...
```

`print_job_logs()` uses `LogContinuationTokens` to track position in the CloudWatch log streams.
Each call picks up only new log lines since the last call — no duplicates.

### Deferrable path (broken)

```
GlueJobOperator.execute()
  → self.defer(trigger=GlueJobCompleteTrigger(verbose=True, ...))
    → AwsBaseWaiterTrigger.run()          ← inherited, no override
      → async_wait(waiter, ...)           ← generic boto waiter utility
        → loop:
            waiter.wait(MaxAttempts=1)
            log "Status of AWS Glue job is: RUNNING"   ← JMESPath extraction only
            sleep(waiter_delay)
```

The trigger stores `self.verbose = True` in `__init__`:

```python
# triggers/glue.py — GlueJobCompleteTrigger.__init__() line 50
def __init__(self, job_name, run_id, verbose=False, waiter_delay=60, ...):
    super().__init__(
        serialized_fields={"job_name": job_name, "run_id": run_id, "verbose": verbose},
        waiter_name="job_complete",
        waiter_args={"JobName": job_name, "RunId": run_id},
        status_message="Status of AWS Glue job is",
        status_queries=["JobRun.JobRunState"],
        ...
    )
    self.verbose = verbose  # ← stored but NEVER READ
```

But `GlueJobCompleteTrigger` does not override `run()`. It inherits from `AwsBaseWaiterTrigger`:

```python
# triggers/base.py — AwsBaseWaiterTrigger.run() line 143
async def run(self) -> AsyncIterator[TriggerEvent]:
    hook = self.hook()
    async with await hook.get_async_conn() as client:
        waiter = hook.get_waiter(self.waiter_name, deferrable=True, client=client)
        await async_wait(
            waiter, self.waiter_delay, self.attempts,
            self.waiter_args, self.failure_message,
            self.status_message, self.status_queries,  # ← only JMESPath status extraction
        )
        yield TriggerEvent({"status": "success", self.return_key: self.return_value})
```

`async_wait()` is a generic utility in `waiter_with_logging.py`. It calls `waiter.wait()` in a
loop and extracts status via JMESPath queries. It has zero knowledge of Glue-specific CloudWatch
log fetching. There is no hook point for subclasses to inject per-poll behavior.

**The `verbose` flag is dead code in the deferrable path.**

---

## Existing Async Infrastructure

The hook already has an async version of `job_completion`:

```python
# hooks/glue.py — GlueJobHook.async_job_completion() line 395
async def async_job_completion(self, job_name, run_id, verbose=False):
    next_log_tokens = self.LogContinuationTokens()
    while True:
        job_run_state = await self.async_get_job_state(job_name, run_id)  # ← truly async
        ret = self._handle_state(job_run_state, job_name, run_id, verbose, next_log_tokens)
        if ret:
            return ret
        await asyncio.sleep(self.job_poll_interval)
```

This method uses `async_get_job_state()` (async boto3) for polling state, but then calls
`_handle_state()` which calls `print_job_logs()` — and `print_job_logs()` is fully synchronous
(sync boto3 clients for CloudWatch `filter_log_events` and Glue `get_job_run`).

---

## The Async/Sync Problem

`print_job_logs()` makes multiple synchronous boto3 API calls:
1. `self.conn.get_job_run(...)` — sync Glue client
2. `log_client.get_paginator("filter_log_events")` — sync CloudWatch Logs client
3. Paginated iteration over `filter_log_events` responses

Triggers run inside the triggerer's async event loop. Calling sync boto3 from an async context
blocks the entire event loop — while one Glue trigger fetches CloudWatch logs, every other
trigger in the triggerer process is frozen (no other triggers can fire, poll, or yield events).

The sync path doesn't have this problem because `job_completion()` runs in a worker process
with its own thread.

---

## Solution Options

### Option A: Override `run()` with `run_in_executor` for log fetching (Recommended)

Override `run()` in `GlueJobCompleteTrigger`. When `verbose=False`, delegate to `super().run()`
(zero behavior change). When `verbose=True`, implement a custom polling loop that uses
`async_get_job_state()` for state checks and wraps the sync `print_job_logs()` in
`run_in_executor()` to avoid blocking the event loop.

```python
# triggers/glue.py — GlueJobCompleteTrigger

async def run(self) -> AsyncIterator[TriggerEvent]:
    if not self.verbose:
        async for event in super().run():
            yield event
        return

    hook = self.hook()
    next_log_tokens = hook.LogContinuationTokens()
    try:
        while True:
            job_run_state = await hook.async_get_job_state(self.job_name, self.run_id)

            # Offload sync CloudWatch log fetching to a thread pool
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                hook.print_job_logs,
                self.job_name,
                self.run_id,
                next_log_tokens,
            )

            if job_run_state in ("FAILED", "TIMEOUT"):
                raise AirflowException(f"Exiting Job {self.run_id} Run State: {job_run_state}")
            if job_run_state in ("SUCCEEDED", "STOPPED"):
                self.log.info("Exiting Job %s Run State: %s", self.run_id, job_run_state)
                yield TriggerEvent({"status": "success", "run_id": self.run_id})
                return

            self.log.info(
                "Polling for AWS Glue Job %s current run state with status %s",
                self.job_name,
                job_run_state,
            )
            await asyncio.sleep(self.waiter_delay)
    except AirflowException:
        yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})
```

**Pros:**
- Non-blocking: `run_in_executor` offloads sync boto3 calls to a thread pool, so other triggers
  aren't frozen during CloudWatch API calls
- `verbose=False` path is completely unchanged (delegates to `super().run()`)
- Reuses existing `print_job_logs()` and `LogContinuationTokens` — no duplicate logic
- Uses `self.waiter_delay` for poll interval, consistent with the non-verbose trigger behavior

**Cons:**
- Duplicates some state-checking logic from `_handle_state()` (the FAILED/SUCCEEDED checks)
  rather than calling `_handle_state()` directly, because `_handle_state` bundles state checking
  with log fetching and we need them separated for the executor pattern
- Thread pool usage adds minor overhead (but CloudWatch API calls are I/O-bound, so this is
  exactly what thread pools are for)

**Risk: Low.** `run_in_executor` with the default thread pool is a well-established asyncio
pattern. The sync `print_job_logs()` is already designed to be called repeatedly with
continuation tokens. No new boto3 clients or API calls are introduced.

---

### Option B: Use `async_job_completion()` directly

Override `run()` to call `hook.async_job_completion()` when `verbose=True`.

```python
async def run(self) -> AsyncIterator[TriggerEvent]:
    if not self.verbose:
        async for event in super().run():
            yield event
        return

    hook = self.hook()
    hook.job_poll_interval = self.waiter_delay  # align with trigger's poll interval
    try:
        result = await hook.async_job_completion(
            job_name=self.job_name, run_id=self.run_id, verbose=True
        )
        yield TriggerEvent({"status": "success", "run_id": self.run_id})
    except AirflowException as e:
        yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})
```

**Pros:**
- Minimal code in the trigger — delegates everything to the hook
- Reuses the exact same code path as the sync `job_completion()` (via shared `_handle_state`)

**Cons:**
- **Blocks the event loop.** `async_job_completion()` calls `_handle_state()` → `print_job_logs()`
  which uses sync boto3 clients. These sync calls run directly on the event loop thread, blocking
  all other triggers in the triggerer process for the duration of each CloudWatch API call.
- Mutates `hook.job_poll_interval` which is a bit hacky — the hook's default is 6 seconds but
  the trigger's `waiter_delay` is typically 30-60 seconds.
- `async_job_completion` doesn't respect `waiter_max_attempts` — it loops forever until the job
  finishes or fails. The waiter-based path has a max attempts limit.

**Risk: Medium.** The event loop blocking is the main concern. For a single Glue trigger it's
probably fine in practice (CloudWatch API calls take ~100-500ms), but if multiple verbose Glue
triggers run concurrently, or if CloudWatch is slow, it could degrade the triggerer's
responsiveness for all triggers.

---

### Option C: Write a fully async `print_job_logs` using async boto3

Create a new `async_print_job_logs()` method on `GlueJobHook` that uses the async boto3 client
for both `get_job_run` and `filter_log_events`.

```python
# hooks/glue.py — new method
async def async_print_job_logs(self, job_name, run_id, continuation_tokens):
    async with await self.get_async_conn() as glue_client:
        job_run = await glue_client.get_job_run(JobName=job_name, RunId=run_id)
        # ... async CloudWatch log fetching ...
        async with await self.logs_hook.get_async_conn() as logs_client:
            # async paginated filter_log_events
            ...
```

Then call it directly from the trigger's `run()` without needing `run_in_executor`.

**Pros:**
- Fully non-blocking, native async throughout
- No thread pool overhead
- "Cleanest" from an async architecture perspective

**Cons:**
- Significant new code: need to rewrite `print_job_logs()` logic with async boto3 pagination
  (CloudWatch `filter_log_events` pagination with async client is non-trivial)
- Need to create/manage an async CloudWatch Logs client (`logs_hook.get_async_conn()`) — unclear
  if `AwsLogsHook` supports async connections today
- Duplicates logic between sync `print_job_logs()` and async `async_print_job_logs()` — two
  implementations to maintain
- Much larger diff for what is fundamentally a bug fix

**Risk: Medium-High.** More code means more surface area for bugs. The async CloudWatch pagination
needs careful testing. Maintaining two parallel implementations of log fetching is a long-term
burden. Overkill for this fix.

---

## Recommendation

**Option A** is the right choice. It's the smallest safe change that fixes the bug:

1. `verbose=False` is completely untouched (delegates to `super().run()`)
2. `verbose=True` reuses existing `print_job_logs()` with continuation tokens — proven logic
3. `run_in_executor` prevents event loop blocking — standard asyncio pattern
4. Uses `self.waiter_delay` and `self.attempts` for consistency with the non-verbose path
5. Minimal new code, easy to review

---

## Files to Modify

1. `providers/amazon/src/airflow/providers/amazon/aws/triggers/glue.py`
   - Override `run()` in `GlueJobCompleteTrigger`

2. `providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py`
   - Add tests for verbose deferrable path (mock `async_get_job_state` + `print_job_logs`)
   - Test `verbose=False` still delegates to `super().run()`

3. `providers/amazon/tests/unit/amazon/aws/operators/test_glue.py`
   - Optionally add test verifying the operator passes `verbose=True` to the trigger correctly
