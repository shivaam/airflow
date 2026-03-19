# Solutions: Glue Deferrable Verbose Logging

**Issue:** https://github.com/apache/airflow/issues/56535

## Problem Summary

`GlueJobOperator(deferrable=True, verbose=True)` never shows CloudWatch logs.
The `verbose` flag is stored in the trigger but never read — the inherited
`AwsBaseWaiterTrigger.run()` only polls job status via a boto3 waiter.

## Key Facts

- Glue writes to 2 CloudWatch log streams per run:
  - `{LogGroupName}/output/{run_id}` — stdout
  - `{LogGroupName}/error/{run_id}` — stderr (also contains useful non-error logs)
- `LogGroupName` comes from `get_job_run()["JobRun"]["LogGroupName"]`
  - Default: `/aws-glue/python-jobs` (pythonshell) or `/aws-glue/jobs` (spark)
- The sync path uses `filter_log_events` (paginated, multi-stream capable)
- The async CloudWatch client supports `get_log_events` natively (single stream, simpler)
- ECS is the only existing trigger that does async log tailing — uses `get_log_events`

## Files Involved

| File | Role |
|------|------|
| `providers/.../triggers/glue.py` | `GlueJobCompleteTrigger` — needs `run()` override |
| `providers/.../hooks/glue.py` | `GlueJobHook` — has sync `print_job_logs()`, async `async_get_job_state()` |
| `providers/.../hooks/logs.py` | `AwsLogsHook` — has `get_log_events_async()`, `describe_log_streams_async()` |
| `providers/.../triggers/base.py` | `AwsBaseWaiterTrigger` — generic waiter `run()` we inherit from |
| `providers/.../triggers/ecs.py` | `TaskDoneTrigger` — reference implementation for async log tailing |

---

## Option A: ECS-style async `get_log_events` in the trigger (RECOMMENDED)

Override `run()` in `GlueJobCompleteTrigger`. When `verbose=True`, use a custom poll loop
that fetches logs from both CloudWatch streams using the async client directly.

### How it works

```python
# triggers/glue.py

class GlueJobCompleteTrigger(AwsBaseWaiterTrigger):

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # When not verbose, use the standard waiter path — zero behavior change
        if not self.verbose:
            async for event in super().run():
                yield event
            return

        # Verbose path: custom poll loop with log tailing
        hook = self.hook()
        async with (
            await hook.get_async_conn() as glue_client,
            await AwsLogsHook(
                aws_conn_id=self.aws_conn_id, region_name=self.region_name
            ).get_async_conn() as logs_client,
        ):
            # Get log group name from the job run metadata (one-time call)
            job_run_response = await glue_client.get_job_run(
                JobName=self.job_name, RunId=self.run_id
            )
            job_run = job_run_response["JobRun"]
            log_group_prefix = job_run.get("LogGroupName", "/aws-glue/jobs")
            log_group_output = f"{log_group_prefix}/output"
            log_group_error = f"{log_group_prefix}/error"

            # Continuation tokens for each stream
            output_token = None
            error_token = None

            for _attempt in range(self.attempts):
                # Check job state
                state_response = await glue_client.get_job_run(
                    JobName=self.job_name, RunId=self.run_id
                )
                job_run_state = state_response["JobRun"]["JobRunState"]

                # Fetch logs from both streams
                output_token = await self._forward_logs(
                    logs_client, log_group_output, self.run_id, output_token
                )
                error_token = await self._forward_logs(
                    logs_client, log_group_error, self.run_id, error_token
                )

                # Check terminal states
                if job_run_state in ("FAILED", "TIMEOUT"):
                    raise AirflowException(
                        f"Exiting Job {self.run_id} Run State: {job_run_state}"
                    )
                if job_run_state in ("SUCCEEDED", "STOPPED"):
                    self.log.info(
                        "Exiting Job %s Run State: %s", self.run_id, job_run_state
                    )
                    yield TriggerEvent(
                        {"status": "success", "run_id": self.run_id}
                    )
                    return

                self.log.info(
                    "Polling for AWS Glue Job %s current run state: %s",
                    self.job_name, job_run_state,
                )
                await asyncio.sleep(self.waiter_delay)

            raise AirflowException("Waiter exceeded max attempts")

    async def _forward_logs(
        self,
        logs_client,
        log_group: str,
        log_stream: str,
        next_token: str | None,
    ) -> str | None:
        """Fetch new CloudWatch log events and print them. Returns updated token."""
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

### Pros
- Fully async — no event loop blocking, no thread pool
- Proven pattern — ECS `TaskDoneTrigger._forward_logs()` does the same thing
- `verbose=False` delegates to `super().run()` — zero risk to existing behavior
- Reads both output and error streams (matching sync behavior)
- Uses `self.attempts` and `self.waiter_delay` — consistent with waiter config

### Cons
- Can't reuse `AwsBaseWaiterTrigger.run()` for the verbose path (need custom loop)
- Duplicates some state-checking logic from `_handle_state()`
- Two `get_job_run` calls per iteration (state check + initial log group fetch) —
  could optimize by caching the log group name after the first call

### Complexity: Low-Medium
~60 lines of new code in the trigger. No changes to the hook.

---

## Option B: `run_in_executor` wrapping sync `print_job_logs()`

Override `run()` to offload the existing sync `print_job_logs()` to a thread pool.

### How it works

```python
# triggers/glue.py

class GlueJobCompleteTrigger(AwsBaseWaiterTrigger):

    async def run(self) -> AsyncIterator[TriggerEvent]:
        if not self.verbose:
            async for event in super().run():
                yield event
            return

        hook = self.hook()
        next_log_tokens = hook.LogContinuationTokens()

        for _attempt in range(self.attempts):
            job_run_state = await hook.async_get_job_state(self.job_name, self.run_id)

            # Offload sync CloudWatch calls to thread pool
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,  # default thread pool
                hook.print_job_logs,
                self.job_name,
                self.run_id,
                next_log_tokens,
            )

            if job_run_state in ("FAILED", "TIMEOUT"):
                raise AirflowException(
                    f"Exiting Job {self.run_id} Run State: {job_run_state}"
                )
            if job_run_state in ("SUCCEEDED", "STOPPED"):
                self.log.info(
                    "Exiting Job %s Run State: %s", self.run_id, job_run_state
                )
                yield TriggerEvent({"status": "success", "run_id": self.run_id})
                return

            self.log.info(
                "Polling for AWS Glue Job %s current run state: %s",
                self.job_name, job_run_state,
            )
            await asyncio.sleep(self.waiter_delay)

        raise AirflowException("Waiter exceeded max attempts")
```

### Pros
- Reuses existing `print_job_logs()` — no duplicate log-fetching logic
- `run_in_executor` prevents event loop blocking (standard asyncio pattern)
- Smallest code change (~30 lines)
- Automatically gets both output and error streams (print_job_logs handles both)

### Cons
- Uses a thread from the default thread pool on every poll cycle
- `print_job_logs()` creates sync boto3 clients internally — each executor call
  creates new connections (no connection reuse across polls)
- `print_job_logs()` calls `get_job_run()` (sync) on every poll to get `StartedOn` —
  redundant work since we already have the run_id
- Mixes sync and async boto3 clients in the same trigger (not ideal)
- Thread pool has a default size of `min(32, os.cpu_count() + 4)` — if many verbose
  Glue triggers run concurrently, could exhaust the pool

### Complexity: Low
~30 lines of new code. No changes to the hook.

---

## Option C: New `async_print_job_logs()` on the hook

Add a fully async version of `print_job_logs()` to `GlueJobHook` that uses
`AwsLogsHook.get_log_events_async()`.

### How it works

```python
# hooks/glue.py — new method on GlueJobHook

async def async_print_job_logs(
    self,
    job_name: str,
    run_id: str,
    continuation_tokens: LogContinuationTokens,
):
    """Async version of print_job_logs using AwsLogsHook async methods."""
    async with await self.get_async_conn() as glue_client:
        job_run = (await glue_client.get_job_run(
            JobName=job_name, RunId=run_id
        ))["JobRun"]

    log_group_prefix = job_run["LogGroupName"]
    logs_hook = AwsLogsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    async def display_logs_from_async(log_group, continuation_token):
        fetched_logs = []
        next_token = continuation_token
        try:
            async for event in logs_hook.get_log_events_async(
                log_group=log_group,
                log_stream_name=run_id,
                start_time=int(job_run["StartedOn"].timestamp() * 1000),
                # Note: get_log_events_async doesn't support continuation tokens
                # the same way filter_log_events does — would need rework
            ):
                fetched_logs.append(event["message"])
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                self.log.warning("No new Glue driver logs so far.")
            else:
                raise

        if fetched_logs:
            messages = "\t".join(fetched_logs)
            self.log.info("Glue Job Run %s Logs:\n\t%s", log_group, messages)
        else:
            self.log.info("No new log from the Glue Job in %s", log_group)
        return next_token

    log_group_default = f"{log_group_prefix}/{DEFAULT_LOG_SUFFIX}"
    log_group_error = f"{log_group_prefix}/{ERROR_LOG_SUFFIX}"
    continuation_tokens.output_stream_continuation = await display_logs_from_async(
        log_group_default, continuation_tokens.output_stream_continuation
    )
    continuation_tokens.error_stream_continuation = await display_logs_from_async(
        log_group_error, continuation_tokens.error_stream_continuation
    )
```

Then the trigger calls it:

```python
# triggers/glue.py
async def run(self):
    if not self.verbose:
        async for event in super().run():
            yield event
        return

    hook = self.hook()
    next_log_tokens = hook.LogContinuationTokens()
    for _attempt in range(self.attempts):
        job_run_state = await hook.async_get_job_state(self.job_name, self.run_id)
        await hook.async_print_job_logs(self.job_name, self.run_id, next_log_tokens)
        # ... state checking, sleep ...
```

### Pros
- Fully async, no thread pool
- Keeps log logic in the hook (consistent with SageMaker's pattern)
- Clean separation: trigger handles polling, hook handles log fetching

### Cons
- `get_log_events_async()` in AwsLogsHook has a bug: it creates a new
  `get_async_conn()` on every iteration of its inner loop (line 199 of logs.py)
- `get_log_events_async()` doesn't support continuation tokens the same way
  `filter_log_events` does — it uses `start_time` + `skip`, not `nextToken`.
  This means you'd re-read all events from the beginning each time, or need to
  track the last timestamp yourself
- Two parallel implementations of log fetching to maintain (sync + async)
- Larger diff: changes to both hook and trigger
- Would need to fix or work around the AwsLogsHook async connection issue

### Complexity: Medium-High
~80 lines across hook + trigger. Needs careful handling of AwsLogsHook quirks.

---

## Comparison Matrix

| Criteria | Option A (ECS-style) | Option B (run_in_executor) | Option C (async hook method) |
|----------|---------------------|---------------------------|------------------------------|
| Event loop blocking | None | None (thread pool) | None |
| New code | ~60 lines (trigger) | ~30 lines (trigger) | ~80 lines (hook + trigger) |
| Files changed | 1 (trigger) | 1 (trigger) | 2 (hook + trigger) |
| Reuses existing code | No (new log fetch) | Yes (print_job_logs) | Partial (new async version) |
| Connection efficiency | Good (reuses client) | Poor (new conn per poll) | Poor (AwsLogsHook bug) |
| Precedent in codebase | ECS TaskDoneTrigger | None in triggers | SageMaker hook (unused) |
| Risk to verbose=False | None (super().run()) | None (super().run()) | None (super().run()) |
| Maintenance burden | Low | Low | Medium (two implementations) |

---

## Recommendation: Option A

Option A is the best fit because:

1. It follows the ECS pattern that already exists and is proven in this codebase
2. It's fully async with no thread pool overhead
3. It only changes one file (the trigger)
4. Connection reuse is good — one async logs client for the entire poll loop
5. The `verbose=False` path is completely untouched
6. It handles both output and error streams naturally

The main trade-off vs Option B is slightly more code (~60 vs ~30 lines), but that
extra code is straightforward `get_log_events` pagination — well-understood and
directly copied from the ECS pattern.

Option B is a reasonable fallback if reviewers prefer reusing `print_job_logs()`,
but the thread pool usage and sync client creation on every poll are downsides.

Option C is over-engineered for a bug fix and runs into AwsLogsHook async quirks.
