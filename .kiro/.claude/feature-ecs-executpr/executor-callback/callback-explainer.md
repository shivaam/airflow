# Executor Callbacks — How It All Works

A walkthrough of the executor callback system, from DAG definition to worker execution.

## The Big Picture

A DAG author says: "If this DAG isn't done in 15 minutes, send a Slack message." Airflow needs to:
1. Monitor the deadline
2. Detect when it's missed
3. Run the Slack notification somewhere safe

The "somewhere safe" is the executor/worker — because the callback function lives in user code, and the scheduler never runs user code.

## Stage 1: DAG Author Defines a Deadline

```python
with DAG(
    dag_id="my_etl",
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,  # start counting from when DAG was queued
        interval=timedelta(minutes=15),                 # if not done in 15 min...
        callback=SyncCallback(SlackWebhookNotifier, kwargs={"text": "Late!"}),  # ...send this
    ),
):
```

`DeadlineAlert` takes three things:
- **reference**: When to start counting (queued time, scheduled time, fixed time, or average runtime)
- **interval**: How long to wait before firing
- **callback**: What to run — either `SyncCallback` (runs on executor) or `AsyncCallback` (runs on triggerer)

At this point, `SyncCallback` converts `SlackWebhookNotifier` into a dotted import path string:
```
"airflow.providers.slack.notifications.slack_webhook.SlackWebhookNotifier"
```
This path is what gets stored and later used to import and call the function.

**Source:** `task-sdk/src/airflow/sdk/definitions/deadline.py` (DeadlineAlert class)
**Source:** `task-sdk/src/airflow/sdk/definitions/callback.py` (SyncCallback class, line 62 for path conversion)

## Stage 2: Deadline Row Gets Created in the Database

When the DAG is parsed and a DAG run is created, Airflow computes:

```
deadline_time = dagrun.queued_at + 15 minutes
```

It writes a `Deadline` row to the database:
- `deadline_time` = the computed timestamp
- `callback` = an `ExecutorCallback` record (with the Slack path + kwargs stored in a JSON `data` field)
- `missed` = False

**Source:** `airflow-core/src/airflow/models/deadline.py` (Deadline ORM model, lines 85-120)

## Stage 3: Scheduler Detects the Miss

The scheduler runs a continuous loop. Every iteration, it runs a simple database query:

```python
# "Give me all deadlines that have passed but haven't been handled yet"
for deadline in session.scalars(
    select(Deadline)
    .where(Deadline.deadline_time < now())   # time has passed
    .where(~Deadline.missed)                  # not yet handled
):
    deadline.handle_miss(session)
```

`handle_miss()` does two things:
1. Sets `callback.state = PENDING` in the database
2. Sets `deadline.missed = True` so it won't be picked up again

No scheduling happens here — it just marks the callback as ready.

**Source:** `airflow-core/src/airflow/jobs/scheduler_job_runner.py` (lines 1620-1626)
**Source:** `airflow-core/src/airflow/models/deadline.py` (handle_miss method, lines 213-256)

## Stage 4: Scheduler Sends Callback to Executor

Immediately after checking deadlines, the scheduler calls `_enqueue_executor_callbacks()`:

```python
# Query all PENDING executor callbacks
pending_callbacks = session.scalars(
    select(ExecutorCallback)
    .where(ExecutorCallback.state == CallbackState.PENDING)
).all()

# For each one, create a workload and hand to executor
for callback in callbacks:
    workload = ExecuteCallback.make(callback=callback, dag_run=dag_run)
    executor.queue_workload(workload, session=session)
    callback.state = CallbackState.QUEUED
```

The `ExecuteCallback` workload contains:
- `CallbackDTO` with `id`, `fetch_method`, and `data` (path + kwargs)
- DAG bundle info (so the worker knows where to find the code)
- A JWT token for authentication

**Source:** `airflow-core/src/airflow/jobs/scheduler_job_runner.py` (lines 1047-1098)

## Stage 5: Executor Queues and Dispatches

The executor receives the workload via `queue_workload()` and stores it in `self.queued_callbacks` (keyed by callback UUID string). Callbacks are prioritized over tasks — they complete existing work.

When the executor processes workloads, it serializes the callback the same way as a task:

```python
command = ["python", "-m", "airflow.sdk.execution_time.execute_workload",
           "--json-string", workload.model_dump_json()]
```

This command is what the container (ECS, Lambda, etc.) actually runs. The serialization is identical for tasks and callbacks — `model_dump_json()` works on any pydantic model.

**Source:** `airflow-core/src/airflow/executors/base_executor.py` (queue_workload, lines 222-238)

## Stage 6: Worker Executes the Callback

On the worker side, the callback path gets resolved and called. Here's the step-by-step for `SlackWebhookNotifier`:

```python
# path = "airflow.providers.slack.notifications.slack_webhook.SlackWebhookNotifier"

# 1. Split the dotted path into module + attribute
module_path, function_name = callback_path.rsplit(".", 1)
# module_path = "airflow.providers.slack.notifications.slack_webhook"
# function_name = "SlackWebhookNotifier"

# 2. Import the module
module = import_module(module_path)

# 3. Get the class/function from the module
callback_callable = getattr(module, function_name)
# callback_callable = <class SlackWebhookNotifier>

# 4. Call it — for a class, this creates an instance
result = callback_callable(**callback_kwargs)
# result = SlackWebhookNotifier(text="Late!")  ← an object, not a return value

# 5. Check if the result is callable (i.e. it was a class, not a function)
if callable(result):
    result = result(context)    # calls __call__ → triggers self.notify() → sends Slack message
```

This is similar to how AWS Lambda resolves a handler path like `my_module.handler` — split, import, call.

**Two callable patterns:**
- **Function** (e.g. `my_module.alert_func`): Step 4 calls it and returns a value (string, None). Step 5 skips because the result isn't callable. Done.
- **Class** (e.g. `SlackWebhookNotifier`): Step 4 creates an instance. Step 5 detects it's callable (has `__call__` from BaseNotifier) and calls it again. The actual work (sending the message) happens in step 5.

The `callable(result)` check is used instead of `isinstance` because the code doesn't know which specific class the callback might be — it could be any notifier or any user-defined class with `__call__`.

**Source:** `airflow-core/src/airflow/executors/workloads/callback.py` (execute_callback_workload, lines 104-164)

## Why Executor? Why Not Scheduler or Triggerer?

**Why not the scheduler?**
Architecture rule: the scheduler never runs user code. The callback function lives in DAG files / user packages. Running it in the scheduler would violate this boundary.

**Why not the triggerer?**
The triggerer runs an asyncio event loop. Most notification libraries (Slack SDK, SMTP, PagerDuty SDK) are synchronous. Running blocking sync code in the triggerer's event loop would stall all other triggers sharing that loop.

That said, `AsyncCallback` exists for async callbacks — those DO run on the triggerer. `SyncCallback` is for everything else, which is most real-world notifiers.

**Why not inline like old task callbacks?**
Old-style callbacks (`on_success_callback`, `on_failure_callback`) run inline in the worker right after a task finishes. Deadline callbacks are different — they're detected by the scheduler (not by a worker), so they need to be scheduled as separate workloads.

## Callbacks vs Tasks — Key Differences

| Aspect | Task | Callback |
|--------|------|----------|
| Key type | `TaskInstanceKey` (dag_id, task_id, run_id, etc.) | UUID string (callbacks aren't tasks — no dag_id/task_id) |
| Queue | `self.queued_tasks` | `self.queued_callbacks` |
| Priority | After callbacks | Before tasks |
| Execution | Full lifecycle — setup, execute, teardown, heartbeats, XCom | Just import a function, call it, report success/failure |
| executor_config | Yes | No |
| Adoption | Supported | Not yet |
| Retries | Yes | No |
| Result | Rich state machine | Simple `(success: bool, error_message: str)` |

## Callback State Machine

```
[Deadline row created with missed=False]
         |
         | scheduler detects deadline_time < now()
         v
    handle_miss()
         |
         | callback.state = PENDING, deadline.missed = True
         v
    _enqueue_executor_callbacks()
         |
         | executor.queue_workload(), callback.state = QUEUED
         v
    Executor dispatches to worker
         |
         | worker starts executing
         v
      RUNNING
       /    \
      /      \
   SUCCESS  FAILED
```

## Q&A From Our Discussion

**Q: Is the callback similar to how a Lambda handler works?**
Yes. Lambda takes a path like `my_module.handler`, splits it, imports the module, grabs the function, and calls it. Airflow callbacks do the exact same thing with `rsplit(".", 1)` → `import_module()` → `getattr()` → call.

**Q: Why do callbacks use a UUID string key instead of TaskInstanceKey?**
Because a callback isn't a task. `TaskInstanceKey` is a named tuple of `(dag_id, task_id, run_id, try_number, map_index)` — a callback has none of those fields. It's just a function to run, so it gets a UUID.

**Q: Why `callable(result)` instead of `isinstance`?**
Because the code doesn't know what class to check against. The callback could be any notifier class or any user-defined class. `callable()` is a generic check that works for anything with `__call__`, without importing or knowing the class hierarchy.

**Q: Could callbacks run on the triggerer like triggers do?**
Yes — and `AsyncCallback` already does. But most notifiers are sync code (Slack SDK, SMTP, etc.), and running sync code in the triggerer's async event loop would block it. So `SyncCallback` exists to run those on executor workers instead.

**Q: What was `SyncCallback` before this system?**
It didn't exist. Old task-level callbacks (`on_success_callback`, etc.) ran inline in the worker after a task finished. `SyncCallback` is new for Deadline Alerts — needed because deadlines are detected by the scheduler, which can't run user code, so it packages the callback as a workload for the executor.
