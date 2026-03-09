# ECS Executor Callback Support — Research

## What's the Task?

Airflow 3.2 introduces "Executor Callbacks" — a way for executors to run synchronous callback functions (like Deadline Alerts) in workers, just like regular DAG tasks. The ECS Executor currently only handles `ExecuteTask` workloads and raises `RuntimeError` when it sees anything else. We need to make it handle `ExecuteCallback` workloads too.

## What is a Callback?

A callback is a user-defined function that Airflow needs to run in response to some event — for example, "if this DAG isn't done by 5pm, call this alert function." The callback is represented as an `ExecuteCallback` workload, which contains a `CallbackDTO` with:
- `id` — unique identifier
- `fetch_method` — how to find the callback (currently `IMPORT_PATH` for deadline callbacks)
- `data` — dict with `path` (dotted import path like `my_module.alert_func`) and `kwargs`

The actual execution is done by `execute_callback_workload()` from `airflow.executors.workloads.callback`, which imports the function by path and calls it.

## What is a "Synchronous Callback Workload"?

Airflow has two kinds of callbacks:
- **Async callbacks** — go to the Triggerer (for async code)
- **Synchronous callbacks** — need to run in a worker, just like a DAG task

The "Executor Synchronous Callback Workload" (`ExecuteCallback`) is the mechanism for the second kind. The scheduler packages the callback into an `ExecuteCallback` workload and hands it to the executor. The executor then sends it to a worker (local process, Celery worker, ECS container, etc.) where it gets executed.

The workload flows through the same pipeline as a task:
1. Scheduler creates `ExecuteCallback` workload → calls `executor.queue_workload()`
2. Base executor stores it in `self.queued_callbacks` (prioritized over tasks)
3. Executor's `_process_workloads()` picks it up and sends it for execution
4. On the worker side, `execute_callback_workload()` imports the function by path and calls it
5. Result (success/failure) flows back through the same state tracking as tasks

The key difference from a task: callbacks use a string ID as their key (`callback.id`) instead of a `TaskInstanceKey` tuple. The `WorkloadKey` union type (`TaskInstanceKey | str`) handles both.

## How Do Existing Executors Handle It?

### LocalExecutor (simplest reference)
- Sets `supports_callbacks = True` on the class
- In its worker process function, checks `isinstance(workload, workloads.ExecuteCallback)`
- Calls `execute_callback_workload(workload.callback, log)` directly in the worker process
- Reports state back via a queue: `RUNNING` → `SUCCESS` or `FAILED`

### CeleryExecutor (remote/distributed reference)
- Sets `supports_callbacks = True` on the class
- In `_process_workloads()`, handles `ExecuteCallback` alongside `ExecuteTask` — both get sent to Celery as serialized workloads
- On the Celery worker side, the `execute_workload` Celery task deserializes the JSON via `TypeAdapter[workloads.All]`, checks the type, and calls `execute_callback_workload()` for callbacks
- Key insight: the callback runs on the remote worker, not in the scheduler

### Base Executor (what we get for free)
- `queue_workload()` already handles `ExecuteCallback` if `supports_callbacks = True` — stores them in `self.queued_callbacks` dict (keyed by callback ID string)
- `_get_workloads_to_schedule()` prioritizes callbacks over tasks (callbacks go first)
- State tracking uses `WorkloadKey` which is `TaskInstanceKey | CallbackKey` (where `CallbackKey = str`)

## How the ECS Executor Currently Works

1. Scheduler calls `queue_workload()` → currently rejects non-`ExecuteTask` with RuntimeError
2. `_process_workloads()` takes workloads, serializes them to commands, and puts them in `pending_tasks` deque via `execute_async()`
3. `attempt_task_runs()` pops from `pending_tasks` and calls `_run_task()` which calls `ecs.run_task()` to launch an ECS task
4. The ECS container runs: `python -m airflow.sdk.execution_time.execute_workload --json-string <json>`
5. `sync_running_tasks()` polls ECS to check task status and calls `success()`/`fail()` accordingly

The command serialization happens in `_serialize_workload_to_command()` which produces:
```
["python", "-m", "airflow.sdk.execution_time.execute_workload", "--json-string", workload.model_dump_json()]
```

## What We Need to Implement

### 1. Set `supports_callbacks = True` on `AwsEcsExecutor`

### 2. Remove the `queue_workload()` override
The ECS executor currently overrides `queue_workload()` to reject non-task workloads. We should either remove it entirely (let the base class handle it) or update it to also accept `ExecuteCallback`.

### 3. Update `_process_workloads()` to handle `ExecuteCallback`
Currently it only handles `ExecuteTask`. For callbacks, we need to:
- Serialize the `ExecuteCallback` workload to a command (same pattern as tasks)
- Remove it from `self.queued_callbacks`
- Put it in `pending_tasks` and `self.running`

The key for callbacks is `workload.callback.id` (a string), not a `TaskInstanceKey`.

### 4. Update `execute_async()` to handle `ExecuteCallback` workloads
Currently it only knows how to serialize `ExecuteTask`. It needs to also handle `ExecuteCallback`.

### 5. Update `_serialize_workload_to_command()` to handle any workload type
Currently it's typed for `ExecuteTask` only, but the serialization is generic — `model_dump_json()` works on any workload. The `execute_workload.py` module on the container side uses `TypeAdapter[workloads.All]` to deserialize, so it can handle any type.

### 6. Update `execute_workload.py` (container-side) to handle `ExecuteCallback`
The `task-sdk/src/airflow/sdk/execution_time/execute_workload.py` module currently only handles `ExecuteTask` and raises `ValueError` for anything else. It needs an `ExecuteCallback` branch that calls `execute_callback_workload()`. This is needed for ALL containerized executors (ECS, Batch, K8s), not just ECS.

### 7. Handle callback keys in tracking collections
`EcsTaskCollection` and `EcsQueuedTask` use `TaskInstanceKey` for keys. Callback keys are plain strings. We need to make sure the tracking works for both, or use `WorkloadKey` type.

### 8. Add tests

## Open Questions

- Should `execute_workload.py` changes be a separate PR since they benefit all containerized executors?
- The `EcsQueuedTask.key` is typed as `TaskInstanceKey` — do we need to widen this to `WorkloadKey`?
- The `attempt_task_runs()` method calls `self.log_task_event()` on failure, which expects a `TaskInstanceKey` — callbacks won't have that. Need to handle gracefully.
- The `__handle_failed_task` and retry logic is tightly coupled to `TaskInstanceKey` — need to verify callbacks can flow through the same retry path.

## Reference PRs
- Batch executor callback support: #62984 → see [batch-executor-reference.md](./batch-executor-reference.md)
- Lambda executor callback support: #63035 → see [lambda-executor-reference.md](./lambda-executor-reference.md)
