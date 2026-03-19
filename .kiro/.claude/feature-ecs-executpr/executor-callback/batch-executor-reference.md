# Batch Executor Callback Support — Reference (PR #62984)

How the Batch executor adds `ExecuteCallback` support. This is a close sibling to ECS since both are AWS container-based executors.

## Changes Overview

### 1. `version_compat.py` — Add version gate

```python
AIRFLOW_V_3_2_PLUS: bool = get_base_airflow_version_tuple() >= (3, 2, 0)
```

Added to `__all__` exports. This gate is used everywhere to conditionally enable callback support only on Airflow 3.2+.

### 2. `batch_executor.py` — Executor class changes

#### a. Set `supports_callbacks = True`

```python
class AwsBatchExecutor(BaseExecutor):
    supports_multi_team: bool = True
    supports_callbacks: bool = True  # NEW
```

#### b. `queue_workload()` — Accept both task and callback workloads

Before: rejected anything that wasn't `ExecuteTask`.

After: checks for `ExecuteCallback` first (guarded by `AIRFLOW_V_3_2_PLUS`), then `ExecuteTask`, else raises.

```python
def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
    from airflow.executors import workloads

    if AIRFLOW_V_3_2_PLUS and isinstance(workload, workloads.ExecuteCallback):
        self.queued_callbacks[workload.callback.id] = workload
    elif isinstance(workload, workloads.ExecuteTask):
        ti = workload.ti
        self.queued_tasks[ti.key] = workload
    else:
        raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")
```

Key pattern: callbacks go into `self.queued_callbacks` (from base class) keyed by `workload.callback.id` (a string). Tasks go into `self.queued_tasks` keyed by `TaskInstanceKey`.

#### c. `_process_workloads()` — Handle both workload types

Before: only handled `ExecuteTask`, raised for anything else.

After: branches on type. Both paths serialize the workload into a command list `[w]` and call `execute_async()`.

```python
def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
    from airflow.executors import workloads as wl

    for w in workloads:
        if isinstance(w, wl.ExecuteTask):
            command = [w]
            key = w.ti.key
            queue = w.ti.queue
            executor_config = w.ti.executor_config or {}
            del self.queued_tasks[key]
            self.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)
            self.running.add(key)

        elif AIRFLOW_V_3_2_PLUS and isinstance(w, wl.ExecuteCallback):
            command = [w]
            key = w.callback.id          # string, not TaskInstanceKey
            queue = None
            if isinstance(w.callback.data, dict) and "queue" in w.callback.data:
                queue = w.callback.data["queue"]
            del self.queued_callbacks[key]
            self.execute_async(key=key, command=command, queue=queue)
            self.running.add(key)

        else:
            raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(w)}")
```

Key differences for callbacks vs tasks:
- Key is `w.callback.id` (string) instead of `w.ti.key` (TaskInstanceKey)
- Removed from `self.queued_callbacks` instead of `self.queued_tasks`
- No `executor_config` (callbacks don't have one)
- Queue is extracted from `w.callback.data["queue"]` if present, otherwise `None`

#### d. `execute_async()` — Widen key type, handle both workload types

Signature change: `key: TaskInstanceKey` → `key: TaskInstanceKey | str`

The workload serialization check now accepts both types:

```python
def execute_async(self, key: TaskInstanceKey | str, command: CommandType, queue=None, executor_config=None):
    ...
    if len(command) == 1:
        from airflow.executors import workloads

        if isinstance(command[0], workloads.ExecuteTask) or (
            AIRFLOW_V_3_2_PLUS and isinstance(command[0], workloads.ExecuteCallback)
        ):
            workload = command[0]
            ser_input = workload.model_dump_json()
            command = [
                "python", "-m", "airflow.sdk.execution_time.execute_workload",
                "--json-string", ser_input,
            ]
```

Both `ExecuteTask` and `ExecuteCallback` serialize the same way — `model_dump_json()` produces the JSON, and the container-side `execute_workload.py` deserializes via `TypeAdapter[workloads.All]`.

### 3. `utils.py` — Widen key types in data structures

#### `BatchQueuedJob`
```python
@dataclass
class BatchQueuedJob:
    key: TaskInstanceKey | str   # was: TaskInstanceKey
    ...
```

#### `BatchJobCollection`
```python
class BatchJobCollection:
    def __init__(self):
        self.key_to_id: dict[TaskInstanceKey | str, str] = {}       # was: dict[TaskInstanceKey, str]
        self.id_to_key: dict[str, TaskInstanceKey | str] = {}       # was: dict[str, TaskInstanceKey]
        ...

    def add_job(self,
        job_id: str,
        airflow_workload_key: TaskInstanceKey | str,  # was: airflow_task_key: TaskInstanceKey
        ...
    ):
        self.key_to_id[airflow_workload_key] = job_id
        self.id_to_key[job_id] = airflow_workload_key
        ...

    def pop_by_id(self, job_id: str) -> TaskInstanceKey | str:  # was: -> TaskInstanceKey
        ...
```

The rename from `airflow_task_key` to `airflow_workload_key` reflects that it now handles both tasks and callbacks.

## Pattern Summary

The Batch approach is straightforward:
1. Gate everything behind `AIRFLOW_V_3_2_PLUS`
2. Widen all key types from `TaskInstanceKey` to `TaskInstanceKey | str`
3. Branch on workload type in `queue_workload()` and `_process_workloads()`
4. Both workload types serialize identically via `model_dump_json()` and run through the same container command
5. The container-side deserialization (`TypeAdapter[workloads.All]`) handles both types automatically
