# Lambda Executor Callback Support — Reference (PR #63035)

How the Lambda executor adds `ExecuteCallback` support. Lambda is a simpler execution model (no long-running containers), but the callback pattern is the same.

## Changes Overview

### 1. `version_compat.py` — Same as Batch

```python
AIRFLOW_V_3_2_PLUS: bool = get_base_airflow_version_tuple() >= (3, 2, 0)
```

### 2. `lambda_executor.py` — Executor class changes

#### a. Set `supports_callbacks` conditionally

```python
class AwsLambdaExecutor(BaseExecutor):
    supports_multi_team: bool = True

    if AIRFLOW_V_3_2_PLUS:
        supports_callbacks: bool = True
```

Note: Lambda gates the class attribute itself behind `AIRFLOW_V_3_2_PLUS`, unlike Batch which sets it unconditionally. Both approaches work since the base executor checks the flag at runtime.

#### b. `__init__()` — Initialize callback tracking

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.pending_tasks: deque = deque()
    self.running_tasks: dict[str, TaskInstanceKey | str] = {}  # was: dict[str, TaskInstanceKey]

    if AIRFLOW_V_3_2_PLUS:
        self.queued_callbacks: dict[str, workloads.ExecuteCallback] = {}
```

Lambda explicitly initializes `queued_callbacks` (the base class also does this, but Lambda re-declares it). The `running_tasks` dict value type is widened to accept string callback keys.

#### c. `queue_workload()` — Accept both types

```python
def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
    from airflow.executors import workloads

    if isinstance(workload, workloads.ExecuteTask):
        ti = workload.ti
        self.queued_tasks[ti.key] = workload
        return

    if AIRFLOW_V_3_2_PLUS and isinstance(workload, workloads.ExecuteCallback):
        self.queued_callbacks[workload.callback.id] = workload
        return

    raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")
```

Uses early returns instead of if/elif — slightly different style from Batch but same logic.

#### d. `_process_workloads()` — Handle both types

```python
def _process_workloads(self, workload_items: Sequence[workloads.All]) -> None:
    for w in workload_items:
        key: TaskInstanceKey | str
        command: list[workloads.All]
        queue: str | None

        if isinstance(w, workloads.ExecuteTask):
            command = [w]
            key = w.ti.key
            queue = w.ti.queue
            executor_config = w.ti.executor_config or {}
            del self.queued_tasks[key]
            self.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)
            self.running.add(key)
            continue

        if AIRFLOW_V_3_2_PLUS and isinstance(w, workloads.ExecuteCallback):
            command = [w]
            key = w.callback.id
            queue = None
            if isinstance(w.callback.data, dict) and "queue" in w.callback.data:
                queue = w.callback.data["queue"]
            del self.queued_callbacks[key]
            self.execute_async(key=key, command=command, queue=queue)
            self.running.add(key)
            continue

        raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(w)}")
```

Uses `continue` instead of `elif` — same logic as Batch, different control flow style.

#### e. `execute_async()` — Widen types, handle both workloads

Signature changes:
- `key: TaskInstanceKey` → `key: TaskInstanceKey | str`
- `command: CommandType` → `command: CommandType | Sequence[workloads.All]`

```python
def execute_async(self, key: TaskInstanceKey | str, command: CommandType | Sequence[workloads.All],
                  queue=None, executor_config=None):
    if len(command) == 1:
        if AIRFLOW_V_3_2_PLUS:
            if not isinstance(command[0], (workloads.ExecuteTask, workloads.ExecuteCallback)):
                raise RuntimeError(...)
        else:
            if not isinstance(command[0], workloads.ExecuteTask):
                raise RuntimeError(...)

        workload = command[0]
        ser_input = workload.model_dump_json()
        command = [
            "python", "-m", "airflow.sdk.execution_time.execute_workload",
            "--json-string", ser_input,
        ]
```

Same serialization pattern as Batch — both workload types use `model_dump_json()`.

#### f. `attempt_task_runs()` — Handle callback key serialization

Callbacks use string IDs, not `TaskInstanceKey` tuples. The serialization of the key for the Lambda payload needs to handle both:

```python
try:
    ser_task_key = json.dumps(task_key._asdict())
except AttributeError:
    # Callback workloads use string id.
    ser_task_key = task_key
```

`TaskInstanceKey` is a named tuple with `_asdict()`. Callback keys are plain strings, so `_asdict()` raises `AttributeError` — caught and handled.

#### g. `sync_running_tasks()` / `process_queue()` — Widen type annotations

State reporting calls use `type: ignore[arg-type]` since `success()` and `fail()` on the base executor expect `TaskInstanceKey` but callbacks pass strings:

```python
self.success(task_key)  # type: ignore[arg-type]
self.fail(task_key)     # type: ignore[arg-type]
```

#### h. `try_adopt_task_instances()` — Handle callback keys in deserialization

```python
for ti, ser_task_key in serialized_task_keys:
    try:
        data = json.loads(ser_task_key)
        task_key = TaskInstanceKey.from_dict(data)
    except Exception:
        # Callback workloads use string keys.
        task_key = ser_task_key
    self.running_tasks[ser_task_key] = task_key
    adopted_tis.append(ti)
```

If JSON deserialization into `TaskInstanceKey` fails, it's a callback key — use the raw string.

### 3. `utils.py` — Widen key types

```python
@dataclass
class LambdaQueuedTask:
    """Represents a Lambda workload that is queued."""
    key: TaskInstanceKey | str   # was: TaskInstanceKey
    ...

CommandType = Sequence[Any]  # was: Sequence[str] — widened to accept workload objects
```

## Key Differences from Batch

| Aspect | Batch | Lambda |
|--------|-------|--------|
| `supports_callbacks` | Set unconditionally | Gated behind `AIRFLOW_V_3_2_PLUS` |
| `queued_callbacks` init | Inherited from base | Explicitly initialized in `__init__` |
| Control flow style | `if/elif/else` | `if/continue` with early returns |
| Key serialization | N/A (uses job IDs) | `json.dumps(_asdict())` with `AttributeError` fallback |
| `CommandType` | Unchanged | Widened to `Sequence[Any]` |
| Task adoption | N/A for callbacks | Handles callback key deserialization fallback |

## Pattern Summary

Lambda follows the same core pattern as Batch:
1. Gate behind `AIRFLOW_V_3_2_PLUS`
2. Widen key types to `TaskInstanceKey | str`
3. Branch on workload type in queue/process/execute methods
4. Both types serialize via `model_dump_json()` to the same container command
5. Extra handling needed for Lambda-specific key serialization (since Lambda uses serialized keys as external IDs and SQS message identifiers)
