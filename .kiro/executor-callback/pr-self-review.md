# ECS Executor Callback Support — PR Self-Review Guide

Detailed walkthrough of every change in this PR, with reasoning, reviewer context, and things to discuss during review.

---

## File 1: `providers/amazon/src/airflow/providers/amazon/version_compat.py`

### Change: Added `AIRFLOW_V_3_2_PLUS`

```python
AIRFLOW_V_3_2_PLUS: bool = get_base_airflow_version_tuple() >= (3, 2, 0)
```

**Why**: `ExecuteCallback` was introduced in Airflow 3.2. The Amazon provider supports `apache-airflow>=2.11.0`, so we can't import 3.2-only types unconditionally. Every callback code path must be gated behind this constant.

**Reviewer context**: Both ferruzzi and o-nikolas enforced version gating in the Batch (#62984) and Lambda (#63035) PRs. ferruzzi suggested the conditional import pattern:
```python
if AIRFLOW_V_3_2_PLUS:
    from airflow.executors.workloads.types import WorkloadKey
```

**Note**: Also added to `__all__` exports so other modules in the provider can import it.

---

## File 2: `providers/amazon/src/airflow/providers/amazon/aws/executors/ecs/utils.py`

### Change: Widened `EcsQueuedTask` key and queue types

```python
# Before:
key: TaskInstanceKey
queue: str

# After:
key: TaskInstanceKey | str
queue: str | None
```

**Why**: Callbacks use a UUID string as their key (e.g. `"019cef0b-0712-7751-a7f7-dd672a696779"`), not a `TaskInstanceKey` named tuple. Callbacks also don't have a queue — they're dispatched to whatever executor handles the team, so `queue` can be `None`.

**Reviewer context**: The Batch PR (#62984) made the same change to `BatchQueuedJob.key`. Reviewer ferruzzi asked for the param name rename from `airflow_task_key` → `airflow_workload_key` in `add_task()` to reflect that it now handles both.

### Change: Widened `EcsTaskInfo.queue`

```python
queue: str | None  # was: str
```

**Why**: `EcsTaskInfo` stores queue info for running workloads. When a callback is tracked here, the queue will be `None`. Without this change, constructing `EcsTaskInfo` for a callback would fail the type checker.

### Change: Widened all `EcsTaskCollection` dictionary types and method signatures

```python
# All 5 internal dicts changed from:
dict[TaskInstanceKey, ...]
# to:
dict[TaskInstanceKey | str, ...]
```

**Why**: `EcsTaskCollection` is the central tracking structure — it maps between Airflow workload keys, ECS ARNs, commands, and task info. When a callback container is launched, its string key needs to be stored in the same collection alongside task tuple keys.

**What changed**:
- `key_to_arn`: maps workload key → ECS ARN
- `arn_to_key`: maps ECS ARN → workload key (used in `sync_running_tasks()` to look up which workload an ECS task belongs to)
- `key_to_failure_counts`: tracks retry attempts per workload
- `key_to_task_info`: stores command, queue, and config per workload
- All method signatures: `task_by_key()`, `pop_by_key()`, `failure_count_by_key()`, `increment_failure_count()`, `info_by_key()`, `get_all_task_keys()`

**Reviewer context**: o-nikolas was strict about updating docstrings when signatures change. We updated all docstrings from "task" to "workload" in touched methods. We kept methods we didn't touch (like `__getitem__`, `__len__`) as-is per the "Don't conflate changes" feedback.

### Change: Renamed `airflow_task_key` → `airflow_workload_key` in `add_task()`

**Why**: This parameter now accepts both task keys and callback keys. Reviewer ferruzzi explicitly asked for this rename in the Batch PR to reflect the dual-purpose nature.

**Note**: We did NOT rename the method itself (`add_task`) because:
1. It's a public API used by the executor and tests
2. The Batch/Lambda PRs didn't rename their equivalent methods either
3. The reviewer feedback said to rename in "touched code" — the method name wasn't the thing that changed

---

## File 3: `providers/amazon/src/airflow/providers/amazon/aws/executors/ecs/ecs_executor.py`

### Change: Added `cast` import

```python
from typing import TYPE_CHECKING, cast
```

**Why**: The base executor's `success()`, `fail()`, and `running_state()` methods are typed as `(key: TaskInstanceKey)`. When we pass a callback string key, `cast("TaskInstanceKey", task_key)` tells the type checker "trust me, this is fine" without silently suppressing the warning like `# type: ignore` would.

**Reviewer context**: o-nikolas pushed back on `# type: ignore` in the Lambda PR. The Lambda author switched to `cast()` after feedback. Preferred hierarchy: get types right > `cast()` > `type: ignore`.

### Change: Added `AIRFLOW_V_3_2_PLUS` import

```python
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS
```

### Change: Added `WorkloadKey` conditional import

```python
if TYPE_CHECKING:
    ...
    if AIRFLOW_V_3_2_PLUS:
        from airflow.executors.workloads.types import WorkloadKey
```

**Why**: `WorkloadKey` is defined as `TypeAlias = TaskInstanceKey | CallbackKey` in `airflow-core`. It's only available in 3.2+ and is a `TypeAlias`, which means:
1. Can't use it with `isinstance()` at runtime — use concrete types (`ExecuteTask`, `ExecuteCallback`) instead
2. Can only use it in type hints
3. Must be conditionally imported since the provider supports pre-3.2

The double nesting (`TYPE_CHECKING` + `AIRFLOW_V_3_2_PLUS`) is necessary because:
- `TYPE_CHECKING` ensures it's never imported at runtime (avoids import errors on <3.2)
- `AIRFLOW_V_3_2_PLUS` ensures the type checker only sees it when the types exist

**Where it's used**: `execute_async()`, `_run_task()`, `_run_task_kwargs()` signatures.

### Change: `supports_callbacks = True` (version-gated)

```python
if AIRFLOW_V_3_2_PLUS:
    supports_callbacks: bool = True
```

**Why**: This class attribute tells the base executor "yes, I can handle `ExecuteCallback` workloads." Without it, `base_executor.queue_workload()` raises `NotImplementedError` when it receives a callback.

**Why version-gated**: The `supports_callbacks` attribute and `ExecuteCallback` class don't exist pre-3.2. Setting it unconditionally would work at runtime (the base executor checks it dynamically), but gating it makes the intent explicit.

**Reviewer context**: Lambda PR used conditional `if AIRFLOW_V_3_2_PLUS:`, Batch PR set it unconditionally. Both approaches work since the base executor checks the flag at runtime. We chose the conditional approach for explicitness.

### Change: `queue_workload()` — Accept `ExecuteCallback`

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

**Why callback check is first**: Callbacks are checked before tasks because they should be prioritized. The base executor's `_get_workloads_to_schedule()` already prioritizes callbacks over tasks when dispatching, but checking for them first in `queue_workload()` is consistent with that intent.

**Key difference**: Callbacks go into `self.queued_callbacks` (dict keyed by callback UUID string), tasks go into `self.queued_tasks` (dict keyed by `TaskInstanceKey`). `queued_callbacks` is inherited from the base executor — we don't need to initialize it.

**Why `isinstance()` instead of checking `supports_callbacks`**: Runtime type checking with concrete types is more reliable than checking a flag. `isinstance()` with `workloads.ExecuteCallback` directly answers "is this a callback?" without indirection.

**Why `from airflow.executors import workloads` inside the method**: Lazy import to avoid circular imports. The `workloads` module imports executor types, so importing at module level would create a cycle. This pattern is consistent with the existing code.

### Change: `_process_workloads()` — Handle both workload types

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
            key = w.callback.id
            queue = None
            del self.queued_callbacks[key]
            self.execute_async(key=key, command=command, queue=queue)
            self.running.add(key)

        else:
            raise RuntimeError(...)
```

**Key differences for callbacks**:
- `key = w.callback.id` — string UUID, not `TaskInstanceKey` tuple
- `queue = None` — callbacks don't have executor queues
- `del self.queued_callbacks[key]` — removed from callback queue, not task queue
- No `executor_config` — callbacks don't support per-workload container overrides

**Why `from airflow.executors import workloads as wl`**: The method parameter is also named `workloads`, so we alias the import to avoid shadowing.

**Reviewer context**: The Batch PR used the same `if/elif/else` pattern. The Lambda PR used `if/continue` with early returns. Both are fine — we chose `if/elif` to match the Batch approach since ECS is closer to Batch architecturally.

### Change: `execute_async()` — Widened signature, accepts both workload types

```python
def execute_async(
    self, key: WorkloadKey, command: CommandType, queue=None, executor_config=None
):
```

**Type change**: `key: TaskInstanceKey` → `key: WorkloadKey`. Uses the new `WorkloadKey` type alias instead of raw `TaskInstanceKey | str` per reviewer feedback.

**Workload serialization**: Now accepts both `ExecuteTask` and `ExecuteCallback`:
```python
if isinstance(command[0], workloads.ExecuteTask) or (
    AIRFLOW_V_3_2_PLUS and isinstance(command[0], workloads.ExecuteCallback)
):
    command = self._serialize_workload_to_command(command[0])
```

Both serialize identically via `model_dump_json()` — Pydantic handles the type discrimination through the `type` field.

### Change: `_run_task()` / `_run_task_kwargs()` — Widened types

```python
task_id: WorkloadKey   # was: TaskInstanceKey
queue: str | None      # was: str
```

**Note**: These methods are not renamed to `_run_workload()` because:
1. They specifically call AWS ECS `run_task` API — the method name reflects the AWS operation
2. Renaming would be a larger refactor beyond callback support scope
3. Batch/Lambda PRs didn't rename their equivalent methods

### Change: `__update_running_task()` — `cast()` on `success()`

```python
self.success(cast("TaskInstanceKey", task_key))
```

**Why cast, not type: ignore**: `cast()` is explicit — it says "I know `task_key` might be a string here (for callbacks), and the base executor's `success()` is typed for `TaskInstanceKey`, but at this point the base executor handles both key types correctly at runtime." The `type: ignore` approach silently suppresses the warning without explaining why.

**Why this is safe at runtime**: The base executor's `success()` puts the key into `self.event_buffer` which is `dict[WorkloadKey, ...]`. It works with both `TaskInstanceKey` and string keys. The type annotation on `success()` is narrower than what it actually supports.

**Note**: Same pattern applied to `fail()` (2 places), `running_state()` (1 place).

### Change: `__handle_failed_task()` — Renamed log messages

```python
"Airflow workload %s failed due to..."    # was: "Airflow task %s failed due to..."
"Airflow workload %s has failed a maximum..."  # was: "Airflow task %s has failed a maximum..."
```

**Reviewer context**: o-nikolas and ferruzzi both insisted on renaming "task" → "workload" in log messages, variable names, and docstrings wherever the code now handles both types. ferruzzi's suggestion: "Teach users what 'workload' means" — using "workload" in logs helps users understand that the executor processes both tasks and callbacks.

### Change: `attempt_task_runs()` — Guard `log_task_event()` for callback keys

```python
if isinstance(task_key, tuple):
    self.log_task_event(
        event="ecs task submit failure",
        ti_key=task_key,
        ...
    )
```

**Why**: `log_task_event()` on the base executor is typed as `ti_key: TaskInstanceKey` and writes to the task event log table, which expects a `TaskInstanceKey`. Callback string keys can't be written to this table. The `isinstance(task_key, tuple)` check is used because `TaskInstanceKey` is a named tuple — tuples are callbacks' string keys are not.

**Why not skip `fail()` too**: `fail()` still needs to be called for callbacks to report the failure state back to the scheduler. The `cast()` there is safe because the base executor's event buffer handles both key types.

### Change: `_serialize_workload_to_command()` — Updated docstring

```python
:param workload: ExecuteTask or ExecuteCallback workload to serialize
```

**Why no code change**: `model_dump_json()` is a Pydantic method that works on any model. Both `ExecuteTask` and `ExecuteCallback` inherit from `BaseDagBundleWorkload`. The container-side `TypeAdapter[workloads.All]` deserializer handles type discrimination via the `type` field in the JSON. No special serialization logic needed.

### Not changed: `try_adopt_task_instances()`

**Why**: This method only receives `Sequence[TaskInstance]` objects from the scheduler. Callbacks are not task instances — they don't have `external_executor_id`, `key`, `queue`, etc. The method naturally excludes callbacks without any code changes.

**Reviewer context**: ferruzzi confirmed: "I don't believe [callbacks] support adoption yet, but that might/should/could be added in the future (not this PR, obviously)."

---

## File 4: `providers/amazon/tests/unit/amazon/aws/executors/ecs/test_ecs_executor.py`

### New: `TestEcsExecutorCallbackSupport` (8 tests)

Tests callback workloads flowing through the executor:

1. **`test_supports_callbacks_attribute`** — Verifies the flag is set on the class
2. **`test_queue_callback_workload`** — Callback stored in `queued_callbacks` with correct key
3. **`test_queue_workload_rejects_unknown_type`** — Unknown types raise `RuntimeError`
4. **`test_process_callback_workload`** — Callback removed from queue, added to `running` and `pending_tasks`
5. **`test_execute_async_callback_workload`** — Workload serialized to `execute_workload` command with `--json-string`
6. **`test_serialize_callback_workload_to_command`** — Serialized JSON contains `"ExecuteCallback"` type discriminator
7. **`test_callback_sync_running_success`** — Successful ECS container exit → workload removed from `active_workers`
8. **`test_attempt_task_runs_skips_log_task_event_for_callbacks`** — `log_task_event` not called for string keys, verifies graceful handling

### New: `TestEcsTaskCollectionWithStringKeys` (3 tests)

Tests that the collection data structure works with callback string keys:

1. **`test_add_task_with_string_key`** — String key maps to ARN correctly
2. **`test_pop_by_string_key`** — Pop removes the entry and returns the task
3. **`test_mixed_key_types`** — Both `TaskInstanceKey` (mock tuple) and string keys coexist

### Modified: 2 existing test assertions

Updated from `"Airflow task"` → `"Airflow workload"` to match renamed log messages. These are in `test_task_retry_on_api_failure_all_tasks_fail` which asserts on caplog messages.

### Test fixture: `callback_workload`

Creates a real `ExecuteCallback` workload (not a mock) using:
```python
CallbackDTO(
    id="12345678-1234-5678-1234-567812345678",
    fetch_method=CallbackFetchMethod.IMPORT_PATH,
    data={"path": "test.module.alert_func", "kwargs": {}},
)
```

This mirrors how the base executor tests construct callback workloads.

---

## Known Limitations & Discussion Points

### 1. Container-side DAG bundle gap
The `execute_workload.py` fix (separate commit, not part of this PR) calls `execute_callback_workload()` which does `import_module(callback_path)`. For callbacks defined in DAG files, the module isn't on `sys.path` in the container because the DAG bundle isn't set up. This affects ALL container-based executors (ECS, Batch, K8s). See [ecs-callback-error-logs.md](ecs-callback-error-logs.md).

**Impact**: Callbacks using installed packages (e.g. `SlackWebhookNotifier`) work. User-defined callbacks in DAG files don't — they fail with `ModuleNotFoundError`.

**Resolution**: PR #62645 was expected to fix this but doesn't — its `supervise_callback()` also does raw `import_module()` without bundle setup. This needs a separate fix across all container executors.

### 2. `WorkloadKey` is only a type hint
`WorkloadKey` from `airflow.executors.workloads.types` is a `TypeAlias` inside `TYPE_CHECKING`. It can't be used with `isinstance()` at runtime. We use concrete types (`ExecuteTask`, `ExecuteCallback`) for runtime checks and `WorkloadKey` only in function signatures.

### 3. `cast()` vs proper typing
We use `cast("TaskInstanceKey", task_key)` in 4 places where the base executor expects `TaskInstanceKey` but we may pass a string. This is a known typing gap — the base executor's `success()`/`fail()`/`running_state()` work correctly with string keys at runtime, but their signatures are narrower than their actual behavior. This will be resolved when the base executor signatures are updated to use `WorkloadKey`.

### 4. PR #63491 (Unify workload queues) — could require rewrite
If this merges before our PR, the entire `queued_tasks`/`queued_callbacks`/`supports_callbacks` pattern gets replaced by a generic `executor_queues`/`supported_workload_types` system. Our `queue_workload()` and `_process_workloads()` would need rewriting. Check status before merging.
