# ECS Executor Callback — PR Review Feedback & Implementation Guidelines

Lessons learned from reviewer feedback on the Batch (#62984) and Lambda (#63035) executor callback PRs. Apply these when implementing ECS callback support.

## How Callbacks Work (Quick Reference)

1. DAG author defines a `SyncCallback` with a dotted import path to a callable (function or notifier class)
2. When a deadline is missed, scheduler creates an `ExecutorCallback` DB record (state: PENDING)
3. Scheduler queries PENDING callbacks, creates `ExecuteCallback` workload, calls `executor.queue_workload()`
4. Executor schedules it — callbacks get priority over tasks
5. Worker deserializes the workload, imports the callable by path, executes it, reports SUCCESS/FAILED

Callbacks are lightweight workloads — no heartbeats, no XCom, no retries. Same serialization path as tasks (`model_dump_json()` → `execute_workload` entrypoint). The reason they run on executors (not scheduler/triggerer) is that the callback function lives in user code (DAG bundles), and the scheduler never runs user code. Similar to how a Lambda handler resolves a dotted path.

Two callable patterns:
- **Function**: imported and called directly with kwargs
- **Class** (e.g. `BaseNotifier`): calling the class creates an instance, then the instance's `__call__` is invoked with context — two-step execution

## Reviewer Feedback Summary

### Typing (Critical — reviewers enforced this consistently)

- **Use `WorkloadKey`** from `airflow-core/src/airflow/executors/workloads/types.py` instead of raw `TaskInstanceKey | str`. Both ferruzzi and o-nikolas flagged this repeatedly.
- **Avoid `# type: ignore`** — o-nikolas pushed back on this. Use `cast()` if needed, but prefer getting types right. The Lambda PR author switched from `type: ignore` to `cast()` after feedback.
- **Backwards compatibility tension**: The Amazon provider supports `apache-airflow>=2.11.0`, so newer types from 3.2 can't be imported unconditionally. ferruzzi suggested conditional imports:
  ```python
  if AIRFLOW_V_3_2_PLUS:
      from airflow.executors.workloads.types import SchedulerWorkload
  ```
- **Note**: `SchedulerWorkload` is a `TypeAlias`, so `isinstance()` won't work with it. Use concrete types (`ExecuteTask`, `ExecuteCallback`) for runtime checks.
- **`BaseExecutor.queue_workload`** uses `workloads.All` in its signature — subclasses are constrained by this.

### Naming (Reviewers were strict about this)

- **Rename "task" → "workload"** in variable names, method names, docstrings, comments, and log messages wherever the code now handles both tasks and callbacks.
- **Use full names** — no abbreviations. e.g. `callback` not `cb`. o-nikolas: "the clarity is worth 4 more characters."
- **Teach users what "workload" means** in docstrings. ferruzzi's suggestion: *"Check in on currently running tasks and callbacks and attempt to run any new workloads that have been queued."* Expand so users learn the term.
- **Exception**: Some names are OK to keep as-is when the underlying data structure specifically refers to tasks (e.g. `ser_task_key` when it comes from a JSON key named `task_key`). The Lambda author explained this and o-nikolas agreed it was out of scope.

### Scope & Cleanliness

- **Don't conflate changes** — o-nikolas explicitly asked to remove an unrelated queue parameter change from the Batch PR. Keep the PR focused on callback support only.
- **Clean up while you're in there** — ferruzzi asked to rename bad variable names like `w` when touching the same code. Small improvements in touched code are expected.
- **Clean up before submitting** — o-nikolas: "Please be sure to cleanup your code before submitting a PR. Make sure types are correct, all comments, variables, method names updated. It takes a long time for maintainers to review code so we want to be using that time wisely."

### Callback Adoption

- **Callbacks don't support adoption yet.** ferruzzi confirmed: "I don't believe they support adoption yet, but that might/should/could be added in the future (not this PR, obviously)."
- In `try_adopt_task_instances`, either skip callbacks or just don't log errors for them. Don't add full adoption logic.

### Version Gating

- Gate all callback code behind `AIRFLOW_V_3_2_PLUS` from `version_compat.py`
- Both approaches seen in PRs work:
  - Batch: `supports_callbacks: bool = True` (unconditional class attribute)
  - Lambda: `if AIRFLOW_V_3_2_PLUS: supports_callbacks: bool = True` (conditional)
- The base executor checks the flag at runtime either way

### Serialization

- Both `ExecuteTask` and `ExecuteCallback` serialize identically via `model_dump_json()`
- Container runs: `python -m airflow.sdk.execution_time.execute_workload --json-string <json>`
- Container-side `TypeAdapter[workloads.All]` handles deserialization of both types
- No special serialization logic needed per workload type

## Key Differences: Callbacks vs Tasks

| Aspect | Task | Callback |
|--------|------|----------|
| Key type | `TaskInstanceKey` (named tuple) | `str` (UUID) |
| Queue source | `queued_tasks` | `queued_callbacks` |
| `executor_config` | Yes | No |
| Scheduling priority | After callbacks | Before tasks |
| Result | Rich state machine | Simple success/failure |
| Adoption | Supported | Not yet |
| Key serialization | `json.dumps(key._asdict())` | Plain string |

## Related PRs to Watch

These open PRs could affect our ECS implementation if they merge first:

- **#62645** — "Move ExecutorCallback execution into a supervised process" — changes how callbacks are executed on the worker side. If merged, the execution model shifts and we may not need container-side changes.
- **#63491** — "Unify executor workload queues" — simplifies the dual-queue pattern (`queued_tasks` + `queued_callbacks`). If merged, our `queue_workload()` and `_process_workloads()` changes get simpler.
- **#63454** — K8s executor callback support — another container-based executor. Useful as an additional reference alongside Batch and Lambda.

**Check the status of these before starting implementation.** If #63491 merges, the queue handling changes significantly.

## Checklist for ECS Implementation

Based on reviewer expectations:

- [ ] Set `supports_callbacks = True` (with version gate)
- [ ] Add `AIRFLOW_V_3_2_PLUS` to `version_compat.py`
- [ ] Use `WorkloadKey` type where key can be task or callback
- [ ] Handle `ExecuteCallback` in `queue_workload()` — or remove override if base class handles it
- [ ] Handle `ExecuteCallback` in `_process_workloads()`
- [ ] Handle `ExecuteCallback` in `execute_async()` / `_serialize_workload_to_command()`
- [ ] Update `EcsTaskCollection` / `EcsQueuedTask` key types
- [ ] Rename variables/methods from "task" to "workload" in touched code
- [ ] Use full variable names, no abbreviations
- [ ] Skip callbacks in `try_adopt_task_instances` gracefully
- [ ] Handle callback string keys in any key serialization/deserialization
- [ ] Add tests for callback queuing, processing, dispatching
- [ ] Keep PR focused — no unrelated changes
- [ ] Run `prek run ruff --from-ref main` and `prek run ruff-format --from-ref main`
