# ECS Executor Callback Support — Implementation Status

## What We've Done

### 1. `version_compat.py` — Version gate
- Added `AIRFLOW_V_3_2_PLUS` constant to `providers/amazon/src/airflow/providers/amazon/version_compat.py`
- Added to `__all__` exports

### 2. `utils.py` — Widened key types
- `EcsQueuedTask.key`: `TaskInstanceKey` → `TaskInstanceKey | str` (callbacks use UUID strings)
- `EcsQueuedTask.queue`: `str` → `str | None` (callbacks don't have a queue)
- `EcsTaskInfo.queue`: `str` → `str | None`
- `EcsTaskCollection` — all 5 internal dicts widened from `TaskInstanceKey` to `TaskInstanceKey | str`
- All method signatures (`add_task`, `pop_by_key`, `task_by_key`, `failure_count_by_key`, `increment_failure_count`, `info_by_key`, `get_all_task_keys`) widened to accept `TaskInstanceKey | str`
- Renamed `airflow_task_key` → `airflow_workload_key` in `add_task()` per reviewer feedback
- Updated docstrings from "task" to "workload" where applicable

### 3. `ecs_executor.py` — Core callback support
- **`supports_callbacks = True`** — gated behind `if AIRFLOW_V_3_2_PLUS:`
- **`queue_workload()`** — accepts `ExecuteCallback`, stores in `self.queued_callbacks[workload.callback.id]`
- **`_process_workloads()`** — branches on type: `ExecuteTask` vs `ExecuteCallback`. Callbacks use `w.callback.id` as key, `queue=None`, removed from `queued_callbacks`
- **`execute_async()`** — widened `key` param to `TaskInstanceKey | str`, accepts both `ExecuteTask` and `ExecuteCallback` in the command serialization check
- **`_serialize_workload_to_command()`** — updated docstring; implementation was already generic (`model_dump_json()` works on both types)
- **`_run_task()` / `_run_task_kwargs()`** — widened `task_id` to `TaskInstanceKey | str`, `queue` to `str | None`
- **`attempt_task_runs()`** — `log_task_event()` guarded with `isinstance(task_key, tuple)` since it requires `TaskInstanceKey`; `self.fail()` and `self.running_state()` annotated with `# type: ignore[arg-type]`
- **`__handle_failed_task()`** — `self.fail()` annotated with `# type: ignore[arg-type]`
- **`__update_running_task()`** — `self.success()` annotated with `# type: ignore[arg-type]`
- **Log messages** — changed "Airflow task" → "Airflow workload" in touched code (`__handle_failed_task`, `__update_running_task`, `attempt_task_runs`)
- **`try_adopt_task_instances()`** — no changes needed; callbacks don't support adoption and this method only receives `TaskInstance` objects

### 4. `execute_workload.py` (task-sdk) — Container-side fix
- Added `ExecuteCallback` branch in `execute_workload()` that calls `execute_callback_workload()` from `airflow.executors.workloads.callback`
- Returns early — no API server connection needed for callbacks
- This is a **shared fix** that benefits all container-based executors (ECS, Batch, K8s)

### 5. Tests — 11 new tests
**`TestEcsExecutorCallbackSupport`** (8 tests):
- `test_supports_callbacks_attribute` — verifies flag is set
- `test_queue_callback_workload` — callback stored in `queued_callbacks`
- `test_queue_workload_rejects_unknown_type` — unknown types raise `RuntimeError`
- `test_process_callback_workload` — callback removed from queue, added to running + pending
- `test_execute_async_callback_workload` — serialized to `execute_workload` command
- `test_serialize_callback_workload_to_command` — JSON contains `"ExecuteCallback"`
- `test_callback_sync_running_success` — successful ECS task removed from active workers
- `test_attempt_task_runs_skips_log_task_event_for_callbacks` — `log_task_event` not called for string keys

**`TestEcsTaskCollectionWithStringKeys`** (3 tests):
- `test_add_task_with_string_key` — collection works with UUID string key
- `test_pop_by_string_key` — pop works with string key
- `test_mixed_key_types` — collection holds both `TaskInstanceKey` and string keys simultaneously

### 6. Existing test fixes
- Updated 2 test assertions from `"Airflow task"` → `"Airflow workload"` to match renamed log messages

## Commits

1. `38f8750` — **Add ExecuteCallback support to AWS ECS Executor** (ecs_executor.py, utils.py, version_compat.py, tests)
2. `90a7bb3` — **Add container-side ExecuteCallback handling and research docs** (execute_workload.py, .kiro docs)

## Next Steps

- [ ] Push branch to fork: `git push origin feature/ecs-executpr`
- [ ] Deploy to EC2 test environment: `bash /opt/airflow-scripts/switch-branch.sh feature/ecs-executpr`
- [ ] Verify worker image is built from source (Breeze) and pushed to ECR
- [ ] Deploy a test DAG with `DeadlineAlert` + `SyncCallback` and a short deadline (~30s)
- [ ] Trigger the DAG, wait for deadline miss, confirm callback executes on ECS
- [ ] Run `prek run ruff --from-ref main` and `prek run ruff-format --from-ref main` (full suite)
- [ ] Run `prek run --from-ref main --stage pre-commit` (all fast static checks)
- [ ] Open PR against `apache/airflow` main

## Be Careful About

### PR #63491 (Unify executor workload queues) — OPEN
If this merges before our PR, the entire queue management changes. `queued_callbacks`, `queued_tasks`, and `supports_callbacks` get replaced by a generic `executor_queues` / `supported_workload_types` system. Our `queue_workload()` and `_process_workloads()` would need rewriting. **Check status before opening PR.**

### PR #62645 (Supervised callback process) — OPEN
Changes how callbacks execute on the worker side. Adds `supervise_callback()`. If this merges, our `execute_workload.py` patch may conflict or become redundant. The ECS executor-side changes are unaffected.

### `execute_workload.py` is shared code
Our container-side fix is in `task-sdk/`, not in the Amazon provider. Reviewers may ask us to split it into a separate PR since it benefits all container-based executors (ECS, Batch, K8s). Be prepared to either justify including it or split it out.

### `type: ignore[arg-type]` on `success()`, `fail()`, `running_state()`
The base executor types these as `TaskInstanceKey`. We pass string keys for callbacks. The Batch/Lambda PRs use the same `type: ignore` pattern. If reviewers push back, use `cast()` instead. But this is a known limitation until `WorkloadKey` is used in the base class signatures.

### Reviewer expectations (from pr-review-feedback.md)
- Use **full variable names** — no abbreviations (`callback` not `cb`)
- **No unrelated changes** — keep PR focused on callback support only
- **Clean up touched code** — rename "task" → "workload" in code you modify, but don't touch untouched code
- Callback **adoption is not supported** — don't add adoption logic, just skip gracefully
- Gate all callback code behind **`AIRFLOW_V_3_2_PLUS`**

### Worker image must include the `execute_workload.py` fix
Without it, ECS containers will crash with `ValueError: Executor does not know how to handle ExecuteCallback`. The `rebuild-worker-image.sh` script on EC2 uses Breeze to build from source, which includes this fix when on our branch.
