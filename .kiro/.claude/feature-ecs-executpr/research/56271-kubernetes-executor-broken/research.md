# Issue #56271: KubernetesExecutor Feature Broken in 3.1.0+

**Issue:** https://github.com/apache/airflow/issues/56271
**Affected versions:** 3.1.0, 3.1.1, 3.1.3, 3.1.5 (confirmed by reporters)
**Severity:** High — breaks KubernetesExecutor for all users who specify `executor='KubernetesExecutor'` on tasks

## Problem Summary

When a task specifies `executor='KubernetesExecutor'`, the KubernetesExecutor-spawned worker pod
fails with `UnknownExecutorException` during DAG parsing. The worker pod's
`AIRFLOW__CORE__EXECUTOR` is set to `LocalExecutor` (as documented), but the new
`_validate_executor_fields()` function rejects any executor not present in the pod's local config.

The workaround reported by users is to add `KubernetesExecutor` to `AIRFLOW__CORE__EXECUTOR` in
the pod template file, but this shouldn't be necessary — the docs say only `LocalExecutor` is
needed in the pod template.

## Root Cause Analysis

### The Validation Chain

1. **Worker pod starts a task** → `task_runner.parse()` in `task-sdk/src/airflow/sdk/execution_time/task_runner.py` (line ~740)
2. **Creates a `BundleDagBag`** → `airflow-core/src/airflow/dag_processing/dagbag.py`
3. **`BundleDagBag.process_file()`** calls `_validate_executor_fields(dag, self.bundle_name)` (line ~350)
4. **`_validate_executor_fields()`** calls `_executor_exists()` (line ~122)
5. **`_executor_exists()`** calls `ExecutorLoader.lookup_executor_name_by_str()` (line ~108)
6. **`lookup_executor_name_by_str()`** checks `_alias_to_executors_per_team`, `_module_to_executors_per_team`, `_classname_to_executors_per_team` — all populated from the pod's local `[core] executor` config
7. **Pod config only has `LocalExecutor`** → `KubernetesExecutor` not found → `UnknownExecutorException`

### Why This Broke in 3.1.0

PR #54383 removed `airflow.models.dag.DAG` and consolidated everything into `airflow.sdk.DAG`.
As part of this, `_validate_executor_fields()` was added as a standalone function called during
`_process_modules()` / `process_file()` for ALL DAGs. Previously, `airflow.sdk.DAG.validate()`
did NOT include executor validation, so worker pods could parse DAGs without knowing about all
executors.

### The Design Flaw

The validation assumes all executors referenced in a DAG must be available in the **current
environment**. But in a KubernetesExecutor setup:

- **Scheduler/DAG Processor** has all executors configured (e.g., `CeleryExecutor,KubernetesExecutor`)
- **Worker pods** only need `LocalExecutor` to execute their assigned task
- **Task-level `executor=` specifications** are instructions for the **scheduler** about which
  executor to use for dispatching, not requirements for the worker pod's environment

The validation is correct when run in the DAG Processor (scheduler-side), but incorrect when
run in the worker pod (task execution side).

## Key Files

| File | Relevance |
|------|-----------|
| `airflow-core/src/airflow/dag_processing/dagbag.py` | `_validate_executor_fields()` (L122-160), `_executor_exists()` (L102-120), `BundleDagBag.process_file()` call site (L350) |
| `airflow-core/src/airflow/executors/executor_loader.py` | `lookup_executor_name_by_str()` (L315-335) — resolves executor names against local config |
| `task-sdk/src/airflow/sdk/execution_time/task_runner.py` | `parse()` (L740-760) — creates `BundleDagBag` in worker pod, triggering validation |
| `airflow-core/src/airflow/dag_processing/processor.py` | `_parse_file()` (L208) — scheduler-side DAG parsing (validation is correct here) |
| `airflow-core/tests/unit/dag_processing/test_dagbag.py` | Test suite for `_validate_executor_fields()` (L78-275) |

## Possible Fix Approaches

### Approach 1: Skip executor validation in worker pods (Recommended)

The `parse()` function in `task_runner.py` runs in the worker pod context. The worker pod
doesn't need to validate executors — it just needs to load the DAG to execute its assigned task.
The executor validation should only happen during DAG Processor parsing (scheduler-side).

**Option A:** Add a parameter to `BundleDagBag` to skip executor validation:
```python
# In task_runner.py parse():
bag = BundleDagBag(
    dag_folder=dag_absolute_path,
    safe_mode=False,
    load_op_links=False,
    bundle_path=bundle_instance.path,
    bundle_name=bundle_info.name,
    validate_executors=False,  # Skip in worker pod context
)
```

**Option B:** Check the process context before validating:
```python
# In dagbag.py _validate_executor_fields():
import os
if os.environ.get("_AIRFLOW_PROCESS_CONTEXT") == "client":
    return  # Skip validation in worker/client context
```

The `_AIRFLOW_PROCESS_CONTEXT` env var is already set to `"client"` in `_parse_file_entrypoint()`
(processor.py L183), so this could be a clean signal. However, the DAG Processor also sets this
to `"client"`, so we'd need a more specific check — or a different approach.

**Option C:** Only validate in `_parse_file()` (processor.py), not in `BundleDagBag.process_file()`:
Move the `_validate_executor_fields()` call out of `BundleDagBag.process_file()` and into
`_parse_file()` in `processor.py`. This way it only runs during scheduler-side DAG parsing.

### Approach 2: Make executor validation aware of the full cluster config

Instead of validating against the local pod's executor config, validate against a known list of
valid executor class names (built-in executors). This is more complex and may not cover custom
executors.

### Approach 3: Catch and warn instead of raising

Change `_validate_executor_fields()` to log a warning instead of raising an exception when
running in a non-scheduler context. This is less clean but backward-compatible.

## Recommended Fix: Approach 1, Option C

Move `_validate_executor_fields()` out of `BundleDagBag.process_file()` and into the
DAG Processor's `_parse_file()` function. This is the cleanest separation because:

1. `BundleDagBag` is used in both scheduler-side parsing AND worker-pod task execution
2. `_parse_file()` in `processor.py` is only called during scheduler-side DAG parsing
3. The scheduler has the full executor configuration
4. No new parameters or environment variable checks needed

The change would be:
- **Remove** `_validate_executor_fields(dag, self.bundle_name)` from `BundleDagBag.process_file()` (dagbag.py L350)
- **Add** executor validation in `_parse_file()` (processor.py) after `BundleDagBag` returns parsed DAGs

## Reproduction Steps

1. Deploy Airflow 3.1.x with `AIRFLOW__CORE__EXECUTOR=CeleryExecutor,KubernetesExecutor`
2. In pod template file, set `AIRFLOW__CORE__EXECUTOR=LocalExecutor` (as documented)
3. Create a DAG with a task that specifies `executor='KubernetesExecutor'`
4. The task will fail with `UnknownExecutorException` in the worker pod

## Related Issues / PRs

- PR #54383 — Introduced the regression by adding `_validate_executor_fields()` to the common DAG parsing path
- PR #49433 — Related to LocalExecutor in pod_template_file not being needed
- Issue discussion confirms the workaround: add `KubernetesExecutor` to `AIRFLOW__CORE__EXECUTOR` in pod template
