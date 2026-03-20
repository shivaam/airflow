# Issue #56271: KubernetesExecutor Feature Broken in 3.1.0

## Problem Description

After upgrading from Airflow 3.0.3 to 3.1.0, the **multi-executor feature** breaks when using
`KubernetesExecutor`. The configuration:

```
AIRFLOW__CORE__EXECUTOR=CeleryExecutor,KubernetesExecutor
```

worked in 3.0.3 but now causes tasks assigned to `KubernetesExecutor` to fail during DAG parsing
with:

```
UnknownExecutorException: Task 'my_task' specifies executor 'KubernetesExecutor',
which is not available. Make sure it is listed in your [core] executors configuration.
```

### Reproduction

```python
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG("example_dag", ...) as dag:
    task = BashOperator(
        task_id="k8s_task",
        bash_command="echo hello",
        executor='KubernetesExecutor'  # Fails in 3.1.0
    )
```

The error occurs specifically on **worker pods** that parse DAGs with only `LocalExecutor`
configured locally, while the scheduler has the full multi-executor list.

---

## Root Cause Analysis

### The Code Path

1. **DAG parsing triggers validation unconditionally**

   `airflow-core/src/airflow/dag_processing/dagbag.py:350` — During `DagBag._parse_file()`,
   `_validate_executor_fields(dag, self.bundle_name)` is called for every parsed DAG, regardless
   of the execution context (scheduler, worker, dag-processor).

2. **Validation checks executors against local config**

   `airflow-core/src/airflow/dag_processing/dagbag.py:122-160` — `_validate_executor_fields()`
   iterates all tasks in the DAG. For each task with a non-None `executor` field, it calls
   `_executor_exists()`:

   ```python
   def _validate_executor_fields(dag: DAG, bundle_name: str | None = None) -> None:
       ...
       for task in dag.tasks:
           if not task.executor:
               continue
           if not _executor_exists(task.executor, dag_team_name):
               raise UnknownExecutorException(...)
   ```

3. **Executor existence check reads from local `[core] executors` config**

   `airflow-core/src/airflow/dag_processing/dagbag.py:102-119` — `_executor_exists()` calls
   `ExecutorLoader.lookup_executor_name_by_str()` which reads from the local Airflow configuration:

   ```python
   def _executor_exists(executor_name: str, team_name: str | None) -> bool:
       try:
           ExecutorLoader.lookup_executor_name_by_str(
               executor_name, team_name=team_name, validate_teams=False
           )
           return True
       except UnknownExecutorException:
           ...
       return False
   ```

4. **ExecutorLoader resolves against local config**

   `airflow-core/src/airflow/executors/executor_loader.py` — `ExecutorLoader._get_executor_names()`
   reads `[core] executors` from the local Airflow config. On a KubernetesExecutor worker pod, the
   local config typically only has `LocalExecutor`, not the full multi-executor list that the
   scheduler has.

### What Changed in 3.1.0

In Airflow 3.0.3, two DAG implementations coexisted:

- **Legacy `airflow.models.dag.DAG`**: Included executor validation during `dag.validate()`
- **Recommended `airflow.sdk.DAG`**: Did **not** perform executor validation

Workers using `airflow.sdk.DAG` (the recommended import) would parse DAGs without executor
validation, so even if their local config only had `LocalExecutor`, DAGs with
`executor='KubernetesExecutor'` would parse successfully.

**PR #54383** in 3.1.0 eliminated the legacy `airflow.models.dag.DAG` class entirely and unified
everything under `airflow.sdk.DAG`. A new standalone function `_validate_executor_fields()` was
added to `dagbag.py` and is called **unconditionally** during `_parse_file()`, regardless of the
execution context. This means every component that parses DAGs — including workers — now
validates executors against their local config.

---

## Architecture of the Multi-Executor Feature

### Component Roles

```
                        ┌─────────────────────┐
                        │     Scheduler        │
                        │                      │
                        │ Config: CeleryExec,  │
                        │   KubernetesExec     │
                        │                      │
                        │ Reads serialized DAGs│
                        │ Creates DagRuns/TIs  │
                        │ Routes tasks to the  │
                        │ correct executor     │
                        └──────────┬───────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼                              ▼
        ┌───────────────────┐          ┌───────────────────┐
        │  Celery Workers   │          │  K8s Worker Pods   │
        │                   │          │                    │
        │ Config: Celery    │          │ Config: Local      │
        │  Executor         │          │  Executor          │
        │                   │          │                    │
        │ Parse DAGs to     │          │ Parse DAGs to      │
        │ execute tasks     │          │ execute tasks      │
        └───────────────────┘          └────────────────────┘
```

### Key Architectural Principles

1. **Scheduler owns the full executor list** — it reads `[core] executors` and routes task
   instances to the appropriate executor based on `task.executor`.

2. **Workers may run with a subset** — KubernetesExecutor worker pods typically have only
   `LocalExecutor` in their config. They don't need to know about other executors; they just
   execute the task they're assigned.

3. **DAG parsing on workers is for execution, not scheduling** — workers parse DAGs only to
   reconstruct the task for execution. Executor validation is a scheduling concern, not an
   execution concern.

### DAG Parsing Flow

```
DagBag._parse_file()
  └─→ loader.load_file()
       └─→ for each DAG found:
            └─→ _validate_executor_fields(dag, bundle_name)  ← BREAKS HERE
                 └─→ for each task:
                      └─→ _executor_exists(task.executor, team_name)
                           └─→ ExecutorLoader.lookup_executor_name_by_str()
                                └─→ reads [core] executors from LOCAL config
```

---

## Proposed Solutions

### Solution A: Context-Aware Validation (Recommended)

Skip or downgrade executor validation when running on a worker context. The validation is only
meaningful on the scheduler/dag-processor where the full executor config is available.

**Implementation**: In `_validate_executor_fields()`, check if we're in a DAG parsing context
(worker) vs. a scheduling context. The `_airflow_parsing_context_manager` or a similar mechanism
could be used to detect this.

**Effort**: Small — 1-2 files, <50 lines changed.

**Files to modify**:
- `airflow-core/src/airflow/dag_processing/dagbag.py` — add context check in `_validate_executor_fields()`

### Solution B: Downgrade to Warning

Instead of raising `UnknownExecutorException`, log a warning when an executor is not found
locally. The scheduler already validated the executor when the DAG was first parsed and serialized.

**Effort**: Tiny — 5-10 lines changed in `dagbag.py`.

### Solution C: Pass Full Executor Config to Workers

Ensure workers inherit the full `[core] executors` configuration from the scheduler/API server.

**Effort**: Medium — requires config propagation changes across components.

---

## Blind Spots and Potential Problems

1. **Team-scoped executor validation**: The `_validate_executor_fields` function also handles
   `multi_team` mode (lines 129-142). Any fix must not break team-scoped validation where
   executors are restricted per team.

2. **Security implications of skipping validation**: If validation is skipped on workers, a
   malicious DAG could specify an arbitrary executor name. However, since the scheduler validates
   before scheduling, this is low risk.

3. **Serialized DAG consistency**: Workers typically use serialized DAGs from the metadata DB
   (which were already validated by the scheduler). The parsing-time validation is redundant for
   serialized DAGs but catches issues in direct file parsing.

4. **Testing coverage needed**:
   - Single executor config
   - Multi-executor config with KubernetesExecutor
   - Worker-only config (LocalExecutor) parsing DAGs with KubernetesExecutor tasks
   - Team-based executor scoping

5. **Existing test file**: `airflow-core/tests/unit/dag_processing/test_dagbag.py` has tests for
   `_validate_executor_fields` that need to be extended.
