# Issue #56271 — Possible Solutions, Edge Cases & Impact Analysis

## Where BundleDagBag Is Used (All Call Sites)

Before picking a fix, it's important to know every place `BundleDagBag` is instantiated,
because `_validate_executor_fields()` currently runs inside `BundleDagBag.process_file()`
and affects all of them:

| Location | Context | Has full executor config? |
|----------|---------|--------------------------|
| `dag_processing/processor.py` — `_parse_file()` | DAG Processor (scheduler-side) | Yes |
| `task_runner.py` — `parse()` | Worker pod (task execution) | No — only LocalExecutor |
| `utils/cli.py` — `get_dag()` | CLI commands (`airflow dags test`, etc.) | Depends on user env |
| `cli/commands/dag_command.py` — list, report, reserialize | CLI commands | Depends on user env |
| `sdk/definitions/dag.py` — `DAG.test()` | DAG test method | Depends on user env |


The worker pod case is the broken one. The CLI cases are also interesting — a user running
`airflow dags list` on a machine that only has `LocalExecutor` configured would also
hit this error if any DAG r

The worker pod case is the broken one. The CLI cases are also interesting — a user running
`airflow dags list` on a machine that only has `LocalExecutor` would also hit this error
if any DAG references `KubernetesExecutor`.

---

## Possible Solutions

### Solution A: Move validation to `_parse_file()` only (Scheduler-side)

Remove `_validate_executor_fields()` from `BundleDagBag.process_file()` and call it only
in `_parse_file()` in `processor.py`.

Pros:
- Clean separation — validation only where the full config exists
- Minimal code change
- CLI and worker pods stop failing on executor references they don't need

Cons:
- Validation errors change reporting path slightly (import errors in DAG Processor)
- CLI commands like `airflow dags list` would no longer validate executors (arguably fine)

### Solution B: Add a `skip_executor_validation` flag to BundleDagBag

Pass `validate_executors=False` from `task_runner.py`, keep default `True` elsewhere.

Pros:
- Explicit opt-out, easy to understand
- Other call sites keep validation by default

Cons:
- Adds a parameter to BundleDagBag's interface
- CLI call sites would still fail if local env doesn't have all executors

### Solution C: Check process context before validating

Use an env var or global flag to skip validation in worker pod context.

Pros:
- No API changes to BundleDagBag

Cons:
- Implicit behavior based on runtime context — harder to reason about
- `_AIRFLOW_PROCESS_CONTEXT=client` is set in both DAG Processor AND worker pod
- Would need a new env var or value to distinguish them

### Solution D: Validate only known executor types, not config availability

Check if the executor is a valid class name rather than checking config.

Pros:
- Works everywhere regardless of config

Cons:
- Doesn't catch typos in team-specific executor configs
- Significant rework of validation logic
- Custom executors need special handling

---

## Recommended: Solution A (with B as fallback)

Solution A is cleanest. Validation belongs in the DAG Processor — that's where the full
executor config lives and where config errors should be caught early at parse time.

If there's concern about losing validation in CLI commands, Solution B can complement it.

---

## Edge Cases to Consider

### 1. CLI commands on machines without full executor config

A developer running `airflow dags list` or `airflow dags test` on their laptop might only
have `LocalExecutor` but DAGs reference `KubernetesExecutor`. Currently fails. Solution A
fixes this. Correct behavior — CLI inspection shouldn't require production executor config.

### 2. DAG.test() method

`DAG.test()` creates a `BundleDagBag`. If the test env only has `LocalExecutor` but the
DAG references `KubernetesExecutor`, currently fails. Solution A fixes this too.

### 3. Multi-team executor validation

`_validate_executor_fields()` uses `bundle_name` for team lookups. In `_parse_file()`,
`msg.bundle_name` is available from `DagFileParseRequest`. No issue.

### 4. Custom executors loaded via plugins

If a custom executor is only available on the scheduler, validation in `_parse_file()`
correctly catches it. Worker pods don't try to validate. Correct behavior.

### 5. DAG Processor subprocess isolation

DAG Processor runs in isolated subprocesses with DB access blocked. Existing test
`test_validate_executor_fields_does_not_access_database` ensures validation works without
DB. Moving the call doesn't change this.

### 6. Callback handling in worker pods

`task_runner.py` loads DAGs for callbacks too. Moving validation out of `BundleDagBag`
means callbacks also stop failing. Correct — callbacks don't need executor validation.

### 7. DAG serialization commands

`dag_command.py` uses `BundleDagBag` for `dag_reserialize`. Runs on scheduler machine
with full config. With Solution A, it skips validation during parsing but DAG Processor
catches errors on next parse cycle. Acceptable tradeoff.

### 8. Race condition: config changes between parse and execution

If an executor is removed from config after DAG Processor validates but before the worker
executes, the scheduler handles this at dispatch time. Unrelated to this fix.

---

## Impact on Other Features

### Features that BENEFIT from the fix

| Feature | Impact |
|---------|--------|
| KubernetesExecutor | Primary fix — tasks with `executor='KubernetesExecutor'` work again |
| Mixed executors (Celery+K8s) | Both directions work |
| CLI commands | `airflow dags list/test` work without full executor config |
| DAG.test() | Testing DAGs locally works without production executor config |
| Pod template simplification | Users can use `LocalExecutor` only in pod templates (as documented) |

### Features NOT impacted (no behavior change)

| Feature | Why |
|---------|-----|
| DAG Processor validation | Still validates executors during scheduler-side parsing |
| Multi-team executor config | Validation logic unchanged, just called from different location |
| Executor loading/dispatching | Scheduler-side executor loading untouched |
| Task execution | Worker pods never needed executor validation |

### Features that need TESTING after the fix

| Feature | What to test |
|---------|-------------|
| DAG import errors | Invalid executor refs still show as import errors in UI |
| Multi-team executor validation | Team-based validation still works in DAG Processor |
| CLI `airflow dags list` | Works with partial executor config |
| `airflow dags test` | Works with partial executor config |
| DAG serialization | `dag_reserialize` still works correctly |

---

## Test Plan Outline

1. Unit: `_validate_executor_fields()` still catches invalid executors from `_parse_file()`
2. Unit: `BundleDagBag.process_file()` no longer calls `_validate_executor_fields()`
3. Unit: Worker pod context loads DAG with `executor='KubernetesExecutor'` when only
   `LocalExecutor` is configured — should succeed
4. Integration: DAG with `executor='KubernetesExecutor'` parses in DAG Processor when
   `KubernetesExecutor` is in scheduler config
5. Integration: DAG with invalid executor still produces import error in DAG Processor
