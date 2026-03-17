# Callback Unification Investigation

## Goal

Migrate Airflow's legacy `on_success_callback` / `on_failure_callback` / `on_retry_callback`
system from DAG Processor execution to the new `SyncCallback` / `AsyncCallback` executor-based
framework — without breaking existing DAGs.

## Context

Airflow has two callback systems today:

1. **Legacy (on_foo_callback)**: User defines a Python callable on a DAG or task. The scheduler
   stores a boolean flag (`has_on_failure_callback`). When triggered, a `DagCallbackRequest` /
   `TaskCallbackRequest` is written to the `callback` DB table. The DAG Processor fetches it,
   re-parses the entire DAG file, resolves the callable from memory, builds context, and executes
   it. This code is 4-5+ years old.

2. **New (DeadlineAlert callbacks)**: User defines a `SyncCallback` or `AsyncCallback` with an
   import path string. The scheduler creates an `ExecutorCallback` or `TriggererCallback` ORM
   record. The executor dispatches an `ExecuteCallback` workload to a worker, which imports the
   callable by dotted path and executes it. Introduced in Airflow 3.2 for deadline alerts.

The legacy system works but is architecturally wrong — the DAG Processor's job is to parse DAGs,
not execute user code. Moving callbacks to executors means they run in proper workers, with
bundle support, proper isolation, and executor-specific infrastructure (ECS, Kubernetes, Celery).

## Stakeholders

- **Ferruzzi (Dennis)** — created the new callback framework, supervising this effort
- **Anish Giri** — working on workload queue unification (tier-based scheduling PR)
- **Sebastian Daum** — interested in driving this migration, potentially writing an AIP

## Key Decisions Needed (for dev list / community call)

1. Should DAG/task callbacks run in workers instead of the DAG Processor? (consensus: yes)
2. What scheduling priority should callbacks have relative to tasks? (needs discussion)
3. Can we do this without breaking existing DAGs? (investigation below)
4. If breaking changes are needed, should we deprecate `on_foo_callback` in favor of
   `state_callback(target_state, SyncCallback | AsyncCallback)`?

## Investigation Files

| File | Contents |
|------|----------|
| [01_current_system.md](01_current_system.md) | How the legacy callback system works end-to-end |
| [02_new_system.md](02_new_system.md) | How SyncCallback/AsyncCallback/ExecuteCallback works |
| [03_user_experience.md](03_user_experience.md) | Current UX, desired UX, what must not break |
| [04_problems.md](04_problems.md) | What's wrong with the current system |
| [05_backwards_compatible.md](05_backwards_compatible.md) | Migration approach preserving existing API |
| [06_non_backwards_compatible.md](06_non_backwards_compatible.md) | Clean-slate approach with deprecation |
| [07_risks_and_constraints.md](07_risks_and_constraints.md) | Edge cases, compat challenges, executor differences |
| [08_modules_and_files.md](08_modules_and_files.md) | Every file that would be touched |
| [09_poc_plan.md](09_poc_plan.md) | Minimal POC to validate the approach |
| [10_effort_estimate.md](10_effort_estimate.md) | Effort sizing by phase |
