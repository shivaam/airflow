# Issue #56271 — Conversation Summary

**Issue:** https://github.com/apache/airflow/issues/56271
**Opened:** Sep 30, 2025 by @XD-DENG
**Status:** Open, no PR submitted, no milestone assigned

## Timeline

### Sep 30, 2025 — @XD-DENG opens the issue

Upgraded from 3.0.3 to 3.1.0. Tasks with `executor='KubernetesExecutor'` started failing with
`UnknownExecutorException`. Config uses `CeleryExecutor,KubernetesExecutor` for
`AIRFLOW__CORE__EXECUTOR`. The error occurs during DAG parsing in the worker pod:

```
UnknownExecutorException: Task 'xd_asked_for_another_task' specifies executor
'KubernetesExecutor', which is not available.
```

Initially titled "Multi Executor feature is broken" — later updated to "KubernetesExecutor
feature may be broken" after realizing it's specifically about KubernetesExecutor validation
in worker pods.

### Oct 1, 2025 — @kaxil responds with a hypothesis

Kaxil identified that PR #54383 consolidated `airflow.models.dag.DAG` into `airflow.sdk.DAG`.
His hypothesis: in 3.0.3, there were two DAG classes:
- `airflow.models.dag.DAG` (legacy) — had `validate_executor_field()` in its `validate()` method
- `airflow.sdk.DAG` (recommended) — did NOT have executor validation

He suggested the bug was latent — if the reporter had used `airflow.models.dag.DAG` in 3.0.3,
they would have hit the same error. He asked XD-DENG to test this.

He also noted that the docs about `LocalExecutor` in pod_template_file might need updating,
referencing PR #49433.

### Oct 1-2, 2025 — @XD-DENG pushes back

XD-DENG confirmed they were already using `from airflow.sdk import DAG` in both 3.0.3 and
3.1.0, and only got the error in 3.1.0. Asked directly: "So do you think this is a bug
needing fix?"

### Oct 2, 2025 — @potiuk clarifies

Potiuk (while on vacation) clarified Kaxil's ask: the point was to confirm the hypothesis by
testing the *opposite* — use `airflow.models.dag.DAG` in 3.0.3 to see if it fails the same
way. This would confirm that the validation logic existed before but was only triggered for
the legacy DAG class, and 3.1.0 made it apply to all DAGs.

### Oct 6, 2025 — @kaxil confirms

Kaxil confirmed Potiuk's clarification. The conversation stalled here.

### Oct 21, 2025 — Milestone removed

Kaxil removed the issue from the Airflow 3.1.1 milestone. No new milestone assigned.

### Oct 29, 2025 — @Ferdinanddb reports same issue in 3.1.1

Confirmed the error was caused by the `podTemplate` block in `values.yaml` setting
`AIRFLOW__CORE__EXECUTOR`. After commenting out the podTemplate block, the error went away.
Noted that the pod template had `LocalExecutor` as the only executor, which made sense for
the old `KubernetesCeleryExecutor` but breaks with the new validation.

### Dec 1, 2025 — @snowsky reports related error

Hit "Dag not found during start up" error in one AKS cluster but not another testing env.

### Dec 18, 2025 — @dor-bernstein reports reverse case in 3.1.3

Uses KubernetesExecutor as default, wants CeleryExecutor on specific tasks — same root cause,
opposite direction. Asked for help.

### Jan 1, 2026 — @dor-bernstein follows up

Pinged maintainers again, still blocked.

### Jan 3, 2026 — @potiuk suggests trying 3.1.5

Some executor fixes had landed. Asked XD-DENG if the problem was resolved.

### Jan 3, 2026 — @XD-DENG confirms still broken in 3.1.5

The workaround (adding `KubernetesExecutor` to `AIRFLOW__CORE__EXECUTOR` in pod_template_file)
still required. Said they plan to submit a PR but schedule is tight.

### ~Mar 5, 2026 — @shivaam adds to backlog

Added to personal Open Tasks project, moved from Backlog to Ready.

## Key Takeaways

1. The bug is confirmed and reproducible across 3.1.0 through 3.1.5.
2. The root cause is understood — executor validation runs in worker pods where the full
   executor config isn't available.
3. The workaround is known — add all executors to `AIRFLOW__CORE__EXECUTOR` in pod template.
4. No PR has been submitted in ~5 months.
5. Multiple users are affected, including mixed CeleryExecutor+KubernetesExecutor setups and
   the reverse (KubernetesExecutor default + CeleryExecutor on specific tasks).
6. The issue was deprioritized (removed from 3.1.1 milestone) and has been sitting in the
   backlog since.
