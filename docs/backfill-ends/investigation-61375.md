# Investigation: Backfill Job Marked as Completed Immediately

**GitHub Issue:** [#61375](https://github.com/apache/airflow/issues/61375)
**Affected Versions:** Airflow 3.1.6, 3.1.7 (not reproducible on 3.0.6)
**Status:** Root cause identified, bug confirmed by test

## Summary

When running `airflow backfill create`, the backfill job transitions to "completed" status
almost immediately, even though the DAG runs it triggered are still executing in the background.

## Root Cause: Race Condition Between Backfill Creation and Scheduler Completion Check

There is a two-phase commit in `_create_backfill()` (`airflow-core/src/airflow/models/backfill.py`):

1. **Phase 1 — Backfill record committed** (line ~601–605):
   ```python
   br = Backfill(...)
   session.add(br)
   session.commit()   # Backfill row is now visible to other transactions
   ```

2. **Phase 2 — DagRuns created** (line ~614–630):
   After the commit, the function proceeds to create DagRun rows via
   `_create_runs_partitioned()` or `_create_runs_non_partitioned()`.
   These are committed when the `create_session()` context manager exits.

Meanwhile, the scheduler calls `_mark_backfills_complete()` every **30 seconds**
(`scheduler_job_runner.py`, line ~1564). That method runs this query:

```python
select(Backfill).where(
    Backfill.completed_at.is_(None),
    ~exists(
        select(DagRun.id).where(
            and_(
                DagRun.backfill_id == Backfill.id,
                DagRun.state.in_((DagRunState.RUNNING, DagRunState.QUEUED))
            )
        )
    ),
)
```

If the scheduler fires this query **after Phase 1 but before Phase 2 completes**, it finds:
- A Backfill with `completed_at IS NULL` ✓
- Zero unfinished DagRuns for that backfill (they don't exist yet) ✓

Result: the backfill is marked completed. The DagRuns are then created afterward but the
backfill is already "done."

## Timeline Diagram

```
CLI Process                          Scheduler (every 30s)
───────────                          ─────────────────────
INSERT Backfill row
session.commit()
  │                                  _mark_backfills_complete()
  │                                    SELECT backfills WHERE completed_at IS NULL
  │                                      AND no unfinished DagRuns exist
  │                                    → finds the new backfill (0 DagRuns)
  │                                    → sets completed_at = now()  ← BUG
  ▼
Create DagRun #1
Create DagRun #2
  ...
Create DagRun #N
session.commit()
  → DagRuns exist, but backfill already marked completed
```

## Evidence from the Issue

The screenshot in the issue shows backfills with durations of ~8 seconds or less — these are
the ones where the scheduler's 30-second timer happened to fire during the creation window.
Backfills with durations of 1–3 minutes completed normally because all DagRuns were committed
before the next scheduler check.

## Relevant Code Locations

| File | Function/Line | Role |
|------|--------------|------|
| `airflow-core/src/airflow/models/backfill.py` | `_create_backfill()` (~L547) | Creates Backfill row, then DagRuns in separate phase |
| `airflow-core/src/airflow/models/backfill.py` | `_create_runs_non_partitioned()` (~L666) | Iterates DagRunInfo list, creates DagRuns one by one |
| `airflow-core/src/airflow/models/backfill.py` | `_create_runs_partitioned()` (~L637) | Same for partitioned DAGs |
| `airflow-core/src/airflow/jobs/scheduler_job_runner.py` | `_mark_backfills_complete()` (~L1890) | Marks backfills complete when no unfinished DagRuns exist |
| `airflow-core/src/airflow/jobs/scheduler_job_runner.py` | Timer registration (~L1564) | Calls `_mark_backfills_complete` every 30 seconds |
| `airflow-core/src/airflow/models/backfill.py` | `BackfillDagRun` model (~L181) | Association table linking Backfill ↔ DagRun |

## Proposed Fix

Add a guard to `_mark_backfills_complete()` requiring at least one `BackfillDagRun` row to
exist before considering a backfill eligible for completion. A backfill with zero
`BackfillDagRun` associations is still being initialized, not finished.

```python
# In _mark_backfills_complete():
query = select(Backfill).where(
    Backfill.completed_at.is_(None),
    # Guard: backfill must have at least one BackfillDagRun association,
    # otherwise it is still being set up (see #61375).
    exists(
        select(BackfillDagRun.id).where(BackfillDagRun.backfill_id == Backfill.id)
    ),
    ~exists(
        select(DagRun.id).where(
            and_(
                DagRun.backfill_id == Backfill.id,
                DagRun.state.in_(unfinished_states),
            )
        )
    ),
)
```

This requires importing `BackfillDagRun` in `scheduler_job_runner.py`.

## Alternative Approaches Considered

1. **Move the Backfill commit after DagRun creation** — Would eliminate the window entirely,
   but the early commit is intentional: it reserves the backfill slot and prevents duplicate
   backfills for the same DAG (the `AlreadyRunningBackfill` check depends on it).

2. **Add an explicit `is_ready` / `status` field to Backfill** — More invasive, requires a
   migration. The `BackfillDagRun` existence check achieves the same thing without schema changes.

3. **Use `is_paused=True` during creation, flip after** — Fragile; `is_paused` controls
   whether new DagRuns are created, not whether the backfill is considered complete.

## How to Reproduce

### All entry points converge on the same code

There are three ways to create a backfill, and all of them call the same
`_create_backfill()` function in `airflow-core/src/airflow/models/backfill.py`:

| Entry Point | File | How it calls `_create_backfill()` |
|-------------|------|----------------------------------|
| CLI (`airflow backfill create`) | `airflow-core/src/airflow/cli/commands/backfill_command.py` | Direct call, synchronous |
| REST API / UI | `airflow-core/src/airflow/api_fastapi/core_api/routes/public/backfills.py` → `create_backfill()` | Direct call, synchronous (no `BackgroundTasks`) |
| airflow-ctl | `airflow-ctl/` | HTTP POST to `/backfills`, hits the same API endpoint |

The UI's React code (`useCreateBackfill.ts`) calls `useBackfillServiceCreateBackfill` which
is a TanStack Query mutation that POSTs to the `/backfills` endpoint. The FastAPI handler is
a plain synchronous `def` (not `async def`), so the entire `_create_backfill()` runs in the
request thread. The response is only sent back to the UI after all DagRuns are created.

This means the race condition is identical regardless of entry point. The vulnerability is
between the Backfill row commit and the DagRun creation, both inside `_create_backfill()`.

### Approach 1: Unit test (deterministic, recommended)

This is the most reliable way because you control the exact state. The existing test
`test_mark_backfills_completed` in `tests/unit/jobs/test_scheduler_job.py` doesn't catch
the bug because `_create_backfill()` creates both the Backfill and DagRuns before the test
calls `_mark_backfills_complete()`.

To reproduce the race window deterministically:

```python
def test_mark_backfills_complete_race_condition(dag_maker, session):
    """
    Simulate the window where a Backfill row exists but DagRuns
    have not been created yet. _mark_backfills_complete() should
    NOT mark it as completed.
    """
    with dag_maker(serialized=True, dag_id="test_race", schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")

    # Phase 1 only: insert a Backfill row with no DagRuns
    b = Backfill(
        dag_id="test_race",
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add(b)
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type),
        executors=[MockExecutor(do_update=False)],
    )
    runner._mark_backfills_complete()

    session.refresh(b)
    # BUG: with current code, b.completed_at is NOT None here
    assert b.completed_at is None, "Backfill was marked complete before DagRuns were created"
```

Run with: `breeze run pytest tests/unit/jobs/test_scheduler_job.py::test_mark_backfills_complete_race_condition -xvs`

This test will FAIL on the current code, proving the bug.

### Approach 2: CLI (probabilistic)

Use a DAG with a large date range to widen the creation window:

```bash
# A daily DAG over ~6 months = ~180 DagRuns to create
breeze run airflow backfill create \
    --dag-id <your_daily_dag> \
    --from-date 2025-01-01 \
    --to-date 2025-07-01 \
    --max-active-runs 1

# Immediately check:
breeze run airflow backfill list
```

If the scheduler's 30-second timer fires while the DagRuns are being created, the backfill
will show as completed. This is timing-dependent — you may need to run it several times.

### Approach 3: UI (probabilistic, same race)

1. Navigate to a daily-scheduled DAG's detail page
2. Click "Backfill"
3. Set a wide date range (6+ months) to maximize creation time
4. Set `max_active_runs = 1`
5. Submit and switch to the Backfills tab

The UI calls the same synchronous API endpoint. The HTTP request blocks until all DagRuns
are created, but the scheduler is running independently in another process. If the scheduler
checks during that window, the backfill gets marked complete.

The UI is actually slightly harder to reproduce with because:
- The API request blocks the UI until completion (you see a loading spinner)
- By the time the response comes back and the UI refreshes the backfill list, the backfill
  may have already been incorrectly marked complete and then "un-completed" by subsequent
  scheduler runs... except it won't be un-completed because `completed_at` is never cleared

So if you see a backfill in the UI list with a very short duration (seconds) while DagRuns
are still running, that's the bug. The screenshot in the original issue shows exactly this.

### Why it's harder to reproduce on smaller date ranges

The race window is the time between `session.commit()` of the Backfill row and the final
commit of all DagRuns. For a DAG with 3 runs, this might be milliseconds. For 180 runs,
it could be several seconds — enough to overlap with the scheduler's 30-second cycle.

The `reprocess_behavior` also matters: `"all_runs"` mode does more work per DagRun (locking,
clearing) than `"missing_runs"`, so it widens the window further.

## Test Considerations

Two tests were added to `airflow-core/tests/unit/jobs/test_scheduler_job.py`:

| Test | Result | Purpose |
|------|--------|---------|
| `test_mark_backfills_complete_does_not_complete_backfill_without_dag_runs` | FAILS (confirms bug) | Creates a bare Backfill row with no DagRuns, calls `_mark_backfills_complete()`. Current code incorrectly sets `completed_at`. |
| `test_mark_backfills_complete_completes_backfill_with_finished_dag_runs` | PASSES | Creates a backfill via `_create_backfill()`, marks all DagRuns SUCCESS, confirms it is correctly marked complete. |

Run commands:
```bash
breeze run pytest tests/unit/jobs/test_scheduler_job.py::test_mark_backfills_complete_does_not_complete_backfill_without_dag_runs -xvs
breeze run pytest tests/unit/jobs/test_scheduler_job.py::test_mark_backfills_complete_completes_backfill_with_finished_dag_runs -xvs
```

The first test will pass once the proposed fix is applied. The existing test
`test_mark_backfills_completed` doesn't catch the bug because `_create_backfill()` creates
both the Backfill and DagRuns before `_mark_backfills_complete()` is called.
