# Backfill System Deep Dive

## Overview

Backfills allow users to retroactively create DagRuns for historical time periods. A backfill targets a single DAG, a date range, and creates DagRuns for each timetable interval within that range.

## Data Model

### Backfill Table
- `id` — auto-increment PK
- `dag_id` — which DAG this backfill is for
- `from_date` / `to_date` — the date range
- `dag_run_conf` — JSON config passed to each DagRun
- `is_paused` — controls whether new DagRuns get started (does NOT pause running ones)
- `reprocess_behavior` — NONE (skip existing), FAILED (rerun failed), COMPLETED (rerun all)
- `max_active_runs` — concurrency limit for this backfill's DagRuns
- `created_at` / `completed_at` / `updated_at` — lifecycle timestamps
- `triggering_user_name` — who created it

### BackfillDagRun Table (association table)
- `id` — auto-increment PK
- `backfill_id` — FK to Backfill
- `dag_run_id` — FK to DagRun (nullable — NULL when date was skipped)
- `exception_reason` — why a DagRun wasn't created: IN_FLIGHT, ALREADY_EXISTS, UNKNOWN
- `logical_date` / `partition_key` — which timetable slot this represents
- `sort_ordinal` — execution order (≥ 1), used for ordering in scheduler queue

### DagRun Table
- `backfill_id` — FK to Backfill (nullable). Set directly on the DagRun row.
- `run_type` — set to `backfill_job` for backfill-created runs

### Key Relationships
- Backfill 1:N BackfillDagRun (every timetable slot gets a row)
- BackfillDagRun N:1 DagRun (nullable — skipped slots have no DagRun)
- DagRun has `backfill_id` directly (redundant with BackfillDagRun join, used for scheduler queries)

## Entry Points

### Creation
- REST API: `POST /backfills` → calls `_create_backfill()`
- CLI: `airflow backfills create` → calls `_create_backfill()` directly
- Both go through the same code path in `airflow-core/src/airflow/models/backfill.py`

### Management
- `PUT /backfills/{id}/pause` — sets `is_paused = True`
- `PUT /backfills/{id}/unpause` — sets `is_paused = False`
- `PUT /backfills/{id}/cancel` — pauses, fails QUEUED DagRuns, sets `completed_at`
- `GET /backfills` — list backfills for a DAG
- `GET /backfills/{id}` — get single backfill

### Scheduler
- `_mark_backfills_complete()` — runs every 30 seconds, marks backfills as done when all DagRuns finish
- `_start_queued_dagruns()` — picks up QUEUED backfill DagRuns, respects `max_active_runs` and `sort_ordinal`
- `_lock_backfills()` — row-level locking to prevent concurrent scheduler instances from conflicting

## Creation Flow (_create_backfill)

File: `airflow-core/src/airflow/models/backfill.py` L547+

### Transaction 1 (explicit commit at L605)
1. Query SerializedDagModel and DagModel
2. Check `num_active` — count of Backfill rows with `completed_at IS NULL` for this dag_id
3. If > 0, raise `AlreadyRunningBackfill`
4. Validate params (depends_on_past, future dates, etc.)
5. Create Backfill object, `session.add(br)`
6. `session.commit()` — Backfill row is now permanent, `br.id` is assigned

### Transaction 2 (committed by create_session __exit__)
7. Build `dagrun_info_list` from timetable (pure Python)
8. Loop through each date, calling `_create_backfill_dag_run_non_partitioned()` or `_create_backfill_dag_run_partitioned()`
9. Each iteration creates a BackfillDagRun row (always) and optionally a DagRun row
10. All rows commit together when `create_session()` exits

### Session Configuration
- `autocommit=False`, `autoflush=False`, `expire_on_commit=False`
- `autoflush=False` means pending `session.add()` objects are NOT flushed before queries
- All DagRun/BackfillDagRun INSERTs accumulate in memory until the final commit

## DagRun Creation Per Date

File: `_create_backfill_dag_run_non_partitioned()` L313+

Each date goes through one of these paths:

1. **Existing DagRun, can't reprocess** → BackfillDagRun with `dag_run_id=None`, `exception_reason=IN_FLIGHT` or `ALREADY_EXISTS`
2. **Existing DagRun, can reprocess but locked** → BackfillDagRun with `dag_run_id=None`, `exception_reason=IN_FLIGHT`
3. **Existing DagRun, can reprocess and lockable** → clears the DagRun via `_handle_clear_run()`, creates BackfillDagRun pointing to existing DagRun
4. **No existing DagRun** → creates new DagRun + BackfillDagRun
5. **IntegrityError on create** → rollback savepoint, BackfillDagRun with `dag_run_id=None`, `exception_reason=IN_FLIGHT`

Uses `session.begin_nested()` (savepoints) so individual date failures don't kill the whole batch.

## Completion Detection

File: `scheduler_job_runner.py` `_mark_backfills_complete()` L1890+

Runs every 30 seconds. Query:
```python
select(Backfill).where(
    Backfill.completed_at.is_(None),
    exists(BackfillDagRun where backfill_id = Backfill.id),  # has at least one association
    ~exists(DagRun where backfill_id = Backfill.id AND state IN (RUNNING, QUEUED)),  # no unfinished runs
)
```

Sets `completed_at = utcnow()` on matching backfills.

Note: checks `DagRun.backfill_id` directly, NOT through BackfillDagRun join. BackfillDagRun rows with `dag_run_id=None` (skipped dates) have no corresponding DagRun, so they don't block completion.

## Cancellation Flow

File: `api_fastapi/core_api/routes/public/backfills.py` `cancel_backfill()`

Three separate transactions:
1. Set `is_paused = True`, commit — prevents scheduler from starting new runs
2. `UPDATE dag_run SET state = 'failed' WHERE id IN (SELECT dag_run_id FROM backfill_dag_run WHERE backfill_id = ?) AND state = 'queued'`, commit
3. Set `completed_at = utcnow()` — committed on response return

Only QUEUED runs are failed. RUNNING runs continue to completion.

## Scheduler DagRun Pickup

File: `DagRun.get_queued_dag_runs_to_set_running()` in `dagrun.py`

- Joins DagRun → BackfillDagRun → Backfill
- Filters: `state = QUEUED`, DAG not paused/stale, backfill not paused
- Enforces `max_active_runs` per (dag_id, backfill_id) pair
- Orders by `sort_ordinal` (backfill runs have lower priority than scheduled runs since sort_ordinal ≥ 1, and NULL sorts first)

## Known Race Conditions

### Race 1: Premature Completion (the bug PR #62561 fixes)
Between Transaction 1 commit (Backfill visible) and Transaction 2 commit (DagRuns created), `_mark_backfills_complete()` can see a Backfill with zero DagRuns and mark it complete.

**Fix:** EXISTS guard on BackfillDagRun — skip backfills with zero association rows.

### Race 2: Orphaned Backfill
If `_create_backfill` fails after Transaction 1 but before Transaction 2 commits (e.g., `RuntimeError("No runs to create")`, DB error, process crash), the Backfill row persists with `completed_at=NULL` and zero BackfillDagRun rows.

With the EXISTS guard, `_mark_backfills_complete` skips it forever. Combined with `AlreadyRunningBackfill`, this permanently blocks new backfills for that DAG.

**Fix options:**
- Age-based fallback in `_mark_backfills_complete`: mark complete if zero BackfillDagRun rows AND created > 10 min ago
- Error cleanup in `_create_backfill`: catch exceptions after Transaction 1, mark orphan complete using a separate session
- Cancel endpoint already handles this correctly (sets `completed_at` regardless of BackfillDagRun count)

### Race 3: TOCTOU on num_active (pre-existing, accepted)
Two concurrent `_create_backfill` requests can both see `num_active = 0` before either commits. Window is microseconds (between SELECT and COMMIT). Accepted risk.

### Race 4: Cancel During Initialization
If cancel is called during the initialization window (Backfill committed, no DagRuns yet), it works correctly — sets `completed_at`, cleaning up the orphan. The UPDATE on DagRuns is a no-op since none exist.

## Design Observations

### No Explicit State Machine
Lifecycle tracked by `is_paused` (bool) + `completed_at` (nullable timestamp). No INITIALIZING / RUNNING / PAUSED / COMPLETED / FAILED states. Makes it hard to distinguish "still being set up" from "actively running" from "orphaned."

### Two-Transaction Creation
Root cause of most race conditions. Ideally atomic, but `flush()` instead of `commit()` would widen the TOCTOU window on `num_active` from microseconds to seconds under READ COMMITTED isolation.

### No Retry for Skipped Dates
BackfillDagRun rows with `exception_reason=IN_FLIGHT` are never retried. If a DagRun was running when the backfill was created, that date is permanently skipped.

### Cancel Doesn't Stop Running DagRuns
Only QUEUED runs get failed. RUNNING ones continue to completion. No "force cancel" option.

### No Progress Visibility
No API endpoint aggregates BackfillDagRun states into a progress summary. The UI shows an indeterminate spinner.

### No Delete
Backfill DagRun rows persist forever. No cleanup mechanism.

## Key Files

| File | What |
|------|------|
| `airflow-core/src/airflow/models/backfill.py` | Backfill/BackfillDagRun models, `_create_backfill()`, DagRun creation per date |
| `airflow-core/src/airflow/jobs/scheduler_job_runner.py` | `_mark_backfills_complete()`, `_start_queued_dagruns()`, `_lock_backfills()` |
| `airflow-core/src/airflow/models/dagrun.py` | `get_queued_dag_runs_to_set_running()` — scheduler pickup query |
| `airflow-core/src/airflow/api_fastapi/core_api/routes/public/backfills.py` | REST API endpoints (CRUD, pause, cancel) |
| `airflow-core/src/airflow/api_fastapi/core_api/datamodels/backfills.py` | API response models |
| `airflow-core/src/airflow/cli/commands/backfill_command.py` | CLI entry point |
| `airflow-core/src/airflow/utils/session.py` | `create_session()` context manager |
| `airflow-core/src/airflow/settings.py` | Session config (autoflush=False, expire_on_commit=False) |
| `airflow-core/src/airflow/ui/src/components/Banner/BackfillBanner.tsx` | Active backfill banner UI |
| `airflow-core/src/airflow/ui/src/pages/Dag/Backfills/Backfills.tsx` | Backfills list table UI |
