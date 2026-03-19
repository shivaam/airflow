# Fix Backfill Marked Complete Before DagRuns Are Created

**PR:** [#62561](https://github.com/apache/airflow/pull/62561)
**Issue:** [#61375](https://github.com/apache/airflow/issues/61375)
**Milestone:** Airflow 3.1.9

## Problem

The scheduler's `_mark_backfills_complete()` prematurely marks a backfill as completed when it runs during the window between the Backfill row commit and the DagRun creation in `_create_backfill()`.

## Relevant Code & Files

| File | What | Line |
|------|------|------|
| `#[[file:airflow-core/src/airflow/models/backfill.py]]` | `_create_backfill()` — creates Backfill + DagRuns | L547 |
| `#[[file:airflow-core/src/airflow/jobs/scheduler_job_runner.py]]` | `_mark_backfills_complete()` — marks done backfills | L1890 |
| `#[[file:airflow-core/src/airflow/utils/session.py]]` | `create_session()` — context manager, commits on exit | L32 |
| `#[[file:airflow-core/src/airflow/settings.py]]` | Session config: `autoflush=False`, `expire_on_commit=False` | L424 |
| `#[[file:airflow-core/src/airflow/settings.py]]` | DB isolation: `READ COMMITTED` (Postgres default, MySQL explicit) | L522 |

## Deep Dive: Transaction Lifecycle in `_create_backfill()`

### Session Configuration (Important)

Airflow configures SQLAlchemy sessions with (settings.py L424-427):
```python
sessionmaker(
    autocommit=False,
    autoflush=False,       # ← no automatic flush before queries
    expire_on_commit=False, # ← objects stay usable after commit
)
```

`autoflush=False` means pending `session.add()` objects are NOT automatically flushed before queries. You must explicitly `flush()` or `commit()` to send SQL to the DB.

### The Current Flow — Step by Step

```python
with create_session() as session:          # BEGIN (implicit)
```

**Transaction 1 starts** (implicit BEGIN when session is first used).

```python
    # L565-567: Read-only queries — no writes yet
    serdag = session.scalar(...)           # SELECT serialized_dag
    dag_model = session.scalar(...)        # SELECT dag_model
```

These SELECTs see only committed data (READ COMMITTED). No pending writes exist.

```python
    # L583-589: The "only one active backfill" guard
    num_active = session.scalar(
        select(func.count()).where(
            Backfill.dag_id == dag_id,
            Backfill.completed_at.is_(None),
        )
    )
    if num_active > 0:
        raise AlreadyRunningBackfill(...)
```

This SELECT counts committed Backfill rows. Since `autoflush=False`, even if we had pending objects in the session, they wouldn't be flushed before this query. But at this point we haven't added anything yet, so it doesn't matter.

```python
    # L596-604: Create the Backfill object
    br = Backfill(dag_id=dag_id, ...)
    session.add(br)
```

`session.add(br)` puts the Backfill in the session's identity map as "new" (pending). No SQL sent. `br.id` is `None`. The object exists only in Python memory.

```python
    # L605: THE CRITICAL LINE
    session.commit()
```

This does three things atomically:
1. **flush** — sends `INSERT INTO backfill ...` to the DB, gets back `br.id` (auto-increment)
2. **COMMIT** — makes the row permanent and visible to all other sessions/transactions
3. **Starts a new implicit transaction** for subsequent operations

After this line: `br.id` has a value, and every other database session can see this Backfill row. Transaction 1 is done.

**Transaction 2 starts** (implicit BEGIN on next DB operation).

```python
    # L607: Re-query DagModel (unclear why, possibly to refresh)
    session.scalars(select(DagModel)...).one()

    # L609-615: Build the list of DagRun dates
    dagrun_info_list = _get_info_list(...)  # Pure Python, no DB
    if not dagrun_info_list:
        raise RuntimeError(...)  # ← DANGER: Backfill committed but no DagRuns
```

```python
    # L617-632: Create all DagRun + BackfillDagRun rows
    _create_runs_non_partitioned(...)  # or _create_runs_partitioned(...)
```

Inside `_create_runs_non_partitioned` (L666), it loops over each date and calls `_create_backfill_dag_run_non_partitioned()`. Here's the call chain for each iteration:

```python
# _create_runs_non_partitioned (backfill.py L666)
for backfill_sort_ordinal, info in enumerate(dagrun_info_list, start=1):
    _create_backfill_dag_run_non_partitioned(
        dag=dag, info=info, backfill_id=br.id, ..., session=session,
    )
```

Inside `_create_backfill_dag_run_non_partitioned` (backfill.py L313), for the new-run path:

```python
with session.begin_nested() as nested:       # SAVEPOINT
    # ... checks for existing runs ...

    # Create the DagRun via dag.create_dagrun()
    dr = dag.create_dagrun(
        run_type=DagRunType.BACKFILL_JOB,
        state=DagRunState.QUEUED,
        backfill_id=backfill_id,
        session=session,
        ...
    )

    # Add the BackfillDagRun association
    session.add(BackfillDagRun(
        backfill_id=backfill_id,
        dag_run_id=dr.id,          # ← needs dr.id from flush
        sort_ordinal=...,
        logical_date=...,
    ))
```

`dag.create_dagrun()` calls `_create_orm_dagrun()` (serialization/definitions/dag.py L1101), which does:

```python
run = DagRun(dag_id=dag.dag_id, run_id=run_id, ...)
session.add(run)
session.flush()                              # ← FLUSH! DagRun row sent to DB
run.verify_integrity(session=session, ...)   # creates TaskInstance rows
return run
```

So there IS a `session.flush()` per DagRun — inside `_create_orm_dagrun`. This means each DagRun row is written to the DB within the current transaction during the loop. However, since there's no `commit()`, these flushed rows are only visible within the current session (Transaction 2). Other sessions (like the scheduler) cannot see them under `READ COMMITTED`.

The `BackfillDagRun` rows added via `session.add()` after each `create_dagrun` call are NOT flushed immediately — there's no explicit flush for them. They sit as pending "new" objects in the session. However, when the NEXT loop iteration calls `_create_orm_dagrun` → `session.flush()`, SQLAlchemy flushes ALL pending objects in the session, not just the DagRun it was asked to flush. So the BackfillDagRun from the previous iteration gets written to the DB at that point as a side effect. The very last BackfillDagRun (from the final iteration) has no subsequent flush to carry it — it gets flushed at the final `session.commit()` when `create_session()` exits.

Summary of what's in the DB during the loop (Transaction 2, uncommitted):
- DagRun rows: flushed to DB each iteration (by `_create_orm_dagrun`), but uncommitted — invisible to other sessions
- TaskInstance rows: flushed via `verify_integrity` inside `_create_orm_dagrun`, but uncommitted
- BackfillDagRun rows: flushed to DB one iteration behind (each `session.flush()` in `_create_orm_dagrun` flushes all pending objects, including the BackfillDagRun added after the previous DagRun was created). The last one flushes at final commit.

```python
# End of `with create_session() as session:` block
# create_session() calls session.commit()
```

This final commit (from `create_session`'s `__exit__`) does:
1. **flush** — sends ALL the accumulated INSERTs (DagRun rows, BackfillDagRun rows) to the DB in one batch
2. **COMMIT** — makes them all visible atomically

### The Two Race Windows

#### Window 1: Between `session.commit()` (L605) and `create_session()` exit

```
Timeline:
─────────────────────────────────────────────────────────────────
L605: session.commit()
  │   Backfill row is COMMITTED and VISIBLE to everyone
  │   br.id is assigned
  │   Transaction 1 ends, Transaction 2 begins
  │
  │   ┌─── RACE WINDOW 1 ──────────────────────────────────┐
  │   │                                                      │
  │   │  Scheduler's _mark_backfills_complete() runs:        │
  │   │  - Sees Backfill with completed_at=NULL              │
  │   │  - Sees NO DagRuns (they don't exist yet)            │
  │   │  - Marks backfill as completed ← BUG                │
  │   │                                                      │
  │   │  Another _create_backfill() request:                 │
  │   │  - num_active check sees the committed Backfill      │
  │   │  - Correctly raises AlreadyRunningBackfill ← GOOD   │
  │   │                                                      │
  │   └─────────────────────────────────────────────────────┘
  │
  │   _get_info_list() — pure Python, builds date list
  │   _create_runs_non_partitioned() — session.add() in a loop
  │     (autoflush=False, so NO SQL sent during loop)
  │
create_session().__exit__ → session.commit()
  │   All DagRun + BackfillDagRun rows flushed and committed
  │   Transaction 2 ends
─────────────────────────────────────────────────────────────────
```

This is the bug that PR #62561 fixes. The window is as long as it takes to build the dagrun_info_list and loop through all the `_create_backfill_dag_run_non_partitioned` calls. For 100 runs, this could be seconds.

#### Window 2: The `num_active` check-then-act (existing, tiny)

```
Request A                              Request B
─────────                              ─────────
num_active → 0 ✓                       
                                       num_active → 0 ✓
session.add(br)                        session.add(br)
session.commit() ← A visible          session.commit() ← B visible
```

This is a classic TOCTOU (time-of-check-time-of-use) race. Both requests check `num_active` before either commits. Both see 0 and proceed. This window is microseconds (between the SELECT and the COMMIT) so it's unlikely but theoretically possible. The current code accepts this risk.

### What Happens If We Change `commit()` to `flush()` (Kaxil's Suggestion)

```python
    session.add(br)
    session.flush()    # ← sends INSERT, gets br.id, but does NOT commit
```

Now the entire function runs in ONE transaction. The Backfill row and all DagRun rows commit together atomically when `create_session()` exits. This eliminates Window 1 entirely.

But it blows Window 2 wide open:

```
Request A                              Request B
─────────                              ─────────
num_active → 0 ✓
session.add(br)
session.flush()
  (br.id assigned, but NOT committed)
  (invisible to other sessions under READ COMMITTED)
                                       num_active → 0 ✓  ← can't see A's row!
                                       session.add(br)
                                       session.flush()
Creating 100 DagRuns... (seconds)      Creating 100 DagRuns... (seconds)
create_session exits → COMMIT          create_session exits → COMMIT
  Both backfills now exist for same DAG ← BUG
```

Under `READ COMMITTED`, `flush()` writes to the DB but the row is tagged with Transaction A's ID. Transaction B's `SELECT COUNT(*)` skips uncommitted rows from other transactions. So B sees 0 active backfills and proceeds.

The race window goes from microseconds (current) to the entire DagRun creation duration (seconds). For 100 runs, this is practically guaranteed to be exploitable.

### Why `autoflush=False` Matters

If Airflow used `autoflush=True` (the SQLAlchemy default), the `num_active` SELECT at L583 would trigger an automatic flush of any pending objects before executing. This would mean:
- If you had done `session.add(br)` before the query, it would flush the INSERT first
- But in `_create_backfill`, the `session.add(br)` happens AFTER the `num_active` check, so autoflush wouldn't change anything here

The `autoflush=False` setting matters more for the DagRun creation phase: since there's no autoflush, all the `session.add()` calls in the loop accumulate without sending SQL. They all go out in one batch at the final `commit()`. With `autoflush=True`, each `session.add()` followed by any query would trigger a flush, sending INSERTs one at a time — slower but would make rows visible to the session's own queries sooner.

### The Orphaned Backfill Problem

With the current `commit()` at L605, if `_create_backfill` fails AFTER committing the Backfill but BEFORE creating DagRuns:

```python
    session.commit()                    # Backfill committed ✓
    dagrun_info_list = _get_info_list(...)
    if not dagrun_info_list:
        raise RuntimeError(...)         # ← Exception here!
```

The `create_session()` context manager catches the exception and calls `session.rollback()`. But the Backfill was already committed in Transaction 1 — rollback only undoes Transaction 2 (which has nothing in it yet). The Backfill row persists in the DB with `completed_at=NULL` and zero BackfillDagRun rows.

With the PR #62561 guard (`EXISTS` check on BackfillDagRun), `_mark_backfills_complete` will never clean this up because it requires at least one BackfillDagRun. Combined with `AlreadyRunningBackfill`, this orphan permanently blocks new backfills for that DAG.

This is a pre-existing issue (the orphan can happen today without the guard — the scheduler would just immediately mark it complete, which is wrong but at least unblocks future backfills). The guard makes the orphan problem worse by preventing cleanup.

## Proposed Fix (PR #62561)

Add an `EXISTS` guard on the `backfill_dag_run` table in `_mark_backfills_complete()`:

```python
exists(
    select(BackfillDagRun.id).where(BackfillDagRun.backfill_id == Backfill.id)
)
```

A backfill needs at least one `BackfillDagRun` row before it can be marked complete. If it has zero, it's still being set up and gets skipped.

### What This Fixes
- Window 1 (premature completion) — scheduler skips backfills with no BackfillDagRun rows

### What This Doesn't Fix
- Window 2 (TOCTOU on `num_active`) — still microseconds, accepted risk
- Orphaned backfills — a Backfill committed but with zero DagRuns will never be cleaned up

### Possible Follow-Up: Orphan Cleanup

A separate mechanism could clean up orphaned backfills — e.g., mark a backfill as completed if it has zero BackfillDagRun rows AND was created more than N minutes ago. This would handle the failure-after-commit edge case without re-introducing the premature completion bug.

## Status

- 1 approval (eladkal, LGTM)
- Awaiting second reviewer from scheduler core area (Lee-W, kaxil, or uranusjr)
- Kaxil's `flush()` suggestion is not viable — it trades a narrow race (premature completion, fixable with the guard) for a wide race (concurrent backfill bypass under READ COMMITTED)

---

## Options

### Option A: EXISTS Guard Only (PR #62561 as-is)

Add the `EXISTS` subquery check in `_mark_backfills_complete()` so backfills with zero `BackfillDagRun` rows are skipped.

**File:** `#[[file:airflow-core/src/airflow/jobs/scheduler_job_runner.py]]`
**Method:** `SchedulerJobRunner._mark_backfills_complete` (L1890)
**Change:** Add `exists(select(BackfillDagRun.id).where(BackfillDagRun.backfill_id == Backfill.id))` to the query's `.where()` clause.

**Test file:** `#[[file:airflow-core/tests/unit/jobs/test_scheduler_job.py]]`
**Test:** `test_mark_backfills_complete_skips_initializing_backfill` (already in PR)

| Pros | Cons |
|------|------|
| Fixes premature completion (Window 1) | Orphaned backfills permanently block the DAG |
| Minimal diff, already reviewed | No cleanup for Backfill rows left behind after crash |
| No change to transaction structure | Makes orphan problem worse than today (today scheduler would at least mark it complete) |

**Risk:** Low for the fix itself. Medium for the orphan edge case — requires manual DB cleanup if triggered.

---

### Option B: `flush()` Instead of `commit()` (Kaxil's Suggestion)

Change `session.commit()` to `session.flush()` at L605 in `_create_backfill()` so the Backfill row and all DagRun rows commit atomically when `create_session()` exits.

**File:** `#[[file:airflow-core/src/airflow/models/backfill.py]]`
**Function:** `_create_backfill` (L547)
**Change:** Replace `session.commit()` at L605 with `session.flush()`.

| Pros | Cons |
|------|------|
| Eliminates Window 1 entirely | Blows Window 2 wide open (concurrent backfill bypass) |
| Atomic transaction — all or nothing | Under READ COMMITTED, flushed row invisible to other sessions |
| No need for EXISTS guard | Race window goes from microseconds to seconds |
| | Practically guaranteed exploitable with many DagRuns |

**Risk:** High. Two concurrent API requests can create duplicate backfills for the same DAG. This is worse than the original bug.

**Verdict:** Not viable.

---

### Option C: EXISTS Guard + Time-Based Orphan Cleanup in Scheduler

Keep the EXISTS guard from Option A. Additionally, in `_mark_backfills_complete()`, add a second query that cleans up orphaned backfills: those with zero `BackfillDagRun` rows AND `created_at` older than a threshold (e.g., 10 minutes).

**File:** `#[[file:airflow-core/src/airflow/jobs/scheduler_job_runner.py]]`
**Method:** `SchedulerJobRunner._mark_backfills_complete` (L1890)
**Changes:**
1. Keep the EXISTS guard from Option A (skip backfills with zero BackfillDagRun rows in the main completion query).
2. Add a new query after the main completion logic:
```python
# Clean up orphaned backfills — committed but never got any DagRuns
orphan_cutoff = now - timedelta(minutes=10)
orphaned = session.scalars(
    select(Backfill).where(
        Backfill.completed_at.is_(None),
        Backfill.created_at < orphan_cutoff,
        ~exists(select(BackfillDagRun.id).where(BackfillDagRun.backfill_id == Backfill.id)),
    )
)
for b in orphaned:
    b.completed_at = now
```

**Test file:** `#[[file:airflow-core/tests/unit/jobs/test_scheduler_job.py]]`
**Tests needed:**
- `test_mark_backfills_complete_skips_initializing_backfill` (existing from PR)
- `test_mark_backfills_complete_cleans_up_orphaned_backfill` (new — create a Backfill with `created_at` 15 min ago, zero BackfillDagRun rows, verify it gets `completed_at` set)

| Pros | Cons |
|------|------|
| Fixes premature completion (Window 1) | Slightly more code |
| Self-healing orphan cleanup | Need to pick a timeout value |
| No manual DB intervention needed | Orphan blocks DAG for up to N minutes before cleanup |
| No change to transaction structure | Extremely slow DagRun creation (>N min) could be cleaned up prematurely (unlikely) |

**Risk:** Low. Covers both the original bug and the orphan problem.

---

### Option D: Inline Cleanup on Failure in `_create_backfill()`

Wrap everything after `session.commit()` in a try/except. If DagRun creation fails, mark the Backfill as completed (or delete it) before re-raising the exception. This prevents the orphan from ever existing.

**File:** `#[[file:airflow-core/src/airflow/models/backfill.py]]`
**Function:** `_create_backfill` (L547)
**Change:** Wrap L607-632 in try/except:
```python
        session.add(br)
        session.commit()

        try:
            session.scalars(select(DagModel).where(DagModel.dag_id == dag_id)).one()

            dagrun_info_list = _get_info_list(...)
            if not dagrun_info_list:
                raise RuntimeError(f"No runs to create for Dag {dag_id}")

            first_info = dagrun_info_list[0]
            if first_info.partition_key:
                _create_runs_partitioned(...)
            else:
                _create_runs_non_partitioned(...)
        except Exception:
            log.exception("Failed to create DagRuns for backfill %s, cleaning up.", br.id)
            br.completed_at = timezone.utcnow()
            session.commit()
            raise
```

**Failure points covered:**
1. `session.scalars(...).one()` — `NoResultFound` if DAG deleted mid-flight
2. `_get_info_list()` returning empty → `RuntimeError("No runs to create...")`
3. `_create_runs_*()` — any exception during DagRun creation (IntegrityError, DB connection loss, etc.)

**Test file:** `#[[file:airflow-core/tests/unit/jobs/test_scheduler_job.py]]` or a new test in `#[[file:airflow-core/tests/unit/models/test_backfill.py]]`
**Tests needed:**
- `test_create_backfill_cleans_up_on_empty_dagrun_list` — mock `_get_info_list` to return empty, verify Backfill gets `completed_at` set
- `test_create_backfill_cleans_up_on_dagrun_creation_failure` — mock `_create_runs_non_partitioned` to raise, verify cleanup

| Pros | Cons |
|------|------|
| Immediate cleanup — orphan never exists | Doesn't handle hard crashes (SIGKILL, OOM, power loss) |
| No waiting period | Cleanup commit itself could fail (DB connection lost) |
| Handles 99% of failure cases | Doesn't fix premature completion (still needs Option A's guard) |
| Clean error handling pattern | |

**Risk:** Low. Straightforward error handling. But not sufficient alone — needs Option A for the premature completion fix.

---

### Option C + D: Recommended Approach (Belt and Suspenders)

Combine all three mechanisms:

1. **Option A** (EXISTS guard in `_mark_backfills_complete`) — prevents premature completion
2. **Option D** (inline try/except in `_create_backfill`) — immediate cleanup on code-level failures (99% of cases)
3. **Option C** (time-based orphan cleanup in scheduler) — safety net for hard crashes where Option D's cleanup code never runs

**Files changed:**
- `#[[file:airflow-core/src/airflow/models/backfill.py]]` — try/except around DagRun creation (Option D)
- `#[[file:airflow-core/src/airflow/jobs/scheduler_job_runner.py]]` — EXISTS guard + orphan cleanup query (Options A + C)
- `#[[file:airflow-core/tests/unit/jobs/test_scheduler_job.py]]` — tests for all three behaviors

| Scenario | Handled by |
|----------|-----------|
| Scheduler runs during DagRun creation window | Option A (EXISTS guard) |
| `_create_backfill` fails after commit (code error) | Option D (inline cleanup) |
| Process killed mid-creation (SIGKILL, OOM) | Option C (scheduler orphan sweep) |
| Concurrent backfill requests | Existing `AlreadyRunningBackfill` check (unchanged) |

**Risk:** Low. Each layer covers what the others miss. No change to the commit/transaction structure, so `AlreadyRunningBackfill` concurrency protection stays intact.
