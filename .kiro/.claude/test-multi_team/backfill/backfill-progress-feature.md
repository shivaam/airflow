# Backfill Progress Visibility Feature

## Problem

Users have no way to see how a backfill is progressing. The current UI shows:
- An indeterminate spinner in the backfill banner (no actual numbers)
- A backfills table with from/to dates, created/completed timestamps, and duration — but no progress breakdown
- DagRuns list shows `run_type = backfill_job` but no link to which backfill spawned them

All the data needed for progress is already in the DB (BackfillDagRun + DagRun tables). It just isn't aggregated or exposed.

## Proposed Changes

### 1. Progress API Endpoint

`GET /backfills/{backfill_id}/progress`

Single aggregation query joining BackfillDagRun with DagRun:

```sql
SELECT
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE bdr.dag_run_id IS NOT NULL AND dr.state = 'success') as completed,
  COUNT(*) FILTER (WHERE bdr.dag_run_id IS NOT NULL AND dr.state = 'running') as running,
  COUNT(*) FILTER (WHERE bdr.dag_run_id IS NOT NULL AND dr.state = 'queued') as queued,
  COUNT(*) FILTER (WHERE bdr.dag_run_id IS NOT NULL AND dr.state = 'failed') as failed,
  COUNT(*) FILTER (WHERE bdr.dag_run_id IS NULL) as skipped
FROM backfill_dag_run bdr
LEFT JOIN dag_run dr ON bdr.dag_run_id = dr.id
WHERE bdr.backfill_id = :backfill_id
```

Response model:

```json
{
  "backfill_id": 42,
  "total": 100,
  "completed": 65,
  "running": 3,
  "queued": 12,
  "failed": 2,
  "skipped": 18
}
```

Notes:
- `total` = all BackfillDagRun rows for this backfill (every timetable slot)
- `skipped` = BackfillDagRun rows where `dag_run_id IS NULL` (exception_reason is IN_FLIGHT, ALREADY_EXISTS, or UNKNOWN)
- `completed + running + queued + failed + skipped = total`
- MySQL doesn't support `FILTER (WHERE ...)` — use `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` instead for cross-DB compatibility

#### Alternative: Embed in BackfillResponse

Instead of a separate endpoint, add progress fields directly to `BackfillResponse`. This avoids an extra API call but makes the list endpoint heavier. Could use a query param like `?include_progress=true` to opt in.

### 2. Backfill Banner — Real Progress Bar

Current: `BackfillBanner.tsx` shows `<ProgressBar size="xs" visibility="visible" />` — an indeterminate spinner.

Replace with a determinate progress bar showing actual numbers:

```
Backfill in progress: Jan 1 - Apr 10  [████████░░░░] 65/100 (3 running, 2 failed)
```

Implementation:
- Call the progress endpoint (or use embedded progress from list endpoint)
- Replace `<ProgressBar>` with `<ProgressBar value={(completed / total) * 100} />`
- Add text showing `{completed}/{total}` and a summary of active states
- Already auto-refreshes via `refetchInterval` — progress updates automatically

### 3. Backfills Table — Progress Column

Current columns: from, to, reprocess behavior, created, completed, duration, max_active_runs.

Add a "Progress" column:
- Active backfills: `65/100` with a mini progress bar, colored segments for running/failed/skipped
- Completed backfills: `100/100 ✓` or `95/100 (5 skipped)`
- Could show a tooltip with the full breakdown on hover

### 4. Backfill Tag on DagRuns

DagRun already has `backfill_id` on the model. Currently not exposed in the API response.

Changes needed:

#### API
- Add `backfill_id` to the DagRun response serializer (it's already on the SQLAlchemy model)

#### UI — DagRuns List (`DagRuns.tsx`)
- For runs where `backfill_id` is set, show a small badge/tag: "Backfill #42"
- Make it a link to the backfill detail (the Backfills tab on the DAG page, or a future backfill detail page)
- Could use the existing `RunTypeIcon` area — backfill runs already show the backfill_job icon, just add the ID

#### UI — DagRun Detail (`Run/Header.tsx`, `Run/Details.tsx`)
- Show "Part of Backfill #42" in the run details
- Link to the backfill

#### UI — Grid View
- Backfill DagRuns could have a subtle visual indicator (border, background tint, small icon) to distinguish them from scheduled runs

## Implementation Plan

### Phase 1: API (backend)
1. Create `BackfillProgressResponse` datamodel in `datamodels/backfills.py`
2. Add `GET /backfills/{backfill_id}/progress` endpoint in `routes/public/backfills.py`
3. Add `backfill_id` to DagRun response serializer
4. Add corresponding UI endpoint if needed (`routes/ui/backfills.py`)

### Phase 2: Banner Progress (frontend)
1. Add query hook for the progress endpoint
2. Replace indeterminate `<ProgressBar>` with determinate version in `BackfillBanner.tsx`
3. Add progress text

### Phase 3: Backfills Table (frontend)
1. Add progress column to `Backfills.tsx`
2. Fetch progress for each backfill (batch or embedded in list response)

### Phase 4: DagRun Tags (frontend)
1. Add backfill badge to DagRuns list
2. Add backfill info to DagRun detail page
3. Link badges to backfill detail

## Files to Modify

### Backend
| File | Change |
|------|--------|
| `airflow-core/src/airflow/api_fastapi/core_api/datamodels/backfills.py` | Add `BackfillProgressResponse` model |
| `airflow-core/src/airflow/api_fastapi/core_api/routes/public/backfills.py` | Add progress endpoint |
| `airflow-core/src/airflow/api_fastapi/core_api/routes/ui/backfills.py` | Add UI progress endpoint if needed |
| DagRun response serializer | Expose `backfill_id` field |

### Frontend
| File | Change |
|------|--------|
| `airflow-core/src/airflow/ui/src/components/Banner/BackfillBanner.tsx` | Real progress bar + numbers |
| `airflow-core/src/airflow/ui/src/pages/Dag/Backfills/Backfills.tsx` | Progress column in table |
| `airflow-core/src/airflow/ui/src/pages/DagRuns.tsx` | Backfill tag on DagRun rows |
| `airflow-core/src/airflow/ui/src/pages/Run/Header.tsx` | Backfill info in run detail |
| `airflow-core/src/airflow/ui/src/pages/Run/Details.tsx` | Backfill info in run detail |
| OpenAPI generated types | Will auto-update from backend schema |

## Open Questions

1. **Separate endpoint vs embedded progress?** A separate endpoint is cleaner but requires an extra call per backfill in the list view. Embedding in BackfillResponse is more efficient for the table but makes the list query heavier.

2. **Skipped date detail?** Should the progress response break down skipped reasons (IN_FLIGHT vs ALREADY_EXISTS)? Useful for debugging but adds complexity.

3. **Backfill detail page?** Currently there's no dedicated page for a single backfill. The Backfills tab shows a table. A detail page could show the full progress breakdown, list of DagRuns with their states, and the skipped dates with reasons. Worth doing as a follow-up?

4. **Grid view integration?** The grid view is the primary way users monitor DagRuns. Should backfill runs have a visual indicator there? This could be complex depending on how the grid is rendered.
