# Effort Estimate

## Overview

Total estimated effort: **4-8 weeks** of focused work across 5 phases, spread over
2-3 Airflow minor releases (3.3 → 3.4 → 4.0).

The bulk of the complexity is not in the code changes themselves but in:
1. Context compatibility (making the new path produce identical `context` dicts)
2. Testing (large matrix of callback_type x executor x importable_or_not)
3. Community alignment (dev list discussion, potential AIP)

## Phase Breakdown

### Phase 0: Community Alignment
**Effort: 1-2 weeks** (calendar time, not coding time)

| Task | Effort | Notes |
|------|--------|-------|
| Draft dev list email | 4 hours | Concrete proposal, not open-ended |
| Respond to feedback | 2-3 days | Iterate on proposal |
| Write AIP (if needed) | 2-3 days | Formal proposal document |
| Present at community call | 30 min | Optional, per ferruzzi's offer |

**Deliverable**: Community consensus on approach, scheduling priority, timeline.

### Phase 1: DAG-Level Callbacks → Executor
**Effort: 1.5-2 weeks**

| Task | Hours | Complexity |
|------|-------|------------|
| Import path resolution utility + tests | 6-8 | Low |
| Add callback_path to serialized DAG | 4-6 | Low |
| Modify scheduler to create ExecutorCallback for DAG callbacks | 8-12 | Medium |
| Modify callback sink to route based on importability | 4-6 | Medium |
| DAG Processor fallback for non-importable callbacks | 4-6 | Low |
| Deprecation warnings | 2-3 | Low |
| Unit tests | 8-12 | Medium |
| Integration tests (LocalExecutor) | 6-8 | Medium |
| **Subtotal** | **42-61** | |

**Deliverable**: DAG-level `on_success_callback` / `on_failure_callback` run in executor
for importable callables. Fallback to DAG Processor for lambdas/closures.

### Phase 2: Task-Level Callbacks → Executor
**Effort: 1.5-2 weeks**

| Task | Hours | Complexity |
|------|-------|------------|
| Modify BaseOperator to store callback paths | 6-8 | Medium |
| Extend serialized operator with callback paths | 4-6 | Low |
| Modify scheduler task callback handling | 8-12 | Medium |
| Handle on_retry, on_execute, on_skipped | 6-8 | Medium |
| Heartbeat timeout callback path | 4-6 | Medium |
| Unit tests | 8-12 | Medium |
| Integration tests | 6-8 | Medium |
| **Subtotal** | **42-60** | |

**Deliverable**: Task-level callbacks run in executor. Same fallback pattern.

### Phase 3: Context Compatibility
**Effort: 1-2 weeks**

This is the hardest and most uncertain phase.

| Task | Hours | Complexity |
|------|-------|------------|
| Design context building approach (API vs serialized) | 4-6 | High |
| Execution API endpoint for callback context (if needed) | 8-12 | Medium |
| Context builder in callback supervisor | 8-12 | High |
| Ensure context parity with legacy system | 8-12 | High |
| Template rendering in worker context | 4-6 | Medium |
| Tests for context compatibility | 8-12 | High |
| **Subtotal** | **40-60** | |

**Deliverable**: Callbacks in workers receive the same `context` dict as legacy callbacks.

**Risk**: This phase may reveal that full context parity is very difficult. If so,
we may need to accept a reduced context and deprecate specific context keys, which
changes the migration from "transparent" to "requires user code changes."

### Phase 4: Email Callbacks + Notifiers
**Effort: 0.5-1 week**

| Task | Hours | Complexity |
|------|-------|------------|
| Migrate EmailRequest to executor path | 4-6 | Low |
| Ensure Notifier instances serialize correctly | 4-6 | Medium |
| Deprecate email_on_failure / email_on_retry | 2-3 | Low |
| Tests | 4-6 | Low |
| **Subtotal** | **14-21** | |

**Deliverable**: All callback types go through executor.

### Phase 5: Cleanup (Airflow 4.0)
**Effort: 1 week**

| Task | Hours | Complexity |
|------|-------|------------|
| Remove DagCallbackRequest, TaskCallbackRequest, EmailRequest | 4-6 | Low |
| Remove DatabaseCallbackSink | 2-3 | Low |
| Remove DagProcessorCallback | 2-3 | Low |
| Remove DAG Processor callback execution code | 4-6 | Low |
| Remove fallback paths | 2-3 | Low |
| Update all tests | 8-12 | Medium |
| Migration guide + docs | 4-6 | Low |
| **Subtotal** | **26-39** | |

## Effort Summary

| Phase | Effort (hours) | Effort (weeks) | Release Target |
|-------|---------------|----------------|----------------|
| 0: Community Alignment | 20-30 | 1-2 (calendar) | Pre-3.3 |
| 1: DAG-Level Callbacks | 42-61 | 1.5-2 | 3.3 |
| 2: Task-Level Callbacks | 42-60 | 1.5-2 | 3.3 |
| 3: Context Compatibility | 40-60 | 1-2 | 3.3 or 3.4 |
| 4: Email + Notifiers | 14-21 | 0.5-1 | 3.4 |
| 5: Cleanup | 26-39 | 1 | 4.0 |
| **Total** | **184-271** | **6.5-10** | |

## Dependencies

```
Phase 0 ──→ Phase 1 ──→ Phase 2
                │              │
                └──→ Phase 3 ──┘
                        │
                        └──→ Phase 4 ──→ Phase 5
```

- Phase 0 must complete before any code work
- Phase 1 and Phase 3 can be parallelized (1 does simplified context, 3 adds full context)
- Phase 2 depends on Phase 1 (same patterns, extended)
- Phase 4 is independent but low priority
- Phase 5 is 4.0 only — distant future

## External Dependencies

| Dependency | Owner | Impact |
|------------|-------|--------|
| Anish's workload queue unification PR | Anish Giri | Provides tier-based scheduling — callback priority assignment depends on this |
| ferruzzi's supervised workload PR | ferruzzi | Moves workloads into supervised processes — callback supervisor depends on this |
| Executor callback support (ECS, K8s, Batch) | Various | All executors need `supports_callbacks=True` before migration is complete |

## Recommended Starting Point

1. **Start with Phase 0** — write the dev list email with this investigation as backing
2. **Do the POC** (see `09_poc_plan.md`) — 1-2 days to validate approach
3. **Phase 1** once community agrees — ship in 3.3
4. **Phase 3** in parallel if possible — context compat is the biggest risk

## Complexity Rating

| Aspect | Rating | Reason |
|--------|--------|--------|
| Code volume | Medium | ~15-20 files touched, most changes are moderate |
| Architectural complexity | High | Crosses scheduler, executor, DAG processor, serialization, API |
| Backwards compatibility | High | Must not break existing DAGs |
| Testing | High | Large test matrix, integration tests needed |
| Community process | Medium | Needs consensus but has general support |
| Risk | Medium-High | Context compatibility is the biggest unknown |
