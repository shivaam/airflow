# Proposed Fix: `--celery-hostname` Causes Workers to Reserve but Never Execute Tasks

**Issue:** [GitHub #59707](https://github.com/apache/airflow/issues/59707)
**Date:** 2026-03-21

---

## Table of Contents

1. [Fix Overview](#1-fix-overview)
2. [Fix 1: Convert All Options to Strings (Primary)](#2-fix-1-convert-all-options-to-strings-primary)
3. [Fix 2: Repair Duplicate Hostname Detection (Secondary)](#3-fix-2-repair-duplicate-hostname-detection-secondary)
4. [Fix 3: Upstream Celery Bug Report (If Needed)](#4-fix-3-upstream-celery-bug-report-if-needed)
5. [Deciding Which Fix to Apply](#5-deciding-which-fix-to-apply)
6. [Files Changed](#6-files-changed)
7. [Pre-Submission Checklist](#7-pre-submission-checklist)

---

## 1. Fix Overview

The investigation identified two likely causes and one secondary bug:

| # | Issue | Confidence | Fix |
|---|-------|-----------|-----|
| **Primary** | `args.concurrency` is passed as `int` to `worker_main()`, which does CLI-style parsing and may not handle non-string types | High | Convert all options to strings |
| **Celery-side** | Celery's `worker_main()` may have a bug with `--hostname` + `-O fair` | Medium | Report upstream if confirmed by integration test |
| **Secondary** | Duplicate hostname check uses `endswith(f"@{hostname}")` which fails when hostname already contains `@` | Confirmed by code reading | Fix the comparison logic |

The test plan (see [test-plan.md](test-plan.md)) determines which fix is needed. The primary fix should be applied regardless since passing `int` to CLI arg parsing is incorrect even if it happens to work in some cases.

---

## 2. Fix 1: Convert All Options to Strings (Primary)

### Problem

In `celery_command.py`, the options list passed to `celery_app.worker_main()` mixes strings and integers:

```python
# celery_command.py:273-283 (CURRENT — BUGGY)
options = [
    "worker",
    "-O",
    "fair",
    "--queues",
    args.queues,          # str
    "--concurrency",
    args.concurrency,     # int! (ARG_CONCURRENCY has type=int)
    "--loglevel",
    celery_log_level,     # str
]
```

`Celery.worker_main(argv)` passes `argv` to `click`/`argparse`-style CLI parsing. These parsers expect `list[str]`. Passing an `int` in the list is undefined behavior — it may work in some configurations but fail in others (like when additional options shift argument positions).

### Proposed Change

**File:** `providers/celery/src/airflow/providers/celery/cli/celery_command.py`

```python
# celery_command.py:273-283 (FIXED)
options = [
    "worker",
    "-O",
    "fair",
    "--queues",
    args.queues,
    "--concurrency",
    str(args.concurrency),     # <-- FIX: explicitly convert to str
    "--loglevel",
    celery_log_level,
]
```

### Why This Is the Right Fix

1. **Correctness:** `worker_main()` is a CLI entry point wrapper — it expects string arguments, the same way `sys.argv` contains strings.
2. **Minimal change:** One-line fix with no side effects.
3. **Prevents the class of bug:** Even if the `int` works today in some Celery version, future Celery versions could break.
4. **The existing test already expects `int`** — the test at line 179 asserts `int(concurrency)` is in the options. This test is wrong because it validates the bug. The test needs updating too.

### Test Update Required

**File:** `providers/celery/tests/unit/celery/cli/test_celery_command.py`

The existing test on line 179 asserts the wrong thing:
```python
# CURRENT (wrong — asserts the buggy int behavior)
mock_celery_app.worker_main.assert_called_once_with(
    [
        "worker",
        "-O", "fair",
        "--queues", queues,
        "--concurrency", int(concurrency),  # Asserts int
        "--loglevel", ...,
        "--hostname", celery_hostname,
        ...
    ]
)
```

Should become:
```python
# FIXED (asserts string behavior)
mock_celery_app.worker_main.assert_called_once_with(
    [
        "worker",
        "-O", "fair",
        "--queues", queues,
        "--concurrency", concurrency,  # Now a str ("1")
        "--loglevel", ...,
        "--hostname", celery_hostname,
        ...
    ]
)
```

---

## 3. Fix 2: Repair Duplicate Hostname Detection (Secondary)

### Problem

The duplicate hostname check at lines 222-233 of `celery_command.py` is broken when the hostname contains `@`:

```python
# CURRENT (BUGGY)
if args.celery_hostname:
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        active_worker_names = list(active_workers.keys())
        # BUG: If hostname is "myworker@%h", this checks endswith("@myworker@%h")
        # But active workers report as "myworker@machine", which does NOT end with "@myworker@%h"
        if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
            raise SystemExit(...)
```

Example failure:
- User passes: `--celery-hostname "myworker@mymachine"`
- Active worker: `"myworker@mymachine"`
- Check: `"myworker@mymachine".endswith("@myworker@mymachine")` → **False** (the string doesn't start with `@`)
- Result: Duplicate detection fails silently, allows starting a second worker with same name

This is separate from the execution bug but should be fixed together.

### Proposed Change

```python
# FIXED
if args.celery_hostname:
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        active_worker_names = list(active_workers.keys())
        celery_hostname = args.celery_hostname

        # Check for exact match first (user passed full "name@host" format)
        # Then check suffix match (user passed just the hostname part)
        if any(
            name == celery_hostname or name.endswith(f"@{celery_hostname}")
            for name in active_worker_names
        ):
            raise SystemExit(
                f"Error: A worker with hostname '{celery_hostname}' is already running. "
                "Please use a different hostname or stop the existing worker first."
            )
```

### Why This Fix Works

| User Input | Active Worker | Old Check | New Check |
|-----------|--------------|-----------|-----------|
| `"worker1"` | `"celery@worker1"` | `endswith("@worker1")` = True | `endswith("@worker1")` = True |
| `"myworker@machine"` | `"myworker@machine"` | `endswith("@myworker@machine")` = **False (BUG)** | `== "myworker@machine"` = **True (FIXED)** |
| `"celery@%h"` (unexpanded) | `"celery@machine"` | `endswith("@celery@%h")` = **False** | Both fail — but this is expected since `%h` hasn't been expanded yet |

Note: The `%h` format specifier case is inherently tricky. Celery expands `%h` when the worker starts, but this check runs BEFORE the worker starts. A fully correct solution would need to expand the format specifiers before comparing, but that's a much larger change and the current behavior (not detecting duplicates for format specifiers) is acceptable since the worst case is a warning, not a crash.

---

## 4. Fix 3: Upstream Celery Bug Report (If Needed)

### When to File This

If the standalone Celery integration test (Test 3.1B in test-plan.md) shows that pure Celery with `str` concurrency + `--hostname` still fails, the bug is in Celery itself.

### Bug Report Template

```markdown
## Description

When using `Celery.worker_main()` with `--hostname` and `-O fair`, tasks are
reserved by the consumer but never dispatched to pool workers.

## Steps to Reproduce

```python
from celery import Celery

app = Celery("test", broker="redis://localhost:6379/0")
app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

@app.task(name="test")
def test_task():
    return "done"

# This works:
app.worker_main(["worker", "-O", "fair", "--concurrency", "1", "--queues", "default"])

# This does NOT work (tasks stuck in reserved):
app.worker_main(["worker", "-O", "fair", "--concurrency", "1", "--queues", "default",
                  "--hostname", "myworker@%h"])
```

## Expected Behavior

Tasks should execute regardless of `--hostname` value.

## Actual Behavior

Tasks are reserved (acknowledged=False, worker_pid=None) and never execute.

## Environment

- Celery version: 5.5.x
- Broker: Redis
- OS: Linux (Ubuntu 22.04)
```

---

## 5. Deciding Which Fix to Apply

Use this decision tree based on the test plan results:

```
Run Unit Test 2.1 (options all strings?)
  │
  ├─ FAIL (int found) ─── Apply Fix 1 (str conversion)
  │     │
  │     └─ Run Live Test 4.3 (does fix work?)
  │           │
  │           ├─ PASS ─── Done! Submit PR with Fix 1 + Fix 2
  │           │
  │           └─ FAIL ─── Also a Celery bug
  │                 │
  │                 └─ Run Integration Test 3.1B
  │                       │
  │                       ├─ FAIL ─── File Celery upstream bug (Fix 3)
  │                       │           Apply workaround in Airflow
  │                       │
  │                       └─ PASS ─── Something else in Airflow
  │                                   Add more instrumentation
  │
  └─ PASS (all strings) ─── Unlikely, but:
        │
        └─ Run Integration Test 3.1B
              │
              ├─ FAIL ─── Pure Celery bug → Fix 3
              │
              └─ PASS ─── Airflow-specific interaction
                          Add instrumentation to worker startup
```

### Most Likely Outcome

Fix 1 (str conversion) resolves the issue. The `int` in the options list causes Celery's internal argument parser to misparse subsequent options when `--hostname` shifts positions.

---

## 6. Files Changed

### Summary

| File | Change | Type |
|------|--------|------|
| `providers/celery/src/.../cli/celery_command.py` | `str(args.concurrency)` on line 280 | Bug fix |
| `providers/celery/src/.../cli/celery_command.py` | Fix duplicate hostname detection on lines 222-233 | Bug fix |
| `providers/celery/tests/.../cli/test_celery_command.py` | Update `int(concurrency)` assertion to `str` | Test fix |
| `providers/celery/tests/.../cli/test_celery_command.py` | Add `TestWorkerOptionsTypes` regression test | New test |
| `providers/celery/tests/.../cli/test_celery_command.py` | Add duplicate hostname with `@` test | New test |

### Detailed Diff Preview

#### `celery_command.py` — Line 280

```diff
     options = [
         "worker",
         "-O",
         "fair",
         "--queues",
         args.queues,
         "--concurrency",
-        args.concurrency,
+        str(args.concurrency),
         "--loglevel",
         celery_log_level,
     ]
```

#### `celery_command.py` — Lines 222-233

```diff
     if args.celery_hostname:
         inspect = celery_app.control.inspect()
         active_workers = inspect.active_queues()
         if active_workers:
             active_worker_names = list(active_workers.keys())
-            if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
+            celery_hostname = args.celery_hostname
+            if any(
+                name == celery_hostname or name.endswith(f"@{celery_hostname}")
+                for name in active_worker_names
+            ):
                 raise SystemExit(
-                    f"Error: A worker with hostname '{args.celery_hostname}' is already running. "
+                    f"Error: A worker with hostname '{celery_hostname}' is already running. "
                     "Please use a different hostname or stop the existing worker first."
                 )
```

#### `test_celery_command.py` — Line 179

```diff
         mock_celery_app.worker_main.assert_called_once_with(
             [
                 "worker",
                 "-O",
                 "fair",
                 "--queues",
                 queues,
                 "--concurrency",
-                int(concurrency),
+                concurrency,
                 "--loglevel",
                 conf.get("logging", "CELERY_LOGGING_LEVEL"),
                 "--hostname",
                 celery_hostname,
                 "--autoscale",
                 autoscale,
                 "--without-mingle",
                 "--without-gossip",
                 "--pool",
                 "prefork",
             ]
         )
```

---

## 7. Pre-Submission Checklist

Before submitting the PR, verify:

- [ ] **Unit tests pass:** `uv run --project providers/celery pytest providers/celery/tests/unit/celery/cli/test_celery_command.py -xvs`
- [ ] **Standalone Celery test passes:** Run `dev/test_celery_hostname_standalone.py` combinations A-D
- [ ] **Live test passes:** Worker with `--celery-hostname` executes tasks (Test 4.3)
- [ ] **Multiple hostname formats work:** Test 4.4 passes for all formats
- [ ] **Multi-worker test passes:** Test 4.5 with two workers
- [ ] **Ruff format:** `uv run ruff format providers/celery/src/airflow/providers/celery/cli/celery_command.py`
- [ ] **Ruff check:** `uv run ruff check --fix providers/celery/src/airflow/providers/celery/cli/celery_command.py`
- [ ] **Static checks:** `prek run --from-ref main --stage pre-commit`
- [ ] **Newsfragment created:** `echo "Fix Celery worker not executing tasks when using --celery-hostname" > providers/celery/newsfragments/59707.bugfix.rst`

### PR Title

```
Fix Celery worker not executing tasks when using --celery-hostname
```

### PR Body

```markdown
## Summary

- Fix `--celery-hostname` causing Celery workers to reserve but never execute tasks
- Convert `args.concurrency` from `int` to `str` in the options list passed to
  `celery_app.worker_main()`, which expects CLI-style string arguments
- Fix duplicate hostname detection when hostname contains `@` character

closes: #59707

## Test plan

- [ ] Unit tests verify all options are strings
- [ ] Standalone Celery integration test confirms fix
- [ ] Live environment test with `--celery-hostname "myworker@%h"` executes tasks
- [ ] Multiple hostname formats tested (simple, with @, with %h)
- [ ] Multi-worker scenario with different hostnames works
```
