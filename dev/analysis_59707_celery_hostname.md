# Issue #59707: --celery-hostname Causes Workers to Reserve but Never Execute Tasks

## Problem Description

When launching a Celery worker with a custom hostname:

```bash
airflow celery worker --queues my_queue --concurrency 1 --celery-hostname "myworker@%h"
```

Tasks enter a **stuck state**:
- Status: Reserved but not executing
- `acknowledged: False`
- `worker_pid: None`
- `time_start: None`

Omitting the `--celery-hostname` parameter allows normal task execution.

### Affected Versions
- Apache Airflow: 3.1.5+ (confirmed through 3.1.7)
- Celery Provider: 3.14.0+ (confirmed through 3.15.2)
- **Regression**: Downgrading to Celery Provider **3.13.1** resolves the issue

---

## Root Cause Analysis

There are **two bugs** in the duplicate hostname check introduced in celery provider 3.14.0.

### Bug 1: Stale Broker Connection (Critical)

**File**: `providers/celery/src/airflow/providers/celery/cli/celery_command.py:222-233`

```python
# Check if a worker with the same hostname already exists
if args.celery_hostname:
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()       # Opens broker connection
    if active_workers:
        active_worker_names = list(active_workers.keys())
        if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
            raise SystemExit(...)
# ← Broker connection is NEVER closed here
```

**What happens**:

1. `celery_app.control.inspect()` opens a connection to the message broker (Redis/RabbitMQ).
2. `inspect.active_queues()` uses this connection to query active workers.
3. After the check, the connection **remains open** in the Celery app's connection pool.
4. Later, `celery_app.worker_main(options)` starts the worker using the **prefork** pool model.
5. Forked child processes **inherit the stale broker connection** file descriptors from the parent.
6. These stale connections cause message consumption failures — tasks are received/reserved by the
   parent process but child processes cannot acknowledge or execute them because they're using
   corrupted inherited connections.

This is a classic **pre-fork connection inheritance bug**. The broker connection opened before
`fork()` becomes shared between parent and children, leading to protocol-level corruption.

### Bug 2: Hostname Format Variables Not Expanded

**File**: `providers/celery/src/airflow/providers/celery/cli/celery_command.py:229`

```python
if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
```

When the user passes `--celery-hostname "myworker@%h"`, `args.celery_hostname` contains the
**raw format string** `"myworker@%h"`. However, Celery expands `%h` to the actual hostname at
worker startup time. The comparison checks:

```
"celery@myworker@actualhostname".endswith("@myworker@%h")  → False (never matches)
```

This means the duplicate check **silently fails** — it can never detect actual duplicates when
format variables are used. While this doesn't cause the stuck-task bug directly, it means the
safety check is useless for the most common hostname patterns.

---

## Architecture of the Celery Worker Feature

### Worker Startup Flow

```
airflow celery worker --celery-hostname "myworker@%h"
    │
    ▼
celery_command.py: worker_command()
    │
    ├─→ [1] Duplicate hostname check (lines 222-233)
    │       └─→ inspect = celery_app.control.inspect()
    │       └─→ active_workers = inspect.active_queues()
    │       └─→ ⚠️ Broker connection left open
    │
    ├─→ [2] Configure worker options (lines 272-291)
    │       └─→ options = ["worker", "-O", "fair", "--queues", ...]
    │       └─→ options.extend(["--hostname", args.celery_hostname])
    │
    └─→ [3] Start worker (line 321)
            └─→ celery_app.worker_main(options)
                    │
                    ├─→ Parent process (manages broker)
                    │       └─→ Has stale connection from step [1]
                    │
                    └─→ fork() → Child processes (execute tasks)
                            └─→ Inherit stale broker FDs
                            └─→ Cannot properly acknowledge/execute tasks
```

### Celery Prefork Pool Model

```
┌──────────────────────────────────────────┐
│          Parent Process                   │
│                                           │
│  ┌─────────────────────────────────┐     │
│  │  Broker Connection Pool          │     │
│  │  - Connection from inspect() ◄── STALE│
│  │  - New connection for worker     │     │
│  └─────────────────────────────────┘     │
│                                           │
│  Receives messages from broker            │
│  Dispatches to child processes            │
└───────────┬───────────┬──────────────────┘
            │           │
     fork() │    fork() │
            ▼           ▼
    ┌───────────┐ ┌───────────┐
    │  Child 1  │ │  Child 2  │
    │           │ │           │
    │ Inherits  │ │ Inherits  │
    │ stale FDs │ │ stale FDs │
    │           │ │           │
    │ Task exec │ │ Task exec │
    │ FAILS     │ │ FAILS     │
    └───────────┘ └───────────┘
```

### Other Hostname-Dependent Code Paths

The following functions in `celery_command.py` also use `args.celery_hostname`:

| Function | Line | Usage | Format Bug? |
|---|---|---|---|
| `_check_if_active_celery_worker()` | 351 | `hostname not in active_workers` | Yes — raw format vs expanded |
| `shutdown_worker()` | 389-393 | `destination=[args.celery_hostname]` | Yes — sends raw format |
| `add_queue()` | 418-424 | `destination=[args.celery_hostname]` | Yes — sends raw format |
| `remove_queue()` | 431-437 | `destination=[args.celery_hostname]` | Yes — sends raw format |
| `remove_all_queues()` | 444-467 | Lookup + cancel_consumer | Yes — raw format |

These functions don't have the stale-connection bug (they don't fork), but they have the
**format-expansion bug** — they send the raw `%h` format to Celery control commands instead of
the expanded hostname.

---

## Proposed Solutions

### Fix 1: Close Broker Connection After Inspect (Critical)

After the duplicate hostname check, explicitly close/reset the broker connection pool:

```python
if args.celery_hostname:
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        active_worker_names = list(active_workers.keys())
        if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
            raise SystemExit(...)
    # Close broker connections to prevent stale FDs in forked children
    celery_app.connection_for_write().release()
    celery_app.pool.force_close_all()
```

**Effort**: 2-3 lines added.

### Fix 2: Expand Hostname Format Before Comparison

Use Celery's `host_format()` utility to expand `%h`, `%n`, `%d` etc. before comparing:

```python
from celery.utils.nodenames import host_format

if args.celery_hostname:
    expanded_hostname = host_format(args.celery_hostname)
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        active_worker_names = list(active_workers.keys())
        if any(name.endswith(f"@{expanded_hostname}") for name in active_worker_names):
            raise SystemExit(...)
```

**Effort**: 5-10 lines.

### Fix 3: Apply Format Expansion to All Hostname-Dependent Commands

Apply the same `host_format()` expansion in `_check_if_active_celery_worker()` and all
control commands (shutdown, add_queue, remove_queue, remove_all_queues).

**Effort**: ~20 lines across multiple functions.

---

## Blind Spots and Potential Problems

1. **Connection closing approach**: The exact API to close Celery broker connections may vary
   between Celery versions and broker backends (Redis vs RabbitMQ vs SQS). Need to verify
   `pool.force_close_all()` works across all backends.

2. **Regression window**: The duplicate hostname check was likely added in celery provider 3.14.0.
   Need to verify via `git log` what exact PR introduced it. The safest minimal fix may be to
   simply **remove** the duplicate check entirely (it was never present before 3.14.0) and add
   it back properly later.

3. **`host_format()` import**: Need to verify `celery.utils.nodenames.host_format` is available
   in all supported Celery versions. Alternatively, check if `%h` is present and skip the
   comparison.

4. **Multiple format specifiers**: Celery supports `%h` (full hostname), `%n` (name only),
   `%d` (domain only). The fix should handle all of them.

5. **Impact on other CLI commands**: The `shutdown_worker`, `add_queue`, `remove_queue`, and
   `remove_all_queues` commands pass the raw format string to `celery_app.control.shutdown(
   destination=[...])`. These Celery control API calls may not expand format variables,
   causing silent failures when using hostnames with `%h`.

6. **Testing difficulty**: This bug requires a running Celery broker and prefork worker pool to
   reproduce, making unit testing challenging. Integration tests with `breeze` would be needed.
