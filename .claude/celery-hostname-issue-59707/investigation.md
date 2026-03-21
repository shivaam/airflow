# Investigation: `--celery-hostname` Causes Workers to Reserve but Never Execute Tasks

**Issue:** [GitHub #59707](https://github.com/apache/airflow/issues/59707)
**Reporter:** @CreeperBeatz
**Airflow Version:** 3.1.5
**Celery Provider Version:** 3.14.1
**Date:** 2026-03-21

---

## Table of Contents

1. [What is Celery?](#1-what-is-celery)
2. [How Celery Works with Airflow](#2-how-celery-works-with-airflow)
3. [The Issue Explained](#3-the-issue-explained)
4. [Code Walkthrough](#4-code-walkthrough)
5. [Root Cause Analysis](#5-root-cause-analysis)
6. [How to Reproduce](#6-how-to-reproduce)
7. [How to Explore Further](#7-how-to-explore-further)
8. [Potential Fix Directions](#8-potential-fix-directions)

---

## 1. What is Celery?

Celery is a distributed task queue system for Python. It allows you to offload work from your main application to separate worker processes, potentially running on different machines. Key concepts:

- **Broker**: A message queue (Redis, RabbitMQ) that holds tasks waiting to be processed
- **Worker**: A process that consumes tasks from the broker and executes them
- **Task**: A Python function decorated with `@app.task` that can be sent to the broker
- **Result Backend**: Where task results/states are stored (database, Redis)
- **Celery App**: The central configuration object that ties everything together

### How Celery Processes Tasks

```
Producer (Scheduler)                     Consumer (Worker)
       |                                      |
       |  1. task.apply_async(queue="default") |
       |  ──────────────────────────────────>  |
       |         [Broker: Redis/RabbitMQ]      |
       |                                       |
       |                          2. Worker prefetches message
       |                          3. Worker looks up task in registry
       |                          4. Worker dispatches to pool process
       |                          5. Pool process executes function
       |                          6. Result stored in backend
       |                          7. Message acknowledged (with task_acks_late)
```

### Worker Node Names

Every Celery worker has a **node name** in the format `name@hostname`:
- Default: `celery@machine-hostname` (e.g., `celery@DESKTOP-MAG5124`)
- Custom: Set via `--hostname` flag (e.g., `--hostname "myworker@%h"`)
- Format specifiers: `%h` = full hostname, `%n` = hostname only, `%d` = domain only

### Running Celery

```bash
# Start a Celery worker directly
celery -A myapp worker --loglevel=info

# Start with custom hostname
celery -A myapp worker --hostname "worker1@%h"

# Inspect workers
celery -A myapp inspect active_queues
celery -A myapp inspect reserved
celery -A myapp inspect active
```

### Key Configuration That Matters for This Issue

| Setting | Value | Meaning |
|---------|-------|---------|
| `task_acks_late` | `True` | Task is acknowledged AFTER execution, not on receipt |
| `worker_prefetch_multiplier` | `1` | Worker prefetches only 1 task at a time |
| `-O fair` | (CLI flag) | Uses "fair" scheduling — waits for pool availability before dispatching |
| `task_track_started` | `True` | Track when task transitions to STARTED state |

---

## 2. How Celery Works with Airflow

### Architecture Overview

```
                    Airflow System
    ┌──────────────────────────────────────────────────┐
    │                                                  │
    │  Scheduler                                       │
    │    │                                             │
    │    ▼                                             │
    │  CeleryExecutor                                  │
    │    │  _process_workloads()                        │
    │    │  _send_tasks()                              │
    │    │  _send_tasks_to_celery()                    │
    │    │    └─ ProcessPoolExecutor                   │
    │    │         └─ send_workload_to_executor()      │
    │    │              └─ task.apply_async(queue=...)  │
    │    │                                             │
    │    ▼                                             │
    │  Broker (Redis/RabbitMQ)                         │
    │    │                                             │
    │    ▼                                             │
    │  Celery Worker (airflow celery worker)           │
    │    │  Subscribed to queues via --queues flag      │
    │    │  execute_workload() function runs here      │
    │    │    └─ Deserializes workload JSON             │
    │    │    └─ Calls supervise() from Task SDK        │
    │    │    └─ Communicates with Execution API        │
    │    │                                             │
    │    ▼                                             │
    │  Result Backend (DB/Redis)                       │
    │    │  Stores task state: PENDING→STARTED→SUCCESS  │
    │    │                                             │
    │    ▼                                             │
    │  CeleryExecutor.sync()                           │
    │    └─ BulkStateFetcher polls result backend      │
    │    └─ Updates event_buffer with task states       │
    └──────────────────────────────────────────────────┘
```

### Key Files in the Codebase

| File | Purpose |
|------|---------|
| `providers/celery/src/.../executors/celery_executor.py` | Main `CeleryExecutor` class; lazy-imports `app` from utils |
| `providers/celery/src/.../executors/celery_executor_utils.py` | Core utilities: creates Celery app, registers tasks (`execute_workload`), sends tasks to broker |
| `providers/celery/src/.../executors/default_celery.py` | Builds Celery configuration dict from Airflow config |
| `providers/celery/src/.../cli/celery_command.py` | CLI commands: `worker()`, `flower()`, `shutdown_worker()`, etc. |
| `providers/celery/src/.../cli/definition.py` | CLI argument definitions (`ARG_CELERY_HOSTNAME`, etc.) |

### The Celery App and Task Registration Chain

```python
# celery_executor_utils.py — Module-level app creation (line 148)
app = _get_celery_app()
#  └─ Celery(celery_app_name, config_source=get_celery_configuration())

# celery_executor_utils.py — Task registration (line 190)
@app.task(name="execute_workload")
def execute_workload(input: str) -> None:
    # Deserializes workload, calls supervise()
    ...

# celery_executor.py — Triggers registration at import time (line 42-44)
from airflow.providers.celery.executors import (
    celery_executor_utils as _celery_executor_utils,
    # ^^ This import triggers module-level code, registering tasks
    # See issue #63043 for why this was needed
)

# celery_executor.py — Lazy app attribute (line 66-74)
def __getattr__(name):
    if name == "app":
        from airflow.providers.celery.executors.celery_executor_utils import app
        return app
    raise AttributeError(...)
```

---

## 3. The Issue Explained

### Symptoms

When starting a Celery worker with `--celery-hostname`:
```bash
airflow celery worker --queues my_queue --concurrency 1 --celery-hostname "myworker@%h"
```

1. Worker connects to the broker successfully
2. Worker subscribes to the correct queues
3. Tasks ARE delivered to the worker (they appear in "reserved" state)
4. Tasks are NEVER executed:
   - `acknowledged: False` — task was never acknowledged
   - `worker_pid: None` — no pool process picked it up
   - `time_start: None` — execution never started

### The Critical Observation

From the issue reporter's `celery inspect reserved` output:
```json
{
    "id": "923f016a-...",
    "name": "execute_workload",
    "hostname": "default@DESKTOP-MAG5124",
    "time_start": null,
    "acknowledged": false,
    "worker_pid": null
}
```

Compare with the **working** case (no `--celery-hostname`):
- Worker name: `celery@DESKTOP-MAG5124`
- `celery inspect reserved` returns empty (tasks execute immediately)

### What "Reserved but Not Executed" Means

In Celery's task lifecycle with `-O fair` and `task_acks_late=True`:

```
    Message received from broker
           │
           ▼
    ┌──────────────┐
    │   RESERVED   │  ◄── Consumer received the message
    │              │      and put it in its internal buffer
    │ ack: false   │
    │ pid: null    │
    │ time: null   │
    └──────┬───────┘
           │
           ▼  (Consumer dispatches to pool worker — THIS STEP FAILS)
    ┌──────────────┐
    │   STARTED    │  ◄── Pool worker picks up the task
    │              │
    │ ack: false   │
    │ pid: 12345   │
    │ time: 14:30  │
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │  SUCCESS     │  ◄── Task completes, message acknowledged
    │              │
    │ ack: true    │
    └──────────────┘
```

The task is stuck at RESERVED: the consumer received it from the broker but never dispatched it to a pool worker for execution.

---

## 4. Code Walkthrough

### Step 1: CLI Argument Definition

**File:** `providers/celery/src/.../cli/definition.py` (lines 83-86)

```python
ARG_CELERY_HOSTNAME = Arg(
    ("-H", "--celery-hostname"),
    help="Set the hostname of celery worker if you have multiple workers on a single machine",
)
```

The argument is optional. When not provided, `args.celery_hostname` is `None`.

### Step 2: Worker Startup

**File:** `providers/celery/src/.../cli/celery_command.py` (lines 191-327)

```python
@cli_utils.action_cli(check_db=not AIRFLOW_V_3_0_PLUS)
def worker(args):
    """Start Airflow Celery worker."""

    # === STEP 2a: Get the Celery app ===
    # (For non-team mode, which is the common case)
    from airflow.providers.celery.executors.celery_executor import app as celery_app
    # This triggers the import chain:
    #   celery_executor.py imports celery_executor_utils
    #   celery_executor_utils.py creates app and registers execute_workload
    #   celery_executor.__getattr__("app") returns the module-level app

    # === STEP 2b: Duplicate hostname check (lines 222-233) ===
    if args.celery_hostname:
        inspect = celery_app.control.inspect()
        active_workers = inspect.active_queues()
        if active_workers:
            active_worker_names = list(active_workers.keys())
            if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
                raise SystemExit("Error: A worker with hostname '...' is already running.")
    # NOTE: This check uses the RAW hostname (with %h unexpanded).
    # The endswith() check means "celery@myhost" would match "@myhost".

    # === STEP 2c: Build Celery worker options (lines 273-301) ===
    options = [
        "worker",
        "-O", "fair",                    # Fair scheduling strategy
        "--queues", args.queues,         # Queue(s) to subscribe to
        "--concurrency", args.concurrency,  # NOTE: This is an int, not str!
        "--loglevel", celery_log_level,
    ]
    if args.celery_hostname:
        options.extend(["--hostname", args.celery_hostname])  # <-- THE KEY LINE
    if autoscale:
        options.extend(["--autoscale", autoscale])
    if args.without_mingle:
        options.append("--without-mingle")
    if args.without_gossip:
        options.append("--without-gossip")
    if config.has_option("celery", "pool"):
        pool = config.get("celery", "pool")
        options.extend(["--pool", pool])
        maybe_patch_concurrency(["-P", pool])

    # === STEP 2d: Start the worker (lines 311-313) ===
    def run_celery_worker():
        with _serve_logs(skip_serve_logs), _run_stale_bundle_cleanup():
            celery_app.worker_main(options)
```

### Step 3: What `celery_app.worker_main(options)` Does

This is Celery library code. It:
1. Parses the `options` list as CLI arguments
2. Creates a `Worker` instance with the configuration from the `celery_app`
3. Sets the worker's hostname (either from `--hostname` or the default)
4. Creates a pool of worker processes (prefork by default)
5. Starts the consumer loop to receive and dispatch tasks

### Step 4: Task Dispatch from Scheduler Side

**File:** `providers/celery/src/.../executors/celery_executor_utils.py` (lines 328-383)

```python
def send_workload_to_executor(workload_tuple):
    key, args, queue, team_name = workload_tuple

    # Reconstruct Celery app (for subprocess isolation)
    celery_app = create_celery_app(_conf)

    # Get the registered task
    task_to_run = celery_app.tasks["execute_workload"]
    args = (args.model_dump_json(),)  # Serialize workload to JSON

    # Send to broker — ROUTED BY QUEUE, NOT HOSTNAME
    result = task_to_run.apply_async(args=args, queue=queue)

    return key, args, result
```

Key insight: Tasks are routed to queues, NOT to specific workers by hostname. The hostname is purely for worker identity, not task routing.

### Step 5: Task Execution on Worker Side

**File:** `providers/celery/src/.../executors/celery_executor_utils.py` (lines 190-226)

```python
@app.task(name="execute_workload")
def execute_workload(input: str) -> None:
    from pydantic import TypeAdapter
    from airflow.executors import workloads
    from airflow.sdk.execution_time.supervisor import supervise

    decoder = TypeAdapter[workloads.All](workloads.All)
    workload = decoder.validate_json(input)

    celery_task_id = app.current_task.request.id
    log.info("[%s] Executing workload in Celery: %s", celery_task_id, workload)

    # ... supervise() call for ExecuteTask workloads
```

---

## 5. Root Cause Analysis

### Difference Between Working and Non-Working Paths

| Aspect | Without `--celery-hostname` | With `--celery-hostname` |
|--------|---------------------------|--------------------------|
| Options passed to `worker_main` | `["worker", "-O", "fair", "--queues", "default", "--concurrency", 1, "--loglevel", "INFO"]` | Same + `["--hostname", "myworker@%h"]` |
| Worker node name | `celery@MACHINE` (Celery default) | `myworker@MACHINE` (from `--hostname`) |
| Task execution | Works | Stuck in RESERVED |

### Theory 1: Celery `-O fair` + `--hostname` Interaction (Most Likely)

The `-O fair` optimization flag changes Celery's task scheduling strategy. With `-O fair`:
- The consumer does NOT eagerly prefetch tasks to pool workers
- Instead, it waits for a pool worker to signal availability
- Only then does it dispatch the next reserved task

This "fair" strategy relies on internal communication between the consumer and the pool. The `--hostname` option changes the worker's identity, which could affect:
1. The internal mailbox addressing between consumer and pool workers
2. The QoS (Quality of Service) bucket assignment
3. The internal routing of control messages

This is a known class of bugs in Celery where the fair scheduling mechanism can break when the worker's identity is modified. The consumer thinks it has dispatched the task, but the pool worker never receives it because the internal addressing is wrong.

**Evidence:** The symptoms (reserved, not acknowledged, no worker_pid) perfectly match a fair-scheduling dispatch failure.

### Theory 2: The `args.concurrency` Type Issue

Looking at the options list:
```python
options = [
    "worker",
    "-O", "fair",
    "--queues", args.queues,       # str
    "--concurrency", args.concurrency,  # int! (type=int in Arg definition)
    "--loglevel", celery_log_level,     # str
]
```

`args.concurrency` is defined with `type=int` (line 77-81 of `definition.py`), so it's an `int` in the options list. Celery's `worker_main()` calls `sys.argv`-style argument parsing. Most CLI parsers expect strings.

When `--hostname` is NOT in the list, Celery might still work because the `int` happens to be in a position that doesn't cause issues. When `--hostname` IS added, the positions shift, and the `int` type might cause the argument parser to misinterpret subsequent options.

However, the existing tests pass an `int(concurrency)` and assert it works, so this may be a non-issue if Celery handles it gracefully.

### Theory 3: Task Registry Mismatch

When `execute_workload` is registered:
```python
@app.task(name="execute_workload")
def execute_workload(input: str) -> None: ...
```

It's bound to the module-level `app`. The task is registered under the name `execute_workload` in `app.tasks`.

When a worker starts, it uses this same `app` instance. The worker's consumer receives a message with `"type": "execute_workload"` and looks it up in `app.tasks`. This should always work regardless of hostname.

**This theory is unlikely** — the task IS being reserved, which means the consumer found it in the registry.

### Theory 4: Celery Library Bug with `worker_main()` + `--hostname`

The `worker_main()` method is an alternative to running `celery worker` from the command line. It might have subtle differences in how it processes the `--hostname` argument compared to the CLI entry point.

In particular, `worker_main()` might not properly initialize the hostname expansion (`%h` → actual hostname) in all internal components, leaving some components with the unexpanded format string.

---

## 6. How to Reproduce

### Prerequisites

```bash
# Install Airflow with Celery executor
uv pip install "apache-airflow[celery]==3.1.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt"

# Start Redis (or RabbitMQ) as the broker
docker run -d --name redis -p 6379:6379 redis:7

# Configure Airflow
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
export AIRFLOW__CELERY__RESULT_BACKEND=db+sqlite:///airflow.db
```

### Reproducing the Bug

```bash
# Terminal 1: Start scheduler
airflow scheduler

# Terminal 2: Start worker WITH --celery-hostname (BROKEN)
airflow celery worker --queues default --concurrency 1 --celery-hostname "myworker@%h"

# Terminal 3: Trigger a DAG run
airflow dags trigger example_dag

# Terminal 4: Inspect — task should be stuck in reserved
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
# Expected: Task shows with acknowledged=False, worker_pid=None

# Compare: Start worker WITHOUT --celery-hostname (WORKS)
# (Stop the broken worker first)
airflow celery worker --queues default --concurrency 1
# Trigger another DAG run — task should execute immediately
```

### Using Breeze (Recommended for This Repo)

```bash
# Start Breeze with Celery executor and Redis
breeze --backend postgres --executor CeleryExecutor

# Inside Breeze, test with hostname
airflow celery worker --queues default --concurrency 1 --celery-hostname "test@%h"

# In another Breeze shell
airflow dags trigger example_bash_operator

# Check task status
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active
```

---

## 7. How to Explore Further

### Debugging Approach 1: Celery Logging

Increase Celery's internal logging to see the consumer/pool communication:

```bash
airflow celery worker --queues default --concurrency 1 \
  --celery-hostname "test@%h" --loglevel DEBUG
```

Look for:
- `"Task received"` vs `"Task accepted"` messages
- Any errors about pool communication
- Consumer strategy messages

### Debugging Approach 2: Add Instrumentation

Add debug logging to the worker startup in `celery_command.py`:

```python
# Before celery_app.worker_main(options)
log.info("Starting worker with options: %s", options)
log.info("Registered tasks: %s", list(celery_app.tasks.keys()))
log.info("App name: %s", celery_app.main)
```

### Debugging Approach 3: Test with Celery Directly (Bypass Airflow CLI)

```python
# test_celery_hostname.py — place in dev/ for Breeze
from celery import Celery

app = Celery('test', broker='redis://localhost:6379/0')

@app.task(name="test_task")
def test_task(x):
    print(f"Executing task with x={x}")
    return x * 2

if __name__ == '__main__':
    # Test 1: Without hostname
    app.worker_main(["worker", "-O", "fair", "--concurrency", "1", "--queues", "default"])

    # Test 2: With hostname — does this also break?
    # app.worker_main(["worker", "-O", "fair", "--concurrency", "1", "--queues", "default",
    #                   "--hostname", "test@%h"])
```

If this breaks too, it's a Celery library bug. If not, it's specific to how Airflow constructs the options.

### Debugging Approach 4: Binary Elimination

Test different combinations to isolate the cause:

```bash
# 1. Just hostname, no -O fair
# (Modify celery_command.py temporarily: remove "-O", "fair" from options)

# 2. Hostname with string concurrency
# (Change args.concurrency to str(args.concurrency) in options)

# 3. Hostname without task_acks_late
# (Set task_acks_late=False in airflow.cfg)

# 4. Different pool types
# (Set pool=solo in [celery] config)
```

### Debugging Approach 5: Check Celery Version

The issue was reported with Celery provider 3.14.1 which uses `celery[redis]>=5.5.0,<6`.
Check if newer Celery versions fix this:

```bash
pip show celery  # Check current version
# Try with a specific version
pip install "celery[redis]==5.5.1"
```

### Key Files to Read in Celery's Source

- `celery/worker/worker.py` — Worker startup, hostname initialization
- `celery/worker/consumer/consumer.py` — Consumer loop, task dispatching
- `celery/worker/strategy.py` — The `-O fair` scheduling strategy
- `celery/app/base.py` — `worker_main()` method

---

## 8. Potential Fix Directions

### Fix 1: Ensure `--concurrency` is a String

The simplest check — ensure all options are strings:

```python
# In celery_command.py, line 280
"--concurrency",
str(args.concurrency),  # Explicitly convert to string
```

### Fix 2: Remove `-O fair` When `--hostname` is Used

If `-O fair` is the root cause of the interaction:

```python
options = ["worker"]
if not args.celery_hostname:
    options.extend(["-O", "fair"])
# ... rest of options
```

This is a workaround, not a real fix. The `-O fair` flag is important for preventing task starvation.

### Fix 3: Set Hostname via Celery Config Instead of CLI

Instead of passing `--hostname` via CLI options to `worker_main()`, set it in the Celery app configuration:

```python
if args.celery_hostname:
    celery_app.conf.update(worker_hostname=args.celery_hostname)
# Don't add --hostname to options list
```

### Fix 4: Use `celery_app.Worker` Directly

Instead of `worker_main(options)` which does CLI-style parsing, create the worker programmatically:

```python
from celery.bin.worker import worker as celery_worker_cmd

worker = celery_worker_cmd(app=celery_app)
worker.run(
    hostname=args.celery_hostname,
    optimization="fair",
    queues=args.queues,
    concurrency=args.concurrency,
    loglevel=celery_log_level,
)
```

This avoids the CLI argument parsing entirely and passes parameters directly.

### Fix 5: Report Upstream to Celery

If the issue is confirmed to be in Celery's `worker_main()` + `--hostname` interaction, file a bug with the Celery project at https://github.com/celery/celery/issues.

---

## Appendix: Key Code References

### The Options Construction (celery_command.py:273-301)

```python
options = [
    "worker",
    "-O",
    "fair",
    "--queues",
    args.queues,
    "--concurrency",
    args.concurrency,    # int type from argparse
    "--loglevel",
    celery_log_level,
]
if args.celery_hostname:
    options.extend(["--hostname", args.celery_hostname])
if autoscale:
    options.extend(["--autoscale", autoscale])
if args.without_mingle:
    options.append("--without-mingle")
if args.without_gossip:
    options.append("--without-gossip")

if config.has_option("celery", "pool"):
    pool = config.get("celery", "pool")
    options.extend(["--pool", pool])
    maybe_patch_concurrency(["-P", pool])
```

### The Default Celery Configuration (default_celery.py:106-129)

```python
config = {
    "accept_content": ["json"],
    "event_serializer": "json",
    "worker_prefetch_multiplier": 1,
    "task_acks_late": True,
    "task_default_queue": "default",
    "task_default_exchange": "default",
    "task_track_started": True,
    "broker_url": broker_url,
    "result_backend": result_backend,
    "worker_concurrency": 16,
    "worker_enable_remote_control": True,
}
```

### The Task Registration (celery_executor_utils.py:190)

```python
@app.task(name="execute_workload")
def execute_workload(input: str) -> None:
    # Deserializes workload JSON and calls supervise()
    ...
```
