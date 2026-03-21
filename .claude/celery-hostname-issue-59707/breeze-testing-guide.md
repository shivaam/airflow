# Breeze Testing Guide: Reproduce and Verify the `--celery-hostname` Fix

**Issue:** [GitHub #59707](https://github.com/apache/airflow/issues/59707)
**Date:** 2026-03-21

---

## Table of Contents

1. [What is Breeze?](#1-what-is-breeze)
2. [How Celery Runs Inside Breeze](#2-how-celery-runs-inside-breeze)
3. [Prerequisites](#3-prerequisites)
4. [Step-by-Step: Reproduce the Bug](#4-step-by-step-reproduce-the-bug)
5. [The Fix](#5-the-fix)
6. [Step-by-Step: Verify the Fix](#6-step-by-step-verify-the-fix)
7. [Running the Standalone Celery Isolation Test](#7-running-the-standalone-celery-isolation-test)
8. [Breeze Command Reference](#8-breeze-command-reference)

---

## 1. What is Breeze?

Breeze is Airflow's **Docker Compose-based development environment**. It's a Python CLI
tool that spins up a fully isolated Airflow stack in containers — database, scheduler,
webserver, triggerer, and optionally Celery workers with a Redis or RabbitMQ broker.

- **Source:** `dev/breeze/` in the repo
- **Docs:** `dev/breeze/doc/README.rst`
- **How it works:** Wraps Docker Compose with convenience commands. The repo's source
  code is **mounted into the container**, so edits you make on the host are immediately
  reflected inside — no rebuild needed.

### Key things to know

- `breeze shell` — drops you into an interactive shell inside the container
- `breeze exec` — opens another shell into an **already running** container
- `breeze start-airflow` — starts the full Airflow stack (scheduler, webserver, etc.)
  automatically using a terminal multiplexer (mprocs or tmux)
- `breeze down` — stops everything

---

## 2. How Celery Runs Inside Breeze

When you start Breeze with `--executor CeleryExecutor` and `--integration celery`, it
brings up:

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Compose Network                │
│                                                         │
│  ┌─────────┐   ┌───────────┐   ┌──────────────────┐    │
│  │  Redis   │   │ Postgres  │   │  Airflow Container│   │
│  │ (broker) │   │   (DB)    │   │                  │    │
│  │ :6379    │   │  :5432    │   │  - Scheduler     │    │
│  └────┬─────┘   └─────┬─────┘   │  - API Server   │    │
│       │               │         │  - Triggerer     │    │
│       │               │         │  - Celery Worker │    │
│       └───────────────┴─────────│  - (your shell)  │    │
│                                 └──────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

| Component | Role in This Test |
|-----------|-------------------|
| **Redis** | Message broker — Celery sends/receives task messages through it |
| **Postgres** | Metadata DB — stores DAG runs, task states |
| **Scheduler** | Picks up triggered DAGs, sends tasks to Celery via Redis |
| **Celery Worker** | Consumes tasks from Redis and executes them — **this is where the bug lives** |
| **Your shell** | Where you trigger DAGs and inspect Celery state |

### Why the source mount matters

The Airflow source code is mounted into the container at `/opt/airflow/`. When you edit
`providers/celery/src/airflow/providers/celery/cli/celery_command.py` on your host machine,
the change is **immediately visible** inside the container. You just restart the worker
process — no Docker rebuild required.

---

## 3. Prerequisites

```bash
# 1. Make sure Docker is running
docker info

# 2. Make sure Breeze is available
breeze --help

# 3. If Breeze isn't installed yet
pip install -e ./dev/breeze
```

---

## 4. Step-by-Step: Reproduce the Bug

### Step 4.1: Start Breeze with Celery

```bash
# From the repo root — start a Breeze shell with Celery integration and Postgres
breeze shell --integration celery --backend postgres
```

This starts Redis + Postgres containers and drops you into a shell inside the Airflow
container. Wait for the prompt to appear.

### Step 4.2: Initialize the database

```bash
# Inside the Breeze container
airflow db migrate
```

### Step 4.3: Start the scheduler (background)

```bash
# Inside the Breeze container — start scheduler in background
airflow scheduler &
```

Wait a few seconds for it to initialize.

### Step 4.4: Start a Celery worker WITH `--celery-hostname`

```bash
# Inside the Breeze container — this is the buggy scenario
airflow celery worker \
  --queues default \
  --concurrency 1 \
  --celery-hostname "myworker@%h" &
```

Wait for the worker to log `celery@... ready` or similar.

### Step 4.5: Trigger a DAG

```bash
# Trigger a built-in example DAG
airflow dags trigger example_bash_operator
```

### Step 4.6: Observe the bug — tasks stuck in reserved

Wait ~15 seconds, then inspect:

```bash
# Check if tasks are stuck in "reserved" (received but not executing)
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
```

**Expected buggy output:**
```
->  myworker@<machine>: OK
    * {'id': '...', 'name': 'execute_workload', ...
       'time_start': None, 'acknowledged': False, 'worker_pid': None}
```

The three `None`/`False` values confirm the task is stuck — the consumer received it but
never dispatched it to a pool worker.

Also check that nothing is actively running:
```bash
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active
```

**Expected:** Empty — no active tasks, because they're stuck in reserved.

And check the Airflow task state:
```bash
airflow tasks states-for-dag-run example_bash_operator latest
```

**Expected:** Task stuck in `queued` state.

### Step 4.7: Confirm baseline works (optional but recommended)

Stop the buggy worker and start one WITHOUT `--celery-hostname`:

```bash
# Kill the background worker
pkill -f "celery worker"

# Start worker without --celery-hostname
airflow celery worker --queues default --concurrency 1 &

# Trigger another DAG run
airflow dags trigger example_bash_operator --run-id baseline_test

# Wait ~15 seconds, then check
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
# Expected: empty — tasks execute immediately

celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active
# Expected: shows actively running task, or empty if already completed

airflow tasks states-for-dag-run example_bash_operator baseline_test
# Expected: Tasks in 'running' or 'success' state
```

This confirms that the worker works fine without `--celery-hostname`.

---

## 5. The Fix

The fix is a **one-line change** in `celery_command.py`.

### The Problem

In `providers/celery/src/airflow/providers/celery/cli/celery_command.py`, line 280,
`args.concurrency` is passed as an `int` to `celery_app.worker_main()`:

```python
# Line 273-283 — CURRENT (BUGGY)
options = [
    "worker",
    "-O",
    "fair",
    "--queues",
    args.queues,          # str
    "--concurrency",
    args.concurrency,     # <-- BUG: this is an int (argparse type=int)
    "--loglevel",
    celery_log_level,     # str
]
```

`worker_main()` is a CLI entry point — it expects `list[str]`, the same way `sys.argv`
is always a list of strings. Passing an `int` is undefined behavior. It happens to work
when `--hostname` is absent, but breaks when `--hostname` shifts argument positions.

### The Change

```diff
--- a/providers/celery/src/airflow/providers/celery/cli/celery_command.py
+++ b/providers/celery/src/airflow/providers/celery/cli/celery_command.py
@@ -277,7 +277,7 @@
         "--queues",
         args.queues,
         "--concurrency",
-        args.concurrency,
+        str(args.concurrency),
         "--loglevel",
         celery_log_level,
     ]
```

That's it. One line. `str(1)` becomes `"1"` — Celery parses it back to an integer
internally, which is how CLI arguments are supposed to work.

### How to Apply

**Option A: Edit on the host** (reflected immediately inside Breeze via mount):
```bash
# On your host machine, edit the file
# Change line 280 from:
#     args.concurrency,
# To:
#     str(args.concurrency),
```

**Option B: Edit inside the container:**
```bash
# Inside Breeze
vi /opt/airflow/providers/celery/src/airflow/providers/celery/cli/celery_command.py
# Go to line 280, change args.concurrency to str(args.concurrency)
```

---

## 6. Step-by-Step: Verify the Fix

### Step 6.1: Apply the fix

Edit `celery_command.py` line 280 as described in Section 5.

### Step 6.2: Kill the old worker

```bash
# Inside Breeze
pkill -f "celery worker"
```

### Step 6.3: Start worker WITH `--celery-hostname` (should now work)

```bash
airflow celery worker \
  --queues default \
  --concurrency 1 \
  --celery-hostname "myworker@%h" &
```

### Step 6.4: Trigger a DAG

```bash
airflow dags trigger example_bash_operator --run-id fix_test_1
```

### Step 6.5: Verify tasks execute

```bash
# Wait ~15 seconds

# Should be EMPTY — tasks are no longer stuck
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved

# Should show running task, or empty if completed quickly
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active

# Should show 'running' or 'success'
airflow tasks states-for-dag-run example_bash_operator fix_test_1
```

**Expected after fix:** Tasks execute normally. `reserved` is empty. Tasks reach
`success` state.

### Step 6.6: Test multiple hostname formats

Repeat Steps 6.2-6.5 with different hostname values to confirm none are broken:

```bash
# Format 1: simple name
airflow celery worker --queues default --concurrency 1 --celery-hostname "worker1" &
airflow dags trigger example_bash_operator --run-id fix_test_fmt1

# Format 2: name@host with format specifier
pkill -f "celery worker"
airflow celery worker --queues default --concurrency 1 --celery-hostname "custom@%h" &
airflow dags trigger example_bash_operator --run-id fix_test_fmt2

# Format 3: same as default (celery@%h)
pkill -f "celery worker"
airflow celery worker --queues default --concurrency 1 --celery-hostname "celery@%h" &
airflow dags trigger example_bash_operator --run-id fix_test_fmt3
```

For each, verify tasks reach `success`:
```bash
airflow tasks states-for-dag-run example_bash_operator fix_test_fmt1
airflow tasks states-for-dag-run example_bash_operator fix_test_fmt2
airflow tasks states-for-dag-run example_bash_operator fix_test_fmt3
```

### Step 6.7: Test multiple workers with different hostnames

```bash
pkill -f "celery worker"

# Start two workers with different hostnames
airflow celery worker --queues default --concurrency 1 --celery-hostname "worker-a@%h" &
airflow celery worker --queues default --concurrency 1 --celery-hostname "worker-b@%h" &

# Verify both workers are visible
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active_queues
# Expected: both worker-a and worker-b listed

# Send several tasks
for i in 1 2 3 4 5; do
    airflow dags trigger example_bash_operator --run-id "multi_test_$i"
done

# Wait ~30 seconds, verify all complete
for i in 1 2 3 4 5; do
    airflow tasks states-for-dag-run example_bash_operator "multi_test_$i"
done
# Expected: all tasks in 'success' state
```

---

## 7. Running the Standalone Celery Isolation Test

This test uses Celery directly (no Airflow) to determine whether the bug is in Airflow's
options construction or in Celery's `worker_main()`. Redis is already available inside
Breeze.

### Step 7.1: Create the test script

On your host, create `dev/test_celery_hostname_standalone.py` with the content from the
test plan, or run it inline inside Breeze:

```bash
# Inside Breeze — create a quick inline test
cat > /opt/airflow/dev/test_celery_hostname_standalone.py << 'SCRIPT'
"""Standalone Celery test: does worker_main() + --hostname break execution?"""
import sys
from celery import Celery

app = Celery("test_hostname", broker="redis://redis:6379/0", backend="redis://redis:6379/1")
app.conf.update(
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
)

@app.task(name="test_task")
def test_task(message):
    print(f"EXECUTED: {message}")
    return f"done: {message}"

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if cmd == "worker-str":
        # String concurrency, no hostname (baseline)
        app.worker_main(["worker", "-O", "fair", "--queues", "default",
                         "--concurrency", "1", "--loglevel", "INFO"])

    elif cmd == "worker-str-host":
        # String concurrency + hostname
        app.worker_main(["worker", "-O", "fair", "--queues", "default",
                         "--concurrency", "1", "--loglevel", "INFO",
                         "--hostname", "myworker@%h"])

    elif cmd == "worker-int":
        # Int concurrency, no hostname (mimics Airflow's current code)
        app.worker_main(["worker", "-O", "fair", "--queues", "default",
                         "--concurrency", 1, "--loglevel", "INFO"])

    elif cmd == "worker-int-host":
        # Int concurrency + hostname (mimics the exact bug scenario)
        app.worker_main(["worker", "-O", "fair", "--queues", "default",
                         "--concurrency", 1, "--loglevel", "INFO",
                         "--hostname", "myworker@%h"])

    elif cmd == "send":
        result = test_task.apply_async(args=["hello"], queue="default")
        print(f"Sent task {result.id}, waiting...")
        try:
            val = result.get(timeout=30)
            print(f"SUCCESS: {val}")
        except Exception as e:
            print(f"FAILED: {e} (state={result.state})")

    elif cmd == "inspect":
        insp = app.control.inspect()
        print("Reserved:", insp.reserved())
        print("Active:", insp.active())
        print("Queues:", insp.active_queues())

    else:
        print("Usage: python test_celery_hostname_standalone.py "
              "[worker-str|worker-str-host|worker-int|worker-int-host|send|inspect]")
SCRIPT
```

### Step 7.2: Run the four combinations

Open two Breeze shells (`breeze exec` for the second one).

**Test A — str concurrency, no hostname (baseline):**
```bash
# Shell 1
python /opt/airflow/dev/test_celery_hostname_standalone.py worker-str
# Shell 2
python /opt/airflow/dev/test_celery_hostname_standalone.py send
# Expected: SUCCESS
```

**Test B — str concurrency, with hostname:**
```bash
# Shell 1 (Ctrl+C the previous worker first)
python /opt/airflow/dev/test_celery_hostname_standalone.py worker-str-host
# Shell 2
python /opt/airflow/dev/test_celery_hostname_standalone.py send
# If FAILED → bug is in Celery's worker_main() + --hostname
# If SUCCESS → bug is specific to Airflow's options construction
```

**Test C — int concurrency, no hostname:**
```bash
# Shell 1
python /opt/airflow/dev/test_celery_hostname_standalone.py worker-int
# Shell 2
python /opt/airflow/dev/test_celery_hostname_standalone.py send
# If FAILED → int type alone is the problem
```

**Test D — int concurrency, with hostname (exact bug scenario):**
```bash
# Shell 1
python /opt/airflow/dev/test_celery_hostname_standalone.py worker-int-host
# Shell 2
python /opt/airflow/dev/test_celery_hostname_standalone.py send
# If FAILED → confirms int + hostname combination is the trigger
```

### Decision Table

| A (str, no host) | B (str, host) | C (int, no host) | D (int, host) | Conclusion |
|:-:|:-:|:-:|:-:|---|
| Pass | Pass | Pass | **Fail** | int + hostname interaction — `str()` fix is correct |
| Pass | Pass | **Fail** | **Fail** | int alone breaks it — `str()` fix is correct |
| Pass | **Fail** | Pass | **Fail** | Celery bug with `--hostname` — file upstream issue |
| Pass | **Fail** | **Fail** | **Fail** | Multiple issues — `str()` fix + upstream Celery bug |

---

## 8. Breeze Command Reference

| Command | What It Does |
|---------|-------------|
| `breeze shell --integration celery --backend postgres` | Start an interactive Breeze shell with Redis broker and Postgres DB |
| `breeze exec` | Open another shell into the already-running Breeze container |
| `breeze start-airflow --executor CeleryExecutor` | Start full Airflow stack automatically (scheduler, worker, webserver, etc.) |
| `breeze start-airflow --executor CeleryExecutor --celery-flower` | Same but also starts Flower monitoring dashboard |
| `breeze start-airflow --executor CeleryExecutor --celery-broker rabbitmq` | Use RabbitMQ instead of Redis |
| `breeze down` | Stop all Breeze containers |
| `breeze --help` | Full command reference |

### Inside the Container

| Command | What It Does |
|---------|-------------|
| `airflow db migrate` | Initialize/upgrade the metadata database |
| `airflow scheduler` | Start the scheduler |
| `airflow celery worker` | Start a Celery worker |
| `airflow celery worker --celery-hostname "name@%h"` | Start with custom hostname (the buggy scenario) |
| `airflow dags trigger <dag_id>` | Trigger a DAG run |
| `airflow dags trigger <dag_id> --run-id <id>` | Trigger with a specific run ID |
| `airflow tasks states-for-dag-run <dag_id> <run_id>` | Check task states for a DAG run |
| `celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved` | Show tasks stuck in reserved state |
| `celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active` | Show actively executing tasks |
| `celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active_queues` | Show which workers are subscribed to which queues |
| `pkill -f "celery worker"` | Kill all Celery worker processes |
