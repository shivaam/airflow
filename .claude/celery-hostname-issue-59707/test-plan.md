# Test Plan: `--celery-hostname` Causes Workers to Reserve but Never Execute Tasks

**Issue:** [GitHub #59707](https://github.com/apache/airflow/issues/59707)
**Date:** 2026-03-21

---

## Table of Contents

1. [Testing Strategy Overview](#1-testing-strategy-overview)
2. [Unit Tests](#2-unit-tests)
3. [Integration Test with Celery Directly](#3-integration-test-with-celery-directly)
4. [Live Environment Test](#4-live-environment-test)
5. [Regression Tests](#5-regression-tests)
6. [Test Matrix](#6-test-matrix)

---

## 1. Testing Strategy Overview

This issue requires a multi-layered testing approach because the bug manifests at runtime during actual Celery worker operation, not during argument parsing or configuration.

| Layer | What It Tests | Can Catch This Bug? |
|-------|--------------|---------------------|
| **Unit tests** | Argument parsing, options construction, type correctness | Partially — can catch type issues and options format |
| **Celery-only integration** | Whether `worker_main()` + `--hostname` breaks task execution in pure Celery | Yes — isolates if this is a Celery library bug |
| **Live Airflow environment** | Full end-to-end: scheduler dispatches task, worker executes it with custom hostname | Yes — definitive proof |
| **Regression tests** | Prevents future breakage after a fix | Prevents regressions |

### Key Question the Tests Must Answer

> Does the bug live in **Airflow's options construction** (e.g., int types, argument ordering) or in **Celery's `worker_main()` handling of `--hostname`**?

The Celery-only integration test (Section 3) answers this decisively.

---

## 2. Unit Tests

These tests verify the correctness of the options list and argument handling. They run fast and don't need a broker or running worker.

### Test 2.1: Options list contains only strings

**Location:** `providers/celery/tests/unit/celery/cli/test_celery_command.py`

**Rationale:** The existing code passes `args.concurrency` as `int` (because `ARG_CONCURRENCY` has `type=int`). Celery's `worker_main()` does CLI-style argument parsing and may not handle non-string elements correctly.

```python
class TestWorkerOptionsFormat:
    """Verify the options list passed to worker_main is well-formed."""

    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_all_options_are_strings(self, mock_celery_app, mock_popen, mock_locations):
        """All elements in the options list should be strings for Celery's arg parser."""
        mock_locations.return_value = ("pid_file", None, None, None)
        args = self.parser.parse_args([
            "celery", "worker",
            "--concurrency", "4",
            "--celery-hostname", "myworker@%h",
            "--queues", "default",
        ])

        celery_command.worker(args)

        options = mock_celery_app.worker_main.call_args[0][0]
        for i, opt in enumerate(options):
            assert isinstance(opt, str), (
                f"Option at index {i} is {type(opt).__name__} ({opt!r}), expected str. "
                f"Full options: {options}"
            )
```

**Expected outcome before fix:** This test will likely FAIL because `args.concurrency` is an `int`.

### Test 2.2: Hostname is passed correctly to worker_main

**Rationale:** Verify that various hostname formats are passed through correctly.

```python
    @pytest.mark.parametrize(
        "hostname_arg,expected_hostname",
        [
            ("myworker@%h", "myworker@%h"),
            ("celery@%h", "celery@%h"),
            ("worker1", "worker1"),
            ("custom-name@my.domain.com", "custom-name@my.domain.com"),
        ],
    )
    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_hostname_formats_passed_correctly(
        self, mock_celery_app, mock_popen, mock_locations,
        hostname_arg, expected_hostname,
    ):
        """Various hostname formats should be passed through to Celery unchanged."""
        mock_locations.return_value = ("pid_file", None, None, None)
        args = self.parser.parse_args([
            "celery", "worker",
            "--concurrency", "1",
            "--celery-hostname", hostname_arg,
            "--queues", "default",
        ])

        celery_command.worker(args)

        options = mock_celery_app.worker_main.call_args[0][0]
        hostname_idx = options.index("--hostname")
        actual_hostname = options[hostname_idx + 1]
        assert actual_hostname == expected_hostname
```

### Test 2.3: Options without hostname do NOT include --hostname

```python
    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_no_hostname_when_not_specified(
        self, mock_celery_app, mock_popen, mock_locations,
    ):
        """When --celery-hostname is not specified, --hostname should not appear in options."""
        mock_locations.return_value = ("pid_file", None, None, None)
        args = self.parser.parse_args([
            "celery", "worker",
            "--concurrency", "1",
            "--queues", "default",
        ])

        celery_command.worker(args)

        options = mock_celery_app.worker_main.call_args[0][0]
        assert "--hostname" not in options
```

### Test 2.4: Duplicate hostname check with format specifiers

**Rationale:** The duplicate check uses `endswith(f"@{args.celery_hostname}")`. If the hostname contains `@` (like `"myworker@%h"`), the check becomes `endswith("@myworker@%h")` which will never match an expanded worker name like `"celery@myworker@machine"`.

```python
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.inspect")
    def test_duplicate_check_with_at_sign_in_hostname(self, mock_inspect):
        """Duplicate check should handle hostnames containing @ correctly."""
        args = self.parser.parse_args([
            "celery", "worker",
            "--celery-hostname", "myworker@mymachine",
        ])

        mock_instance = MagicMock()
        # Worker is already running with the expanded hostname
        mock_instance.active_queues.return_value = {
            "myworker@mymachine": [{"name": "default"}],
        }
        mock_inspect.return_value = mock_instance

        # Should detect the duplicate and raise SystemExit
        with pytest.raises(SystemExit) as exc_info:
            celery_command.worker(args)
        assert "already running" in str(exc_info.value)
```

**Expected outcome:** This test will FAIL with the current code because `endswith("@myworker@mymachine")` won't match `"myworker@mymachine"`. This reveals a secondary bug in the duplicate hostname detection.

### How to Run Unit Tests

```bash
# From repo root — run just the celery CLI tests
uv run --project providers/celery pytest \
    providers/celery/tests/unit/celery/cli/test_celery_command.py -xvs

# Run a specific test class
uv run --project providers/celery pytest \
    providers/celery/tests/unit/celery/cli/test_celery_command.py::TestWorkerOptionsFormat -xvs

# If system dependencies are missing, use Breeze
breeze run pytest \
    providers/celery/tests/unit/celery/cli/test_celery_command.py -xvs
```

---

## 3. Integration Test with Celery Directly

This is the critical test that isolates whether the bug is in Airflow or in Celery. It uses Celery directly without Airflow.

### Test 3.1: Pure Celery — Does `worker_main()` + `--hostname` Break Task Execution?

**Create this script at `dev/test_celery_hostname_standalone.py`:**

```python
"""
Standalone Celery test to isolate whether --hostname breaks task execution.
Run this OUTSIDE of Airflow to determine if the bug is in Celery or Airflow.

Prerequisites:
    pip install celery[redis]
    docker run -d --name redis -p 6379:6379 redis:7

Usage:
    # Terminal 1: Start worker WITHOUT hostname (baseline)
    python dev/test_celery_hostname_standalone.py worker

    # Terminal 1 (alternative): Start worker WITH hostname (test case)
    python dev/test_celery_hostname_standalone.py worker --hostname "myworker@%h"

    # Terminal 2: Send a task
    python dev/test_celery_hostname_standalone.py send

    # Terminal 3: Check status
    python dev/test_celery_hostname_standalone.py inspect
"""
import sys
import time

from celery import Celery

BROKER_URL = "redis://localhost:6379/0"
RESULT_BACKEND = "redis://localhost:6379/1"

app = Celery("test_hostname", broker=BROKER_URL, backend=RESULT_BACKEND)
app.conf.update(
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
)


@app.task(name="test_task")
def test_task(message):
    print(f"TASK EXECUTED: {message}")
    return f"done: {message}"


def start_worker(hostname=None):
    options = [
        "worker",
        "-O", "fair",
        "--queues", "default",
        "--concurrency", "1",       # String — correct
        "--loglevel", "INFO",
    ]
    if hostname:
        options.extend(["--hostname", hostname])
    print(f"Starting worker with options: {options}")
    app.worker_main(options)


def start_worker_with_int_concurrency(hostname=None):
    """Same as start_worker but passes concurrency as int (mimics Airflow bug)."""
    options = [
        "worker",
        "-O", "fair",
        "--queues", "default",
        "--concurrency", 1,         # Int — mimics Airflow's behavior
        "--loglevel", "INFO",
    ]
    if hostname:
        options.extend(["--hostname", hostname])
    print(f"Starting worker with options (int concurrency): {options}")
    app.worker_main(options)


def send_task():
    result = test_task.apply_async(args=["hello from test"], queue="default")
    print(f"Task sent. Task ID: {result.id}")
    print("Waiting for result...")
    try:
        value = result.get(timeout=30)
        print(f"SUCCESS: Task returned: {value}")
    except Exception as e:
        print(f"FAILED: {e}")
        # Check task state
        print(f"Task state: {result.state}")
        print(f"Task info: {result.info}")


def inspect_workers():
    inspect = app.control.inspect()

    print("=== Active Queues ===")
    queues = inspect.active_queues()
    if queues:
        for worker, worker_queues in queues.items():
            print(f"  {worker}: {[q['name'] for q in worker_queues]}")
    else:
        print("  No workers found!")

    print("\n=== Reserved Tasks ===")
    reserved = inspect.reserved()
    if reserved:
        for worker, tasks in reserved.items():
            print(f"  {worker}:")
            for task in tasks:
                print(f"    - {task['name']} (ack={task.get('acknowledged')}, pid={task.get('worker_pid')})")
    else:
        print("  None")

    print("\n=== Active Tasks ===")
    active = inspect.active()
    if active:
        for worker, tasks in active.items():
            print(f"  {worker}: {len(tasks)} task(s)")
    else:
        print("  None")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_celery_hostname_standalone.py [worker|worker-int|send|inspect]")
        print("  worker              — start worker (no hostname)")
        print("  worker --hostname X — start worker with hostname")
        print("  worker-int          — start worker with int concurrency (no hostname)")
        print("  worker-int --hostname X — start worker with int concurrency + hostname")
        print("  send                — send a test task")
        print("  inspect             — inspect worker state")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "worker":
        hostname = None
        if "--hostname" in sys.argv:
            hostname = sys.argv[sys.argv.index("--hostname") + 1]
        start_worker(hostname)

    elif cmd == "worker-int":
        hostname = None
        if "--hostname" in sys.argv:
            hostname = sys.argv[sys.argv.index("--hostname") + 1]
        start_worker_with_int_concurrency(hostname)

    elif cmd == "send":
        send_task()

    elif cmd == "inspect":
        inspect_workers()

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
```

### Test 3.1 Execution Plan

Run these 4 combinations to isolate the root cause:

| # | Concurrency Type | Hostname | Command | Expected |
|---|-----------------|----------|---------|----------|
| A | `str` ("1") | None | `worker` | Works (baseline) |
| B | `str` ("1") | `"myworker@%h"` | `worker --hostname "myworker@%h"` | If fails → Celery library bug |
| C | `int` (1) | None | `worker-int` | If fails → int type is the issue |
| D | `int` (1) | `"myworker@%h"` | `worker-int --hostname "myworker@%h"` | If fails → combination is the issue |

**Procedure for each combination:**

```bash
# Terminal 1: Start worker
python dev/test_celery_hostname_standalone.py <command>

# Terminal 2: Send task
python dev/test_celery_hostname_standalone.py send

# Terminal 3: If task doesn't complete in 10s, inspect
python dev/test_celery_hostname_standalone.py inspect
```

**Decision table based on results:**

| A | B | C | D | Root Cause |
|---|---|---|---|-----------|
| Pass | Fail | Pass | Fail | Celery bug: `worker_main()` + `--hostname` is broken |
| Pass | Pass | Fail | Fail | Airflow bug: `int` concurrency breaks `worker_main()` |
| Pass | Pass | Pass | Fail | Combination bug: int + hostname interaction |
| Pass | Fail | Fail | Fail | Multiple issues |

### How to Run (Breeze)

```bash
# Start Breeze with Redis
breeze --backend postgres

# Inside Breeze container — install standalone test deps (already available)
# Terminal 1
python /opt/airflow/dev/test_celery_hostname_standalone.py worker

# Terminal 2 (open another Breeze shell)
breeze exec bash
python /opt/airflow/dev/test_celery_hostname_standalone.py send
```

---

## 4. Live Environment Test

This is the definitive end-to-end test using Airflow with the CeleryExecutor.

### Prerequisites

```bash
# Option A: Breeze (recommended for development)
breeze --backend postgres

# Option B: Standalone install (matches the reporter's setup)
uv pip install "apache-airflow[celery]" --constraint "..."
docker run -d --name redis -p 6379:6379 redis:7
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
export AIRFLOW__CELERY__RESULT_BACKEND=db+sqlite:///airflow.db
airflow db migrate
```

### Test DAG

**Create at `dev/dags/test_celery_hostname.py`:**

```python
"""
Test DAG to verify task execution with --celery-hostname.

Run with:
    airflow dags trigger test_celery_hostname

Expected: task 'say_hello' completes successfully.
The task prints a message and exits. If the worker is stuck,
the task will remain in 'queued' state indefinitely.
"""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _check_worker_identity():
    import socket
    import os
    print(f"Hostname: {socket.gethostname()}")
    print(f"PID: {os.getpid()}")
    print(f"Task executed successfully!")
    return "success"


with DAG(
    "test_celery_hostname",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task_bash = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Celery worker!' && hostname && date",
        queue="default",
    )

    task_python = PythonOperator(
        task_id="check_identity",
        python_callable=_check_worker_identity,
        queue="default",
    )

    task_bash >> task_python
```

### Test 4.1: Baseline — Worker Without Hostname (Should Pass)

```bash
# Terminal 1: Start scheduler
airflow scheduler

# Terminal 2: Start worker WITHOUT --celery-hostname
airflow celery worker --queues default --concurrency 1

# Terminal 3: Trigger the DAG
airflow dags trigger test_celery_hostname

# Terminal 3: Monitor task state (should go from queued → running → success)
watch -n 2 'airflow tasks states-for-dag-run test_celery_hostname latest'

# Terminal 3: Inspect Celery workers
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active_queues
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active
```

**Expected:** Both tasks complete successfully within ~30 seconds.

### Test 4.2: Bug Reproduction — Worker With Hostname (Should Fail Before Fix)

```bash
# Stop the previous worker (Ctrl+C), then:

# Terminal 2: Start worker WITH --celery-hostname
airflow celery worker --queues default --concurrency 1 --celery-hostname "myworker@%h"

# Terminal 3: Trigger the DAG
airflow dags trigger test_celery_hostname

# Terminal 3: After 30 seconds, check state
airflow tasks states-for-dag-run test_celery_hostname latest
# Expected: Tasks stuck in 'queued' state

# Terminal 3: Inspect reserved tasks
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
# Expected: Tasks show acknowledged=False, worker_pid=None
```

**Expected (before fix):** Tasks remain in `queued`/`reserved` state indefinitely.

### Test 4.3: Verify Fix — Worker With Hostname (Should Pass After Fix)

Same as Test 4.2, but after applying the fix. Both tasks should complete.

### Test 4.4: Additional Hostname Formats

Repeat Test 4.2 with different hostname values:

```bash
# Format 1: Simple name
airflow celery worker --queues default --concurrency 1 --celery-hostname "worker1"

# Format 2: Name with @ and %h
airflow celery worker --queues default --concurrency 1 --celery-hostname "myhost@%h"

# Format 3: Same as default
airflow celery worker --queues default --concurrency 1 --celery-hostname "celery@%h"

# Format 4: Name without @ (just the hostname part)
airflow celery worker --queues default --concurrency 1 --celery-hostname "custom-name"
```

All should execute tasks successfully after the fix.

### Test 4.5: Multi-Worker with Different Hostnames

```bash
# Terminal 2: Worker A
airflow celery worker --queues default --concurrency 1 --celery-hostname "worker-a@%h"

# Terminal 3: Worker B
airflow celery worker --queues default --concurrency 1 --celery-hostname "worker-b@%h"

# Terminal 4: Send multiple tasks
for i in $(seq 1 10); do
    airflow dags trigger test_celery_hostname --run-id "run_$i"
done

# Terminal 4: Verify both workers executed tasks
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active_queues
# Should show both worker-a and worker-b
```

### Collecting Evidence

For each test, capture:

```bash
# 1. Worker options being passed
# (Add temporary logging or check worker startup output)

# 2. Task state over time
airflow tasks states-for-dag-run test_celery_hostname latest

# 3. Celery inspection
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active_queues

# 4. Worker logs
# Check the terminal where the worker is running for errors

# 5. Redis queue contents (if using Redis)
redis-cli LLEN default  # Number of messages in the 'default' queue
redis-cli LRANGE default 0 -1  # Messages in the queue
```

---

## 5. Regression Tests

Once the fix is applied, add these tests to prevent regressions.

### Test 5.1: Worker Options Always Contain Strings

Add to `test_celery_command.py`:

```python
class TestWorkerOptionsTypes:
    """Regression: all options passed to worker_main must be strings."""

    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        "extra_args",
        [
            [],
            ["--celery-hostname", "myworker@%h"],
            ["--celery-hostname", "worker1"],
            ["--autoscale", "10,3"],
            ["--without-mingle"],
            ["--without-gossip"],
            ["--celery-hostname", "w1@%h", "--without-mingle", "--without-gossip"],
        ],
        ids=[
            "no-extra-args",
            "hostname-with-format",
            "hostname-simple",
            "autoscale",
            "without-mingle",
            "without-gossip",
            "all-optional-args",
        ],
    )
    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_options_are_all_strings(
        self, mock_celery_app, mock_popen, mock_locations, extra_args,
    ):
        mock_locations.return_value = ("pid_file", None, None, None)
        args = self.parser.parse_args(
            ["celery", "worker", "--concurrency", "1", "--queues", "default"]
            + extra_args
        )

        celery_command.worker(args)

        options = mock_celery_app.worker_main.call_args[0][0]
        non_str = [(i, type(v).__name__, v) for i, v in enumerate(options) if not isinstance(v, str)]
        assert not non_str, f"Non-string options found: {non_str}"
```

### Test 5.2: Hostname Passed Through End-to-End

```python
    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_hostname_in_worker_main_options(
        self, mock_celery_app, mock_popen, mock_locations,
    ):
        """Hostname should appear as --hostname <value> in the options list."""
        mock_locations.return_value = ("pid_file", None, None, None)
        args = self.parser.parse_args([
            "celery", "worker",
            "--concurrency", "1",
            "--queues", "default",
            "--celery-hostname", "my-worker@%h",
        ])

        celery_command.worker(args)

        options = mock_celery_app.worker_main.call_args[0][0]
        assert "--hostname" in options
        idx = options.index("--hostname")
        assert options[idx + 1] == "my-worker@%h"
```

---

## 6. Test Matrix

### Summary of All Tests

| Test ID | Type | Description | Needs Broker? | Catches Bug? |
|---------|------|-------------|---------------|--------------|
| 2.1 | Unit | Options list contains only strings | No | Likely yes (type issue) |
| 2.2 | Unit | Hostname formats passed correctly | No | No (verification only) |
| 2.3 | Unit | No hostname when not specified | No | No (verification only) |
| 2.4 | Unit | Duplicate check with @ in hostname | No | Yes (secondary bug) |
| 3.1A | Integration | Pure Celery, str concurrency, no hostname | Yes (Redis) | Baseline |
| 3.1B | Integration | Pure Celery, str concurrency, with hostname | Yes (Redis) | Isolates Celery bug |
| 3.1C | Integration | Pure Celery, int concurrency, no hostname | Yes (Redis) | Isolates int issue |
| 3.1D | Integration | Pure Celery, int concurrency, with hostname | Yes (Redis) | Isolates combination |
| 4.1 | Live E2E | Airflow worker, no hostname | Yes (full stack) | Baseline |
| 4.2 | Live E2E | Airflow worker, with hostname | Yes (full stack) | Reproduces bug |
| 4.3 | Live E2E | Airflow worker, with hostname, after fix | Yes (full stack) | Verifies fix |
| 4.4 | Live E2E | Various hostname formats | Yes (full stack) | Edge cases |
| 4.5 | Live E2E | Multiple workers, different hostnames | Yes (full stack) | Real-world scenario |
| 5.1 | Regression | All options are strings (parametrized) | No | Prevents regression |
| 5.2 | Regression | Hostname in options end-to-end | No | Prevents regression |

### Execution Order

1. **Run unit tests first** (fast, no infrastructure needed)
   - Tests 2.1-2.4 — identify type issues and secondary bugs
2. **Run Celery-only integration** (needs Redis only)
   - Tests 3.1A-D — isolate whether bug is Celery or Airflow
3. **Run live tests** (needs full Airflow stack)
   - Tests 4.1-4.2 — reproduce the bug
   - Apply fix
   - Tests 4.3-4.5 — verify the fix
4. **Add regression tests** (run in CI)
   - Tests 5.1-5.2 — prevent future breakage

---

## 7. Updated Root Cause Analysis (2026-03-21)

### What We Discovered

The original theories (int-to-str, Celery library bug) were **not the primary root cause**. Through investigation we identified the actual regression and root cause:

### Regression Source

**Commit `16829d7694`** — "Add duplicate hostname check for Celery workers (#58591)" — landed in **celery provider 3.14.0**. This is confirmed by:
- `git log --oneline providers-celery/3.13.1..providers-celery/3.14.0 -- providers/celery/src/airflow/providers/celery/cli/celery_command.py` shows this is the **only** change to `celery_command.py` between 3.13.1 and 3.14.0
- GitHub issue comments confirm: downgrading to 3.13.1 fixes the issue, and the bug persists through 3.15.2

### Root Cause: `inspect()` Pre-initializes `app.amqp._producer_pool`

The duplicate hostname check (lines 222-236 of `celery_command.py`) calls:
```python
inspect = celery_app.control.inspect()
active_workers = inspect.active_queues()
```

This triggers lazy initialization of `celery_app.amqp._producer_pool` — a `kombu.pools.ProducerPool` that holds a Redis connection. Verified locally:

```
BEFORE inspect(): app.amqp._producer_pool = None
AFTER  inspect(): app.amqp._producer_pool = <kombu.pools.ProducerPool object at 0x...>
```

When `worker_main()` subsequently starts and forks prefork pool workers:
1. Child processes **inherit** the pre-initialized producer pool with its parent-process Redis connection
2. Celery's `_after_fork_cleanup_control` only cleans `control.mailbox.producer_pool`, **not** `app.amqp._producer_pool`
3. The forked pool workers use this stale inherited producer pool for internal pidbox/mailbox communication
4. Consumer-to-pool task dispatch silently fails — consumer receives the task but can't hand it to a pool worker
5. Tasks sit in RESERVED state with `acknowledged=False`, `worker_pid=None`, `time_start=None`

### Why It Only Happens With `--celery-hostname`

The duplicate hostname check is guarded by `if args.celery_hostname:` (line 223). Without `--celery-hostname`, the `inspect()` call never happens, `_producer_pool` stays `None`, and `worker_main()` initializes it fresh in the correct process context.

### Why the int-to-str Fix Didn't Solve It

The `str(args.concurrency)` fix (line 283) is technically correct — CLI args should be strings — but it's not the cause of the stuck tasks. Celery's Click-based parser handles int-to-str coercion. The worker banner showing `concurrency: 1 (prefork)` confirms Celery parsed the concurrency correctly even as an int.

### CRITICAL FINDING: `kombu.pools` Is GLOBAL, Keyed by Broker URL

**Discovered 2026-03-21 via `dev/compare_constructors.py`**:

```
A.pool is B.pool: True
A.amqp.producer_pool is B.amqp.producer_pool: True
```

`kombu.pools` is a **process-global** registry. Its keys are computed from the broker URL + transport_options, NOT from the app identity. This means:

1. **ANY** Celery app connecting to the same broker URL shares the same connection pool and producer pool
2. Creating a "throwaway temp app" for inspection would **still pollute** `kombu.pools` because it connects to the same Redis URL
3. The `inspect()` call opens socket connections that get registered in `kombu.pools` — these sockets are then inherited by forked children via `worker_main()`

**Evidence from EC2 dump**:
```
BEFORE inspect: socket FDs = {}
AFTER inspect:  socket FDs = {8: 'socket:[360571]', 9: 'socket:[360572]', 11: 'socket:[360573]', 12: 'socket:[360176]'}
```

The `inspect()` call opens 4 TCP sockets to Redis. These survive in `kombu.pools` and get inherited by forked pool workers.

### Proposed Fix (Updated)

**Option B is INVALIDATED** — using a temp app doesn't help because `kombu.pools` is keyed by broker URL, not app identity.

**Option A (minimal, correct):** Reset `kombu.pools` after the inspect block:
```python
# After the duplicate hostname check block (after line 236)
import kombu.pools
kombu.pools.reset()
```
This clears ALL global connection and producer pools, ensuring `worker_main()` starts with a clean slate. The pools will be lazily re-created when `worker_main()` needs them.

**Option C (safest, simplest):** Remove the duplicate hostname check entirely. The check was added in #58591 to give a friendlier error message, but it introduced this critical bug. Celery already handles duplicate hostnames by refusing to start.

**Option D (conservative):** Keep the check but add `kombu.pools.reset()` after it + reset `app.amqp._producer_pool = None`:
```python
if args.celery_hostname:
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        ...  # existing check
    # Clean up all global state created by inspect()
    import kombu.pools
    kombu.pools.reset()
    celery_app.amqp._producer_pool = None
```

---

## 8. Verification Plan for Root Cause Confirmation

### Phase 1: Confirm Producer Pool Theory (Current Step)

Debug logging has been added (commit `653d06b15e`) that traces `app.amqp._producer_pool` state at 4 points:

1. **BEFORE duplicate hostname check** — expect `None`
2. **AFTER inspect.active_queues()** — expect `<ProducerPool>` (only with `--celery-hostname`)
3. **AFTER duplicate hostname check block** — expect `<ProducerPool>` still set
4. **JUST BEFORE worker_main()** — expect `<ProducerPool>` still set (the smoking gun)

Plus a CRITICAL log line:
```
[DEBUG-59707] CRITICAL: app.amqp._producer_pool is NON-NONE (BAD!) at worker_main() time.
If non-None, prefork children will inherit a stale producer pool!
```

### Steps to Run

```bash
# 1. Start Breeze with CeleryExecutor
breeze --backend postgres --executor CeleryExecutor

# 2. TEST A: Worker WITH --celery-hostname (expect NON-NONE BAD)
airflow celery worker --queues default --concurrency 1 --celery-hostname "test@%h"
# Look for: [DEBUG-59707] CRITICAL: ... NON-NONE (BAD!)

# 3. In another Breeze shell, trigger a task
airflow dags trigger example_bash_operator

# 4. Check if task is stuck
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect reserved
celery -A airflow.providers.celery.executors.celery_executor_utils.app inspect active

# 5. Stop the worker (Ctrl+C)

# 6. TEST B: Worker WITHOUT --celery-hostname (expect None GOOD)
airflow celery worker --queues default --concurrency 1
# Look for: [DEBUG-59707] CRITICAL: ... None (GOOD)

# 7. Trigger a task again — should execute successfully
airflow dags trigger example_bash_operator --run-id manual_test_2
```

### Expected Log Output

**TEST A (with `--celery-hostname`):**
```
[DEBUG-59707] [BEFORE duplicate hostname check] app.amqp._producer_pool=None | app.pool(limit=10, dirty=0, qsize=10)
[DEBUG-59707] Duplicate hostname check: calling inspect.active_queues() for hostname=test@%h
[DEBUG-59707] [AFTER inspect.active_queues()] app.amqp._producer_pool=<ProducerPool> (limit=10) | app.pool(limit=10, dirty=0, qsize=10)
[DEBUG-59707] [AFTER duplicate hostname check block] app.amqp._producer_pool=<ProducerPool> (limit=10) | ...
[DEBUG-59707] [JUST BEFORE worker_main()] app.amqp._producer_pool=<ProducerPool> (limit=10) | ...
[DEBUG-59707] CRITICAL: app.amqp._producer_pool is NON-NONE (BAD!) at worker_main() time.
```
→ Task gets stuck in RESERVED state.

**TEST B (without `--celery-hostname`):**
```
[DEBUG-59707] [BEFORE duplicate hostname check] app.amqp._producer_pool=None | app.pool(limit=10, dirty=0, qsize=10)
[DEBUG-59707] No --celery-hostname set, skipping duplicate check
[DEBUG-59707] [AFTER duplicate hostname check block] app.amqp._producer_pool=None | ...
[DEBUG-59707] [JUST BEFORE worker_main()] app.amqp._producer_pool=None | ...
[DEBUG-59707] CRITICAL: app.amqp._producer_pool is None (GOOD) at worker_main() time.
```
→ Task executes successfully.

### Phase 2: Confirm Fix Works

After Phase 1 confirms the theory, add the one-liner fix:
```python
# After the duplicate hostname check block
if celery_app.amqp._producer_pool is not None:
    log.info("[DEBUG-59707] Resetting pre-initialized producer pool to prevent stale fork inheritance")
    celery_app.amqp._producer_pool = None
```

Re-run TEST A — tasks should now execute with `--celery-hostname`.

### Phase 3: Full Regression Testing

After the fix is confirmed working:

1. Run existing unit tests:
   ```bash
   uv run --project providers/celery pytest \
       providers/celery/tests/unit/celery/cli/test_celery_command.py -xvs
   ```

2. Run the live E2E tests from Section 4 (Tests 4.1-4.5)

3. Run the pure Celery integration tests from Section 3 (Tests 3.1A-D)

4. Run multi-worker test (Test 4.5) to verify no regressions with multiple workers

### Existing Debug Logs (for reference)

The following `[DEBUG-59707]` logs are already committed across multiple files:

| File | What it logs |
|------|-------------|
| `celery_command.py` | worker_main options, option types, registered tasks, celery config, **producer pool state** |
| `celery_executor.py` | sync() calls, task state transitions (SUCCESS/FAIL/REVOKED/PENDING), revoke_task calls |
| `scheduler_job_runner.py` | Tasks stuck in queued detection, revoke calls from scheduler |
| `celery_executor_utils.py` | BulkStateFetcher backend type, state results for each task |
