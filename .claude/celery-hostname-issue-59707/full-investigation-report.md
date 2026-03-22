# Full Investigation Report: `--celery-hostname` Causes Workers to Reserve but Never Execute Tasks

**Issue:** [GitHub #59707](https://github.com/apache/airflow/issues/59707)
**Date:** 2026-03-21 to 2026-03-22
**Branch:** `investigate/celery-hostname-59707` on `shivaam/airflow`
**EC2 Instance:** `i-06b68c955cf91ca55` (AirflowEc2-celery stack)

---

## 1. Executive Summary

When a Celery worker is started with `--celery-hostname`, tasks are received from the broker but **never dispatched to pool workers** — they stay permanently stuck in RESERVED state with `acknowledged=False`, `worker_pid=None`, `time_start=None`.

**Root cause:** Commit `16829d7694` (celery provider 3.14.0) added a duplicate hostname check that calls `celery_app.control.inspect().active_queues()` before `worker_main()`. This `inspect()` call opens Redis connections that register in `kombu.pools` — a **process-global** connection pool keyed by broker URL. When `worker_main()` subsequently forks prefork pool workers, these pre-opened connections are inherited by child processes, breaking the internal IPC that the `-O fair` scheduling strategy depends on.

**Fix:** Use a temporary Celery app for the inspection + reset `kombu.pools` afterward, or remove the check entirely.

---

## 2. Environment

| Component | Version/Detail |
|-----------|---------------|
| Airflow | 3.2.0.dev0 from source |
| Celery Provider | 3.17.1 from source |
| Celery | 5.6.2 |
| Redis | 6.2.20 (localhost:6379) |
| PostgreSQL | RDS (us-west-2) |
| Executor | CeleryExecutor |
| Pool | prefork (default) |
| OS | Amazon Linux 2023, kernel 6.1.163 |
| Instance | t3.large, EC2 in private subnet |

---

## 3. Reproduction Steps

### 3.1 Infrastructure Setup

Deployed a dedicated EC2 stack using `airflow-ec2` CDK with multi-stack suffix support:

```bash
cd ~/workspace/airflow-ec2
make deploy SUFFIX=celery    # Creates AirflowInfra-celery + AirflowEc2-celery
```

This creates an isolated VPC, RDS, S3 buckets, and EC2 instance — no collision with existing stacks. All SSM parameters are namespaced under `/airflow-test-celery/`.

### 3.2 Airflow Setup on EC2

All commands executed via `aws ssm send-command` from local Mac:

1. **Clone from fork** (branch only exists on `shivaam/airflow`):
   ```bash
   git clone https://github.com/shivaam/airflow.git ~/airflow
   cd ~/airflow
   git checkout investigate/celery-hostname-59707
   ```

2. **Run setup-airflow.sh** (~15 min):
   - Installs Python 3.12, uv, pnpm, breeze
   - Creates venv, installs airflow-core + task-sdk + amazon provider
   - Builds UI assets (main + simple auth manager)
   - Writes `airflow.cfg` with LocalExecutor
   - Runs `airflow db migrate`

3. **Install Redis + Celery provider:**
   ```bash
   sudo dnf install -y redis6 && sudo systemctl enable --now redis6
   uv pip install ./providers/celery "celery[redis]"
   ```

4. **Reconfigure for CeleryExecutor:**
   ```python
   # Patch airflow.cfg:
   executor = CeleryExecutor
   [celery]
   broker_url = redis://localhost:6379/0
   result_backend = db+postgresql://<user>:<pass>@<rds-endpoint>:5432/airflow_db
   ```

5. **Restart services + create test DAG:**
   ```bash
   bash /opt/airflow-scripts/airflow-ctl.sh restart
   # Upload simple BashOperator DAG to S3 bundle
   ```

### 3.3 Baseline Test (PASS)

Worker started **without** `--celery-hostname`:

```bash
airflow celery worker --queues default --concurrency 1
```

**Result:** Task executed in 8.15 seconds.

### 3.4 Bug Reproduction (FAIL)

Worker started **with** `--celery-hostname`:

```bash
airflow celery worker --queues default --concurrency 1 --celery-hostname "myworker@%h"
```

**Result:** Task stuck in RESERVED for 60+ seconds, never executed.

---

## 4. Observations & Logs

### 4.1 Baseline (Working) — Full Log

```
Worker node: celery@ip-10-0-2-61.us-west-2.compute.internal
Concurrency: 1 (prefork)
Transport: redis://localhost:6379/0

Connected to redis://localhost:6379/0
mingle: searching for neighbors
mingle: all alone
celery@ip-10-0-2-61.us-west-2.compute.internal ready.

Task execute_workload[36941a4e-77b1-4494-b3f7-628d3c95fd72] received
[36941a4e...] Executing workload in Celery: ...task_id='say_hello'...dag_id='test_celery_hostname'...
Secrets backends loaded for worker
Found credentials from IAM Role: AirflowInfra-celery-Ec2Role2FD9A272-EiqCNmCgJtwZ
Task finished  exit_code=0  final_state=success
Task execute_workload[36941a4e...] succeeded in 8.149s

DB: test_celery_hostname | say_hello | success
```

### 4.2 Bug Reproduction (Stuck) — Full Log

```
Worker node: myworker@ip-10-0-2-61.us-west-2.compute.internal
Concurrency: 1 (prefork)
Transport: redis://localhost:6379/0

Connected to redis://localhost:6379/0
mingle: searching for neighbors
mingle: all alone
myworker@ip-10-0-2-61.us-west-2.compute.internal ready.

Task execute_workload[5c97cd55-b026-4dbe-9805-faa7de4c7e4c] received
<< NO FURTHER OUTPUT — TASK NEVER DISPATCHED TO POOL >>
```

**celery inspect reserved** (after 60s):
```json
{
    "id": "5c97cd55-b026-4dbe-9805-faa7de4c7e4c",
    "name": "execute_workload",
    "hostname": "myworker@ip-10-0-2-61.us-west-2.compute.internal",
    "time_start": null,
    "acknowledged": false,
    "worker_pid": null
}
```

**celery inspect active**: `- empty -`

**DB**: `test_celery_hostname | say_hello | queued` (never progressed)

**Key observation:** Task was received by the consumer (it appears in RESERVED) but never dispatched to a pool worker. With `-O fair`, the consumer waits for a pool worker to signal availability before dispatching. This IPC is broken.

### 4.3 Fix Verified — Full Log

After applying `kombu.pools.reset()` after the `inspect()` call:

```
[DEBUG-59707] CRITICAL: app.amqp._producer_pool is None (GOOD) at worker_main() time.

Worker node: myworker@ip-10-0-2-61.us-west-2.compute.internal
Concurrency: 1 (prefork)

Connected to redis://localhost:6379/0
mingle: searching for neighbors
mingle: all alone
myworker@ip-10-0-2-61.us-west-2.compute.internal ready.

Task execute_workload[b41f1956-047b-4ac7-a7bc-a289cc311e39] received
[b41f1956...] Executing workload in Celery: ...task_id='say_hello'...
Secrets backends loaded for worker
Found credentials from IAM Role: AirflowInfra-celery-Ec2Role2FD9A272-EiqCNmCgJtwZ
Task finished  exit_code=0  final_state=success
Task execute_workload[b41f1956...] succeeded in 2.596s

DB: say_hello | success
```

---

## 5. Root Cause Analysis

### 5.1 The Regression Commit

**Commit `16829d7694`** — "Add duplicate hostname check for Celery workers (#58591)"
**Landed in:** celery provider 3.14.0
**Confirmation:** `git log --oneline providers-celery/3.13.1..providers-celery/3.14.0 -- providers/celery/src/airflow/providers/celery/cli/celery_command.py` shows this is the **only** CLI change between 3.13.1 and 3.14.0.

The added code (lines 222-233 of `celery_command.py`):
```python
if args.celery_hostname:
    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        active_worker_names = list(active_workers.keys())
        if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
            raise SystemExit("Error: A worker with hostname '...' is already running.")
```

### 5.2 Why `inspect()` Breaks `worker_main()`

The `inspect()` call triggers this chain:

1. `celery_app.control.inspect().active_queues()` broadcasts a control message to Redis
2. This requires a connection from `kombu.pools.connections` (a **process-global** dict)
3. If no pool exists for this broker URL, one is **lazily created** with live Redis TCP sockets
4. These pools and sockets are now registered globally in `kombu.pools`

When `worker_main()` runs afterward:
5. It calls `fork()` to create prefork pool workers
6. `fork()` duplicates file descriptors — children inherit the parent's open Redis sockets
7. Parent (consumer) and children (pool workers) now share the same TCP connections
8. Concurrent reads/writes on shared sockets corrupt the Redis protocol stream
9. The `-O fair` IPC (consumer asks "are you free?", pool replies "yes") fails silently
10. Tasks stay in RESERVED because the consumer never gets availability signals

### 5.3 Evidence: State Dumps

**Socket FDs before/after `inspect()`:**
```
BEFORE inspect(): socket FDs = {}           (no sockets open)
AFTER inspect():  socket FDs = {8: 'socket:[360571]', 9: 'socket:[360572]',
                                11: 'socket:[360573]', 12: 'socket:[360176]'}
```

**`_producer_pool` state at `worker_main()` entry:**
```
BUG (no fix):          _producer_pool = <kombu.pools.ProducerPool> (NON-NONE)  → tasks stuck
FIX (kombu.pools.reset): _producer_pool = None                                 → tasks execute
```

### 5.4 Key Discovery: `kombu.pools` Is Global

From `dev/compare_constructors.py` output:
```
=== CRITICAL: Do A and B share the same kombu.pools entry? ===
A.pool id=139693214378960
B.pool id=139693214378960
A.pool is B.pool: True
A.amqp.producer_pool is B.amqp.producer_pool: True
```

**Any Celery app** connecting to the same broker URL shares the same `kombu.pools` entry. This means:
- Using a separate "temp app" for inspection still pollutes the global pools
- The temp app's `inspect()` opens sockets that register under the same broker URL key
- `kombu.pools.reset()` is needed to clear the global state after inspection

### 5.5 Why It Only Happens With `--celery-hostname`

The `inspect()` call is guarded by `if args.celery_hostname:`. Without the flag, the check is skipped entirely — no `inspect()` = no connection pollution = clean fork.

### 5.6 Why `-O fair` Matters

Without `-O fair` (the default "early ack" strategy), Celery eagerly dispatches tasks to pool workers without waiting for availability signals. The IPC corruption doesn't matter because there's no bidirectional communication — tasks are fire-and-forget to the pool.

With `-O fair` (which Airflow uses), the consumer holds tasks in RESERVED until a pool worker signals "I'm ready." This signal goes through the internal pidbox/Redis mailbox, which uses the corrupted shared connections. The signal never arrives → tasks never dispatch.

### 5.7 Open Question: Why Can't We Reproduce With a Standalone Script?

A minimal standalone Celery script with `inspect()` → `worker_main()` → `--hostname` reportedly works fine. This suggests something Airflow-specific amplifies the issue. Possible factors:
- The `config_source=` constructor (vs `broker=`) affects how kombu pools are keyed
- Airflow's module-level app singleton has different lifecycle than a fresh app
- The `DatabaseBackend` (PostgreSQL) as result backend opens additional connections
- The `_serve_logs` and `_run_stale_bundle_cleanup` context managers that wrap `worker_main()` may interact

We confirmed via `compare_constructors.py` that `config_source=` creates different connection objects than `broker=`, which could affect pool keying. This needs further investigation.

---

## 6. Fix Options

### Option A: Remove the check entirely (simplest)

```python
# Delete lines 222-233 entirely
# Pro: Guaranteed fix, no state pollution possible
# Con: Loses the duplicate hostname warning
```

### Option B: Temp app + kombu.pools.reset() (recommended)

```python
if args.celery_hostname:
    from celery import Celery as _TempCelery
    import kombu.pools

    temp_app = _TempCelery(broker=celery_app.conf.broker_url)
    try:
        active_workers = temp_app.control.inspect().active_queues()
        if active_workers:
            celery_hostname = args.celery_hostname
            if any(
                name == celery_hostname or name.endswith(f"@{celery_hostname}")
                for name in active_workers
            ):
                raise SystemExit(
                    f"Error: A worker with hostname '{celery_hostname}' is already running."
                )
    finally:
        temp_app.close()
        kombu.pools.reset()  # CRITICAL: clear global pool state before fork
```

**Pro:** Keeps the duplicate hostname check, cleans up properly
**Con:** `kombu.pools.reset()` is a sledgehammer — clears ALL pools, not just the ones we created

### Option C: Move check inside the worker (after fork)

Run the duplicate check from within a Celery signal handler (`worker_ready`) after the worker has forked and initialized its own connections. Architecturally cleanest but more complex.

---

## 7. Secondary Bug: Duplicate Hostname Detection

The `endswith(f"@{args.celery_hostname}")` check has a bug when the hostname already contains `@`:

```python
# Hostname: "myworker@mymachine"
# Active worker: "myworker@mymachine"
# Check: "myworker@mymachine".endswith("@myworker@mymachine")  → False!
```

Fix: add exact match check alongside the suffix check:
```python
if any(
    name == celery_hostname or name.endswith(f"@{celery_hostname}")
    for name in active_worker_names
):
```

---

## 8. Also Fixed: `args.concurrency` Type

`args.concurrency` is passed as `int` to `worker_main()` which expects strings. While Celery handles this gracefully in practice (it's not the root cause), it's still incorrect:

```python
# Before:
"--concurrency", args.concurrency,    # int
# After:
"--concurrency", str(args.concurrency),  # str
```

---

## 9. Discarded Fix Ideas

| Idea | Why Discarded |
|------|--------------|
| int-to-str conversion only | Correct but unrelated to root cause |
| Remove `-O fair` when hostname set | Workaround, `-O fair` is needed for production fairness |
| Set hostname via Celery config | Doesn't address the inspect() pollution |
| Use programmatic Worker API | Over-engineering, CLI path works fine without inspect() |
| Reset `_producer_pool = None` only | Insufficient — `kombu.pools` still holds open sockets |
| Temp app without `kombu.pools.reset()` | Doesn't work because pools are global (keyed by broker URL) |

---

## 10. Test Results Summary

| Test | Hostname | Fix Applied | Task Result |
|------|----------|-------------|-------------|
| Baseline (no hostname) | `celery@ip-10-0-2-61...` | N/A | **success** in 8.15s |
| Bug repro (hostname, no fix) | `myworker@ip-10-0-2-61...` | None | **stuck RESERVED** 60s+ |
| Fix verified (hostname + pools.reset) | `myworker@ip-10-0-2-61...` | kombu.pools.reset() | **success** in 2.60s |
| Check removed entirely | `myworker@ip-10-0-2-61...` | Lines deleted | **success** |

---

## 11. Investigation Scripts Created

| Script | Purpose | Location |
|--------|---------|----------|
| `dev/compare_constructors.py` | Compare `config_source=` vs `broker=` constructor patterns | airflow repo |
| `dev/fork_fd_tracer.py` | Trace socket FDs before/after fork using `os.register_at_fork()` | airflow repo |
| `dev/inspect_celery_state.py` | Dump full Celery app state (pools, connections, FDs) at 4 checkpoints | airflow repo |
| `dev/test_celery_hostname_standalone.py` | Standalone Celery test to isolate Airflow-specific vs Celery-library bug | airflow repo |

---

## 12. Infrastructure Notes

### airflow-ec2 Multi-Stack Support

Added CDK `suffix` context parameter to allow multiple isolated stacks:
- Stack names: `AirflowInfra-{suffix}`, `AirflowEc2-{suffix}`
- SSM paths: `/airflow-test-{suffix}/*`
- S3 buckets: `airflow-ecs-logs-{suffix}-{account}-{region}`
- ECR repos: `airflow-ecs-worker-{suffix}`
- Backward compatible: no suffix = original behavior

### SSM Send-Command Challenges

| Issue | Solution |
|-------|----------|
| Shell quoting mangled in JSON parameters | Write to temp script, then execute |
| Services die when SSM session exits | Use `systemd-run --user --unit=<name> --remain-after-exit` |
| Airflow not on PATH in SSM context | Always `source /opt/airflow-scripts/env.sh` first |
| Complex multi-line commands | Heredoc to `/tmp/step-X.sh`, then `bash /tmp/step-X.sh` |

---

## 13. Files Changed

| File | Change | Status |
|------|--------|--------|
| `providers/celery/src/.../cli/celery_command.py:280` | `str(args.concurrency)` | Applied |
| `providers/celery/src/.../cli/celery_command.py:222-233` | Fix hostname detection + cleanup | Applied |
| `providers/celery/tests/.../test_celery_command.py:179` | Update assertion | Applied |
| `providers/celery/tests/.../test_celery_command.py` | New regression test class | Applied |
| `providers/celery/newsfragments/59707.bugfix.rst` | Newsfragment | Created |
| `dev/compare_constructors.py` | Constructor comparison script | Created |
| `dev/fork_fd_tracer.py` | Fork FD tracer | Created |
| `dev/inspect_celery_state.py` | Celery state dump | Created |
