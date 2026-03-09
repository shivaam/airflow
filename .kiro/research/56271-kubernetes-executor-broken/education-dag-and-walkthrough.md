# Issue #56271 — Educational Walkthrough

This doc is for someone with a few months of Airflow experience who wants to understand
what this bug is about, why it happens, and how the internals work.

---

## Quick Jargon Refresher

**DAG (Directed Acyclic Graph):** Your workflow definition — a Python file that describes
tasks and their dependencies. "Acyclic" means no circular dependencies (Task A can't depend
on Task B if Task B depends on Task A).

**Task / Operator:** A single unit of work in a DAG. An "operator" is the class you use to
define it (BashOperator, PythonOperator, etc.). A "task" is a specific instance of an
operator in your DAG.

**Task Instance (TI):** A specific run of a task for a specific date. If your DAG runs daily,
each day produces a new task instance for each task.

**Executor:** The mechanism Airflow uses to actually *run* your tasks. Think of it as the
"backend" that decides where and how tasks execute. Common ones:
- `LocalExecutor` — runs tasks as local processes on the same machine
- `CeleryExecutor` — distributes tasks to Celery workers (separate machines)
- `KubernetesExecutor` — spins up a Kubernetes pod for each task

**DAG Processor:** A background process that continuously reads your DAG files, parses them,
validates them, and stores the results in the metadata database. This is how Airflow "discovers"
your DAGs. It runs on the scheduler side.

**Worker Pod:** When using KubernetesExecutor, each task runs in its own Kubernetes pod. This
pod is a fresh, isolated container that starts up, runs your task, and shuts down.

**Pod Template:** A YAML template that defines what the worker pod looks like — what container
image to use, what environment variables to set, resource limits, etc.

**DagBag:** An internal Airflow class that loads and parses DAG files. When Airflow needs to
know what DAGs exist, it creates a DagBag, points it at your DAG files, and it parses them.

**Serialization:** Airflow parses your DAG Python files and stores a JSON representation in
the database. This way, the scheduler and UI don't need to re-execute your Python code — they
read the serialized version.

---

## The Educational DAG

Here's a DAG that demonstrates the exact scenario that triggers the bug:

```python
"""
educational_executor_dag.py

This DAG demonstrates how Airflow's multi-executor feature works (and where
issue #56271 breaks it). It has four tasks:

  1. extract       — runs via the DEFAULT executor (whatever [core] executor is set to)
  2. transform     — runs via the DEFAULT executor
  3. load_to_s3    — runs via the DEFAULT executor
  4. heavy_ml_task — explicitly asks for KubernetesExecutor (needs its own pod
                     with GPU/more memory)

The idea: most tasks are fine on Celery workers, but one task needs special
resources, so you tell Airflow "run this one in its own Kubernetes pod."
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _transform_data():
    """Simulate a data transformation step."""
    print("Transforming data...")
    # In real life: pandas, spark, dbt, etc.


# ──────────────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="educational_executor_demo",
    description="Shows how task-level executor= works (and triggers #56271)",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["education", "executor", "issue-56271"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    # ── Task 1: Extract ──────────────────────────────────────────────
    # No executor= specified → uses the DEFAULT executor from config.
    # If your config says CeleryExecutor, this runs on a Celery worker.
    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'Extracting data from source system...'",
    )

    # ── Task 2: Transform ────────────────────────────────────────────
    # Also uses the default executor. Nothing special here.
    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform_data,
    )

    # ── Task 3: Load ─────────────────────────────────────────────────
    # Still default executor.
    load_to_s3 = BashOperator(
        task_id="load_to_s3",
        bash_command="echo 'Loading results to S3...'",
    )

    # ── Task 4: Heavy ML Task ────────────────────────────────────────
    # THIS is the interesting one. We explicitly set executor='KubernetesExecutor'.
    #
    # Why? Maybe this task needs:
    #   - A GPU node
    #   - 32GB of RAM
    #   - A special container image with ML libraries
    #
    # By setting executor='KubernetesExecutor', we tell the SCHEDULER:
    #   "Don't send this to a Celery worker. Spin up a dedicated K8s pod for it."
    #
    # ⚠️  THIS IS WHERE BUG #56271 HITS.
    # The scheduler correctly dispatches this to a K8s pod. But when the pod
    # starts up, it re-parses this DAG file to find the task definition.
    # During parsing, Airflow validates that 'KubernetesExecutor' exists in
    # the pod's local config. But the pod only has LocalExecutor configured
    # (because it doesn't need to schedule anything — it just runs the task).
    # So the validation fails and the task crashes.
    heavy_ml_task = BashOperator(
        task_id="heavy_ml_task",
        bash_command="echo 'Running heavy ML training...'",
        executor="KubernetesExecutor",  # <-- triggers the bug in 3.1.0+
    )

    # ── Dependencies ─────────────────────────────────────────────────
    # extract → transform → load_to_s3
    #                     → heavy_ml_task (runs in parallel with load_to_s3)
    extract >> transform >> [load_to_s3, heavy_ml_task]
```

---

## What Happens When This DAG Runs (Step by Step)

Here's the lifecycle, showing exactly where things go wrong:

### Step 1: DAG Processor Parses the File

```
┌─────────────────────────────────────────────────────┐
│  DAG Processor (runs on scheduler machine)          │
│                                                     │
│  Config: AIRFLOW__CORE__EXECUTOR =                  │
│          CeleryExecutor,KubernetesExecutor           │
│                                                     │
│  1. Reads educational_executor_dag.py               │
│  2. Creates a BundleDagBag                          │
│  3. Parses the Python file → finds 4 tasks          │
│  4. Calls _validate_executor_fields()               │
│     - extract: no executor set → skip ✅            │
│     - transform: no executor set → skip ✅          │
│     - load_to_s3: no executor set → skip ✅         │
│     - heavy_ml_task: executor='KubernetesExecutor'  │
│       → looks up in config → FOUND ✅               │
│  5. Serializes DAG to metadata database             │
└─────────────────────────────────────────────────────┘
```

Everything is fine here. The scheduler machine has both executors configured.

### Step 2: Scheduler Creates Task Instances

```
┌─────────────────────────────────────────────────────┐
│  Scheduler                                          │
│                                                     │
│  DAG run triggered for 2025-01-15                   │
│  Creates 4 task instances:                          │
│    - extract        → queue for CeleryExecutor      │
│    - transform      → queue for CeleryExecutor      │
│    - load_to_s3     → queue for CeleryExecutor      │
│    - heavy_ml_task  → queue for KubernetesExecutor  │
└─────────────────────────────────────────────────────┘
```

The scheduler reads the serialized DAG, sees `executor='KubernetesExecutor'` on
`heavy_ml_task`, and routes it to the KubernetesExecutor.

### Step 3: KubernetesExecutor Launches a Worker Pod

```
┌─────────────────────────────────────────────────────┐
│  KubernetesExecutor                                 │
│                                                     │
│  Creates a new pod using the pod template:          │
│    - Image: apache/airflow:3.1.0                    │
│    - Env: AIRFLOW__CORE__EXECUTOR=LocalExecutor     │
│    - Command: run heavy_ml_task                     │
│                                                     │
│  Why LocalExecutor in the pod?                      │
│  The pod only needs to EXECUTE one task.            │
│  It doesn't schedule anything. LocalExecutor is     │
│  the simplest executor that can run a task locally. │
└─────────────────────────────────────────────────────┘
```

### Step 4: Worker Pod Re-Parses the DAG (💥 Bug Hits Here)

```
┌─────────────────────────────────────────────────────┐
│  Worker Pod                                         │
│                                                     │
│  Config: AIRFLOW__CORE__EXECUTOR = LocalExecutor    │
│                                                     │
│  1. Needs to find the task definition to execute it │
│  2. Calls task_runner.parse()                       │
│  3. Creates a BundleDagBag                          │
│  4. Parses educational_executor_dag.py              │
│  5. Calls _validate_executor_fields()               │
│     - heavy_ml_task: executor='KubernetesExecutor'  │
│       → looks up in config → NOT FOUND 💥           │
│       → Pod only knows about LocalExecutor          │
│                                                     │
│  6. UnknownExecutorException raised                 │
│  7. "Dag not found during start up"                 │
│  8. Task FAILS                                      │
└─────────────────────────────────────────────────────┘
```

The worker pod doesn't need to know about KubernetesExecutor. It's already *inside* the
Kubernetes pod — it just needs to run the bash command. But the validation function doesn't
know that. It checks the local config, doesn't find KubernetesExecutor, and blows up.

### Why the Pod Re-Parses the DAG at All

You might wonder: "The scheduler already parsed the DAG. Why does the pod parse it again?"

The worker pod needs the actual Python task object to execute it — the callable, the
parameters, the operator logic. The serialized DAG in the database is a JSON summary used
by the scheduler and UI. The worker needs the real Python code. So it re-parses the DAG
file to get the actual `BashOperator` instance with all its attributes.

---

## The Fix (Conceptually)

The validation is doing the right thing (catching invalid executor names) but in the wrong
place (inside BundleDagBag, which is shared between scheduler and worker).

The fix: move `_validate_executor_fields()` so it only runs during DAG Processor parsing
(Step 1 above), not during worker pod parsing (Step 4). The worker pod doesn't care about
executor names — it just needs to load the task and run it.

```
BEFORE (broken):
  BundleDagBag.process_file()
    → dag.validate()
    → _validate_executor_fields()  ← runs EVERYWHERE (scheduler + worker)

AFTER (fixed):
  BundleDagBag.process_file()
    → dag.validate()
    → (no executor validation here)

  _parse_file() in processor.py    ← runs ONLY on scheduler
    → creates BundleDagBag
    → _validate_executor_fields()  ← validation only where config is complete
```

---

## The Workaround (Until It's Fixed)

In your pod template file, add `KubernetesExecutor` to the executor config:

```yaml
# pod_template.yaml (simplified)
spec:
  containers:
    - name: base
      env:
        - name: AIRFLOW__CORE__EXECUTOR
          # Workaround: include KubernetesExecutor so validation passes
          value: "LocalExecutor,KubernetesExecutor"
          # Ideally this should just be "LocalExecutor" per the docs
```

This makes the validation pass in the worker pod, but it's not ideal — the pod doesn't
actually use KubernetesExecutor for anything. It's just there to satisfy the validator.
