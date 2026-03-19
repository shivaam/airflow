# ECS Callback Error Logs

Raw error logs captured during testing of deadline callback execution on ECS executor.

## Test Setup

- DAG: `test_deadline_callback` with 30s deadline, `SyncCallback`
- Executor: `AwsEcsExecutor` (team_alpha, cluster: alpha-cluster)
- Bundle: `team_alpha_dags` (S3: `s3://airflow-ecs-dags-741443349243-us-west-2/team_alpha/`)

---

## Error 1: Inline function — mangled module name

Using `SyncCallback(deadline_missed_alert)` where the function is defined inside the DAG file.

### ECS Container Log

```json
{
  "timestamp": "2026-03-15T01:12:17.612104Z",
  "level": "info",
  "event": "Executing callback workload",
  "callback_id": "019cef0b-0712-7751-a7f7-dd672a696779",
  "logger": "__main__",
  "filename": "execute_workload.py",
  "lineno": 56
}
```

```json
{
  "timestamp": "2026-03-15T01:12:17.628213Z",
  "level": "critical",
  "event": "Unhandled exception",
  "logger": "unhandled_exception",
  "filename": "structlog.py",
  "lineno": 636,
  "exception": [
    {
      "exc_type": "RuntimeError",
      "exc_value": "Callback execution failed: ModuleNotFoundError: No module named 'unusual_prefix_6538b1c2f34b1c4cff42468481908b4ed3350019_test_deadline_callback'",
      "frames": [
        {"filename": "<frozen runpy>", "lineno": 198, "name": "_run_module_as_main"},
        {"filename": "<frozen runpy>", "lineno": 88, "name": "_run_code"},
        {"filename": "/opt/airflow/task-sdk/src/airflow/sdk/execution_time/execute_workload.py", "lineno": 135, "name": "<module>"},
        {"filename": "/opt/airflow/task-sdk/src/airflow/sdk/execution_time/execute_workload.py", "lineno": 131, "name": "main"},
        {"filename": "/opt/airflow/task-sdk/src/airflow/sdk/execution_time/execute_workload.py", "lineno": 59, "name": "execute_workload"}
      ]
    }
  ]
}
```

### Scheduler Log

```
2026-03-15T01:10:36.347363Z [info] Received executor event with state running for callback 019cef0b-0712-7751-a7f7-dd672a696779
2026-03-15T01:10:36.354375Z [info] Callback 019cef0b-0712-7751-a7f7-dd672a696779 is currently running
2026-03-15T01:12:34.117620Z [info] Received executor event with state running for callback 019cef0b-0712-7751-a7f7-dd672a696779
2026-03-15T01:12:34.120857Z [info] Callback 019cef0b-0712-7751-a7f7-dd672a696779 is currently running
```

---

## Error 2: Separate module — bundle path not on sys.path

Using `SyncCallback("deadline_callback_fn.deadline_missed_alert")` with `deadline_callback_fn.py`
uploaded alongside the DAG to the same S3 bundle prefix.

### ECS Container Log

```json
{
  "timestamp": "2026-03-15T01:42:45.050829Z",
  "level": "info",
  "event": "Executing callback workload",
  "callback_id": "019cef27-023d-7e38-953c-2889bb50c96f",
  "logger": "__main__",
  "filename": "execute_workload.py",
  "lineno": 56
}
```

```json
{
  "timestamp": "2026-03-15T01:42:45.064497Z",
  "level": "error",
  "event": "Callback deadline_callback_fn.deadline_missed_alert({...}) execution failed: Callback execution failed: ModuleNotFoundError: No module named 'deadline_callback_fn'",
  "logger": "__main__",
  "filename": "callback.py",
  "lineno": 163,
  "exception": [
    {
      "exc_type": "ModuleNotFoundError",
      "exc_value": "No module named 'deadline_callback_fn'",
      "frames": [
        {"filename": "/opt/airflow/airflow-core/src/airflow/executors/workloads/callback.py", "lineno": 140, "name": "execute_callback_workload"},
        {"filename": "/usr/python/lib/python3.12/importlib/__init__.py", "lineno": 90, "name": "import_module"},
        {"filename": "<frozen importlib._bootstrap>", "lineno": 1387, "name": "_gcd_import"},
        {"filename": "<frozen importlib._bootstrap>", "lineno": 1360, "name": "_find_and_load"},
        {"filename": "<frozen importlib._bootstrap>", "lineno": 1324, "name": "_find_and_load_unlocked"}
      ]
    }
  ]
}
```

```json
{
  "timestamp": "2026-03-15T01:42:45.065003Z",
  "level": "critical",
  "event": "Unhandled exception",
  "logger": "unhandled_exception",
  "filename": "structlog.py",
  "lineno": 636,
  "exception": [
    {
      "exc_type": "RuntimeError",
      "exc_value": "Callback execution failed: ModuleNotFoundError: No module named 'deadline_callback_fn'",
      "frames": [
        {"filename": "<frozen runpy>", "lineno": 198, "name": "_run_module_as_main"},
        {"filename": "<frozen runpy>", "lineno": 88, "name": "_run_code"},
        {"filename": "/opt/airflow/task-sdk/src/airflow/sdk/execution_time/execute_workload.py", "lineno": 135, "name": "<module>"},
        {"filename": "/opt/airflow/task-sdk/src/airflow/sdk/execution_time/execute_workload.py", "lineno": 131, "name": "main"},
        {"filename": "/opt/airflow/task-sdk/src/airflow/sdk/execution_time/execute_workload.py", "lineno": 59, "name": "execute_workload"}
      ]
    }
  ]
}
```

### Callback Context (from error log)

The callback received full DAG run context, confirming the scheduler correctly built and
dispatched the workload:

```json
{
  "dag_run": {
    "dag_run_id": "manual__2026-03-15T01:40:36.991835+00:00",
    "dag_id": "test_deadline_callback",
    "logical_date": "2026-03-15T01:40:35Z",
    "queued_at": "2026-03-15T01:40:37.002512Z",
    "start_date": "2026-03-15T01:40:37.240155Z",
    "state": "running",
    "triggered_by": "ui",
    "dag_versions": [
      {
        "id": "019cef26-5603-7b0d-b528-210c63bbd0cb",
        "version_number": 13,
        "dag_id": "test_deadline_callback",
        "bundle_name": "team_alpha_dags",
        "bundle_version": null,
        "bundle_url": "https://airflow-ecs-dags-741443349243-us-west-2.s3.amazonaws.com/team_alpha"
      }
    ]
  },
  "deadline": {
    "id": "019cef27-023f-7970-9e89-2080d45c3ede",
    "deadline_time": "2026-03-15T01:41:07.002512Z"
  }
}
```

---

## Scheduler Logs — Successful Deadline Detection

These logs confirm the scheduler correctly detected the missed deadline and dispatched
the callback to ECS. The problem is entirely on the container side.

### Task queued and running on ECS

```
2026-03-15T01:10:03.956680Z [info] Trying to enqueue tasks: [<TaskInstance: test_deadline_callback.slow_task manual__2026-03-15T01:10:03.205133+00:00 [scheduled]>] for executor: AwsEcsExecutor(parallelism=32, team_name='team_alpha')
2026-03-15T01:10:05.008952Z [info] Received executor event with state running for task instance TaskInstanceKey(dag_id='test_deadline_callback', task_id='slow_task', run_id='manual__2026-03-15T01:10:03.205133+00:00', try_number=1, map_index=-1)
2026-03-15T01:10:05.041686Z [info] Setting external_executor_id for <TaskInstance: test_deadline_callback.slow_task manual__2026-03-15T01:10:03.205133+00:00 [queued]> to arn:aws:ecs:us-west-2:741443349243:task/alpha-cluster/3a1959223f2c42728261fc781124c93e
```

### Deadline detected, callback dispatched (~30s after queued_at)

```
2026-03-15T01:10:36.347363Z [info] Received executor event with state running for callback 019cef0b-0712-7751-a7f7-dd672a696779
2026-03-15T01:10:36.354375Z [info] Callback 019cef0b-0712-7751-a7f7-dd672a696779 is currently running
```

### Task completed successfully (slow_task ran for ~125s as expected)

```
2026-03-15T01:12:34.117620Z [info] Received executor event with state running for callback 019cef0b-0712-7751-a7f7-dd672a696779
2026-03-15T01:12:34.120857Z [info] Callback 019cef0b-0712-7751-a7f7-dd672a696779 is currently running
```

---

## DAG Processor Logs — Bundle Sync

Confirms both files were picked up by the S3 bundle:

```
2026-03-15T01:17:44.486358Z [info] Refreshing bundle team_alpha_dags
2026-03-15T01:17:44.518331Z [info] Searching for files in team_alpha_dags at /tmp/airflow/dag_bundles/team_alpha_dags
2026-03-15T01:17:44.520536Z [info] Found 2 files for bundle team_alpha_dags
```
