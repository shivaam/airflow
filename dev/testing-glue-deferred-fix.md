# Testing: GlueJobOperator Deferred Mode Failure Status Fix

**Issue:** [GitHub Discussion #63706](https://github.com/apache/airflow/discussions/63706)
**Branch:** `claude/gluejob-deferred-status-sxBgJ`

## What Changed

Two files in `providers/amazon/`:

| File | Change |
|------|--------|
| `src/.../aws/triggers/base.py` | `AwsBaseWaiterTrigger.run()` now wraps `async_wait()` in try/except — catches `AirflowException` and yields `TriggerEvent(status="error", message=str(e))` instead of letting the exception propagate uncaught |
| `src/.../aws/triggers/glue.py` | `GlueJobCompleteTrigger.status_queries` now includes `"JobRun.ErrorMessage"` in addition to `"JobRun.JobRunState"` |

## The Bug (Before Fix)

```
GlueJobOperator(deferrable=True)
    → defers to GlueJobCompleteTrigger
        → AwsBaseWaiterTrigger.run() calls async_wait()
            → waiter detects FAILED state
            → async_wait() raises AirflowException("AWS Glue job failed.: FAILED\n...")
        → exception propagates UNCAUGHT out of run()
    → triggerer framework catches it generically
    → task fails with "Trigger failure" — execute_complete() NEVER called
    → on_failure_callback gets TaskDeferralError("Trigger failure") — useless
```

## The Fix (After)

```
GlueJobOperator(deferrable=True)
    → defers to GlueJobCompleteTrigger
        → AwsBaseWaiterTrigger.run() calls async_wait()
            → waiter detects FAILED state
            → async_wait() raises AirflowException("AWS Glue job failed.: FAILED - <ErrorMessage>\n...")
        → run() CATCHES the exception
        → yields TriggerEvent(status="error", message="AWS Glue job failed.: FAILED - <ErrorMessage>")
    → triggerer sends event to scheduler normally
    → execute_complete() IS called with the event
    → raises AirflowException("Error in glue job: {'status': 'error', 'message': '...', 'run_id': '...'}")
    → on_failure_callback gets the actual Glue error
```

---

## Test Plan

### 1. Unit Tests (No AWS Required)

These run locally with mocked AWS calls.

```bash
# Run all trigger tests
uv run --no-sync --project providers/amazon pytest \
  providers/amazon/tests/unit/amazon/aws/triggers/test_base.py \
  providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py \
  -xvs
```

**What the tests verify:**

| Test | File | Verifies |
|------|------|----------|
| `test_run` | `test_base.py` | Success path still yields `TriggerEvent(status="success")` |
| `test_run_error_yields_event` | `test_base.py` | **NEW** — When `async_wait` raises `AirflowException`, `run()` yields `TriggerEvent(status="error", message=...)` instead of propagating the exception |
| `test_wait_job` | `test_glue.py` | Glue trigger success path works |
| `test_wait_job_failed` | `test_glue.py` | **UPDATED** — Glue trigger failure now yields error event (previously expected `pytest.raises(AirflowException)`) |
| `test_status_queries_include_error_message` | `test_glue.py` | **NEW** — Confirms `"JobRun.ErrorMessage"` is in `status_queries` |

**Expected output:** All 23 tests pass.

### 2. Simulation DAGs (Breeze, No AWS Required)

These DAGs use custom triggers that simulate the before/after behavior without needing real AWS credentials. They demonstrate the triggerer framework's behavior.

```bash
# Start Breeze with triggerer enabled
breeze start-airflow --use-airflow-version wheel --python 3.11

# Or if already running:
breeze run airflow triggerer &
```

#### DAG: `test_trigger_before_fix`

Simulates the **bug** — trigger raises exception uncaught.

```bash
breeze run airflow dags trigger test_trigger_before_fix
```

**Expected log output:**
```
BEFORE-FIX CALLBACK
Exception type : TaskDeferralError
Exception message: Trigger failure
----------------------------------------------------------
BUG: Message is 'Trigger failure' — actual Glue error is lost!
execute_complete() was never called.
```

**Key things to verify:**
- `execute_complete` log line does NOT appear
- Callback exception type is `TaskDeferralError` (not `AirflowException`)
- Callback message is generic `"Trigger failure"`

#### DAG: `test_trigger_after_fix`

Simulates the **fix** — trigger catches exception, yields error event.

```bash
breeze run airflow dags trigger test_trigger_after_fix
```

**Expected log output:**
```
execute_complete called with: {'status': 'error', 'message': 'AWS Glue job failed...', 'run_id': 'jr_abc123'}

AFTER-FIX CALLBACK
Exception type : AirflowException
Exception message: Error in glue job: {'status': 'error', 'message': 'AWS Glue job failed.: FAILED - Script failed...'}
----------------------------------------------------------
FIX: Message contains the actual Glue error details!
execute_complete() was called, error routed properly.
```

**Key things to verify:**
- `execute_complete` log line DOES appear
- Callback exception type is `AirflowException` (not `TaskDeferralError`)
- Callback message contains the actual error text (`FAILED`, `Script failed`, etc.)

### 3. Real AWS Test (Requires AWS Credentials)

This tests the actual `GlueJobOperator` with `deferrable=True` against real AWS.

#### Step 1: Create a Failing Glue Job

Create a Glue job that's guaranteed to fail. Use AWS Console or CLI:

```bash
# Create a simple PySpark script that raises an error
cat > /tmp/fail_job.py << 'SCRIPT'
import sys
raise RuntimeError("Intentional failure for testing deferred error propagation")
SCRIPT

# Upload to S3
aws s3 cp /tmp/fail_job.py s3://YOUR-BUCKET/glue-scripts/fail_job.py

# Create the Glue job
aws glue create-job \
  --name test-deferred-failure-job \
  --role YOUR-GLUE-ROLE-ARN \
  --command '{"Name":"glueetl","ScriptLocation":"s3://YOUR-BUCKET/glue-scripts/fail_job.py","PythonVersion":"3"}' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X
```

#### Step 2: Configure Airflow Connection

Ensure `aws_default` connection is set up in Airflow with valid credentials:

```bash
breeze run airflow connections add aws_default \
  --conn-type aws \
  --conn-extra '{"region_name": "us-east-1"}'
```

Or use environment variable:
```bash
export AIRFLOW_CONN_AWS_DEFAULT='aws://?region_name=us-east-1'
```

#### Step 3: Run the Test DAG

```bash
breeze run airflow dags trigger test_glue_deferred_failure_real
```

#### Step 4: Check the Logs

```bash
# Find the task log
breeze run airflow tasks logs test_glue_deferred_failure_real glue_deferred_fail <RUN_ID>
```

**Expected output with the fix applied:**
```
GLUE DEFERRED FAILURE TEST — on_failure_callback
Task: glue_deferred_fail
Exception type: AirflowException
Exception message: Error in glue job: {'status': 'error', 'message': 'AWS Glue job failed.: FAILED - Intentional failure for testing deferred error propagation\nWaiter job_complete failed: ...', 'run_id': 'jr_xxxxx'}
----------------------------------------------------------------------
RESULT: FIX WORKING — error message contains actual Glue details
```

**What would appear WITHOUT the fix (on main branch):**
```
Exception type: TaskDeferralError
Exception message: Trigger failure
----------------------------------------------------------------------
RESULT: BUG — still seeing generic 'Trigger failure'
```

### 4. Running the Full Amazon Provider Test Suite

To verify nothing else is broken:

```bash
# Run all Amazon trigger tests
uv run --no-sync --project providers/amazon pytest \
  providers/amazon/tests/unit/amazon/aws/triggers/ -xvs

# Run Glue-specific operator tests
uv run --no-sync --project providers/amazon pytest \
  providers/amazon/tests/unit/amazon/aws/operators/test_glue.py -xvs

# Run Glue waiter tests
uv run --no-sync --project providers/amazon pytest \
  providers/amazon/tests/unit/amazon/aws/waiters/test_glue.py -xvs

# Run the entire Amazon provider test suite (slower, uses Breeze)
breeze testing providers-tests --test-type "Providers[amazon]"
```

---

## Error Message Format — Before vs After

### Before (bug):
```
Task failed with:
  TaskDeferralError: Trigger failure
```

### After (fix) — with `status_queries=["JobRun.JobRunState"]` only:
```
Task failed with:
  AirflowException: Error in glue job: {
    'status': 'error',
    'message': 'AWS Glue job failed.: FAILED\nWaiter job_complete failed: ...',
    'run_id': 'jr_xxxxx'
  }
```

### After (fix) — with `status_queries=["JobRun.JobRunState", "JobRun.ErrorMessage"]`:
```
Task failed with:
  AirflowException: Error in glue job: {
    'status': 'error',
    'message': 'AWS Glue job failed.: FAILED - Script failed with exit code 1: NameError: name foo is not defined\nWaiter job_complete failed: ...',
    'run_id': 'jr_xxxxx'
  }
```

The second `status_queries` change adds the actual Glue error text (the `ErrorMessage` field from the AWS `GetJobRun` response) to the message.

---

## Scope of the Fix

The base trigger change (`AwsBaseWaiterTrigger.run()`) affects ALL AWS operators that use deferred mode via `AwsBaseWaiterTrigger`. This includes:

- `GlueJobOperator` (the reported issue)
- `BatchOperator`
- `EcsRunTaskOperator`
- `EmrContainerOperator`
- `EmrServerlessStartJobOperator`
- `RedshiftClusterOperator`
- `SageMakerTrainingOperator`
- `NeptuneStopDbClusterOperator`
- `SsmRunCommandCompletedSensor`
- ...and all other `AwsBaseWaiterTrigger` subclasses

All of these operators' `execute_complete` methods already check `status != "success"`, so the error event is handled correctly without any changes to the operators themselves. The only Glue-specific change is adding `"JobRun.ErrorMessage"` to `status_queries`.

---

## Waiter Terminal Failure States (Glue)

From `providers/amazon/src/.../aws/waiters/glue.json`:

| JobRunState | Waiter State | Meaning |
|-------------|-------------|---------|
| `STARTING`  | retry       | Job is starting up |
| `RUNNING`   | retry       | Job is running |
| `STOPPING`  | retry       | Job is stopping |
| `STOPPED`   | **failure** | Job was manually stopped |
| `FAILED`    | **failure** | Job script error |
| `ERROR`     | **failure** | Glue infrastructure error |
| `TIMEOUT`   | **failure** | Job exceeded max runtime |
| `SUCCEEDED` | success     | Job completed OK |

When the waiter hits a failure state, it raises `WaiterError` with "terminal failure" in the reason. `async_wait()` catches this and raises `AirflowException` with the extracted status details.

---

## Files in This Branch

```
# Code changes (the actual fix)
providers/amazon/src/airflow/providers/amazon/aws/triggers/base.py    # try/except in run()
providers/amazon/src/airflow/providers/amazon/aws/triggers/glue.py    # added ErrorMessage query
providers/amazon/tests/unit/amazon/aws/triggers/test_base.py          # new error event test
providers/amazon/tests/unit/amazon/aws/triggers/test_glue.py          # updated failure test

# Test DAGs
dev/dags/test_trigger_before_fix.py           # Simulates bug (no AWS needed)
dev/dags/test_trigger_after_fix.py            # Simulates fix (no AWS needed)
dev/dags/test_glue_deferred_failure_real.py   # Real GlueJobOperator test (needs AWS)

# Documentation
dev/testing-glue-deferred-fix.md              # This file
```
