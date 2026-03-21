# GlueJobOperator Deferred Mode Fix — Verified Results

**Issue:** [GitHub Discussion #63706](https://github.com/apache/airflow/discussions/63706)
**Branch:** `claude/gluejob-deferred-status-sxBgJ`
**Test Date:** 2026-03-21

## Problem

When `GlueJobOperator(deferrable=True)` runs a Glue job that fails, the task loses all error details and reports only the generic message `"Trigger failure"`. The `on_failure_callback` receives a `TaskDeferralError("Trigger failure")` with no information about what actually went wrong in the Glue job. `execute_complete()` is never called.

This makes it impossible to route meaningful alerts (Slack, PagerDuty, etc.) from the failure callback.

## Fix

Two changes in `providers/amazon/`:

1. **`triggers/base.py`** — `AwsBaseWaiterTrigger.run()` now wraps `async_wait()` in a try/except. When `async_wait()` raises `AirflowException` on a terminal failure state, `run()` catches it and yields `TriggerEvent(status="error", message=str(e))` instead of letting the exception propagate uncaught to the triggerer framework.

2. **`triggers/glue.py`** — `GlueJobCompleteTrigger.status_queries` now includes `"JobRun.ErrorMessage"` alongside `"JobRun.JobRunState"`, so the actual Glue error text is extracted from the AWS response and included in the failure message.

## Verified Test Run

**DAG:** `test_glue_deferred_failure_real`
**DAG Run:** `manual__2026-03-21T05:53:31.802557+00:00`
**Task:** `glue_deferred_fail`
**Duration:** 00:00:52.806
**Environment:** EC2 with IAM Role (`AirflowInfra-Ec2Role2FD9A272-ATIGsfneLBzO`)

### Trigger Event (from triggerer logs)

```
Trigger fired event result=TriggerEvent<{
  'status': 'error',
  'message': 'AWS Glue job failed.: FAILED - RuntimeError: GLUE_TEST: Intentional failure
    to test error propagation in deferrable mode\nWaiter job_complete failed: Waiter
    encountered a terminal failure state: For expression "JobRun.JobRunState" we matched
    expected path: "FAILED"',
  'run_id': 'jr_a485d84c34f952e1c8d9d3199455d2d6eda07529034ceb7e1b554514d367f417'
}>
```

**Key observations:**
- Trigger yielded a `TriggerEvent` (not an uncaught exception)
- `status` is `"error"` (not missing)
- `message` contains both the `JobRunState` (`FAILED`) and the `ErrorMessage` (`RuntimeError: GLUE_TEST: Intentional failure...`)
- `run_id` is preserved in the event

### execute_complete() Was Called

```
File ".../providers/amazon/src/airflow/providers/amazon/aws/operators/glue.py", line 345
  in execute_complete
```

Before the fix, `execute_complete()` was **never called** — the triggerer framework intercepted the uncaught exception and short-circuited to `TaskDeferralError("Trigger failure")`.

### Task Failure Exception

```
AirflowException: Error in glue job: {
  'run_id': 'jr_a485d84c34f952e1c8d9d3199455d2d6eda07529034ceb7e1b554514d367f417',
  'status': 'error',
  'message': 'AWS Glue job failed.: FAILED - RuntimeError: GLUE_TEST: Intentional failure
    to test error propagation in deferrable mode\nWaiter job_complete failed: Waiter
    encountered a terminal failure state: For expression "JobRun.JobRunState" we matched
    expected path: "FAILED"'
}
```

### on_failure_callback Output

```
GLUE DEFERRED FAILURE TEST — on_failure_callback
Task: glue_deferred_fail
Exception type: AirflowException
Exception message: Error in glue job: {'run_id': 'jr_a485d84c...', 'status': 'error',
  'message': 'AWS Glue job failed.: FAILED - RuntimeError: GLUE_TEST: Intentional failure
  to test error propagation in deferrable mode...'}
----------------------------------------------------------------------
RESULT: FIX WORKING — error message contains actual Glue details
```

**Key observations:**
- Exception type is `AirflowException` (not `TaskDeferralError`)
- Exception message contains the actual Glue error text
- Callback can parse `run_id`, `status`, and `message` for alerting

## Before vs After Comparison

| Aspect | Before (bug) | After (fix) |
|--------|-------------|-------------|
| Exception type in callback | `TaskDeferralError` | `AirflowException` |
| Exception message | `"Trigger failure"` | `"Error in glue job: {'status': 'error', 'message': 'AWS Glue job failed.: FAILED - RuntimeError: ...'}"` |
| `execute_complete()` called | No | Yes |
| Glue error text available | No | Yes — includes `JobRunState` and `ErrorMessage` |
| `run_id` available in callback | No | Yes — in the event payload |
| Useful for alerting | No | Yes |

## Full Log Timeline

```
22:53:41  Found credentials from IAM Role
22:53:41  Status of AWS Glue job is: RUNNING
22:53:52  Status of AWS Glue job is: RUNNING
22:54:02  Status of AWS Glue job is: RUNNING
22:54:12  Status of AWS Glue job is: RUNNING
22:54:22  Trigger fired event — TriggerEvent with status="error" and full message
22:54:22  trigger completed
22:54:25  execute_complete() called → raises AirflowException with full details
22:54:25  on_failure_callback fires with actual Glue error
22:54:25  RESULT: FIX WORKING
```
