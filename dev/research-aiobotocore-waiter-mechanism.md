# Research: Aiobotocore/Botocore Async Waiter Mechanism in Airflow Amazon Provider

## Overview

The Airflow Amazon provider uses botocore/aiobotocore custom waiters to poll AWS services for state changes.
This document traces the full async waiter pipeline from JSON definitions to error handling.

---

## 1. Waiter JSON Definitions

Custom waiters live in `providers/amazon/src/airflow/providers/amazon/aws/waiters/*.json`.

**Structure (botocore waiter model v2):**

```json
{
    "version": 2,
    "waiters": {
        "waiter_name": {
            "operation": "APIOperationName",
            "delay": 60,
            "maxAttempts": 75,
            "acceptors": [
                {
                    "matcher": "path|pathAll|pathAny",
                    "argument": "JMESPath.query",
                    "expected": "value",
                    "state": "success|failure|retry"
                }
            ]
        }
    }
}
```

**Glue `job_complete` waiter** (`waiters/glue.json`):
- Operation: `GetJobRun`
- Retry states: `STARTING`, `RUNNING`, `STOPPING`
- Failure states: `STOPPED`, `FAILED`, `ERROR`, `TIMEOUT`
- Success state: `SUCCEEDED`

When an acceptor matches `state: "failure"`, botocore raises `WaiterError` with `"terminal failure"` in the reason string.

**Matcher types:**
- `path` — Single JMESPath value match
- `pathAll` — ALL array elements must match
- `pathAny` — ANY array element matches

---

## 2. hook.get_waiter() — Loading Waiters

**File:** `providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py` (~line 958)

```python
def get_waiter(self, waiter_name, parameters=None, config_overrides=None,
               deferrable=False, client=None):
```

**Flow:**
1. If `deferrable=True`, requires `client` parameter (async aiobotocore session client)
2. Checks `self.waiter_path` for custom waiter JSON
3. If custom waiter exists: loads JSON, applies `config_overrides`, creates `BaseBotoWaiter`
4. If no custom waiter: falls back to `client.get_waiter()` (official SDK waiters)

**BaseBotoWaiter** (`waiters/base_waiter.py`):
- `deferrable=True` → uses `aiobotocore.waiter.create_waiter_with_client()` (async)
- `deferrable=False` → uses `botocore.waiter.create_waiter_with_client()` (sync)

---

## 3. async_wait() — The Core Polling Loop

**File:** `providers/amazon/src/airflow/providers/amazon/aws/utils/waiter_with_logging.py` (line 99)

```python
async def async_wait(waiter, waiter_delay, waiter_max_attempts, args,
                     failure_message, status_message, status_args):
    for attempt in range(waiter_max_attempts):
        if attempt:
            await asyncio.sleep(waiter_delay)
        try:
            await waiter.wait(**args, WaiterConfig={"MaxAttempts": 1})
        except NoCredentialsError as error:
            log.info(str(error))
        except WaiterError as error:
            error_reason = str(error)
            last_response = error.last_response

            if "terminal failure" in error_reason:
                raise AirflowException(
                    f"{failure_message}: {_LazyStatusFormatter(status_args, last_response)}\n{error}"
                )

            if ("An error occurred" in error_reason
                    and isinstance(last_response.get("Error"), dict)
                    and "Code" in last_response.get("Error")):
                raise AirflowException(f"{failure_message}\n{last_response}\n{error}")

            log.info("%s: %s", status_message, _LazyStatusFormatter(status_args, last_response))
        else:
            break
    else:
        raise AirflowException("Waiter error: max attempts reached")
```

**Key design decisions:**
- Overrides `WaiterConfig={"MaxAttempts": 1}` — polls one attempt at a time so Airflow can log intermediate status
- Uses `_LazyStatusFormatter` with JMESPath `status_args` to extract human-readable status from API response
- Three categories of `WaiterError`:
  1. **Terminal failure** → raises immediately with formatted status (failure acceptor matched)
  2. **Service error** → raises immediately with raw response (API returned error)
  3. **Other** → logs status and retries (retry acceptor matched or no match)

**This is why `status_queries` matters for error messages.** When `async_wait` raises on terminal failure, the exception message includes the JMESPath-extracted values. For Glue, adding `"JobRun.ErrorMessage"` to `status_queries` causes the actual error text to appear in the exception.

---

## 4. Async/Await Flow in Deferrable Operators

```
Operator.execute()
  └─ self.defer(trigger=GlueJobCompleteTrigger(...))
       └─ Triggerer picks up deferred task
            └─ trigger.run()
                 └─ hook.get_async_conn() → aiobotocore session
                      └─ hook.get_waiter(deferrable=True, client=async_client)
                           └─ BaseBotoWaiter → aiobotocore.waiter.create_waiter_with_client()
                                └─ async_wait(async_waiter, ...)
                                     └─ await waiter.wait(**args)  # async botocore call
                                          └─ acceptor evaluation
                                               ├─ success → break → yield TriggerEvent(success)
                                               ├─ failure → WaiterError → AirflowException → yield TriggerEvent(error)
                                               └─ retry → log status → asyncio.sleep → next attempt
```

---

## 5. Synchronous Counterpart

There's also a synchronous `wait()` function (line 55) used by non-deferrable operators:

```python
def wait(waiter, waiter_delay, waiter_max_attempts, args,
         failure_message, status_message, status_args):
```

Same logic, but uses `time.sleep()` instead of `await asyncio.sleep()` and calls `waiter.wait()` synchronously.

---

## Files Referenced

1. `providers/amazon/src/airflow/providers/amazon/aws/utils/waiter_with_logging.py` — `async_wait()` / `wait()`
2. `providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py` — `get_waiter()`
3. `providers/amazon/src/airflow/providers/amazon/aws/waiters/base_waiter.py` — `BaseBotoWaiter`
4. `providers/amazon/src/airflow/providers/amazon/aws/waiters/glue.json` — Glue waiter definitions
5. `providers/amazon/tests/unit/amazon/aws/utils/test_waiter_with_logging.py` — waiter tests
