# AWS Provider Debugging Improvements — Research & Implementation Plan

## Background

The Apache Airflow project has an initiative called "Debugging Improvements — Airflow 3"
(tracked as a GitHub project board). It stems from the **Airflow Debugging Survey 2024**
which found that:

- **55.2%** of respondents find stack traces challenging
- **41.7%** don't find error messages actionable

The project board tracks ~15 issues across 4 themes:

1. **Error & Message Clarity** — Making error messages, stack traces, and logs understandable
2. **Logging Enhancements** — Expanding coverage and improving debug logging across components
3. **UI/UX Improvements** — Color-coding, event log references
4. **Documentation & Guidance** — Debugging guides and tutorials

### Current Status (as of research date)

| Status     | Count | Examples |
|------------|-------|---------|
| Done       | 6     | Log readability, memory monitoring, UI color, error guide, Airbyte/HTTP debug logging |
| In Progress| 2     | Error messages clarity, debugging guide doc |
| Planning   | 5     | Stack traces configurable, comprehensive traces, logging coverage, debug logging, event log |
| Todo       | 1     | Static checks for DAGs |

---

## Patterns from Merged PRs

### PRs Analyzed

| PR | Title | Pattern |
|----|-------|---------|
| #48829 | "Having the entire stacktrace can be confusing" | Simplified confusing stacktrace in airflow-ctl |
| #35716 | "KubernetesPodTrigger: add exception stack trace in TriggerEvent" | Propagated full traceback from triggerer to operator logs |
| #35423 | "Remove before_log in KPO retry and add traceback when interrupted" | Removed misleading retry logs + added traceback on interrupt |
| #33914 | "Cleanup Docker operator logging" | Fixed triplicate traceback printing |
| #47449 | "Add logging message when task_success_overtime exceeded" | Replaced confusing kill-signal trace with clear warning |
| #14456 | "Fix logging error with task error when JSON logging" | Fixed tracebacks lost under JSON logging |
| #51503 | "Add debug logging for Airbyte" | Added self.log.debug() before/after API calls in Airbyte hook |

### Common Patterns

1. **Remove noise** — Strip Airflow internal frames or duplicate prints that confuse users
2. **Propagate context** — Pass full exception objects (not strings) so tracebacks are available
   where users actually look
3. **Replace with clarity** — When a stacktrace is misleading (e.g., SIGKILL), replace with an
   explanatory message
4. **Fix format loss** — Ensure tracebacks survive serialization (JSON logging, TriggerEvent, etc.)
5. **Add debug logging** — Log before/after every external API call

### Notable Open Discussion

Discussion #20060 proposed hiding Airflow runner internals from operator failure tracebacks.
A maintainer pushed back ("you never know when more stack trace is useful") and suggested a
UI-level hide/show toggle instead.

---

## Two Implementation Paths

### Path 1: Debug Logging

**Goal**: Add `self.log.debug()` calls before and after API calls in AWS provider hooks, following
the Airbyte provider pattern from PR #51503.

**Reference Implementation**: The Airbyte hook (`providers/airbyte/src/airflow/providers/airbyte/hooks/airbyte.py`)
demonstrates the pattern:

```python
# Before API call
self.log.debug("Creating job request..")

# After API call
self.log.debug("Job request successful, response: %s", res.job_response)

# During polling
self.log.debug("Job State: %s. Job Details: %s", state, job)

# Connection setup
self.log.debug(
    "Connection attributes are: host - %s, url - %s, description - %s",
    conn.host, conn.schema, conn.description,
)
```

**Key note**: The Airbyte PR did NOT add tests for debug logging. This is consistent with the
project's approach — debug logs are low-risk and testing them would be brittle.

**Scope of changes**:

| Hook | Gaps Found | Changes Made |
|------|-----------|-------------|
| DynamoDB | Zero debug logging. `write_batch_data` and `describe_import` have no logging. | Added debug logs for table loading, batch write start/complete, describe_import calls & responses |
| RDS | Zero debug logging despite 6 state-checking methods and multiple waiter calls. `_wait_for_state` error message is generic ("Max attempts exceeded"). | Added debug logs for all 6 `describe_*` API calls and state polling with attempt counts |
| SQS | Zero logging or error handling anywhere. | Added debug logs for `create_queue`, `send_message` (sync + async) with MessageId |
| EC2 | Zero debug logging despite state polls and instance lifecycle methods. | Added debug logs for `get_instance`, stop/start/terminate responses, async state checks, `wait_for_state` |
| Lambda | Zero debug logging for `invoke` and `create_function`. | Added debug logs for invoke (function name, type, response status) and create_function (ARN, state) |

### Path 2: Stack Trace & Exception Handling

**Goal**: Fix exception chaining and traceback propagation so users see full context when errors occur.

**Key issues found**:

#### Issue 1: BaseWaiterTrigger loses tracebacks

**File**: `providers/amazon/src/airflow/providers/amazon/aws/triggers/base.py`

The trigger catches `AirflowException` and converts it to a string in the TriggerEvent:
```python
except AirflowException as e:
    yield TriggerEvent({"status": "error", "message": str(e), ...})
```

The operator's `execute_complete` method only gets the string message — the full traceback
is lost. This is the same pattern that PR #35716 fixed for KubernetesPodTrigger.

**Fix**: Include `traceback.format_exc()` in the TriggerEvent payload:
```python
except AirflowException as e:
    yield TriggerEvent({
        "status": "error",
        "message": str(e),
        "traceback": traceback.format_exc(),
        ...
    })
```

#### Issue 2: waiter_with_logging drops exception chains

**File**: `providers/amazon/src/airflow/providers/amazon/aws/utils/waiter_with_logging.py`

All 4 `raise AirflowException(...)` statements in WaiterError handlers use plain `raise`
without `from error`, which breaks Python's exception chaining:

```python
# Before (loses __cause__)
except WaiterError as error:
    raise AirflowException(f"{failure_message}: {error}")

# After (preserves __cause__)
except WaiterError as error:
    raise AirflowException(f"{failure_message}: {error}") from error
```

This affects both `wait()` (sync) and `async_wait()` functions.

#### Issue 3: DynamoDB hook loses tracebacks and uses `raise e`

**File**: `providers/amazon/src/airflow/providers/amazon/aws/hooks/dynamodb.py`

- `write_batch_data`: catches broad `Exception`, converts to string, loses traceback
- `get_import_status`: uses `raise e` instead of bare `raise` (less clean traceback)

**Fix**: Add `from general_error` chaining and use bare `raise`.

#### Issue 4: RDS hook uses `raise e` pattern

**File**: `providers/amazon/src/airflow/providers/amazon/aws/hooks/rds.py`

Two locations use `raise e` instead of bare `raise` in ClientError handlers.

---

## Breaking Change Analysis

### Summary: NO BREAKING CHANGES

Every change was analyzed against existing tests and public API surface.

### Detailed Analysis

#### 1. Debug logging additions (all hooks)
**Breaking?** No. `self.log.debug()` calls are no-ops unless the user explicitly enables DEBUG
level logging. They don't change return values, exception types, or control flow.

#### 2. Return-via-variable refactoring (EC2, SQS, Lambda, RDS)
**Breaking?** No. Patterns like `return self.conn.stop_instances(...)` changed to
`result = self.conn.stop_instances(...); return result` are semantically identical.

#### 3. Exception chaining — `from error` (waiter_with_logging, DynamoDB)
**Breaking?** No. Same exception type (`AirflowException`) and same message string are raised.
The only difference is that `exception.__cause__` is now set, which:
- Does NOT change `str(exception)` output
- Does NOT change `except AirflowException` catch behavior
- Adds additional context when printing full traceback (Python shows "caused by" chain)

All existing tests use `pytest.raises(AirflowException)` with type checks or `match=` regex
on the message, both of which are unaffected.

#### 4. `raise e` → bare `raise` (RDS)
**Breaking?** No. Both re-raise the same exception. Bare `raise` is actually preferred because
it preserves the original traceback frame more cleanly (no extra frame from the `raise e` line).

#### 5. Changed error messages

| Location | Old | New | Breaking? |
|----------|-----|-----|-----------|
| DynamoDB `get_import_status` | `"S3 import into Dynamodb job not found."` | `f"S3 import into Dynamodb job not found. Import ARN: {import_arn}"` | No — old text is a prefix of new. Existing tests only check exception type. |
| RDS `_wait_for_state` | `"Max attempts exceeded"` | `f"Max attempts exceeded ({max_attempts} attempts). Target state: '{target_state}', last observed state: '{state}'"` | No — old text is a prefix. Existing test uses `match="Max attempts exceeded"` (regex search on substring), which still matches. |

**Risk**: Only if someone externally does exact string equality like `str(exc) == "Max attempts exceeded"`.
No Airflow tests do this, and it would be a fragile anti-pattern.

#### 6. New `traceback` key in TriggerEvent payload
**Breaking?** No. Verified by analyzing all downstream consumers:
- `validate_execute_complete_event()` only checks for `None`, doesn't reject unknown keys
- All operators access specific keys (`validated_event["status"]`, `validated_event["value"]`)
- No operator uses `**` unpacking or strict schema validation
- The `TriggerEvent` class uses `payload: Any = None` (Pydantic, no `extra="forbid"`)

### Test Compatibility

| Test File | Tests | Status |
|-----------|-------|--------|
| `test_waiter_with_logging.py` | 10 existing + 3 new | All pass — exception tests use type/substring matching |
| `test_base.py` (triggers) | 3 existing + 1 new | All pass — `"AWS Glue job failed." in payload["message"]` still true |
| `test_rds.py` | All existing + 1 new | All pass — `match="Max attempts exceeded"` regex finds substring |
| `test_dynamodb.py` | All existing | All pass — tests only check exception type, not message |
| `test_sqs.py` | All existing | All pass — no exception message tests |
| `test_ec2.py` | All existing | All pass — no exception message tests |
| `test_lambda_function.py` | All existing | All pass — no exception message tests |

---

## Tests Added

### Exception chaining tests (`test_waiter_with_logging.py`)
- `test_wait_with_failure_preserves_exception_chain` — verifies `__cause__` is set for terminal failure (sync)
- `test_async_wait_with_failure_preserves_exception_chain` — same for async
- `test_wait_with_unknown_failure_preserves_exception_chain` — verifies for "An error occurred" path (sync)

### Traceback in TriggerEvent test (`test_base.py`)
- `test_run_error_includes_traceback` — verifies `"traceback"` key exists and contains `"AirflowException"`

### Enriched error message test (`test_rds.py`)
- `test_wait_for_state_error_includes_context` — verifies target state and last observed state in message

### Tests NOT added (intentional)
- No tests for `self.log.debug()` calls — consistent with Airbyte PR pattern. Debug logging is low-risk
  and testing it creates brittle tests that break on message wording changes.

---

## Files Changed

### Source files (7)
| File | Lines Changed | Category |
|------|--------------|----------|
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/dynamodb.py` | +17/-3 | Debug logging + exception chaining |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/rds.py` | +38/-8 | Debug logging + error messages + bare raise |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/sqs.py` | +15/-2 | Debug logging |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/ec2.py` | +22/-6 | Debug logging |
| `providers/amazon/src/airflow/providers/amazon/aws/hooks/lambda_function.py` | +25/-2 | Debug logging |
| `providers/amazon/src/airflow/providers/amazon/aws/triggers/base.py` | +9/-1 | Traceback in TriggerEvent |
| `providers/amazon/src/airflow/providers/amazon/aws/utils/waiter_with_logging.py` | +4/-4 | Exception chaining |

### Test files (3)
| File | Lines Changed | Category |
|------|--------------|----------|
| `providers/amazon/tests/unit/amazon/aws/utils/test_waiter_with_logging.py` | +77 | Exception chaining tests |
| `providers/amazon/tests/unit/amazon/aws/triggers/test_base.py` | +18 | Traceback field test |
| `providers/amazon/tests/unit/amazon/aws/hooks/test_rds.py` | +9 | Error message context test |

---

## Potential Future Work

1. **More AWS hooks** — The same debug logging pattern should be applied to ECS, EMR, SageMaker,
   S3, Batch, Glue, Step Functions, Redshift, and other high-traffic AWS hooks
2. **Operator-level traceback logging** — When operators receive TriggerEvents with the new
   `traceback` field, they could log the full traceback for visibility
3. **Error codes** — The community is discussing implementing error codes (e.g., `AERR001`) for
   all Airflow exceptions with searchable reference documentation
4. **Configurable stack trace depth** — A proposed `AIRFLOW_STACK_TRACK_DEPTH` config to limit
   trace display, with Rich library integration for color coding
