# AthenaSQLHook Parameter Handling Fix

## Overview
Fix the `AthenaSQLHook` to properly handle Athena-specific parameters (`s3_staging_dir`, `work_group`, etc.) when they are passed via `hook_params` from SQL operators. Before the fix, these parameters caused a `TypeError` because they were passed to the parent `AwsGenericHook.__init__()` which doesn't accept them. The fix filters kwargs using an allowlist of params accepted by `AwsGenericHook.__init__()`.

## Problem Statement
When using `SQLValueCheckOperator` (or any SQL operator) with an Athena connection, the operator crashes with:
```
TypeError: AwsGenericHook.__init__() got an unexpected keyword argument 's3_staging_dir'
```

This happens because:
1. `BaseSQLOperator.get_hook()` copies all `connection.extra_dejson` fields to `hook_params`
2. These params include Athena-specific fields like `s3_staging_dir`, `work_group`
3. `AthenaSQLHook.__init__()` passes all kwargs to `super().__init__()`
4. `AwsGenericHook.__init__()` only accepts specific AWS parameters and rejects Athena-specific ones

## References
- GitHub Issue: https://github.com/apache/airflow/issues/55678
- Affected versions: apache-airflow-providers-amazon>=9.11.0, apache-airflow-providers-common-sql>=1.27.0

---

## User Stories

### User Story 1: SQL Operator with Athena Connection
As a data engineer, I want to use standard SQL operators (like `SQLValueCheckOperator`, `SQLExecuteQueryOperator`) with my Athena connection so that I can validate data and run queries without writing custom code.

#### Acceptance Criteria
- AC 1.1: `SQLValueCheckOperator` works with Athena connections that have `s3_staging_dir` in extras
- AC 1.2: `SQLExecuteQueryOperator` works with Athena connections that have `work_group` in extras
- AC 1.3: All Athena-specific parameters in connection extras are handled gracefully
- AC 1.4: The hook still functions correctly for direct instantiation

### User Story 2: Backward Compatibility
As a developer maintaining existing Airflow DAGs, I want the fix to be backward compatible so that my existing code continues to work without modifications.

#### Acceptance Criteria
- AC 2.1: Direct instantiation of `AthenaSQLHook` with explicit parameters still works
- AC 2.2: Existing connection configurations continue to work
- AC 2.3: AWS-generic parameters (`region_name`, `aws_conn_id`, etc.) are still passed to parent class correctly

### User Story 3: Parameter Priority
As a user, I want to be able to override connection-level parameters with operator-level `hook_params` so that I have flexibility in my DAG configurations.

#### Acceptance Criteria
- AC 3.1: Parameters explicitly passed to the hook take precedence over connection extras
- AC 3.2: Connection extras are used as defaults when not explicitly overridden

---

## Athena-Specific Parameters (examples of what gets filtered)

The fix uses an allowlist approach — only `AwsGenericHook.__init__()` params (`aws_conn_id`, `verify`, `region_name`, `client_type`, `resource_type`, `config`) are forwarded. Everything else is filtered out automatically, including but not limited to:

| Parameter | Description | Used By |
|-----------|-------------|---------|
| `s3_staging_dir` | S3 location for query results | PyAthena |
| `work_group` | Athena workgroup name | PyAthena |
| `driver` | PyAthena driver type (rest/jdbc) | AthenaSQLHook |
| `aws_domain` | AWS domain (amazonaws.com) | AthenaSQLHook |
| `catalog_name` | Athena catalog name | PyAthena |
| `poll_interval` | Query polling interval | PyAthena |
| `encryption_option` | Result encryption option | PyAthena |
| `kms_key` | KMS key for encryption | PyAthena |
| `result_reuse_enable` | Enable result reuse | PyAthena |
| `result_reuse_minutes` | Result reuse TTL | PyAthena |

These params are not lost — they remain accessible via `self.conn.extra_dejson` in `get_conn()` and `get_uri()`, which is where they are actually used.

## Reproduction Test Case (from issue)

```python
def test_athena_hook_fail():
    """Test to reproduce the Athena hook issue with s3_staging_dir parameter."""
    from unittest.mock import patch
    from airflow.models.connection import Connection
    from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator

    # Mock Athena connection with s3_staging_dir in extra
    athena_conn = Connection(
        conn_id="athena_conn",
        conn_type="athena",
        description="Connection to a Athena API",
        schema="athena_sql_schema1",
        extra={"s3_staging_dir": "s3://mybucket/athena/", "region_name": "eu-west-1"},
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=athena_conn):
        # This should NOT raise TypeError
        operator = SQLValueCheckOperator(
            task_id="value_check", 
            sql="SELECT TRUE", 
            pass_value=True, 
            conn_id="athena_conn"
        )
        context = {"ds": "2024-01-01", "execution_date": None}
        operator.execute(context)
```
