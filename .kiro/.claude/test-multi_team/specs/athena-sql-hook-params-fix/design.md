# Design: AthenaSQLHook Parameter Handling Fix

## Overview
This design addresses the `TypeError` that occurs when SQL operators pass Athena-specific connection parameters to `AthenaSQLHook`. The fix filters out Athena-specific parameters before calling the parent `AwsGenericHook.__init__()`.

## Background: Key Concepts

### What is a Hook in Airflow?
A hook is Airflow's abstraction for connecting to external systems. It is not a "connection" or a "client" directly — it is a managed client factory. It reads connection credentials from Airflow's connection store (via `conn_id`), creates and manages the underlying client (boto3 client, DB connection, etc.), and provides a consistent interface for operators and tasks to interact with external services.

Flow: Connection config (stored in Airflow) → Hook (reads config, builds client) → Operator (uses hook to do work).

### What is DbApiHook?
`DbApiHook` is Airflow's base class for any hook that talks to a system using Python's DB-API 2.0 standard (PEP 249). DB-API is the standard Python interface for relational databases — it defines `connect()`, `cursor()`, `execute()`, `fetchall()`, etc. Every Python database driver (psycopg2 for Postgres, mysql-connector, pyathena, etc.) implements this interface.

`DbApiHook` provides: `get_conn()`, `run()`, `get_records()`, `get_first()`, `get_df()`, `insert_rows()`, `get_uri()`, `get_sqlalchemy_engine()`, `test_connection()`, and more. Any hook extending `DbApiHook` inherits this full SQL database client experience.

### What is PyAthena?
PyAthena is a Python DB-API 2.0 driver for Amazon Athena — analogous to what `psycopg2` is to Postgres or `mysql-connector` is to MySQL. It makes Athena's async REST API look like a regular database connection. When you call `cursor.execute(sql)`, PyAthena internally calls `StartQueryExecution` via boto3, polls `GetQueryExecution` until the query reaches a terminal state, then fetches results via `GetQueryResults`. The caller never sees this complexity.

### AthenaHook vs AthenaSQLHook
- `AthenaHook` (`athena.py`): A thin wrapper around the boto3 Athena client. It talks directly to the AWS Athena API for low-level query lifecycle management — start query, poll status, fetch results, stop query, get S3 output location. Extends `AwsBaseHook` only.
- `AthenaSQLHook` (`athena_sql.py`): A high-level SQL interface built on PyAthena. Extends both `AwsBaseHook` and `DbApiHook`, making Athena behave like a standard SQL database. Works with `SQLExecuteQueryOperator`, SQLAlchemy, pandas `read_sql()`, etc.

The stack for `AthenaSQLHook`:
```
Your code / Airflow operator
        ↓
  AthenaSQLHook (Airflow - wires up connection params)
        ↓
  PyAthena (DB-API driver - translates SQL calls to AWS API calls)
        ↓
  boto3 / AWS Athena API
```

### What is a Connection?
An Airflow Connection is a stored credential/config record with standard fields (`conn_id`, `conn_type`, `host`, `port`, `login`, `password`, `schema`) and an `extra` JSON blob for anything else. For Athena, the `extra` field holds PyAthena-specific params like `s3_staging_dir`, `work_group`, `region_name`, etc.

### What is SQLValueCheckOperator?
A standard Airflow operator for data validation. You give it a SQL query and an expected value, and it checks if the query result matches. It is database-agnostic — it calls `get_hook(conn_id)` to get the right hook based on `conn_type`, runs the SQL, and compares the result. It does not know or care what database is behind it.

### What is hook_params?
When an operator needs a hook, it calls `get_hook()`. That method loads the connection, grabs everything from `extra_dejson`, and puts it into a dict called `hook_params`. These params then get passed as keyword arguments to the hook's constructor. The idea is: "whatever config the user put on the connection, pass it through to the hook." The problem is it is too generous — it passes everything, including params the hook's parent class does not understand.

---

## Technical Context

### Inheritance Chain
```
AthenaSQLHook
    ├── AwsBaseHook
    │       └── AwsGenericHook (accepts: aws_conn_id, verify, region_name, client_type, resource_type, config)
    └── DbApiHook
```

### Unique Dual Inheritance
`AthenaSQLHook` is the only hook in the entire Airflow Amazon provider that inherits from both `AwsBaseHook` and `DbApiHook`. All other AWS hooks (S3Hook, LambdaHook, SageMakerHook, etc. — 40+ hooks) extend only `AwsBaseHook`. The closest analog, `RedshiftSQLHook`, avoids this by extending only `DbApiHook` and creating throwaway `AwsBaseHook` instances when it needs AWS credentials.

### What AwsBaseHook Provides to AthenaSQLHook
The `AwsBaseHook` inheritance is used solely as a credential provider:
- `self.get_session(region_name=...)` — in `get_conn()`, creates a boto3 Session that PyAthena uses to authenticate
- `self.get_credentials(region_name=...)` — in `get_uri()`, gets access key/secret/token for the SQLAlchemy URL
- `self.aws_conn_id` — to find the AWS connection for credentials
- `self._region_name`, `self._config`, `self._verify` — passed to `AwsConnectionWrapper` in `conn_config`

All actual "talk to Athena" work is done by PyAthena. All "behave like a SQL database" work is done by `DbApiHook`. The AWS inheritance is just the glue that says "here are your AWS creds."

### Root Cause
`AwsGenericHook.__init__()` has a strict, explicit signature — no `**kwargs`. It only accepts:
- `aws_conn_id`
- `verify`
- `region_name`
- `client_type`
- `resource_type`
- `config`

When `BaseSQLOperator.get_hook()` copies `connection.extra_dejson` to `hook_params`, Athena-specific params like `s3_staging_dir` get passed through `AthenaSQLHook.__init__()` → `super().__init__()` → `AwsGenericHook.__init__()`, causing the TypeError.

The irony is these params are not even needed in `__init__`. They are already available later via `self.conn.extra_dejson` when `get_conn()` builds the PyAthena connection. The operator passes them redundantly, and the hook does not filter them out before forwarding to the parent.

### Two Bug Triggers
The bug has two triggers:
1. **Automatic (via BaseSQLOperator):** `BaseSQLOperator.get_hook()` copies all `extra_dejson` into `hook_params` and passes them to the hook constructor. This is the common case.
2. **Manual (via hook_params):** A user explicitly passes Athena-specific params via `hook_params` in their operator definition, e.g., `SQLValueCheckOperator(hook_params={"s3_staging_dir": "s3://bucket/"}, ...)`.

### Airflow 3.x Compatibility Note
The `BaseSQLOperator.get_hook()` method has a TODO comment: `# TODO: can be removed once Airflow min version for this provider is 3.0.0 or higher`. This method is a compatibility shim for Airflow 2.x. When the provider drops Airflow 2.x support, this method (and the automatic `extra_dejson` copying) will be removed. However, this only addresses trigger #1. Trigger #2 (manual `hook_params`) would still cause the TypeError. Additionally, even in the current Airflow 3.x codebase (`Connection.get_hook()` in `airflow-core/src/airflow/models/connection.py`), `hook_params` are still spread directly into the hook constructor with no filtering. So the fix in `AthenaSQLHook` remains valuable regardless of Airflow version.

---

## Solution Design

### Chosen Approach: Allowlist Filtering in `__init__`

Filter kwargs to only include what `AwsGenericHook.__init__` actually accepts (allowlist). The set is defined inline in `__init__` since it's only used there.

```python
def __init__(self, athena_conn_id: str = default_conn_name, *args, **kwargs) -> None:
    # AwsGenericHook.__init__() only accepts these kwargs. Connection extras
    # like s3_staging_dir and work_group are not constructor params — they are
    # read later from the connection in get_conn(). BaseSQLOperator.get_hook()
    # passes all connection extras as kwargs, so we must filter them out here.
    _aws_generic_hook_kwargs = {"aws_conn_id", "verify", "region_name", "client_type", "resource_type", "config"}
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in _aws_generic_hook_kwargs}
    super().__init__(*args, **filtered_kwargs)
    self.athena_conn_id = athena_conn_id
```

### Why Allowlist Over Other Approaches

**Allowlist (chosen):**
- `AwsGenericHook`'s signature is stable — it is a core base class with 40+ subclasses, so adding new params is rare and deliberate
- Any new PyAthena/Athena param is automatically filtered without code changes
- The only risk is if `AwsGenericHook` adds a new init param, but that changes much less frequently and would be caught quickly since all 40+ AWS hooks would be affected

**Blocklist (rejected):**
- Requires listing every Athena/PyAthena-specific param
- If PyAthena adds a new connection param (e.g., `result_cache_ttl`), someone must update the list or the same TypeError returns
- Maintenance burden grows with PyAthena's feature set

**Named params in constructor (rejected):**
- Declaring Athena-specific params as named args (e.g., `s3_staging_dir=None, work_group=None`) would absorb known extras, but `BaseSQLOperator.get_hook()` can pass arbitrary user-defined extras from the connection JSON — you can't anticipate all of them with named params
- Would still need `**kwargs` + filtering for unknown extras, putting you back at the same solution

### Design Decisions

1. **Inline set, not module-level constant**: The set is only used in `__init__`, so keeping it local is simpler and avoids polluting the module namespace.

2. **Dict comprehension filter**: Creates a new dict with only valid keys. Non-AWS params are simply discarded since they are already available via `self.conn.extra_dejson` in `get_conn()`.

3. **Allowlist over blocklist**: More future-proof, less maintenance, aligned with the stable `AwsGenericHook` signature.

---

## Alternatives Considered (Full Analysis)

### Alternative 1: Fix in `AthenaSQLHook.__init__()` (Chosen)
Filter kwargs in the hook's constructor before calling `super().__init__()` using an allowlist of params accepted by `AwsGenericHook.__init__()`.

**Pros:**
- Scoped, safe, minimal change
- No breaking changes
- Fixes both bug triggers (automatic and manual)
- Only file changed: `athena_sql.py`
- Future-proof: new PyAthena/Athena extras are automatically filtered

**Cons:**
- Treats the symptom at the leaf hook rather than the root cause
- Other hooks with similar dual-inheritance patterns (if they ever appear) would need the same fix
- Allowlist needs updating if `AwsGenericHook` adds new init params (rare — 40+ subclasses depend on it)

### Alternative 2: Declare Athena-specific params as named constructor args
Absorb known extras by declaring them explicitly: `def __init__(self, ..., s3_staging_dir=None, work_group=None, driver=None, **kwargs)`.

**Pros:**
- Self-documenting — you can see what Athena-specific params exist
- No hardcoded allowlist set

**Cons:**
- `BaseSQLOperator.get_hook()` passes ALL `connection.extra_dejson` as kwargs — this includes arbitrary user-defined keys from the connection JSON, not just known PyAthena params
- Unknown/unexpected extras would still flow through `**kwargs` to `AwsGenericHook.__init__()` and crash
- Would need `**kwargs` + filtering anyway for unknown extras, ending up at the same solution
- Must update constructor signature every time PyAthena adds a new connection param

**Rejected**: Doesn't solve the general case of arbitrary extras.

### Alternative 3: Fix in `BaseSQLOperator.get_hook()`
Stop blindly copying `extra_dejson` into `hook_params` in `providers/common/sql/src/airflow/providers/common/sql/operators/sql.py`.

**Pros:**
- Fixes the root cause for all SQL hooks
- Would prevent similar issues for any future hook

**Cons:**
- Wide blast radius — affects every SQL hook in Airflow (Postgres, MySQL, Snowflake, Trino, etc.)
- Some hooks may rely on receiving `extra_dejson` params in their constructor
- Requires changes to the `common-sql` provider, not just the `amazon` provider
- The method has a TODO to be removed for Airflow 3.0+ anyway

**Rejected**: Too broad, risk of breaking other hooks.

### Alternative 4: Add `**kwargs` to `AwsGenericHook.__init__()`
Make the parent class accept and ignore unknown params.

**Pros:**
- Fixes the issue for AthenaSQLHook and any future hook with similar patterns

**Cons:**
- Silently swallows typos and invalid params for all 40+ AWS hooks (S3Hook, LambdaHook, SageMakerHook, etc.)
- Loses a useful safety net that catches misconfiguration early — the strict signature is intentional
- Could mask bugs in other hooks that accidentally pass wrong params

**Rejected**: Weakens type safety for the entire AWS hook ecosystem.

### Alternative 5: Remove `AwsBaseHook` from `AthenaSQLHook` inheritance
Follow the `RedshiftSQLHook` pattern — extend only `DbApiHook` and create `AwsBaseHook` instances on the fly when credentials are needed.

**Pros:**
- Eliminates the dual-inheritance problem entirely
- Cleaner architecture, consistent with RedshiftSQLHook

**Cons:**
- Larger refactor, breaking change for anyone doing `isinstance(hook, AwsBaseHook)`
- Need to manually manage `aws_conn_id`, `region_name`, `config`, `verify` as instance attributes
- Need to rewrite `conn_config` property
- Much larger scope than the current bug fix

**Rejected for now**: Right long-term direction but too large for a bug fix PR.

### Alternative 6: Blocklist approach (original design — `ATHENA_SPECIFIC_PARAMS`)
Maintain a set of Athena-specific param names to pop from kwargs.

**Pros:**
- Explicit about what is being filtered
- Easy to understand

**Cons:**
- Requires updating the list when PyAthena adds new connection params
- Maintenance burden

**Superseded by allowlist approach**: Same mechanism but more future-proof.

---

## Correctness Properties

### Property 1: Parent Class Receives Only Valid Parameters
**Validates: AC 1.1, AC 1.2, AC 1.3**

For any kwargs passed to `AthenaSQLHook.__init__()`, after filtering, only parameters accepted by `AwsGenericHook.__init__()` should remain.

```
∀ kwargs: AthenaSQLHook.__init__(kwargs) →
    filtered_kwargs ⊆ {aws_conn_id, verify, region_name, client_type, resource_type, config}
```

### Property 2: Backward Compatibility
**Validates: AC 2.1, AC 2.2, AC 2.3**

Direct instantiation with explicit parameters continues to work. AWS-generic parameters are passed to parent correctly.

```
∀ aws_conn_id, region_name:
    hook = AthenaSQLHook(aws_conn_id=aws_conn_id, region_name=region_name)
    hook.aws_conn_id == aws_conn_id ∧ hook._region_name == region_name
```

### Property 3: Connection Extras Still Available
**Validates: AC 1.4, AC 3.1, AC 3.2**

Athena-specific params are not lost — they remain accessible via `self.conn.extra_dejson` in `get_conn()` and `get_uri()`, which is where they are actually used.

---

## Testing Strategy

### Unit Tests

1. **Test hook instantiation with Athena-specific params**
   - Pass `s3_staging_dir`, `work_group` to `__init__`
   - Verify no TypeError raised

2. **Test hook instantiation with mixed params**
   - Pass both Athena-specific and AWS-generic params
   - Verify AWS params reach parent class (e.g., `hook._region_name == "us-east-1"`)
   - Verify Athena params are filtered (no TypeError)

3. **Test backward compatibility**
   - Instantiate hook without extra params
   - Verify existing behavior unchanged

4. **Test with unknown params**
   - Pass completely unknown params (e.g., `foo="bar"`)
   - Verify they are silently filtered (not passed to parent)

### Integration Test (from issue)

```python
def test_sql_operator_with_athena_connection():
    """Verify SQLValueCheckOperator works with Athena connection extras."""
    athena_conn = Connection(
        conn_id="athena_conn",
        conn_type="athena",
        schema="test_schema",
        extra={"s3_staging_dir": "s3://bucket/path/", "region_name": "us-east-1"},
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=athena_conn):
        operator = SQLValueCheckOperator(
            task_id="test", sql="SELECT 1", pass_value=1, conn_id="athena_conn"
        )
        hook = operator.get_db_hook()
        assert isinstance(hook, AthenaSQLHook)
        # Should not raise TypeError
```

---

## Code Reading Guide

To understand this bug, read the files in this order. Each step shows where the problem propagates.

### Step 1: The Trigger — SQL Operator calls get_hook()

**File:** `providers/common/sql/src/airflow/providers/common/sql/operators/sql.py`
**Lines:** 164-178

```python
@classmethod
def get_hook(cls, conn_id: str, hook_params: dict | None = None) -> BaseHook:
    hook_params = hook_params or {}
    connection = BaseHook.get_connection(conn_id)
    conn_params = connection.extra_dejson          # ← Gets ALL connection extras
    for conn_param in conn_params:
        if conn_param not in hook_params:
            hook_params[conn_param] = conn_params[conn_param]  # ← Copies s3_staging_dir, work_group, etc.
    return connection.get_hook(hook_params=hook_params)        # ← Passes them to hook
```

**What to notice:** This blindly copies ALL `extra_dejson` fields to `hook_params`. No filtering. This method has a TODO to be removed once the provider's minimum Airflow version is 3.0.0+.

### Step 2: Connection instantiates the Hook

**File:** `airflow-core/src/airflow/models/connection.py`
**Lines:** 420-442

```python
def get_hook(self, *, hook_params=None):
    hook = ProvidersManager().hooks.get(self.conn_type, None)
    hook_class = import_string(hook.hook_class_name)  # ← AthenaSQLHook
    if hook_params is None:
        hook_params = {}
    return hook_class(**{hook.connection_id_attribute_name: self.conn_id}, **hook_params)
    #                                                                       ^^^^^^^^^^^^
    #                                          s3_staging_dir, work_group passed here!
```

**What to notice:** `hook_params` (containing Athena-specific params) is spread into the constructor. This is the same in both Airflow 2.x and 3.x.

### Step 3: AthenaSQLHook filters kwargs before passing to parent (THE FIX)

**File:** `providers/amazon/src/airflow/providers/amazon/aws/hooks/athena_sql.py`
**Lines:** 69-78

```python
def __init__(self, athena_conn_id: str = default_conn_name, *args, **kwargs) -> None:
    # AwsGenericHook.__init__() only accepts these kwargs. Connection extras
    # like s3_staging_dir and work_group are not constructor params — they are
    # read later from the connection in get_conn(). BaseSQLOperator.get_hook()
    # passes all connection extras as kwargs, so we must filter them out here.
    _aws_generic_hook_kwargs = {"aws_conn_id", "verify", "region_name", "client_type", "resource_type", "config"}
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in _aws_generic_hook_kwargs}
    super().__init__(*args, **filtered_kwargs)
    self.athena_conn_id = athena_conn_id
```

**What to notice:** Only kwargs accepted by `AwsGenericHook.__init__()` are forwarded. Everything else (Athena/PyAthena extras, arbitrary user extras) is silently dropped — they're already available via `self.conn.extra_dejson` in `get_conn()`.

### Step 4: AwsGenericHook rejects unknown params

**File:** `providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py`
**Lines:** 496-513

```python
def __init__(
    self,
    aws_conn_id: str | None = default_conn_name,
    verify: bool | str | None = None,
    region_name: str | None = None,
    client_type: str | None = None,
    resource_type: str | None = None,
    config: Config | dict[str, Any] | None = None,
) -> None:                                          # ← No **kwargs! Only these 6 params accepted
    super().__init__()
    self.aws_conn_id = aws_conn_id
    # ...
```

**What to notice:** This `__init__` has a fixed signature — no `**kwargs`. The strict signature is intentional — it catches typos and misconfiguration for all 40+ AWS hooks. Python raises `TypeError` for any unexpected argument.

### Step 5: Where Athena params ARE used (for context)

**File:** `providers/amazon/src/airflow/providers/amazon/aws/hooks/athena_sql.py`
**Lines:** 180-192 (`get_conn` method)

```python
def get_conn(self) -> AthenaConnection:
    conn_kwargs: dict = {
        "schema_name": conn_params["schema_name"],
        "region_name": conn_params["region_name"],
        "session": self.get_session(region_name=conn_params["region_name"]),
        **self.conn.extra_dejson,  # ← s3_staging_dir, work_group used HERE via connection
    }
    return pyathena.connect(**conn_kwargs)
```

**What to notice:** Athena params are read from `self.conn.extra_dejson` when connecting, NOT from `__init__` kwargs. The params passed to `__init__` are redundant and cause the crash.

### Quick Reference: File Paths

| Step | File | What Happens |
|------|------|--------------|
| 1 | `providers/common/sql/src/airflow/providers/common/sql/operators/sql.py:164-178` | Copies all connection extras to hook_params |
| 2 | `airflow-core/src/airflow/models/connection.py:420-442` | Passes hook_params to hook constructor |
| 3 | `providers/amazon/src/airflow/providers/amazon/aws/hooks/athena_sql.py:69-71` | Passes kwargs to parent (no filtering) |
| 4 | `providers/amazon/src/airflow/providers/amazon/aws/hooks/base_aws.py:496-513` | Rejects unknown params → TypeError |

### Comparison: How RedshiftSQLHook Avoids This

**File:** `providers/amazon/src/airflow/providers/amazon/aws/hooks/redshift_sql.py`

`RedshiftSQLHook` extends only `DbApiHook` — it does NOT extend `AwsBaseHook`. When it needs AWS credentials (for IAM auth), it creates a separate `AwsBaseHook` instance on the fly:

```python
redshift_client = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type="redshift").conn
```

This avoids the dual-inheritance problem entirely. Redshift's approach is: "I'm a SQL hook that uses AWS when needed" rather than "I am an AWS hook and a SQL hook."

### Minimal Reproduction (No AWS needed)

```python
from airflow.providers.amazon.aws.hooks.athena_sql import AthenaSQLHook

# Simulates what happens when SQL operator passes connection extras
hook = AthenaSQLHook(
    athena_conn_id="test",
    s3_staging_dir="s3://bucket/path/",  # ← This causes TypeError
)
```

---

## Impact Analysis

### Files Changed
- `providers/amazon/src/airflow/providers/amazon/aws/hooks/athena_sql.py`

### Risk Assessment
- **Low risk**: Change is isolated to `AthenaSQLHook.__init__`
- **No breaking changes**: Existing code continues to work
- **No API changes**: Public interface unchanged

### Dependencies
- No new dependencies required
- Compatible with existing PyAthena versions
