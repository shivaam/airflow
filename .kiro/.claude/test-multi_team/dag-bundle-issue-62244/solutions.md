# Issue #62244 — Solution Analysis

## Why S3DagBundle Triggers the Bug

`S3DagBundle.view_url_template()` builds an S3 console URL that includes the
AWS region. The region isn't in the bundle config — it comes from the AWS
connection object stored in the database.

```python
# providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py

def view_url_template(self) -> str | None:
    if self.version:
        raise AirflowException("S3 url with version is not supported")
    if hasattr(self, "_view_url_template") and self._view_url_template:
        return self._view_url_template

    # Build: https://<bucket>.s3.<region>.amazonaws.com/<prefix>
    url = f"https://{self.bucket_name}.s3"
    if self.s3_hook.region_name:          # ← needs the hook just for this
        url += f".{self.s3_hook.region_name}"
    url += ".amazonaws.com"
    if self.prefix:
        url += f"/{self.prefix}"
    return url
```

The call chain from `self.s3_hook.region_name`:

```
self.s3_hook                          # lazy property, creates S3Hook on first access
  └─ S3Hook(aws_conn_id="aws_default")
       └─ AwsGenericHook.__init__()
            └─ self.conn_config       # cached property
                 └─ self.get_connection("aws_default")
                      └─ Connection.get_connection_from_secrets()
                           └─ MetastoreBackend.get_connection()
                                └─ session.expunge_all()   ← destroys pending objects
```

The hook is created, the connection is fetched from the DB, and `expunge_all()`
wipes the shared scoped session — including any `DagBundleModel` objects that
`sync_bundles_to_db` had added but not yet committed.

## Where the Fix Should Go

There are two layers where this can be addressed:

### Layer 1: MetastoreBackend (root cause)

`session.expunge_all()` in `metastore.py` is the actual bug. It was meant to
detach the returned Connection/Variable from the session, but it nukes everything.

**Fix**: Replace `expunge_all()` with `expunge(conn)` — only detach the specific
object that was queried.

```python
# airflow-core/src/airflow/secrets/metastore.py

# BEFORE (buggy):
conn = session.scalar(select(Connection).where(...))
session.expunge_all()    # removes ALL objects from session
return conn

# AFTER (fixed):
conn = session.scalar(select(Connection).where(...))
if conn:
    session.expunge(conn)  # only detach this specific object
return conn
```

Same fix needed for `get_variable()`:

```python
# BEFORE:
var_value = session.scalar(select(Variable).where(...))
session.expunge_all()
if var_value:
    return var_value.val
return None

# AFTER:
var_value = session.scalar(select(Variable).where(...))
if var_value:
    session.expunge(var_value)
    return var_value.val
return None
```

This is the minimal, correct fix. It preserves the original intent (detach the
returned object so callers can use it freely) without the collateral damage.

### Layer 2: S3DagBundle (defense in depth, optional)

Even after fixing MetastoreBackend, the S3DagBundle design is fragile — it
triggers a DB connection lookup from inside a method that's called during a
DB transaction. There are several ways to make it more resilient:

**Option A: Skip region in URL (simplest)**

S3 URLs work without the region segment. `https://my-bucket.s3.amazonaws.com/prefix`
is valid and redirects to the correct region.

```python
def view_url_template(self) -> str | None:
    if self.version:
        raise AirflowException("S3 url with version is not supported")
    if hasattr(self, "_view_url_template") and self._view_url_template:
        return self._view_url_template
    url = f"https://{self.bucket_name}.s3.amazonaws.com"
    if self.prefix:
        url += f"/{self.prefix}"
    return url
```

Pro: No hook needed at all. No DB access. No possible session interference.
Con: URL doesn't include region (minor cosmetic difference, still works).

**Option B: Accept region as a config kwarg**

Let users pass `region_name` in the bundle config instead of resolving it from
the connection.

```python
def __init__(self, *, region_name=None, **kwargs):
    ...
    self._region_name = region_name

def view_url_template(self):
    url = f"https://{self.bucket_name}.s3"
    if self._region_name:
        url += f".{self._region_name}"
    url += ".amazonaws.com"
    ...
```

Pro: No hook needed for URL generation.
Con: Requires config change, region duplicated between connection and bundle config.

**Option C: Catch and handle the expunge side effect**

Move `_extract_and_sign_template` to run before any `session.add()` calls in
`sync_bundles_to_db`, so there's nothing in `session.new` to be expunged.

Pro: No changes to MetastoreBackend or S3DagBundle.
Con: Restructuring `sync_bundles_to_db` is more complex, and the root cause
(`expunge_all`) remains a landmine for other code paths.

### Layer 1b: MetastoreBackend — stop returning session-bound objects (architecturally correct)

The `expunge(conn)` fix (Layer 1) solves the immediate bug, but it still has
MetastoreBackend returning a detached ORM object — something no other secrets
backend does. This creates several problems for unrelated components that depend
on MetastoreBackend:

**Problem 1: Leaky abstraction / fragile coupling to Connection model**

`expunge(conn)` only works because Connection currently has no `relationship()`
definitions. If someone later adds a relationship (e.g., eager-loaded tags,
permissions, or audit records), the related objects would enter the session
during the query but NOT be expunged. Accessing them on the detached Connection
would raise `DetachedInstanceError`. The fix would silently break without any
test catching it, because it's coupled to the internal structure of the
Connection model.

**Problem 2: Inconsistent object state across backends**

Every other backend returns a transient Connection — freshly constructed via
`Connection(conn_id=x, uri=y)`, never in any session. MetastoreBackend returns
a detached Connection — was in a session, then removed. These behave differently:

- Re-adding a detached object to a session triggers a merge, not an insert
- A detached object retains its primary key and SQLAlchemy instance state
- Callers that work with connections from multiple backends get objects with
  different internal states, which can cause subtle bugs

**Problem 3: Session manipulation in a read-only lookup**

A secrets backend should be a pure read operation. Having it reach into the
session and manipulate object lifecycle is a side effect that callers don't
expect. Any component that calls `get_connection` during a transaction (like
`sync_bundles_to_db` does) is at risk of session interference — even with
`expunge(conn)`, the session still gets touched (the object is removed from
the identity map, which could affect other code that loaded the same Connection
row in the same session).

**Fix: Override `get_conn_value` instead of `get_connection`**

Make MetastoreBackend follow the same pattern as every other backend: return a
serialized string, and let the base class handle deserialization into a fresh,
transient Connection object.

```python
# airflow-core/src/airflow/secrets/metastore.py

class MetastoreBackend(BaseSecretsBackend):

    @provide_session
    def get_conn_value(
        self, conn_id: str, team_name: str | None = None, *, session: Session = NEW_SESSION
    ) -> str | None:
        from airflow.models import Connection

        conn = session.scalar(
            select(Connection)
            .where(
                Connection.conn_id == conn_id,
                or_(Connection.team_name == team_name, Connection.team_name.is_(None)),
            )
            .limit(1)
        )
        if conn:
            return conn.get_uri()
        return None
        # No expunge needed — conn stays in the session but we only return a string.
        # The base class get_connection() calls deserialize_connection() to build
        # a fresh, transient Connection from the URI. No session objects leak out.
```

The base class `get_connection()` already does:
```python
def get_connection(self, conn_id, team_name=None):
    value = self.get_conn_value(conn_id=conn_id, team_name=team_name)
    if value:
        return self.deserialize_connection(conn_id=conn_id, value=value)
    return None
```

So callers get a fresh `Connection(conn_id=x, uri=y)` — transient, never in
any session, consistent with every other backend.

**Trade-off: URI round-trip data loss**

`get_uri()` serializes the connection to a URI string, and `deserialize_connection`
parses it back. This round-trip could lose fields that don't survive URI
serialization (like `description`, or the `id` primary key). However:

- `get_connection_from_secrets` already calls `conn.get_uri()` for caching, so
  the URI round-trip is already happening in the main code path
- Callers that need connection data (hooks, operators) use `host`, `login`,
  `password`, `port`, `schema`, `extra` — all of which survive the URI round-trip
- `description` and `id` are metadata fields not used by hooks
- The `extra` field survives as JSON in the URI query string

**For `get_variable`**, the same principle applies — return the value string
directly instead of the ORM object:

```python
    @provide_session
    def get_variable(
        self, key: str, team_name: str | None = None, *, session: Session = NEW_SESSION
    ) -> str | None:
        from airflow.models import Variable

        var_value = session.scalar(
            select(Variable)
            .where(Variable.key == key, or_(Variable.team_name == team_name, Variable.team_name.is_(None)))
            .limit(1)
        )
        if var_value:
            return var_value.val
        return None
        # No expunge needed — we return a plain string, not the ORM object.
```

This is already almost what the current code does — it just removes the
`expunge_all()` call since we're not returning the ORM object.

### Layer 1b Deep Dive: URI Round-Trip Impact Analysis

Switching to `get_conn_value` means every connection lookup goes through a
`get_uri()` → `Connection(uri=...)` round-trip. Here's exactly what survives
and what doesn't:

**Fields that survive the URI round-trip:**

| Field | Survives | How |
|---|---|---|
| `conn_id` | Yes | Passed separately to `Connection(conn_id=x, uri=y)` |
| `conn_type` | Yes | URI scheme (`aws://`, `mysql://`, etc.) |
| `host` | Yes | Including protocol-in-host like `https://` |
| `login` | Yes | URL-encoded in authority block |
| `password` | Yes | URL-encoded in authority block |
| `schema` | Yes | Path component after host |
| `port` | Yes | Port component |
| `extra` | Yes | Query string (flattened or as `__extra__` JSON blob) |

**Fields lost in the URI round-trip:**

| Field | Why lost | Impact |
|---|---|---|
| `description` | Not part of URI format | CLI `airflow connections get` shows empty description |
| `id` (PK) | Not part of URI format | CLI `airflow connections get` shows no ID |
| `is_encrypted` | Internal DB flag | CLI shows wrong encryption status |
| `is_extra_encrypted` | Internal DB flag | Minor — rarely displayed |
| `team_name` | Not part of URI format | Lost, but callers pass team_name separately |

**Caller-by-caller impact:**

1. **Hooks** (AwsGenericHook, S3Hook, all provider hooks via `conn_config`):
   Only access `conn_type`, `host`, `login`, `password`, `schema`, `port`,
   `extra`, `extra_dejson`. All survive. **No impact.**

2. **Execution API** (`ConnectionResponse`): Returns `conn_id`, `conn_type`,
   `host`, `schema`, `login`, `password`, `port`, `extra`. All survive.
   **No impact.**

3. **Supervisor remote logging cache**: Passes Connection to a hook.
   **No impact.**

4. **`airflow connections get` CLI**: Calls `_connection_mapper` which accesses
   `conn.id`, `conn.description`, `conn.is_encrypted`. These would be
   `None`/default after round-trip. **Visible regression** — CLI output loses
   description and shows wrong ID/encryption status.

5. **`airflow connections test` CLI**: Calls `conn.test_connection()` →
   `conn.get_hook()` — only needs `conn_type`. **No impact.**

6. **`get_connection_from_secrets` caching**: Already calls `conn.get_uri()`
   to cache the URI. The round-trip is already happening for the cache path.
   **No impact** (the cache was always lossy).

**Could JSON serialization help?**

`Connection.to_dict()` includes `description` but still loses `id`,
`is_encrypted`, `is_extra_encrypted`, `team_name`. The base class
`deserialize_connection` supports JSON via `from_json`, but `from_json` also
doesn't restore `id` or `is_encrypted`. So JSON doesn't fully solve the
data loss problem either.

**Code that would need to change if we go with Layer 1b:**

The CLI `connections_get` command in `connection_command.py` currently goes
through `Connection.get_connection_from_secrets()` → MetastoreBackend. With
the URI round-trip, it would lose `description` and `id`. Options:

- Accept the regression (description/id missing in CLI output)
- Change `connections_get` to query the DB directly instead of going through
  the secrets abstraction — but that defeats the purpose of the abstraction
- Keep MetastoreBackend's `get_connection` override for the full ORM object
  but also add `get_conn_value` — but then we have two code paths to maintain

**The fundamental tension:**

The secrets backend abstraction was designed for external stores (Vault, SSM,
etc.) where you only get back a serialized string. MetastoreBackend is
fundamentally different — it has direct DB access to the full ORM object with
all fields. Forcing it through the same string-serialization path loses
information that only the DB has. This is an inherent design mismatch that
can't be fully resolved without either accepting data loss or breaking the
abstraction.

## Recommended Approach

**For the PR fix**: Use Layer 1 (`expunge(conn)`) — it's a 2-line change that
fixes the bug without regressing anything. The Connection model has no
relationships, so `expunge(conn)` detaches exactly what was loaded.

**Follow-up**: Layer 1b (`get_conn_value` override) is the architecturally
correct long-term solution, but it requires addressing the CLI regression
and should be its own PR with proper discussion on the mailing list.

Layer 2 options are defense-in-depth and not strictly necessary once the root
cause is fixed.

## Debug Logging Cleanup

Both `metastore.py` and `manager.py` currently contain extensive `[DEBUG-SYNC]`
and `[DEBUG-METASTORE]` warning-level log statements added during investigation.
These should be removed as part of the fix PR.

## Existing Tests to Update

The existing MetastoreBackend tests mock the backend or test it in isolation,
so they won't catch the shared-session interaction. A new test should:

1. Add a pending object to the session
2. Call `MetastoreBackend.get_connection()` on the same session
3. Verify the pending object is still in `session.new` after the call

Test file: `airflow-core/tests/unit/always/test_secrets_backends.py`
