# Learning Path: SQLAlchemy Sessions & Airflow's Session Management

A self-contained guide to the concepts you need to understand issue #62244.
Start from the top and work down — each section builds on the previous one.

---

## Level 1: What is a SQLAlchemy Session?

A Session is a "workspace" for talking to the database. It tracks:

- **Identity map**: Objects loaded from the DB (keyed by primary key)
- **new**: Objects you've called `session.add()` on but haven't committed yet
- **dirty**: Objects whose attributes have changed since they were loaded
- **deleted**: Objects marked for deletion

Think of it like a shopping cart. You add items (`session.add()`), modify items,
remove items, and then checkout (`session.commit()`) to persist everything to the
DB in one transaction. Or you abandon the cart (`session.rollback()`).

```python
session.add(bundle)          # bundle goes into session.new
session.commit()             # INSERT executed, bundle moves to identity map
bundle.active = False        # bundle moves to session.dirty
session.commit()             # UPDATE executed
```

Key point: nothing hits the database until `commit()` (or `flush()`).

---

## Level 2: expunge, expunge_all, and detached objects

### The problem they solve

When an object is in the session, SQLAlchemy tracks it. If you close the session
and then try to access a lazy-loaded attribute on that object, you get:

```
sqlalchemy.orm.exc.DetachedInstanceError:
  Instance <Connection at 0x...> is not bound to a Session
```

### expunge(obj)

Removes ONE specific object from the session. The object becomes "detached" —
it still has its data, but SQLAlchemy no longer tracks it.

```python
conn = session.scalar(select(Connection).where(...))
# conn is in session's identity map
session.expunge(conn)
# conn is now detached — safe to use after session closes
# everything else in the session is untouched
```

### expunge_all()

Removes EVERY object from the session — identity map, new, dirty, deleted, all of it.

```python
session.add(bundle_a)        # bundle_a in session.new
session.add(bundle_b)        # bundle_b in session.new
conn = session.scalar(...)   # conn in identity map

session.expunge_all()
# session.new = empty
# session.identity_map = empty
# bundle_a, bundle_b, conn — all detached
# if you commit now, nothing happens
```

This is the nuclear option. It was used in MetastoreBackend because the author
wanted to detach the Connection object, but used the sledgehammer instead of
the scalpel.

### Object states in SQLAlchemy

```
              session.add()
  Transient ──────────────► Pending (in session.new)
                                │
                          flush/commit
                                │
                                ▼
                           Persistent (in identity map)
                                │
                         session.expunge()
                                │
                                ▼
                            Detached (no session, still has data)
```

- **Transient**: Just created with `MyModel()`, not in any session
- **Pending**: Added to session via `session.add()`, not yet flushed to DB
- **Persistent**: Flushed/committed, has a DB row, tracked by session
- **Detached**: Was in a session, now removed. Has data but no session link

---

## Level 3: Session Factories and sessionmaker

SQLAlchemy doesn't want you creating sessions manually. Instead, you create a
"factory" that produces sessions with consistent configuration:

```python
from sqlalchemy.orm import sessionmaker

# Create a factory (once, at startup)
SessionFactory = sessionmaker(bind=engine, autocommit=False, autoflush=True)

# Create sessions from the factory (many times)
session1 = SessionFactory()
session2 = SessionFactory()
# session1 and session2 are DIFFERENT, INDEPENDENT sessions
```

Each session has its own identity map, its own pending objects, its own transaction.
Changes in session1 don't affect session2.

---

## Level 4: Scoped Sessions (thread-local sessions)

### The problem they solve

In a web app or multi-threaded service, you want each thread/request to have its
own session, but you don't want to pass the session object around everywhere.

### How they work

```python
from sqlalchemy.orm import scoped_session, sessionmaker

Session = scoped_session(sessionmaker(bind=engine))
```

`scoped_session` wraps a session factory and adds a registry keyed by thread ID.
When you call `Session()`, it checks: "do I already have a session for this thread?"

- If yes → return the existing one
- If no → create a new one, store it, return it

```python
# Thread 1:
s1 = Session()    # creates new session for thread 1
s2 = Session()    # returns SAME session (s1 is s2 → True)

# Thread 2:
s3 = Session()    # creates new session for thread 2 (different from s1)
```

### Why this matters for the bug

In Airflow, `settings.Session` is a scoped_session. So within a single thread:

```python
# sync_bundles_to_db — @provide_session creates a session
session_a = settings.Session()   # new session for this thread

# ... later, deep inside S3Hook init ...
# MetastoreBackend.get_connection — @provide_session creates a session
session_b = settings.Session()   # SAME session (same thread)

session_a is session_b  # True!
```

Neither function intended to share a session. They both independently asked for
"a session" and got the same one because scoped_session is thread-local.

---

## Level 5: Airflow's provide_session Decorator

Airflow wraps the scoped session pattern in a decorator:

```python
# airflow/utils/session.py

def provide_session(func):
    def wrapper(*args, **kwargs):
        if "session" in kwargs or session_args_idx < len(args):
            # Caller passed a session explicitly — use it
            return func(*args, **kwargs)
        # No session provided — create one from the scoped factory
        with create_session() as session:
            return func(*args, session=session, **kwargs)
    return wrapper
```

And `create_session()`:

```python
def create_session(scoped=True):
    session = settings.Session()  # scoped_session → thread-local
    try:
        yield session
        session.commit()          # auto-commit on success
    except Exception:
        session.rollback()        # auto-rollback on error
        raise
    finally:
        session.close()
```

So when you write:

```python
@provide_session
def my_function(*, session=NEW_SESSION):
    session.add(something)
    # session.commit() happens automatically when my_function returns
```

The decorator handles session creation, commit, rollback, and cleanup.

### The nesting problem

When `@provide_session` functions call other `@provide_session` functions
without passing the session explicitly, each one independently calls
`settings.Session()` and gets the same scoped session. But each one's
`create_session()` context manager will try to commit/rollback/close
that same session independently. This is where things get messy.

```python
@provide_session
def outer(*, session=NEW_SESSION):
    session.add(bundle)                    # bundle in session.new
    inner()                                # doesn't pass session!

@provide_session
def inner(*, session=NEW_SESSION):
    # Gets SAME session (scoped)
    conn = session.scalar(...)
    session.expunge_all()                  # nukes bundle from session.new
    # inner's create_session context manager will try to commit
    # but there's nothing left to commit
```

If `outer` had passed its session to `inner`, the decorator would use it
directly without wrapping it in create_session. But since it doesn't,
both functions independently manage the same session.

---

## Level 6: Putting It All Together (The Bug)

```
sync_bundles_to_db()
  │
  │  @provide_session → create_session() → Session() → SESSION_A
  │
  ├─ session.add(DagBundleModel("team_alpha"))
  │    SESSION_A.new = [team_alpha]
  │
  ├─ _extract_and_sign_template("team_beta")
  │    └─ S3DagBundle().view_url_template()
  │         └─ S3Hook().conn_config
  │              └─ get_connection("aws_default")
  │                   └─ MetastoreBackend.get_connection()
  │                        │
  │                        │  @provide_session → create_session() → Session() → SESSION_A (same!)
  │                        │
  │                        ├─ conn = session.scalar(select(Connection)...)
  │                        └─ session.expunge_all()
  │                             SESSION_A.new = []  ← team_alpha is GONE
  │
  ├─ session.add(DagBundleModel("team_beta"))
  │    SESSION_A.new = [team_beta]
  │    (will be expunged on next iteration...)
  │
  └─ return → create_session commits → SESSION_A.new is empty → nothing persisted
```

The fix: change `expunge_all()` to `expunge(conn)` in MetastoreBackend.
Only detach the Connection object that was queried. Leave everything else alone.

---

## Concepts Cheat Sheet

| Concept | One-liner |
|---|---|
| Session | A workspace that tracks DB objects and batches changes until commit |
| session.new | Set of objects added via session.add() but not yet flushed |
| session.dirty | Set of persistent objects with modified attributes |
| session.identity_map | Cache of objects loaded from DB, keyed by primary key |
| session.add(obj) | Put obj in session.new (pending insert) |
| session.expunge(obj) | Remove ONE object from session (detach it) |
| session.expunge_all() | Remove ALL objects from session (nuclear option) |
| session.commit() | Flush pending changes to DB and commit the transaction |
| session.flush() | Send pending SQL to DB but don't commit (still in transaction) |
| session.rollback() | Undo all pending changes |
| sessionmaker | Factory that creates sessions with consistent config |
| scoped_session | Wrapper that returns one session per thread (thread-local) |
| @provide_session | Airflow decorator: auto-creates session if none passed |
| create_session() | Airflow context manager: creates session, auto-commits on exit |
| Transient | Object created but not in any session |
| Pending | Object in session.new (added, not flushed) |
| Persistent | Object in identity map (has a DB row) |
| Detached | Object removed from session (has data, no session link) |

---

## Suggested Reading Order

1. This document (you're here)
2. `airflow-core/src/airflow/utils/session.py` — small file, read the whole thing
3. `airflow-core/src/airflow/settings.py` — search for `scoped_session` (line ~431)
4. `airflow-core/src/airflow/secrets/metastore.py` — the buggy `expunge_all()` calls
5. SQLAlchemy docs on Session Basics: https://docs.sqlalchemy.org/en/20/orm/session_basics.html
6. SQLAlchemy docs on scoped_session: https://docs.sqlalchemy.org/en/20/orm/contextual.html
