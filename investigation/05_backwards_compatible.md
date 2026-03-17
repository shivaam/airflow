# Backwards-Compatible Migration Approach

## Strategy

Wrap existing `on_foo_callback` callables into `SyncCallback` objects behind the scenes,
route them through the executor, and fall back to the DAG Processor for cases that can't
be resolved to an import path.

## Phase 1: Detect Importable Callables

During DAG parsing (in the DAG Processor), when a callback is registered:

```python
def resolve_callback_path(callback: Callable) -> str | None:
    """Try to convert a callable to a dotted import path."""
    if isinstance(callback, BaseNotifier):
        # Notifiers are already importable by class path
        return f"{callback.__class__.__module__}.{callback.__class__.__qualname__}"

    module = getattr(callback, "__module__", None)
    qualname = getattr(callback, "__qualname__", None)

    if module is None or qualname is None:
        return None  # Can't resolve

    # Reject lambdas
    if "<lambda>" in qualname:
        return None

    # Reject closures / nested functions
    if "<locals>" in qualname:
        return None

    # Reject if module is a mangled DAG module name
    # (Airflow mangles DAG modules to unusual_prefix_HASH_filename)
    if "unusual_prefix" in module:
        # Use the original file path instead
        # The worker will need the bundle to import this
        return None  # Or: construct path from bundle + filename

    return f"{module}.{qualname}"
```

## Phase 2: Wrap in SyncCallback

If the callback is importable, wrap it:

```python
import_path = resolve_callback_path(user_callback)
if import_path:
    # Route through executor
    sync_callback = SyncCallback(path=import_path)
    # Create ExecutorCallback ORM record
    executor_callback = ExecutorCallback.create_from_sdk_def(sync_callback)
    # ... attach to DAG run context
else:
    # Fall back to legacy DAG Processor path
    # Emit deprecation warning
    warnings.warn(
        f"Callback {user_callback} cannot be resolved to an import path. "
        "It will be executed via the DAG Processor (deprecated). "
        "Move your callback to a separate importable module.",
        DeprecationWarning,
    )
    # Create DagProcessorCallback (legacy path)
```

## Phase 3: Modify Scheduler to Queue ExecuteCallback

In `scheduler_job_runner.py`, where callbacks are currently sent to the callback sink:

**Before:**
```python
# Always sends to DAG Processor
executor.send_callback(dag_callback_request)
```

**After:**
```python
if callback_is_importable:
    # New path: queue as executor workload
    workload = ExecuteCallback.make(callback=executor_callback, dag_run=dag_run, ...)
    executor.queue_workload(workload, session)
else:
    # Legacy path: send to DAG Processor
    executor.send_callback(dag_callback_request)
```

## Phase 4: Build Context for Executor Callbacks

The legacy system provides a rich context dict. The new system must match it.

**Challenge**: The executor callback currently only gets minimal context (dag_run info,
deadline metadata). For on_foo_callbacks, users expect the full template context:
task_instance, macros, XCom, params, etc.

**Solution options**:

### Option A: Build context in scheduler, pass via kwargs
- Scheduler builds the full context dict when creating the callback request
- Serialize it into the callback's `kwargs`
- Problem: context can be large (XCom values, params) and may not serialize cleanly

### Option B: Build context in worker, from API
- Worker receives callback workload with dag_run_id, task_instance_id
- Worker calls Execution API to fetch context data
- Worker builds context dict locally
- This is how tasks already work — callbacks would follow the same pattern
- **Preferred approach** — aligns with architecture (workers don't access metadata DB)

### Option C: Pass minimal context, deprecate rich context
- Only pass dag_id, run_id, task_id, state
- Deprecate full template context for callbacks
- **Breaking change** — many users depend on `context["task_instance"]`

## Phase 5: Handle Edge Cases

### Lambdas
```python
on_failure_callback=lambda ctx: print("failed")
```
- Cannot be converted to import path
- Fall back to DAG Processor with deprecation warning
- Document: "Move lambda callbacks to named functions in importable modules"

### Closures / Nested Functions
```python
def make_dag():
    threshold = 100
    def alert(context):
        if context["ti"].duration > threshold:
            notify()
    return DAG(on_failure_callback=alert)
```
- `alert` has `<locals>` in qualname — not importable
- Fall back to DAG Processor with deprecation warning

### Lists of Callbacks
```python
on_failure_callback=[importable_func, lambda ctx: print("x")]
```
- Resolve each independently
- If all importable: route all through executor
- If any non-importable: route ALL through DAG Processor (keep atomic behavior)
- Or: route importable ones through executor, non-importable through DAG Processor

### Notifier Instances
```python
on_failure_callback=SlackNotifier(text="failed", channel="#alerts")
```
- Class is importable, but constructor args need to be preserved
- Could serialize as `SyncCallback(path="SlackNotifier", kwargs={"text": "failed", ...})`
- Need to handle template rendering in kwargs

### DAG Module Name Mangling
- Airflow mangles DAG module names: `unusual_prefix_HASH_filename`
- A function `my_dag.alert` becomes `unusual_prefix_abc123_my_dag.alert`
- Worker won't have this mangled name
- **Solution**: Use original file path + function name, let worker import from bundle

## Decision Matrix

| Callback Type | Importable? | Route | Action |
|---------------|-------------|-------|--------|
| Module-level function in installed package | Yes | Executor | Auto-wrap in SyncCallback |
| Module-level function in DAG file | Maybe | Executor (with bundle) | Resolve via bundle path |
| Notifier instance | Yes | Executor | Serialize class + kwargs |
| Lambda | No | DAG Processor (deprecated) | Warn user |
| Closure / nested function | No | DAG Processor (deprecated) | Warn user |
| Class method | Depends | Case-by-case | Check qualname |

## Timeline

This approach allows a gradual migration:
1. **3.3**: Introduce auto-wrapping for importable callbacks, deprecate non-importable
2. **3.4**: Make executor path the default, DAG Processor path opt-in
3. **4.0**: Remove DAG Processor callback path entirely
