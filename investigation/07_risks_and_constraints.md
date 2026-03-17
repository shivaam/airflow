# Risks and Constraints

## 1. DAG Module Name Mangling

**Risk: HIGH**

Airflow mangles DAG module names during parsing to avoid collisions:
`my_dag.py` becomes `unusual_prefix_abc123_my_dag`.

A function `my_alert` in `my_dag.py` has:
- `__module__` = `unusual_prefix_abc123_my_dag`
- `__qualname__` = `my_alert`

This path is not valid on the worker. The worker would need to:
1. Know the original file path (available via bundle)
2. Add the bundle directory to `sys.path`
3. Import using the original module name

**Mitigation**: The bundle system already handles this for tasks. The callback supervisor
needs the same bundle setup. We fixed this exact bug in the ECS executor work —
`sys.path` must include the bundle directory.

**Remaining concern**: Even with correct `sys.path`, the import path stored in the callback
must use the UN-mangled module name. During DAG parsing, we'd need to reverse the mangling
or store the original file path separately.

## 2. Lambda and Closure Support

**Risk: MEDIUM**

Lambdas and closures cannot be converted to import paths:
- `lambda ctx: print(ctx)` — no module, no qualname
- Nested functions with `<locals>` in qualname — not importable from outside

Current usage in tests: `on_failure_callback=lambda x: print("hi")`

Users who define callbacks as lambdas will need to either:
- Refactor to named functions in importable modules
- Accept that their callbacks run on the deprecated DAG Processor path

**Mitigation**: Emit deprecation warnings during DAG parsing. Provide a migration guide.
Most production callbacks are already module-level functions or Notifier instances.

## 3. Context Dict Compatibility

**Risk: HIGH**

Legacy callbacks receive a rich context dict built by `RuntimeTaskInstance.get_template_context()`:
- `task_instance`, `dag`, `dag_run`, `ds`, `macros`, `params`, `var`, `conn`, etc.

The new callback system passes only kwargs specified at definition time plus deadline info.

If we route legacy callbacks through the executor, we must either:
1. Build the same context dict in the worker (requires Execution API calls)
2. Build it in the scheduler and serialize it (large, may not serialize cleanly)
3. Pass a reduced context (breaking change)

**Mitigation**: Option 1 is preferred. The worker already has the Execution API available.
Build a `CallbackContextBuilder` that fetches task/dag info from the API and constructs
the same context dict.

**Specific concerns:**
- `context["task_instance"]` — users call methods on this; must be a real TI or equivalent
- `context["var"]` — lazy accessor to Airflow Variables; may need API support
- `context["conn"]` — connection accessor; needs API support
- XCom values — fetched lazily; needs API endpoint

## 4. Email Callbacks

**Risk: LOW**

`EmailRequest` is a callback type for `email_on_failure` / `email_on_retry`. These are
already deprecated in favor of Notifiers. They could:
- Stay in the DAG Processor during migration (least risk)
- Be migrated to executor along with other callbacks
- Be removed if they're deprecated long enough

## 5. Executor Compatibility

**Risk: MEDIUM**

Not all executors support callbacks yet:
- LocalExecutor: Yes
- CeleryExecutor: Yes (3.2+)
- ECS Executor: In progress (our branch)
- KubernetesExecutor: Not yet
- Batch Executor: Not yet

If we route on_foo_callbacks through executors, they'll only work on executors that
have `supports_callbacks = True`. For executors that don't support it yet, we'd need to
either fall back to DAG Processor or error.

**Mitigation**: Add callback support to all executors before making this the default path.
Or: fall back gracefully to DAG Processor for executors without callback support.

## 6. Scheduling Priority

**Risk: MEDIUM (political, not technical)**

ferruzzi's concern: how should callbacks be prioritized relative to tasks?

Options:
1. **Same priority as tasks**: Callbacks compete for worker slots
2. **Higher priority**: Callbacks always run before tasks (like deadline alerts)
3. **Tiered**: Deadline alerts > on_foo_callbacks > tasks
4. **Configurable**: Users set priority per callback

This needs community discussion. Anish's workload queue unification PR may provide
the tier-based framework to support this.

**Risk**: Bikeshedding on the dev list. ferruzzi's advice: propose a specific approach,
don't ask open-ended questions.

## 7. Multi-Team / Multi-Bundle

**Risk: LOW**

Callback requests already include `bundle_name` and `bundle_version`. The new system
also supports this via `BaseDagBundleWorkload`. No additional work needed here.

## 8. Race Conditions

**Risk: MEDIUM**

Current flow:
1. DAG run completes → scheduler creates callback request
2. Request stored in DB → DAG Processor fetches it
3. DAG Processor re-parses DAG file → executes callback

If the DAG file changes between steps 1 and 3, the callback code may differ.

New flow:
1. DAG run completes → scheduler creates ExecutorCallback
2. Executor queues workload → worker picks it up
3. Worker imports callback from bundle at specific version

The new system is actually better here because bundle versioning is explicit. But we need
to ensure the correct bundle version is passed through.

## 9. Testing Complexity

**Risk: MEDIUM**

Testing the migration requires:
- Tests for import path resolution (including edge cases)
- Tests for context building in workers
- Tests for fallback to DAG Processor
- Tests for each executor type
- Integration tests with real DAG files
- Backwards compatibility tests with existing DAG patterns

The test matrix is large: (callback_type) x (executor_type) x (importable_or_not) x
(dag_level_or_task_level).

## 10. Interaction with Anish's Workload Queue PR

**Risk: LOW**

Anish's tier-based scheduling PR changes how workloads are queued. Our migration would
add a new workload type (on_foo_callbacks) that needs a tier assignment. This should be
coordinated, but the PR provides a clean extension point.

## Risk Summary

| Risk | Severity | Likelihood | Mitigation Effort |
|------|----------|------------|-------------------|
| Module name mangling | High | Certain | Medium — use bundle paths |
| Lambda/closure support | Medium | Certain | Low — deprecation warning |
| Context dict compatibility | High | Certain | High — API + builder needed |
| Email callbacks | Low | Certain | Low — keep in DAG Processor |
| Executor compatibility | Medium | Certain | Medium — add support per executor |
| Scheduling priority | Medium | Likely | Low — propose tier, discuss |
| Multi-team/bundle | Low | Unlikely | None — already supported |
| Race conditions | Medium | Possible | Low — bundle versioning |
| Testing complexity | Medium | Certain | High — large test matrix |
| Workload queue interaction | Low | Certain | Low — coordinate with Anish |
