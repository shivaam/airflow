# Problems with the Current Callback System

## 1. Architectural Misplacement

**The DAG Processor's job is to parse DAGs, not execute user code.**

The DAG Processor is a critical infrastructure component that parses DAG files and stores
serialized DAGs in the metadata DB. Running user-defined callbacks here means:

- A slow or hung callback blocks DAG parsing for that file
- A callback that consumes excessive memory/CPU affects all DAG parsing
- Callback failures can cascade into DAG parsing failures
- The DAG Processor becomes a bottleneck for both parsing AND callback execution

## 2. Broken Isolation on Container Executors

When using ECS, Kubernetes, or Batch executors, the whole point is that user code runs
in isolated containers. But callbacks run in the DAG Processor on the scheduler host:

- **Security**: Callback code has access to the DAG Processor's environment, not the
  container's sandboxed environment
- **Dependencies**: If a callback needs packages that are in the container image but not
  on the scheduler host, it fails
- **Secrets**: Callbacks can't access container-specific secrets or IAM roles
- **Resources**: Callbacks compete with DAG parsing for CPU/memory on the scheduler host

## 3. No Callback Visibility

- Callback execution isn't tracked in the UI
- Logs are buried in DAG Processor logs, mixed with parsing output
- No state machine (no QUEUED/RUNNING/SUCCESS/FAILED tracking)
- Users can't tell if a callback ran, failed, or was never triggered
- No metrics beyond `dag.callback_exceptions`

## 4. No Retry Mechanism

- If a callback fails (e.g., Slack API temporarily down), it's gone
- The callback request is deleted from the DB after the attempt
- No retry with backoff, no dead letter queue
- Users must build their own retry logic inside callbacks

## 5. Redundant DAG Parsing

- To execute a callback, the DAG Processor re-parses the entire DAG file
- This is necessary because the callable isn't serialized — only a boolean flag exists
- For DAGs with expensive imports or complex logic, this is wasteful
- The same file may be parsed twice in the same cycle: once for scheduled parsing, once for
  callback execution

## 6. Priority Inversion

- Callbacks are priority-weighted but compete with DAG parsing, not with tasks
- A high-priority callback can't preempt a low-priority task
- There's no way to say "fire this callback before any new tasks start"
- The scheduling priority discussion (ferruzzi's concern) can't even happen until
  callbacks are in the executor queue

## 7. Two Callback Systems

Having both legacy callbacks and DeadlineAlert callbacks means:

- Two code paths to maintain and test
- Two different context formats (full template context vs. minimal context dict)
- Two different execution environments (DAG Processor vs. worker)
- Confusion for users about which to use
- Provider authors must support both patterns

## 8. Ancient, Untouched Code

Per ferruzzi's observation, the DAG Processor callback code is 4-5+ years old.
Key commits on `callback_requests.py`:

```
e2220d738e  Added validation for consumed_asset_event for DagRunContext
a18fc01dbd  Fix scheduler crash with email notifications
2dcb88ff75  Move email notifications from scheduler to DAG processor
ef80507e80  Restore proper DAG callback execution context
a5211f2efd  Run Task failure callbacks on DAG Processor when task is externally killed
e7e89a07ff  Drop support for Python 3.9
243fe86d4b  Move airflow sources to airflow-core package
```

Most changes are bug fixes and relocations, not architectural improvements. The core
design hasn't been revisited since callbacks were first added.

## 9. Bundle Version Consistency

- The current system passes `bundle_name` and `bundle_version` in callback requests
- But the DAG Processor may have a different version of the bundle loaded
- If a DAG file changes between when the callback was requested and when it's executed,
  the callback code may differ from what the user intended
- The executor-based system handles this properly via bundle setup in workers

## 10. Scaling Limitations

- The DAG Processor is a single process (or small pool) per bundle
- Callbacks compete for slots in this limited pool
- On busy systems with many callbacks, this creates a backlog
- Executors (Celery, K8s) have much larger worker pools and auto-scaling
