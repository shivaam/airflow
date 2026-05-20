# Coordinator runtime mode

> **Status:** alpha. Targets the `BaseCoordinator` extension point in
> [PR #65958](https://github.com/apache/airflow/pull/65958). Will land
> as a follow-up to PR #66289 once the coordinator interface merges.

## What it is

A second runtime host alongside the Edge worker. The same handler you
register with `registerTask(...)` runs in either mode — just call the
matching entry function:

```ts
import { registerTask, startCoordinatorRuntime } from "@apache-airflow/ts-sdk";

registerTask("my_dag.say_hello", async ({ ctx, client }) => {
  const greeting = await client.getVariable("greeting");
  await client.setXCom({ key: "echo", value: `node says: ${greeting}` });
  return greeting; // auto-pushed to XCom as "return_value"
});

await startCoordinatorRuntime();
```

You bundle this entry script (one `.mjs` per bundle) and Airflow's
coordinator subprocess invokes it as:

```
node my-bundle.mjs --comm=127.0.0.1:<port> --logs=127.0.0.1:<port>
```

The coordinator passes the address of two TCP sockets: one for
length-prefixed msgpack frames (the comm channel — task lifecycle and
RPC), one for newline-JSON log records.

## How it works

```
Python WatchedSubprocess (handles heartbeat, state, Execution API)
  └── forks child process
        └── BaseCoordinator (PR #65958)
              ├── opens TCP servers (comm + logs)
              ├── spawns: node my-bundle.mjs --comm=... --logs=...
              ├── forwards StartupDetails to subprocess
              ├── bridges bytes: supervisor ↔ subprocess (raw, no interpretation)
              └── bridges logs: subprocess → structlog
```

`BaseCoordinator` is a **transparent byte bridge** — it doesn't
interpret comm messages. The Python supervisor's `handle_requests`
generator does all the RPC work (GetVariable, GetXCom, etc.). Both Java
and TS subprocesses are architecturally equivalent. See
[SDK_COMPARISON.md](SDK_COMPARISON.md) for the full cross-SDK analysis.

## Running it

### Reviewing this branch (no Airflow, no Python needed)

The coordinator-client logic is fully exercised by an in-process
integration test that mirrors what Airflow's real
`BaseCoordinator._runtime_subprocess_entrypoint` does:

```bash
pnpm install
pnpm test           # vitest — includes tests/coordinator/integration.test.ts
pnpm run typecheck
```

`tests/coordinator/integration.test.ts` spins up an in-process TCP
"supervisor", connects the runtime over `--comm`/`--logs`, and walks it
through success, handler failure, and unknown-task scenarios. No Python
or Airflow install is required to verify the protocol end of this work.

### End-to-end against real Airflow (the full picture)

This branch is the **TypeScript side**. The runnable end-to-end path
also needs the Python-side `TypescriptCoordinator` (a `BaseCoordinator`
subclass from [PR #65958](https://github.com/apache/airflow/pull/65958),
not in this branch) plus a Python stub DAG. High level:

1. **Author a handler and bundle it** (one self-contained `.mjs`):

   ```ts
   import { registerTask, startCoordinatorRuntime } from "@apache-airflow/ts-sdk";

   registerTask("hello_typescript.say_hello", async ({ ctx, client }) => {
     const greeting = await client.getVariable("greeting");
     await client.setXCom({ key: "echo", value: `node says: ${greeting}` });
     return greeting;
   });

   await startCoordinatorRuntime();
   ```

   ```bash
   esbuild bundle-src.ts --bundle --platform=node --format=esm \
     --outfile=/path/to/bundles/bundle.mjs
   ```

2. **Register the Python coordinator** (`airflow.cfg`, worker-side):

   ```ini
   [sdk]
   coordinators = [{"name": "ts",
     "classpath": "airflow.sdk.coordinators.typescript.TypescriptCoordinator",
     "kwargs": {"node_executable": "node", "bundles_folder": "/path/to/bundles"}}]
   queue_to_coordinator = {"ts-runtime": "ts"}
   ```

3. **Write a Python stub DAG** — the task body is in the bundle, not Python:

   ```python
   @stub(queue="ts-runtime")
   def say_hello(): ...
   ```

4. **Trigger it.** Scheduler → `queue="ts-runtime"` → `TypescriptCoordinator`
   → `node bundle.mjs --comm=… --logs=…` → handler runs, XCom flows back
   through the Execution API.

This full path (including a Java + TypeScript + Python polyglot DAG
exchanging XCom across all three runtimes) is the dev-call demo, not
reproducible from this branch alone.

## How it differs from Edge worker mode

| Concern | Edge worker | Coordinator runtime |
|---|---|---|
| Process lifetime | Long-lived (polls Edge API) | Per-task (one-shot subprocess) |
| Transport | HTTPS + JWT | TCP + msgpack on localhost |
| Spawned by | systemd / k8s / user | Airflow's `BaseCoordinator` |
| Auth | JWT signed with shared secret | Localhost trust |
| Init | Worker registers + polls | First frame is `StartupDetails` |
| Failure semantics | PATCH state via HTTP | Send `TaskState{failed}` frame |
| Deployment unit | Long-running worker process | Bundled `.mjs` + sidecar metadata |

Both modes share `registerTask()`, the `TaskContext` shape, and the
`TaskClient` interface (Variables, XCom). The `TaskClient` has two
implementations: coordinator mode sends RPC over the comm socket,
edge mode calls the Execution API over HTTP. Task handlers don't know
which transport backs the client.

## TaskClient (cross-mode)

The `TaskClient` interface is available in both modes via
`TaskHandlerArgs.client`:

```ts
registerTask("my_task", async ({ client }) => {
  // Variables
  const val = await client.getVariable("my_key");       // null if missing
  const val2 = await client.getVariableOrThrow("key");  // throws if missing

  // XCom
  const data = await client.getXCom({ key: "upstream_data", taskId: "extract" });
  await client.setXCom({ key: "result", value: { count: 42 } });

  return "done"; // auto-pushed as XCom "return_value"
});
```

**Implementations:**
- `coordinator/client.ts` — `createCoordinatorClient()` backed by comm-socket RPC
- `edge/task-client.ts` — `createEdgeTaskClient()` backed by Execution API HTTP

## Wire protocol

Two TCP sockets:

1. **Comm** (`--comm=host:port`) — length-prefixed msgpack frames.
   - Frame: `[len: uint32 BE][msgpack array]`
   - Request body: `[id: int, body: map]` (arity 2)
   - Response body: `[id: int, body: map, error: map?]` (arity 3)
   - Body is a map with a `type` discriminator (`StartupDetails`,
     `SucceedTask`, etc.)

2. **Logs** (`--logs=host:port`) — newline-delimited JSON.
   - One log record per line
   - Required fields: `event`, `level`, `logger`, `timestamp`
   - Extra keys pass through to structlog as structured fields

The runtime expects either `DagFileParseRequest` (parse mode) or
`StartupDetails` (task execution) as the very first frame after both
sockets connect. It then dispatches to the registered handler and
sends a terminal frame: `DagParsingResult`, `SucceedTask`, or
`TaskState{state: "failed"|"removed"|...}`.

### Important protocol notes

- **Frame routing uses arity, not id.** Both supervisor and runtime
  keep independent id counters that start at 0, so id collision across
  directions is normal. `comm-channel.ts` routes by arity (request vs
  response).
- **Airflow's `_send_startup_details` emits an arity-3 frame** for
  StartupDetails, even though semantically it's the supervisor's first
  request. The runtime's router falls through to the incoming-frame
  path when an arity-3 frame has no matching pending request — this is
  what makes the initial dispatch work.
- **`SucceedTask` MUST include `task_outlets: []` and
  `outlet_events: []`.** The Execution API's
  `TISuccessStatePayload` tagged-union validator rejects null for these
  fields. The runtime always sends empty arrays.

## Task lookup precedence

When `StartupDetails` arrives, the runtime looks up the handler in
this order:

1. `${dag_id}.${task_id}` — namespaced. Use this when one bundle
   serves multiple DAGs that share task names.
2. `${task_id}` — bare. Fine for single-DAG bundles.

This matches the existing Edge worker convention from `registry.ts`
(TaskGroup tasks already use dotted form).

## Testing

```bash
pnpm test
```

The integration test in [`tests/coordinator/integration.test.ts`](tests/coordinator/integration.test.ts)
spins up an in-process TCP "supervisor" that mirrors what Airflow's
real `BaseCoordinator._runtime_subprocess_entrypoint` does, and walks
the runtime through three scenarios: success, handler failure, and
unknown task. No Python or Airflow install needed.

For end-to-end verification against a real Airflow build, see the
spike's runbook at
[`airflow-task-sdk/experiments/coordinator/REAL-AIRFLOW-FINDINGS.md`](https://github.com/randomblueberries/airflow-task-sdk/blob/spike/coordinator-integration/experiments/coordinator/REAL-AIRFLOW-FINDINGS.md).

---

## What's done

- [x] Comm channel — TCP connect, length-prefixed msgpack, arity-based routing
- [x] Log channel — newline-JSON over TCP
- [x] Task execution — StartupDetails dispatch, handler invoke, SucceedTask/TaskState response
- [x] TaskClient (coordinator) — getVariable, getVariableOrThrow, getXCom, setXCom
- [x] TaskClient (edge) — same interface, backed by Execution API HTTP
- [x] Unified TaskClient — shared interface in `src/client.ts`, both modes provide it
- [x] Auto-push return value to XCom as `"return_value"` (matches Python `@task`)
- [x] Remove deprecated `CoordinatorClient` alias
- [x] Trim barrel exports to public-API-only
- [x] DAG parse stub — responds with empty `dags: {}` (sufficient for Python-stub-DAG workflow)
- [x] Deferred<T> for greeting race condition
- [x] Integration tests (success, failure, unknown task, XCom round-trip)

## Next steps

### Near-term (complete the client surface)

- [ ] **`getConnection(connId)`** — add to `TaskClient` interface + both
  implementations. Both Python and Java SDKs have it. Needed for tasks
  that access external services (databases, AWS, APIs) through Airflow's
  secrets management.

### Medium-term (full DAG support — no Python needed)

- [ ] **`provideDags()` API + DAG serialization** — implement `handleParse`
  to return real DAG structure instead of `dags: {}`. This is what makes
  "define entire DAGs in TypeScript" possible. Requires porting the
  serialization logic from the Java SDK's `Serde.kt` (~250 lines) to
  produce the `__version: 3` / `__type` / `__var` structure that
  Airflow's `DagSerialization.from_dict()` expects. Once done, a `.mjs`
  file dropped in the DAG bundle folder is a full DAG — no Python stub.

- [ ] **DAG builder API** — TypeScript-native `Dag` class with `addTask()`,
  schedule, params, tags, etc. Equivalent to the Java SDK's `Dag.kt`.

### Hardening (release blockers)

- [ ] **Startup timeout (L1)** — connected-but-silent supervisor hangs
  Node forever. Add a configurable timeout on `CommChannel.connect()`.

- [ ] **Per-request timeout (L2)** — never-answered RPC hangs the handler
  forever. Add timeout to `CommChannel.request()`.

- [ ] **Log backpressure (B1)** — `LogChannel.send` ignores `sock.write`
  backpressure. Unbounded log buffering for chatty/long tasks.

- [ ] **Large-frame reassembly (B3)** — `FrameReader` `Buffer.concat`
  per chunk is O(n^2) for large frames.

### Future (follow-up PRs)

- [ ] **Per-TI heartbeat** — coordinator mode relies on the Python
  supervisor; edge mode needs its own heartbeat interval.

- [ ] **AbortSignal / graceful SIGTERM** — `ctx.signal` exists but never
  aborts in coordinator mode. Wire SIGTERM to the abort controller.

- [ ] **TaskContext enrichment** — extend with `dagRunConf`, `maxTries`,
  `taskRescheduleCount` from the `TIRunContext` response.

- [ ] **Log forwarding (edge)** — structured log channel over HTTP for
  edge mode.

- [ ] **Subprocess isolation** — spawn each task in a child process for
  memory isolation.

## Background and rationale

The full design rationale, scale benchmarks, and end-to-end test results
that motivated this implementation live in the research repo at
`airflow-task-sdk/experiments/coordinator/`:

- `LEARNING.md` — coordinator architecture deep-dive
- `DESIGN.md` — TS coordinator design choices
- `REAL-AIRFLOW-FINDINGS.md` — end-to-end test against real Airflow
- `scalability/` — four scalability spikes (cold start, concurrent
  load, memory under sustained cadence, long-running tasks)
