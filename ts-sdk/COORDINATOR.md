# Coordinator runtime mode

> **Status:** alpha. The Python-side `TypescriptCoordinator` lives in
> apache/airflow PR [#65958](https://github.com/apache/airflow/pull/65958)
> (open, part of a three-PR chain:
> [#65956](https://github.com/apache/airflow/pull/65956) Java SDK →
> [#65958](https://github.com/apache/airflow/pull/65958) Coordinator Layer →
> [#65959](https://github.com/apache/airflow/pull/65959) Java CI/E2E). The
> supervisor's schema-migration framework (PR
> [#67235](https://github.com/apache/airflow/pull/67235)) is already merged on
> main — our `SUPERVISOR_API_VERSION` pins us to a known schema version that
> the supervisor's Cadwyn migrator can up/downgrade against.

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

  // Connections
  const conn = await client.getConnection("my_postgres"); // null if missing
  // conn.host, conn.port, conn.login, conn.password, conn.extra, ...

  // XCom (generic — caller specifies expected type)
  const data = await client.getXCom<{ count: number }>({ key: "upstream_data", taskId: "extract" });
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

### Schema version

PR [#67235](https://github.com/apache/airflow/pull/67235) (merged on main)
adds a Cadwyn-based migrator at the supervisor: incoming frames from the
SDK get upgraded to head, outgoing frames get downgraded to whatever
schema version the SDK is pinned to.

We vendor `task-sdk/src/airflow/sdk/execution_time/schema/schema.json` to
`ts-sdk/schema/supervisor-schema.json`, then `pnpm run generate:supervisor`
emits `src/generated/supervisor.ts` plus a typed constant:

```ts
export const SUPERVISOR_API_VERSION = "2026-06-16";
```

This constant is **not transmitted on the comm channel today** — the
supervisor learns it out-of-band (e.g. from bundle metadata that
`TypescriptCoordinator` reads, mirroring how `JavaCoordinator` reads the
JAR manifest's `Airflow-Java-SDK-Version` header). Wiring this through
the `TypescriptCoordinator` is on the near-term roadmap (see the design
spec under `docs/superpowers/specs/`).

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

For end-to-end verification against real Airflow, see
[`TESTING.md`](TESTING.md). The full coordinator-mode E2E suite (mirroring
the Java setup from PR [#65959](https://github.com/apache/airflow/pull/65959))
is planned under `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/`
— see the Phase C design spec.

### Verifying the `mapIndex` normalization (F9)

The coordinator wire collapses three caller forms to a single `null`:
omitted, `undefined`, and the user-facing non-mapped sentinel `-1`.
This is asserted directly in unit tests — no Airflow needed:

```bash
pnpm test -- tests/coordinator/client.test.ts
```

The relevant cases live in [`tests/coordinator/client.test.ts`](tests/coordinator/client.test.ts):

- **"defaults dag/task/run + map_index from ctx; allows override"** —
  proves a non-mapped `ctx.mapIndex = -1` becomes `map_index: null` on
  the wire when the caller omits `opts.mapIndex`.
- **"normalizes user-facing mapIndex=-1 to null on the wire"** — proves
  an explicit `opts.mapIndex: -1` is also normalized to `null` (the
  case `??`-fallback alone would miss), while a mapped value like `5`
  passes through unchanged. Uses a context with `mapIndex: 3` to prove
  the caller's `-1` wins over a mapped ctx — i.e. "give me the
  non-mapped row" is honoured.

If you want to eyeball the wire payload in a real coordinator run, the
integration test in `integration.test.ts` is the cheapest path: drop a
`console.log(frame.body)` into the supervisor's `data` handler and run
`pnpm test -- tests/coordinator/integration.test.ts`. You'll see the
`SetXCom` / `GetXCom` frames in the order the handler emits them, with
`map_index: null` for any non-mapped task.

For a Python-side check, the supervisor filters `-1` and `null`
identically (`task-sdk/.../api/client.py:511,554` —
`if map_index is not None and map_index >= 0:`), so the wire choice
doesn't change behaviour against the API server; the test is about
self-consistency between the documented API and the bytes we put on
the wire.

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
- [x] `getConnection(connId)` — both coordinator and edge implementations
- [x] DAG parser — `dag()` builder + Airflow v3 serialization, full DAGs in TypeScript
- [x] DAG parse mode — `handleParse()` returns real `DagFileParsingResult`
- [x] Deferred<T> for greeting race condition
- [x] Integration tests (success, failure, unknown task, XCom round-trip)

## Next steps

### Near-term (complete the client surface)

- [x] **`getConnection(connId)`** — added to `TaskClient` interface + both
  implementations. Returns `Connection | null`.

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

- [ ] **Schema-version advertisement to coordinator** — `TypescriptCoordinator`
  reads `SUPERVISOR_API_VERSION` from bundle metadata (e.g.
  `airflow-metadata.yaml`) and passes it to the supervisor so the Cadwyn
  migrator can downgrade outgoing frames. Mirrors the Java JAR-manifest
  `Airflow-Java-SDK-Version` pattern.

- [ ] **`airflow-metadata.yaml` convention** — sidecar file declaring
  `dags: {…}` + `supervisor_schema_version`. Unblocks the
  `TypescriptCoordinator` BundleScanner analog (resolves the
  `bundle.mjs`-hardcoded TODO at `coordinators/typescript/coordinator.py`
  lines 113-120).

- [ ] **Per-TI heartbeat** — coordinator mode relies on the Python
  supervisor; edge mode needs its own heartbeat interval.

- [ ] **AbortSignal / graceful SIGTERM** — `ctx.signal` exists but never
  aborts in coordinator mode. Wire SIGTERM to the abort controller.

- [ ] **TaskContext enrichment** — extend with `dagRunConf`, `maxTries`,
  `taskRescheduleCount` from the `TIRunContext` response. Add
  `context_carrier` for OTel trace propagation.

- [ ] **Log forwarding (edge)** — structured log channel over HTTP for
  edge mode.

- [ ] **Subprocess isolation** — spawn each task in a child process for
  memory isolation. See "Known limitations" below.

## Known limitations

**Single-threaded event loop.** Node.js runs user task code on the same
event loop as the comm channel, log channel, and any future heartbeat
timer. If a task handler blocks synchronously (CPU-heavy loop, blocking
I/O), the entire process stalls — no RPC responses, no log flushing, no
signal handling.

This is the same fundamental constraint any single-threaded runtime has.
The Python SDK avoids it by forking a subprocess per task; the Java SDK
gets JVM threads. The planned fix for the TS SDK is **subprocess
isolation** (`worker_threads` or `child_process.fork()`): the main
process keeps control of the event loop while the task runs in an
isolated context. This matches the Python supervisor's architecture.

Until subprocess isolation lands, task authors should:
- Use `async`/`await` for I/O (never blocking calls)
- Offload CPU-heavy work to `worker_threads` manually
- Keep task handlers non-blocking

**No graceful SIGTERM.** `ctx.signal` exists for API parity with edge
mode but never fires in coordinator mode. When the coordinator sends
SIGTERM, Node exits before any user cleanup runs. Planned fix: wire
SIGTERM to the `AbortController` so handlers can observe
`ctx.signal.aborted` and clean up.

## Background

Background research (architecture deep-dive, design choices, scalability
spikes, end-to-end findings against real Airflow) lives in a separate
research repo and is **not authoritative** for this implementation. The
load-bearing docs in this folder are this file, [`SDK_COMPARISON.md`](SDK_COMPARISON.md),
and the upstream PRs cited at the top.
