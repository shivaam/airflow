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

registerTask("my_dag.say_hello", async ({ ctx }) => {
  console.log(`Running ${ctx.taskId} in run ${ctx.runId}`);
  return "ok";
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

Both modes share `registerTask()`, the `TaskContext` shape, and (when
shipped) the same XCom / Variables / Connections client.

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

## Background and rationale

The full design rationale, scale benchmarks, and end-to-end test results
that motivated this implementation live in the research repo at
`airflow-task-sdk/experiments/coordinator/`:

- `LEARNING.md` — coordinator architecture deep-dive
- `DESIGN.md` — TS coordinator design choices
- `REAL-AIRFLOW-FINDINGS.md` — end-to-end test against real Airflow
- `scalability/` — four scalability spikes (cold start, concurrent
  load, memory under sustained cadence, long-running tasks)

## Deferred to follow-up PRs

- **Client API** (`xcom`, `variables`, `connections`) — paired with
  the Edge-mode equivalent in PR #3 (per `types.ts` TODOs)
- **Bundle scanner / DAG parse path** — currently parse mode returns
  an empty DAG list. The Java provider's recommended path is the
  Python-stub DAG anyway, which doesn't traverse this code.
- **Per-TI heartbeat / mid-task SIGTERM handling** — coordinator
  forwards SIGTERM but Node exits before any user handler runs
- **AbortSignal wiring from the supervisor** — `ctx.signal` exists for
  API parity with Edge mode but never aborts in coordinator mode yet
