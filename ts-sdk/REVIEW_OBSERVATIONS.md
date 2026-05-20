# Coordinator Code Review — Observations

Observations from self-review of the coordinator client module.
Each item is tagged as a **concept** (something to understand, no code change)
or a **fix** (an actionable code change, possibly for a follow-up PR).

Status legend: `[ ]` open, `[x]` addressed in this session, `[~]` deferred.

---

## Concepts (things to internalise)

### C1. Why Deferred exists
**Files:** `deferred.ts`, `comm-channel.ts`

A standard `new Promise(...)` only exposes `resolve`/`reject` inside the
executor callback. `Deferred<T>` externalises them so the *producer* (socket
`data` handler calling `greeting.resolve(frame)`) and the *consumer*
(`connect()` doing `await greeting.promise`) can live in completely different
call stacks and timings. The `done` flag makes `resolve`/`reject` idempotent
and powers the `settled` getter used in `deliverSupervisorFrame` to tell
"greeting already arrived" from "still waiting."

This is the classic "Deferred" / "CompletableFuture" pattern — common in Node
networking code whenever the value arrives on a socket but is awaited elsewhere.got i

### C2. Promise-as-buffer (greeting race)
**Files:** `comm-channel.ts:59-63`, `comm-channel.ts:80-85`

A settled Promise holds its value forever. If the supervisor sends the greeting
before `connect()` reaches `await greeting.promise`, the value is already
stored — no explicit buffer or state machine needed. If `connect()` awaits
first, it suspends until `greeting.resolve(frame)` fires. Either ordering
works. The promise *is* the buffer.

### C3. Frame routing — arity, not id
**Files:** `comm-channel.ts:133-147`, `frames.ts:34-50`

Both directions start their id counters at 0, so id alone can't tell request
from response. The codec uses msgpack array *arity*: arity-2 = request,
arity-3 = response. `route()` checks `frame.isResponse` first, then matches
by id in `pendingReplies`. Anything non-response is supervisor-initiated
(greeting or protocol anomaly).

### C4. One process per task — no concurrent multiplexing
**Files:** `runtime.ts`

The supervisor spawns one Node subprocess per task instance. Each process
handles exactly one `StartupDetails`, sends one terminal frame, and exits.
There are never concurrent tasks on the same comm channel. The id-based
request/reply routing in `CommChannel` *would* handle concurrency correctly,
but it's not needed in the current architecture.

### C5. "removed" task state
**Files:** `runtime.ts:164`, `protocol.ts:98`

`removed` is a standard Airflow `TaskInstanceState` (defined in
`task-sdk/.../api/datamodels/_generated.py`). It means "this task no longer
exists in the DAG definition." Here it signals to the scheduler that the TS
bundle has no handler for the requested task — functionally equivalent to
"this task was removed from the code."

### C6. task_outlets and outlet_events
**Files:** `runtime.ts:187-193`, `protocol.ts:88-95`

These are Airflow's **Asset** (formerly Dataset) mechanism:
- `task_outlets` — URIs of assets this task *produces*; triggers downstream
  DAGs that depend on them.
- `outlet_events` — metadata/context attached to each outlet update.

The Execution API's `TISuccessStatePayload` requires both to be lists (not
null). The TS SDK doesn't support asset declarations yet, so `[]` is the
correct placeholder.

### C7. Namespaced task lookup (`dag_id.task_id`)
**Files:** `runtime.ts:153-158`

Not from the Java SDK or the Edge SDK. It's a coordinator-mode convenience:
when a single Node bundle serves multiple DAGs, two DAGs could both have a
task named `"extract"`. Users can disambiguate with
`registerTask("my_dag.extract", handler)`. Bare `task_id` still works when
there's no collision. The Edge SDK dispatches directly per-job, so it doesn't
need this.

### C8. `--comm=` parsing only supports `=` form
**Files:** `runtime.ts:64-77`

`arg.startsWith("--comm=")` requires the `=`. The space-separated form
`--comm host:port` won't match. This is intentional — the supervisor
constructs the argv with `=`, and the user never types it. No change needed.

### C9. opts vs process.argv indirection
**Files:** `runtime.ts:82-90`

`StartCoordinatorRuntimeOptions` serves two purposes:
1. **Testing** — unit tests pass addresses directly without faking
   `process.argv`.
2. **Programmatic embedding** — starting the runtime from another Node
   process without manipulating global state.

The fallback to `process.argv` is the production path when the Python
supervisor spawns `node my-bundle.mjs --comm=... --logs=...`.

---

## Fixes (code changes)

### F1. Remove deprecated `CoordinatorClient` alias
**Files:** `client.ts`, `index.ts`, `src/index.ts`
**Status:** [x] Done this session.

Not shipped to npm yet — no backwards-compat obligation. Removed the type
alias and all re-exports.

### F2. Trim barrel exports to public-API-only
**Files:** `coordinator/index.ts`, `src/index.ts`
**Status:** [x] Done this session.

Removed re-exports of internal types (`Frame`, `LogChannel`, `LogLevel`,
`LogRecord`, `StartupDetails`, `DagFileParseRequest`, `MsgFromSupervisor`,
`MsgFromRuntime`, protocol types, etc.). They remain importable via deep
paths for tests and advanced users.

Kept: `startCoordinatorRuntime`, `StartCoordinatorRuntimeOptions`,
`TaskClient`, `GetXComOpts`, `SetXComOpts`, `VariableNotFoundError`.

### F3. Improve startup log — list task names
**Files:** `runtime.ts:95-98`
**Status:** [x] Done this session.

Changed from logging `registered_tasks: count` to logging both the list of
names and the count, so you can immediately see what was registered.

### F4. Unified `TaskClient` abstraction across coordinator and edge
**Files:** `src/client.ts` (new), `coordinator/client.ts`, `edge/task-client.ts` (new),
`edge/execution-client.ts`, `edge/worker.ts`, `types.ts`, `src/index.ts`
**Status:** [x] Done this session.

Extracted `TaskClient` interface into shared `src/client.ts`. Created
`edge/task-client.ts` (`createEdgeTaskClient()`) backed by Execution API
HTTP. `TaskHandlerArgs.client` is now required — both modes provide it.

### F5. Auto-push handler return value to XCom
**Files:** `coordinator/runtime.ts`, `edge/worker.ts`
**Status:** [x] Done this session.

Non-undefined return values are automatically pushed to XCom under the
key `"return_value"` (matches Python's `@task` behaviour). Updated both
coordinator and edge paths.

### F6. Abstract away frame references in client methods
**Files:** `coordinator/client.ts:107-138`
**Status:** [~] Deferred — not worth it yet.

Each client method interprets a `Frame` into domain types. Could extract a
helper, but with only 4 methods the duplication is minimal and the
explicitness helps debugging. Revisit if the method count grows.

### F7. `parseArgs` removed from public exports
**Files:** `coordinator/index.ts`
**Status:** [x] Done this session (side effect of F2).

`parseArgs` was exported but is an internal utility. Removed from barrel
export alongside the other internals.

