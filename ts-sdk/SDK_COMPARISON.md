# SDK Comparison: TypeScript vs Java (Kotlin) vs Python

Side-by-side comparison of the three Airflow task SDK runtimes.
Python is the reference implementation; Java and TS are coordinator-mode SDKs.

**Source PRs:**
- Python SDK: `task-sdk/` in apache/airflow (merged, the reference)
- Java SDK: [PR #65956](https://github.com/apache/airflow/pull/65956) (jason810496, open)
- Coordinator layer: [PR #65958](https://github.com/apache/airflow/pull/65958) (jason810496, open)
- TS SDK: this repo

---

## Architecture

All three runtimes share the same production supervisor: the Python
`WatchedSubprocess` + `BaseCoordinator`. The coordinator-mode SDKs
(Java, TS) are **subprocess-side only** — they connect to TCP sockets,
receive work, execute it, and report back. The Python supervisor handles
all Execution API calls (heartbeat, state reporting, RPC proxying).

```
Python WatchedSubprocess (handles heartbeat, state, Execution API)
  └── forks child process
        └── BaseCoordinator (PR #65958)
              ├── opens TCP servers (comm + logs)
              ├── spawns: java ... --comm=... --logs=...
              │      or:  node  ... --comm=... --logs=...
              ├── forwards StartupDetails to subprocess
              ├── bridges bytes: supervisor ↔ subprocess (raw, no interpretation)
              └── bridges logs: subprocess → structlog
```

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **Execution mode** | Forked subprocess (same machine as supervisor) | Spawned JVM subprocess via `BaseCoordinator` | Spawned Node subprocess via `BaseCoordinator` |
| **Comm transport** | Unix socket (socketpair from fork) | TCP socket (`--comm=host:port`) | TCP socket (`--comm=host:port`) |
| **Log transport** | Unix socket (socketpair) | TCP socket (`--logs=host:port`) | TCP socket (`--logs=host:port`) |
| **Wire format** | msgpack, length-prefixed | msgpack, length-prefixed | msgpack, length-prefixed |
| **Frame arity** | arity-2 = request, arity-3 = response | Same | Same |
| **Who spawns the process** | `WatchedSubprocess` (Python supervisor) | `BaseCoordinator` (Python, via `JavaCoordinator.task_execution_cmd`) | `BaseCoordinator` (Python, via `TypescriptCoordinator.task_execution_cmd`) |
| **Who handles RPC** | Python supervisor's `handle_requests` generator | Same — bytes bridged to Python supervisor | Same — bytes bridged to Python supervisor |

### Key insight
`BaseCoordinator` is a **transparent byte bridge**. It does not interpret
comm messages — it just forwards raw bytes between the Python supervisor
(fd 0) and the language subprocess (TCP socket). The Python supervisor's
`handle_requests` generator does all the RPC work (GetVariable, GetXCom,
etc.) for both Java and TS. The subprocess-side code in Java and TS is
architecturally equivalent.

---

## Task Lookup

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **How tasks are found** | `dag.task_dict[task_id]` — loads the DAG file, looks up by task_id in the parsed DAG | `bundle.dags[dagId]?.tasks[taskId]` — scanned from JARs at startup via `BundleScanner` | `getRegisteredTask(taskId)` — manual registry via `registerTask()` calls |
| **Task not found** | `sys.exit(1)` → supervisor sees non-zero → **FAILED** | `return TaskState("removed")` | `sendResponse({ state: "removed" })` |
| **Namespaced lookup** | No (task_id is globally unique in a DAG) | Yes — `bundle.dags[dagId]?.tasks[taskId]` naturally namespaces by DAG | Yes — tries `dag_id.task_id` then bare `task_id` |
| **DAG awareness** | Full — parses the actual DAG file | Full — scans JARs for DAG/task annotations | None — flat task registry, no DAG concept |

### Difference: task registration model
- **Python:** zero registration needed. The DAG file IS the source of truth. The supervisor loads it and finds the task class.
- **Java:** zero registration needed. `BundleScanner` discovers `@AirflowTask` annotated classes in JARs at startup, organized by DAG.
- **TS:** manual registration. User must call `registerTask("my_task", handler)` before `startCoordinatorRuntime()`. No DAG scanning.

### Difference: task-not-found behavior
Python treats it as a crash (`sys.exit(1)` → FAILED). Java and TS both
send `TaskState("removed")` over the comm socket. This divergence exists
because in Python the supervisor loads the DAG file itself, so "task not
found" means something went wrong with parsing. In coordinator mode, the
subprocess bundle legitimately might not have a handler for a task the
scheduler thinks exists (stale serialized DAG, misconfigured mapping).

---

## Client API (Variable / XCom / Connection)

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **Interface name** | No single interface (methods on `TaskInstance`) | `Client` interface | `TaskClient` interface |
| **Implementations** | Direct — task runner calls supervisor which calls Execution API | `CoordinatorClient` (comm socket) and `HttpExecApiClient` (HTTP) | `createCoordinatorClient` (comm socket only) |
| **getVariable** | `Variable.get(key)` raises on missing | `getVariable(key)` returns `VariableResponse` | `getVariable(key)` returns `string \| null`, `getVariableOrThrow(key)` throws |
| **getConnection** | `Connection.get(conn_id)` | `getConnection(id)` returns `ConnectionResponse` | **Not implemented yet** |
| **getXCom** | `XCom.get_one(...)` | `getXCom(key, dagId, taskId, ...)` returns `XComResponse` | `getXCom(opts)` returns `unknown` |
| **setXCom** | `XCom.set(...)` | `setXCom(key, value, ...)` | `setXCom(opts)` |
| **Return types** | Python domain objects | Generated API model classes (`VariableResponse`, `XComResponse`, `ConnectionResponse`) | Raw — `string \| null` for variables, `unknown` for XCom |
| **Error handling** | Exceptions from supervisor | `ApiError` exception | `isErrorFrame` check, throws `Error` or returns `null` for NOT_FOUND |

### Difference: Java reuses generated API types
The Java SDK imports `ConnectionResponse`, `VariableResponse`, `XComResponse`
from its generated Execution API client. The response types are shared between
`CoordinatorClient` (comm socket) and `HttpExecApiClient` (HTTP). The TS SDK
defines its own response interpretation inline — it checks
`frame.body.type === "VariableResult"` etc.

### Difference: Java has two Client implementations already
The Java SDK has the `Client` interface with two implementations:
- `CoordinatorClient` — sends requests over the comm socket (coordinator mode)
- `HttpExecApiClient` — calls the Execution API over HTTP directly

The TS SDK only has the coordinator path. The edge worker has a separate
`EdgeApiClient` that doesn't implement `TaskClient`.

---

## Comm Channel

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **Class** | `CommsDecoder` | `CoordinatorComm` | `CommChannel` |
| **Connection** | Inherited socket from fork | TCP connect to `--comm=host:port` | TCP connect via `connectTcp(addr)` |
| **Reading** | Synchronous `select` loop in supervisor | `suspend fun processOnce` — reads 4-byte prefix, then payload | Event-driven `sock.on("data")` with `FrameReader` buffer |
| **ID counter** | Supervisor manages IDs | `AtomicInt` counter | Simple `nextId++` |
| **Request/reply correlation** | Supervisor side: generator-based (`handle_requests` yields) | `communicateImpl`: sends request, calls `processOnce` inline to get response | `pendingReplies` Map — stores resolve callback by ID |
| **Greeting handling** | Supervisor sends `StartupDetails` as first frame | `handleIncoming` dispatches `StartupDetails` | `Deferred<Frame>` promise — resolves on first non-response frame |
| **Concurrent requests** | Not applicable (synchronous loop) | **Not supported** — `communicateImpl` is sequential (send, then read next frame) | Supported via ID-keyed `pendingReplies` map (though not used in practice) |

### Difference: concurrency model
- **Java:** `communicateImpl` sends a request then immediately reads the
  next frame. This is **sequential** — if two requests were in flight,
  the second could read the first's response. Not a problem because tasks
  are single-threaded.
- **TS:** uses a `pendingReplies` Map keyed by request ID. Multiple
  concurrent requests would route correctly. Overengineered for the
  current one-task-per-process model, but correct.
- **Python:** the supervisor side uses a generator (`handle_requests`)
  that yields frames. The task side uses `CommsDecoder` which is also
  sequential.

---

## Log Channel

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **Format** | structlog JSON lines over socket | JSON lines over TCP (`LogSender`) | JSON lines over TCP (`LogChannel`) |
| **Levels** | Full structlog levels | `ERROR`, `DEBUG` only | `debug`, `info`, `warning`, `error` |
| **Buffering** | structlog handles it | `LogSender` buffers messages before channel is configured | Writes directly to socket |
| **Level filtering** | structlog config | TODO (stub: always enabled) | No filtering |

---

## Frame Encoding/Decoding

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **Library** | `msgpack-python` | `org.msgpack:msgpack-core` (Java) | `@msgpack/msgpack` (npm) |
| **Serialization** | pydantic models -> dict -> msgpack | Jackson ObjectMapper -> Map -> msgpack packer | Plain objects -> msgpack `encode()` |
| **Deserialization** | msgpack -> dict -> pydantic discriminated union | msgpack -> Map -> type-keyed decoder map (`TaskSdkMessageDecoder`) | msgpack -> `decode()` -> type-checked inline |
| **Type registry** | Pydantic `Discriminator` on `type` field | Explicit `Map<String, Decoder>` per direction (e.g., `toBundleProcessTypes`) | No registry — inline `switch` on `body.type` |

### Difference: Java has per-direction type maps
The Java SDK has separate decoder maps for each direction:
- `toBundleProcessTypes` — what the bundle process receives (StartupDetails,
  DagFileParseRequest, responses)
- `toBundleClientTypes` — what the client receives (VariableResult, XComResult,
  ConnectionResult, ErrorResponse)

This makes it impossible to accidentally decode a message with the wrong
direction's types. The TS SDK has a single `asMsgFromSupervisor` for
incoming supervisor frames and relies on convention for outgoing.

---

## Success Reporting

| Aspect | Python | Java (Kotlin) | TypeScript |
|--------|--------|---------------|------------|
| **Message type** | `SucceedTask` (implicit — exit code 0 + no terminal_state override) | `SucceedTask` class with `taskOutlets`, `outletEvents` | `{ type: "SucceedTask", task_outlets: [], outlet_events: [] }` |
| **task_outlets** | Populated from task's `outlets` attribute | `emptyList()` by default, populated from `SucceedTask` constructor | Always `[]` (no asset support yet) |
| **outlet_events** | Populated from task's outlet events | `emptyList()` by default | Always `[]` |

---

## Features Not Yet in TS SDK

| Feature | Python | Java | TS |
|---------|--------|------|----|
| **Connections** | `getConnection()` | `getConnection()` | Missing |
| **DAG parsing** | Full | `DagParser.kt` with `BundleScanner` | Stub — returns empty `dags: {}` |
| **Asset/outlet support** | Full | Partial (passes through) | None (empty `[]`) |
| **Deferred tasks** | Yes | No | No |
| **Mapped tasks** | Yes | No | No |
| **Heartbeat** | Yes (supervisor) | No (handled by Python supervisor) | No (handled by Python supervisor) |
| **Retry logic** | Yes (UP_FOR_RETRY) | No | No |
| **Multiple client transports** | N/A (single path) | Yes (`CoordinatorClient` + `HttpExecApiClient`) | No (coordinator only; edge is separate) |
| **Level filtering in logs** | Yes | No (TODO) | No |

---

## Summary: What the TS SDK should consider

1. **Add `getConnection()`** — both Python and Java have it, and it's a
   common task need.
2. **Unify the Client interface** — Java already has `Client` with two impls
   (`CoordinatorClient`, `HttpExecApiClient`). The TS SDK's `TaskClient`
   interface is correct, but the edge worker's client doesn't implement it yet.
3. **Reuse API response types** — Java uses generated types (`VariableResponse`,
   `XComResponse`). TS could use its own generated types from the Execution API
   spec instead of inline frame interpretation.
4. **DAG parsing** — currently a stub. The Java SDK has real `BundleScanner` +
   `DagParser`. For TS, this means either a scanning convention (file-based
   discovery) or staying with manual `registerTask()`.
5. **`removed` state is correct** — matches Java SDK behavior. Python is
   different because its supervisor catches "task not found" before the
   subprocess even starts.
