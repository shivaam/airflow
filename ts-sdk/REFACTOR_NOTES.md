# Refactor & Architecture Notes (ts-sdk)

Living scratchpad. Ideas captured as we learn the design — to act on later,
not now. Status legend: `IDEA` (raw) · `AGREED` (decided, do it) ·
`DEFERRED` (decided, but later) · `DONE` · `REJECTED` (with reason).

## Hard constraint (read first)

The **wire format** must byte-match the Python supervisor (`comms.py`):
`[4-byte BE length][msgpack array, arity 2=request / 3=response]`, id
correlation, and the StartupDetails-as-arity-3 wart. `frames.ts` encodes
this and must stay spec-faithful. **Everything above the wire (internal
structure of comm-channel / runtime / client) is free to restructure** —
this is greenfield, author-owned, uncommitted code; no cross-language
structural-parity obligation.

Guiding principle that keeps recurring: **name an abstraction for what it
means to its consumer, not for one implementation or its current
mechanism.**

---

## 1. comm-channel: collapse the Category-B receive side — AGREED

Today there are **four** mechanisms for one direction (supervisor-initiated
frames): `waitForFrame`, `onIncoming`, `inbox`, `dispatchIncoming`, plus a
4-branch `route` cascade.

Plan:
- Make "wait for the first frame" part of **opening** the channel, so there
  is no window where a frame arrives with no consumer → **`inbox` deleted**
  (the timing gap it patches stops existing).
- Option A: `CommChannel.connect()` resolves to `{ channel, firstFrame }`.
- Option B: mandatory incoming-handler passed at construction.
- Leaning Option A (reads cleanly at runtime.ts:94-98, see item 5).
- Net: 6 state fields → 2 (`pendingReplies` + one handler); `route` → 2
  branches (is-it-an-answer-to-my-question? yes→pendingReplies, no→handler).
- Push the "first frame is special (parse vs task)" logic into the
  runtime's own tiny state machine, not the channel.
- NOT introducing an AsyncQueue (considered, rejected — adds a concurrency
  primitive we don't need; the connect()-returns-first-frame shape is
  simpler and queue-free).

## 2. Naming: `onIncoming` → `onSupervisorInitiatedFrame` — AGREED

`onIncoming` is vague — *everything* off the socket is "incoming",
including responses to our own requests. The name should say it's the
supervisor starting a conversation we didn't start.

## 3. Naming: `CoordinatorClient` interface → mode-agnostic — AGREED

The **interface** is the cross-mode contract that edge mode will also
implement; naming it after one mode will read as a lie once
`createEdgeClient(): CoordinatorClient` exists.
- Interface → `TaskClient` (or `AirflowClient` / `RuntimeClient` — pick
  one, TBD).
- Keep `createCoordinatorClient(comm)` and (future) `createEdgeClient(edge)`
  as the two transport implementations of that interface.

## 4. Folder symmetry: add `src/edge/` — AGREED

`coordinator/` is a folder; edge-mode files are loose at `src/` root.
Mirror the structure:
- `src/edge/` ← `edge-client.ts`, `worker.ts`, `worker-options.ts`,
  (likely) `execution-client.ts`.
- Re-check `src/index.ts` exports + `package.json` `"exports"`/`"files"`
  after the move (single public entry must still resolve).
- RESOLVED: `execution-client.ts` is **edge-only** (imported only by
  `worker.ts`; coordinator never makes HTTP — it tunnels Execution-API
  semantics through the comm socket). It moves into `src/edge/`.
- Note: `edge-client.ts` (Edge Worker API: fetch/heartbeat/ack) and
  `execution-client.ts` (Execution API: mark success/failed, XCom,
  two-token lifecycle) are **distinct** responsibilities, not duplicates —
  the edge worker uses both. Keep them as separate files in `src/edge/`.

## 5. runtime.ts — do NOT rewrite — RESOLVED (no action)

Read in full. It is a clean, thin, linear orchestrator (parseArgs →
connect → first-frame branch → handler → finally-close). Earlier suspicion
that it "needs rework" was a vibe; tracing it disproved that. Only change:
it *benefits* from item 1 — if `connect()` returns the first frame,
runtime.ts:94-98 collapse into one line and `waitForFrame` disappears from
its surface. No standalone runtime.ts refactor.

## 6. `decodePayload` validation tidy — DEFERRED

User said "later modify". Chosen shape = "option 2": keep
**specific per-failure error messages** (valuable at a cross-language wire
boundary) but tidy construction — drop the mutable `frame` var, single
return expression, `...(error != null ? { error } : {})` spread. Pure
internal cleanup, no wire change.

---

## Robustness / failure topics to work through

Workflow: Shivam reasons from intuition FIRST, then we audit whether the
code handles it. Do not pre-spoil with answers. Status per item:
`TODO` (not discussed) · `DISCUSSED` · `AUDITED` (checked vs code) ·
`GAP` (code does NOT handle — becomes a refactor item) · `OK`.

Lifecycle / hangs
- [TODO] L1: supervisor never sends first frame — does `waitForFrame()`
  hang forever? startup timeout?
- [TODO] L2: `request()` sent, supervisor never replies — hang forever?
  per-request timeout?
- [TODO] L3: after task finishes, does the Node process actually exit, or
  do dangling sockets/listeners keep the event loop alive?

Connection death
- [TODO] C1: socket closes mid-frame (partial frame buffered in
  FrameReader) — buffered bytes + waiters?
- [TODO] C2: `handleClose` rejects `pendingReplies` — does it also wake
  `waitForFrame` / `inbox` waiters, or can they hang?
- [TODO] C3: Node crashes mid-task (uncaught throw) — supervisor gets clean
  failure or hangs?

Backpressure / volume
- [TODO] B1: Node logs faster than supervisor drains — log channel buffer
  unbounded (memory)?
- [TODO] B2: `sock.write()` with full kernel buffer — Node writable
  backpressure handled?
- [TODO] B3: very large XCom value (multi-MB frame) — framing sane?
  (relates to the `Buffer.concat` O(n^2) note in item-1 discussion)

Cancellation
- [TODO] X1: execution timeout / SIGTERM while task hangs — `buildContext`
  admits "no SIGTERM-drain story"; what happens to in-flight work +
  the response frame?

Concurrency correctness
- [TODO] N1: many `request()`s in flight (pipelining) — id↔response
  correlation stays correct? any accidental FIFO-ordering assumption?

## Parking lot / open questions

- Client ergonomics: `GetXComOpts` makes the user re-pass
  `dagId/taskId/runId` on every call, but those are already in
  `TaskContext`. Consider binding the client to the context so calls are
  `client.getXCom({ key })`. Bigger API decision — revisit when the edge
  client lands (so both modes get the same ergonomic shape).
- Confirm item 1 Option A doesn't complicate the parse-vs-task branch
  (runtime.ts:102-105) — it shouldn't, the branch just moves to consume the
  returned `firstFrame`.
