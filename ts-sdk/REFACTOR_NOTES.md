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

## 1. comm-channel: collapse the Category-B receive side — DONE
> DONE 2026-05-12 (commit `2f3ce280fd`, Option A). Chose A. One first-
> frame latch retained (the supervisor frame can land before
> `connect()` attaches its awaiter) — not an unbounded inbox. `route`
> is the 2-branch arity decision as planned. The "tiny state machine"
> stayed a single parse-vs-task branch in runtime.ts (deliberately did
> NOT build a 2-state FSM abstraction — name for the consumer).

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

## 2. Naming: `onIncoming` → `onSupervisorInitiatedFrame` — DONE
> DONE 2026-05-12 (folded into commit `2f3ce280fd`).

`onIncoming` is vague — *everything* off the socket is "incoming",
including responses to our own requests. The name should say it's the
supervisor starting a conversation we didn't start.

## 3. Naming: `CoordinatorClient` interface → mode-agnostic — DONE
> DONE 2026-05-12 (commit `d6deaaf38f`). Picked `TaskClient`.
> `CoordinatorClient` kept as a `@deprecated` alias for one release.
> Done together with the parking-lot ergonomics item (context-bound
> client) so the whole surface settled in one pass.

The **interface** is the cross-mode contract that edge mode will also
implement; naming it after one mode will read as a lie once
`createEdgeClient(): CoordinatorClient` exists.
- Interface → `TaskClient` (or `AirflowClient` / `RuntimeClient` — pick
  one, TBD).
- Keep `createCoordinatorClient(comm)` and (future) `createEdgeClient(edge)`
  as the two transport implementations of that interface.

## 4. Folder symmetry: add `src/edge/` — DONE
> DONE 2026-05-12 (commit `b1ad8684ef`). git-tracked as renames.
> package.json exports/files are dir-level — public entry unchanged.

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

## 6. `decodePayload` validation tidy — DONE
> DONE 2026-05-12 (commit `d9c07de68b`). Option 2 as planned;
> per-failure messages kept; single return + conditional spread.

User said "later modify". Chosen shape = "option 2": keep
**specific per-failure error messages** (valuable at a cross-language wire
boundary) but tidy construction — drop the mutable `frame` var, single
return expression, `...(error != null ? { error } : {})` spread. Pure
internal cleanup, no wire change.

---

## Robustness / failure topics to work through

Workflow note: the original intent was Shivam-reasons-first, then
audit. For the autonomous polish push these were audited straight
against code (research+log only, no fixes) — see the full evidence in
`airflow-task-sdk/experiments/coordinator/ROBUSTNESS-AUDIT.md`. The
Socratic walk-through is still worth doing for L1/L2/X1 when those get
fixed; the verdicts below won't change, but the reasoning is good to
internalize.

Status: `AUDITED` done · `GAP` code does not handle (future fix) ·
`GAP*` gap by design/documented · `OK`.

Lifecycle / hangs
- [GAP] L1: no startup timeout — connected-but-silent supervisor hangs
  Node forever. High. (release blocker)
- [GAP] L2: no per-request timeout — never-answered RPC hangs the
  handler forever. High. (release blocker)
- [OK] L3: clean exit — finally ends both sockets, no unref'd timers.

Connection death
- [OK] C1: partial-frame buffer GC'd, pending rejected, no hang.
- [OK] C2: **improved by item 1** — first-frame awaiter has its own
  close→reject; `pendingReplies` rejected. No stranded waiter.
- [OK] C3: handler throw → TaskState{failed} (tested); hard crash →
  supervisor derives FAILED from exit (E4-proven, no zombie).

Backpressure / volume
- [GAP] B1: `LogChannel.send` ignores `sock.write` backpressure —
  unbounded log buffering for a chatty/long task. Medium.
- [OK] B2: comm path is lockstep small frames; only the log channel
  (= B1) is exposed.
- [GAP] B3: correctness OK (uint32 framing matches Python 2^32);
  `FrameReader` `Buffer.concat`-per-chunk is O(n²) for large frames.
  Low–Med.

Cancellation
- [GAP*] X1: by design — `ctx.signal` never aborts in coordinator
  mode; SIGTERM → FAILED, no zombie (E4). No graceful drain / handler
  cleanup hook. Documented in COORDINATOR.md "Deferred".

Concurrency correctness
- [OK] N1: id-map + arity correlation, no FIFO assumption. Design-
  correct; add a pipelining stress test when hardening.

## Parking lot / open questions

- [DONE 2026-05-12, commit `d6deaaf38f`] Client ergonomics: client is
  now bound to `TaskContext` — `client.getXCom({ key })` works;
  `dagId/taskId/runId` optional, default from ctx, explicit override
  still allowed for cross-task XCom. Promoted out of the parking lot
  and done with item 3 (one API-surface pass) rather than waiting for
  the edge client; the edge `createEdgeClient` will implement the same
  `TaskClient` interface so both modes still converge.
- [CONFIRMED] Item 1 Option A did not complicate the parse-vs-task
  branch — `runtime.ts` just consumes the returned `firstFrame`;
  integration regression oracle 6/6 unchanged.

## Remaining for a future hardening push (not this scope)

Robustness GAPs from the audit (see
`airflow-task-sdk/experiments/coordinator/ROBUSTNESS-AUDIT.md`),
ordered: **L1** (startup timeout), **L2** (per-request timeout) —
release blockers — then **B1** (log backpressure), **B3** (O(n²)
large-frame reassembly), **X1** (graceful SIGTERM drain / cancellation,
by-design gap). Plus a pipelining stress test for N1.
