/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Task SDK message types — SDK-facing wrappers over the generated wire
// schema.
//
// Raw shapes come from `src/generated/supervisor.ts`, derived from the
// supervisor's canonical `schema.json` (Airflow PR #67235). That file
// also exports `SUPERVISOR_API_VERSION` — the Cadwyn version of the
// schema this SDK was generated against. It is not transmitted on the
// wire; the supervisor learns the SDK's schema version out-of-band
// (e.g. bundle metadata) and runs the migrator accordingly.
//
// ─── Why the generated types need wrapping (root causes) ─────────────
//
// json-schema-to-typescript faithfully reflects what's in `schema.json`,
// but `schema.json` is itself a faithful reflection of the *upstream
// Pydantic IR* — and that IR encodes a few things that don't translate
// cleanly to idiomatic TypeScript or to the actual wire contract.
// There are four recurring mismatches, each with a specific cause:
//
//   1. **Discriminator field comes out optional.**
//      Pydantic models declare the message type as a defaulted literal:
//        `type: Literal["GetVariable"] = "GetVariable"`.
//      Pydantic → JSON Schema turns the default into `"default":
//      "GetVariable"`, which schema-readers treat as "producer may
//      omit". The codegen therefore emits `type?: "GetVariable"`.
//      But TypeScript's discriminated-union narrowing only works on
//      *required* discriminator fields. So for any type that appears
//      in a union (MsgFromSupervisor, MsgFromRuntime) we re-narrow
//      `type` to a required literal. Pattern 2 below.
//
//   2. **Some fields hit the wire before the schema snapshot catches
//      up.** `TaskInstance.queue` and `.language` are real values the
//      supervisor sends today, but the 2026-06-16 schema snapshot
//      doesn't list them — the Pydantic model uses `extra="allow"` so
//      these fields pass through without being declared. We extend
//      `TaskInstance` with the fields we observe on the wire plus an
//      index-signature for true forward-compat. Pattern 3 below.
//
//   3. **String fields without `Literal[...]` annotations don't narrow.**
//      `TaskState.state` is typed as `str` in Pydantic (the runtime
//      validates allowed values via business logic, not via the type
//      annotation), so the schema → TS chain types it as bare `string`.
//      We re-narrow to the literal union we actually send so callers
//      get autocomplete and typos fail at compile time. Pattern 3.
//
//   4. **The schema describes the wire body Pydantic accepts, not
//      what downstream HTTP validators require.** `SucceedTask`'s
//      `task_outlets` and `outlet_events` are marked optional in the
//      schema, but the supervisor turns the message into a
//      `TISuccessStatePayload` which is then sent to the Execution API,
//      whose validator rejects `null` for these fields. So "optional
//      per schema" + "required per the API server" → we narrow to
//      required-with-`[]`-default at the SDK boundary so callers can't
//      construct an invalid message. Pattern 3.
//
// Patterns 2-4 are mechanical and short. The codegen still pays for
// itself: we get drift detection (a renamed field fails to compile),
// 70+ types we don't yet use available for free, and a single
// regeneration step on each schema bump.
//
// ─── How to add a new message type ────────────────────────────────────
//
// `schema.json` exposes ~85 message types; we currently use ~13. Adding
// a new one is mechanical — pick the lightest of three wrapper patterns
// and add it below in the right section (supervisor frame, runtime
// frame, or request/response payload).
//
// Pattern 1 — pass-through (most common, ~70% of types):
//
//     export type { MaskSecret } from "../generated/supervisor.js";
//
//   Use when the generated atom is fine as-is. Most non-discriminated
//   request/response payloads (`MaskSecret`, `SetRenderedFields`,
//   `GetDag`, etc.) need nothing more — callers treat them as bags of
//   fields, not as union members.
//
// Pattern 2 — discriminator-narrow (required for any type that appears
// in a discriminated union like `MsgFromSupervisor` or `MsgFromRuntime`):
//
//     import type { DeferTask as RawDeferTask } from "../generated/supervisor.js";
//     export type DeferTask = Omit<RawDeferTask, "type"> & {
//         type: "DeferTask";
//     };
//
//   The generated atom has `type?: "DeferTask"` (optional const), which
//   TS won't narrow on. Overriding to required literal fixes it.
//
// Pattern 3 — field-narrow / extend (rare; only when the generated
// shape is genuinely wrong for our wire usage):
//
//     // Schema marks `state: string`; narrow to literal union.
//     export type TaskStateMsg = Omit<RawTaskState, "type" | "state"> & {
//         type: "TaskState";
//         state: "failed" | "skipped" | "removed" | "up_for_retry";
//     };
//
//   Or to add fields the snapshot didn't capture:
//
//     export interface TaskInstance extends RawTaskInstance {
//         queue?: string | null;       // observed on wire, not in schema
//         language?: string | null;    // observed on wire, not in schema
//         [k: string]: unknown;        // forward-compat passthrough
//     }
//
//   Use sparingly — every hand-narrow is something the codegen will
//   never re-derive for you on a schema bump. Prefer fixing the
//   upstream Pydantic model when possible.
//
// After adding the type, re-run `pnpm run generate:supervisor` only if
// you've also bumped `schema/supervisor-schema.json` from upstream.
// Pure wrapper additions don't require regeneration.

import type {
    StartupDetails as RawStartupDetails,
    DagFileParseRequest as RawDagFileParseRequest,
    ErrorResponse as RawErrorResponse,
    SucceedTask as RawSucceedTask,
    TaskState as RawTaskState,
    DagFileParsingResult as RawDagFileParsingResult,
    TaskInstance as RawTaskInstance,
} from "../generated/supervisor.js";

export { SUPERVISOR_API_VERSION } from "../generated/supervisor.js";

// -------- Re-exports — generated atoms used by client / runtime --------
//
// These are clean enough out of the box: small, no discriminator
// narrowing issues for callers (we treat them as request/response
// payloads, not as union members).
export type {
    BundleInfo,
    TIRunContext,
    VariableResult,
    XComResult,
    ConnectionResult,
    GetVariable,
    GetXCom,
    SetXCom,
    GetConnection,
} from "../generated/supervisor.js";

// -------- TaskInstance: extend generated with wire-only fields --------

/** Supervisor's TaskInstance with the additional fields we observe on
 *  the wire (`queue`, `language`) but that the snapshot didn't capture,
 *  plus a forward-compat index signature so unknown fields pass through
 *  rather than getting stripped by structural typing. */
export interface TaskInstance extends RawTaskInstance {
    queue?: string | null;
    language?: string | null;
    [k: string]: unknown;
}

// -------- Frames from supervisor (narrowed discriminators) --------

export type StartupDetails = Omit<RawStartupDetails, "ti" | "type"> & {
    type: "StartupDetails";
    ti: TaskInstance;
};

export type DagFileParseRequest = Omit<RawDagFileParseRequest, "type"> & {
    type: "DagFileParseRequest";
};

export type ErrorResponse = Omit<RawErrorResponse, "type"> & {
    type: "ErrorResponse";
};

export type MsgFromSupervisor =
    | StartupDetails
    | DagFileParseRequest
    | ErrorResponse;

// -------- Frames from runtime (narrowed discriminators) --------

/** SucceedTask — schema marks task_outlets / outlet_events as optional,
 *  but the supervisor's Execution API rejects null for these fields, so
 *  we narrow both to required (empty array when none). */
export type SucceedTask = Omit<RawSucceedTask, "type" | "task_outlets" | "outlet_events"> & {
    type: "SucceedTask";
    task_outlets: unknown[];
    outlet_events: unknown[];
};

/** TaskState — schema types `state` as a bare string; narrow to the
 *  values the SDK actually sends so callers get an autocomplete-friendly
 *  union and typos fail at compile time. */
export type TaskStateMsg = Omit<RawTaskState, "type" | "state"> & {
    type: "TaskState";
    state: "failed" | "skipped" | "removed" | "up_for_retry";
};

export type DagFileParsingResult = Omit<RawDagFileParsingResult, "type"> & {
    type: "DagFileParsingResult";
};

export type MsgFromRuntime =
    | SucceedTask
    | TaskStateMsg
    | DagFileParsingResult;

// -------- Decoder: raw map → typed message --------

export function asMsgFromSupervisor(raw: unknown): MsgFromSupervisor {
    const body = normalizeBody(raw);
    switch (body.type) {
        case "StartupDetails":
        case "DagFileParseRequest":
        case "ErrorResponse":
            return body as unknown as MsgFromSupervisor;
        default:
            throw new Error(
                `Unsupported supervisor message type: ${JSON.stringify(body.type)}`,
            );
    }
}

function normalizeBody(raw: unknown): { type: string; [k: string]: unknown } {
    if (raw === null || typeof raw !== "object") {
        throw new Error(`Frame body must be a map, got ${typeof raw}`);
    }
    const mapLike = raw as Record<string, unknown>;
    const type = mapLike["type"];
    if (typeof type !== "string") {
        throw new Error(
            `Frame body missing string 'type'; got keys: ${Object.keys(mapLike).join(",")}`,
        );
    }
    return { ...mapLike, type };
}
