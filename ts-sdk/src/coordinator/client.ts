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

// CoordinatorClient — wraps `CommChannel.request()` so handlers can
// look up Variables / Connections (TODO) and pull/push XCom values
// mid-task. Only available in coordinator mode (`startCoordinatorRuntime`),
// where the supervisor multiplexes API calls through the comm socket.
//
// Edge worker mode hits the Edge API directly and will get an
// equivalent client in a follow-up PR.

import type { CommChannel } from "./comm-channel.js";
import type { TaskContext } from "../types.js";

/** XCom locator. `dagId`/`taskId`/`runId` default to the running
 *  task's own context — pass them only to pull another task's XCom. */
export interface GetXComOpts {
    key: string;
    dagId?: string;
    taskId?: string;
    runId?: string;
    /** -1 / undefined for non-mapped tasks. */
    mapIndex?: number;
    includePriorDates?: boolean;
}

/** XCom push target. `dagId`/`taskId`/`runId` default to the running
 *  task's own context. */
export interface SetXComOpts {
    key: string;
    value: unknown;
    dagId?: string;
    taskId?: string;
    runId?: string;
    mapIndex?: number;
}

export interface TaskClient {
    /** Look up an Airflow Variable. Returns `null` when the key is
     *  missing (supervisor sent a `*_NOT_FOUND` `ErrorResponse`) or
     *  stored with a null value. Throws on any other RPC error.
     *
     *  Note: this is a deliberate JS-idiomatic divergence from Python's
     *  `Variable.get`, which raises on a missing key. Use
     *  `getVariableOrThrow` for the Python-parity behaviour.
     *  See DESIGN.md CD-1. */
    getVariable(key: string): Promise<string | null>;

    /** Like `getVariable`, but throws `VariableNotFoundError` when the
     *  key is missing (matches Python `Variable.get` with no default). */
    getVariableOrThrow(key: string): Promise<string>;

    /** Pull an XCom value. Returns `null` when the row is missing.
     *  Locator fields default to the current task's context. */
    getXCom(opts: GetXComOpts): Promise<unknown>;

    /** Push an XCom value. Resolves once the supervisor has persisted
     *  it (the supervisor's response is an empty arity-3 frame).
     *  Target fields default to the current task's context. */
    setXCom(opts: SetXComOpts): Promise<void>;
}

/** @deprecated Renamed to {@link TaskClient} — the interface is the
 *  cross-mode contract (Edge mode will implement it too), so it must
 *  not be named after one transport. Kept for one release. */
export type CoordinatorClient = TaskClient;

/** Thrown by {@link TaskClient.getVariableOrThrow} on a missing key. */
export class VariableNotFoundError extends Error {
    constructor(public readonly key: string) {
        super(`Variable not found: ${key}`);
        this.name = "VariableNotFoundError";
    }
}

export function createCoordinatorClient(
    comm: CommChannel,
    ctx: TaskContext,
): TaskClient {
    // Non-mapped tasks carry mapIndex = -1; the wire wants null there.
    const ctxMapIndex = ctx.mapIndex >= 0 ? ctx.mapIndex : null;

    async function getVariableFrame(key: string) {
        return comm.request({ type: "GetVariable", key });
    }

    const client: TaskClient = {
        async getVariable(key: string): Promise<string | null> {
            const frame = await getVariableFrame(key);
            if (isErrorFrame(frame)) {
                // *_NOT_FOUND is a normal case — null, not a throw.
                if (isNotFoundError(frame)) return null;
                throw rpcError("GetVariable", frame);
            }
            const body = frame.body as { type?: string; value?: string | null };
            if (body?.type !== "VariableResult") {
                throw new Error(
                    `GetVariable: unexpected response type ${JSON.stringify(body?.type)}`,
                );
            }
            return body.value ?? null;
        },

        async getVariableOrThrow(key: string): Promise<string> {
            const frame = await getVariableFrame(key);
            if (isErrorFrame(frame)) {
                if (isNotFoundError(frame)) throw new VariableNotFoundError(key);
                throw rpcError("GetVariable", frame);
            }
            const body = frame.body as { type?: string; value?: string | null };
            if (body?.type !== "VariableResult") {
                throw new Error(
                    `GetVariable: unexpected response type ${JSON.stringify(body?.type)}`,
                );
            }
            if (body.value == null) throw new VariableNotFoundError(key);
            return body.value;
        },

        async getXCom(opts: GetXComOpts): Promise<unknown> {
            const frame = await comm.request({
                type: "GetXCom",
                key: opts.key,
                dag_id: opts.dagId ?? ctx.dagId,
                task_id: opts.taskId ?? ctx.taskId,
                run_id: opts.runId ?? ctx.runId,
                map_index: opts.mapIndex ?? ctxMapIndex,
                include_prior_dates: opts.includePriorDates ?? false,
            });
            if (isErrorFrame(frame)) {
                if (isNotFoundError(frame)) return null;
                throw rpcError("GetXCom", frame);
            }
            const body = frame.body as { type?: string; value?: unknown };
            if (body?.type !== "XComResult") {
                throw new Error(
                    `GetXCom: unexpected response type ${JSON.stringify(body?.type)}`,
                );
            }
            return body.value ?? null;
        },

        async setXCom(opts: SetXComOpts): Promise<void> {
            const frame = await comm.request({
                type: "SetXCom",
                key: opts.key,
                value: opts.value,
                dag_id: opts.dagId ?? ctx.dagId,
                task_id: opts.taskId ?? ctx.taskId,
                run_id: opts.runId ?? ctx.runId,
                map_index: opts.mapIndex ?? ctxMapIndex,
            });
            if (isErrorFrame(frame)) {
                throw rpcError("SetXCom", frame);
            }
            // Success: supervisor sent an arity-3 frame with body=null.
        },
    };
    return client;
}

interface MaybeErrorFrame {
    body: unknown;
    error?: unknown;
}

function isErrorFrame(frame: MaybeErrorFrame): boolean {
    if (frame.error != null) return true;
    const body = frame.body as { type?: string } | null;
    return body?.type === "ErrorResponse";
}

// Exact supervisor ErrorType codes that mean "absent", not "failed".
// Matches `airflow.sdk.exceptions.ErrorType` — a substring test would
// misread a value/error that merely contains "NOT_FOUND". See
// DESIGN.md CD-2.
const NOT_FOUND_CODES = new Set([
    "VARIABLE_NOT_FOUND",
    "XCOM_NOT_FOUND",
    "CONNECTION_NOT_FOUND",
]);

function isNotFoundError(frame: MaybeErrorFrame): boolean {
    const err = extractError(frame);
    return err != null && NOT_FOUND_CODES.has(err);
}

function rpcError(op: string, frame: MaybeErrorFrame): Error {
    const err = extractError(frame) ?? "unknown_error";
    return new Error(`${op} failed: ${err}`);
}

function extractError(frame: MaybeErrorFrame): string | null {
    if (typeof frame.error === "string") return frame.error;
    if (frame.error && typeof frame.error === "object") {
        const e = (frame.error as Record<string, unknown>).error;
        if (typeof e === "string") return e;
    }
    const body = frame.body as { type?: string; error?: string } | null;
    if (body?.type === "ErrorResponse" && typeof body.error === "string") {
        return body.error;
    }
    return null;
}
