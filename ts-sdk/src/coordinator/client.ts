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

// Coordinator-mode TaskClient — wraps `CommChannel.request()` so
// handlers can look up Variables and pull/push XCom values mid-task.
// The supervisor multiplexes API calls through the comm socket.

import type { CommChannel } from "./comm-channel.js";
import type { TaskContext } from "../types.js";
import {
    type TaskClient,
    type GetXComOpts,
    type SetXComOpts,
    VariableNotFoundError,
} from "../client.js";

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
