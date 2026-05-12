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

export interface GetXComOpts {
    key: string;
    dagId: string;
    taskId: string;
    runId: string;
    /** -1 / undefined for non-mapped tasks. */
    mapIndex?: number;
    includePriorDates?: boolean;
}

export interface SetXComOpts {
    key: string;
    value: unknown;
    dagId: string;
    taskId: string;
    runId: string;
    mapIndex?: number;
}

export interface CoordinatorClient {
    /** Look up an Airflow Variable. Returns `null` when the key is
     *  missing (supervisor sent `ErrorResponse`) or stored with a null
     *  value. Throws on any other RPC error. */
    getVariable(key: string): Promise<string | null>;

    /** Pull an XCom value. Returns `null` when the row is missing. */
    getXCom(opts: GetXComOpts): Promise<unknown>;

    /** Push an XCom value. Resolves once the supervisor has persisted
     *  it (the supervisor's response is an empty arity-3 frame). */
    setXCom(opts: SetXComOpts): Promise<void>;
}

export function createCoordinatorClient(comm: CommChannel): CoordinatorClient {
    return {
        async getVariable(key: string): Promise<string | null> {
            const frame = await comm.request({ type: "GetVariable", key });
            if (isErrorFrame(frame)) {
                // Variable-not-found is a normal case — return null instead of throwing.
                // Any other ErrorType propagates.
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

        async getXCom(opts: GetXComOpts): Promise<unknown> {
            const frame = await comm.request({
                type: "GetXCom",
                key: opts.key,
                dag_id: opts.dagId,
                task_id: opts.taskId,
                run_id: opts.runId,
                map_index: opts.mapIndex ?? null,
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
                dag_id: opts.dagId,
                task_id: opts.taskId,
                run_id: opts.runId,
                map_index: opts.mapIndex ?? null,
            });
            if (isErrorFrame(frame)) {
                throw rpcError("SetXCom", frame);
            }
            // Success: supervisor sent an arity-3 frame with body=null.
        },
    };
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

function isNotFoundError(frame: MaybeErrorFrame): boolean {
    const err = extractError(frame);
    // ErrorType in the SDK is a free-form string; supervisor uses
    // "VARIABLE_NOT_FOUND" and "XCOM_NOT_FOUND" for the not-found paths.
    return typeof err === "string" && err.toUpperCase().includes("NOT_FOUND");
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
