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
import type { LogChannel } from "./log-channel.js";
import type { TaskContext } from "../types.js";
import {
    type Connection,
    type TaskClient,
    type GetXComOpts,
    type SetXComOpts,
    VariableNotFoundError,
} from "../client.js";

/** Normalize a caller-supplied or context map index for the coordinator
 *  wire. Both `undefined` (caller omitted) and the user-facing `-1`
 *  non-mapped sentinel become `null`; mapped values (0…) pass through.
 *  The Python supervisor's XComOperations filters `-1` exactly like
 *  `None` (`task-sdk/.../api/client.py:511,554`), but we send `null`
 *  uniformly so the wire payload matches the documented contract and
 *  matches what `tests/coordinator/client.test.ts` asserts. */
function toWireMapIndex(m: number | null | undefined): number | null {
    return m == null || m < 0 ? null : m;
}

export function createCoordinatorClient(
    comm: CommChannel,
    ctx: TaskContext,
    logs: LogChannel | null = null,
): TaskClient {

    /** Send an RPC request, validate the response type, and extract the
     *  value. Returns `null` for NOT_FOUND errors. Throws on any other error
     *  or unexpected response type. Pass `expectedType: null` to skip type
     *  validation (e.g. SetXCom responds with `body=null`). */
    async function rpc<T>(
        op: string,
        expectedType: string | null,
        request: unknown,
        extract: (body: Record<string, unknown> | null) => T,
    ): Promise<T | null> {
        logs?.debug(`${op} request`);
        const frame = await comm.request(request);
        if (isErrorFrame(frame)) {
            const errCode = extractError(frame);
            if (isNotFoundError(frame)) {
                logs?.debug(`${op} not found`, { error: errCode });
                return null;
            }
            logs?.warning(`${op} failed`, { error: errCode });
            throw rpcError(op, frame);
        }
        const body = frame.body as Record<string, unknown> | null;
        if (expectedType !== null && body?.type !== expectedType) {
            logs?.error(`${op} unexpected response type`, {
                expected: expectedType,
                got: body?.type ?? null,
            });
            throw new Error(
                `${op}: unexpected response type ${JSON.stringify(body?.type)}`,
            );
        }
        logs?.debug(`${op} ok`);
        return extract(body);
    }

    const client: TaskClient = {
        async getVariable(key: string): Promise<string | null> {
            return rpc("GetVariable", "VariableResult",
                { type: "GetVariable", key },
                (body) => (body!.value as string) ?? null,
            );
        },

        async getVariableOrThrow(key: string): Promise<string> {
            const value = await client.getVariable(key);
            if (value == null) throw new VariableNotFoundError(key);
            return value;
        },

        async getXCom<T = unknown>(opts: GetXComOpts): Promise<T | null> {
            return rpc("GetXCom", "XComResult",
                {
                    type: "GetXCom",
                    key: opts.key,
                    dag_id: opts.dagId ?? ctx.dagId,
                    task_id: opts.taskId ?? ctx.taskId,
                    run_id: opts.runId ?? ctx.runId,
                    map_index: toWireMapIndex(opts.mapIndex ?? ctx.mapIndex),
                    include_prior_dates: opts.includePriorDates ?? false,
                },
                (body) => (body!.value as T) ?? null,
            );
        },

        async setXCom(opts: SetXComOpts): Promise<void> {
            await rpc("SetXCom", null,
                {
                    type: "SetXCom",
                    key: opts.key,
                    value: opts.value,
                    dag_id: opts.dagId ?? ctx.dagId,
                    task_id: opts.taskId ?? ctx.taskId,
                    run_id: opts.runId ?? ctx.runId,
                    map_index: toWireMapIndex(opts.mapIndex ?? ctx.mapIndex),
                },
                () => undefined,
            );
        },

        async getConnection(connId: string): Promise<Connection | null> {
            return rpc("GetConnection", "ConnectionResult",
                { type: "GetConnection", conn_id: connId },
                (body) => ({
                    connId: body!.conn_id as string,
                    connType: body!.conn_type as string,
                    host: (body!.host as string) ?? null,
                    schema: (body!.schema as string) ?? null,
                    login: (body!.login as string) ?? null,
                    password: (body!.password as string) ?? null,
                    port: (body!.port as number) ?? null,
                    extra: (body!.extra as string) ?? null,
                }),
            );
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
