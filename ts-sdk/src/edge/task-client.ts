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

// Edge-mode TaskClient — implements the shared TaskClient interface by
// calling the Execution API over HTTP. Reuses the openapi-fetch client
// (with bearer auth + token refresh) from the ExecutionClient.

import type { TaskContext } from "../types.js";
import type { Connection, GetXComOpts, SetXComOpts, TaskClient } from "../client.js";
import { VariableNotFoundError } from "../client.js";
import { ExecutionApiError, formatError } from "../errors.js";
import type { ExecutionHttpClient } from "./execution-client.js";

/** Throw an ExecutionApiError from an openapi-fetch error response. */
function throwApiError(method: string, path: string, status: number, error: unknown): never {
    throw new ExecutionApiError(method, path, status, formatError(error));
}

export function createEdgeTaskClient(
    http: ExecutionHttpClient,
    ctx: TaskContext,
): TaskClient {
    const ctxMapIndex = ctx.mapIndex >= 0 ? ctx.mapIndex : -1;

    const client: TaskClient = {
        async getVariable(key: string): Promise<string | null> {
            const { data, error, response } = await http.GET(
                "/variables/{variable_key}",
                { params: { path: { variable_key: key } } },
            );
            if (response.status === 404) return null;
            if (error !== undefined) throwApiError("GET", `/variables/${key}`, response.status, error);
            return data?.value ?? null;
        },

        async getVariableOrThrow(key: string): Promise<string> {
            const value = await client.getVariable(key);
            if (value == null) throw new VariableNotFoundError(key);
            return value;
        },

        async getXCom<T = unknown>(opts: GetXComOpts): Promise<T | null> {
            const dagId = opts.dagId ?? ctx.dagId;
            const runId = opts.runId ?? ctx.runId;
            const taskId = opts.taskId ?? ctx.taskId;
            const mapIndex = opts.mapIndex ?? ctxMapIndex;
            const path = `/xcoms/${dagId}/${runId}/${taskId}/${opts.key}`;

            const { data, error, response } = await http.GET(
                "/xcoms/{dag_id}/{run_id}/{task_id}/{key}",
                {
                    params: {
                        path: { dag_id: dagId, run_id: runId, task_id: taskId, key: opts.key },
                        query: {
                            map_index: mapIndex,
                            include_prior_dates: opts.includePriorDates ?? false,
                        },
                    },
                },
            );
            if (response.status === 404) return null;
            if (error !== undefined) throwApiError("GET", path, response.status, error);
            return (data?.value as T) ?? null;
        },

        async setXCom(opts: SetXComOpts): Promise<void> {
            const dagId = opts.dagId ?? ctx.dagId;
            const runId = opts.runId ?? ctx.runId;
            const taskId = opts.taskId ?? ctx.taskId;
            const mapIndex = opts.mapIndex ?? ctxMapIndex;
            const path = `/xcoms/${dagId}/${runId}/${taskId}/${opts.key}`;

            const { error, response } = await http.POST(
                "/xcoms/{dag_id}/{run_id}/{task_id}/{key}",
                {
                    params: {
                        path: { dag_id: dagId, run_id: runId, task_id: taskId, key: opts.key },
                        query: { map_index: mapIndex },
                    },
                    // openapi-fetch expects the body type from the spec (JsonValue).
                    // XCom values must be JSON-serializable.
                    body: opts.value as Record<string, unknown>,
                },
            );
            if (error !== undefined) throwApiError("POST", path, response.status, error);
        },

        async getConnection(connId: string): Promise<Connection | null> {
            const { data, error, response } = await http.GET(
                "/connections/{connection_id}",
                { params: { path: { connection_id: connId } } },
            );
            if (response.status === 404) return null;
            if (error !== undefined) throwApiError("GET", `/connections/${connId}`, response.status, error);
            return {
                connId: data!.conn_id,
                connType: data!.conn_type,
                host: data!.host ?? null,
                schema: data!.schema ?? null,
                login: data!.login ?? null,
                password: data!.password ?? null,
                port: data!.port ?? null,
                extra: data!.extra ?? null,
            };
        },
    };
    return client;
}
