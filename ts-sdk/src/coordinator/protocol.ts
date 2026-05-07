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

// Task SDK message types (subset implemented in this PR).
//
// Mirrors `task-sdk/src/airflow/sdk/execution_time/comms.py` and the
// Kotlin SDK's `Comms.kt`. Each frame body is a map with a `type`
// discriminator. `StartupDetails` and `DagFileParseRequest` are the
// first frames the supervisor sends; everything else is request /
// response between supervisor and runtime.
//
// Currently this PR implements only the minimum needed for task
// dispatch + Succeed / Fail. XCom / Variables / Connections are
// deferred to a follow-up — see TODOs in `runtime.ts`.

// -------- TaskInstance shape (trimmed to fields a handler reads) --------

export interface TaskInstance {
    id: string;
    task_id: string;
    dag_id: string;
    run_id: string;
    try_number: number;
    map_index?: number | null;
    hostname?: string | null;
    queue?: string | null;
    language?: string | null;
    [key: string]: unknown; // unknown fields pass through
}

export interface BundleInfo {
    name: string;
    version?: string | null;
}

export interface TIRunContext {
    dag_run?: Record<string, unknown>;
    task_reschedule_count?: number;
    max_tries?: number;
    variables?: Record<string, unknown>;
    connections?: Record<string, unknown>;
    [key: string]: unknown;
}

// -------- Frames from supervisor --------

export interface StartupDetails {
    type: "StartupDetails";
    ti: TaskInstance;
    dag_rel_path: string;
    bundle_info: BundleInfo;
    start_date: string;
    ti_context: TIRunContext;
    sentry_integration?: string;
}

export interface DagFileParseRequest {
    type: "DagFileParseRequest";
    file: string;
    bundle_path: string;
}

export interface ErrorResponse {
    type: "ErrorResponse";
    error: string;
    detail?: unknown;
}

export type MsgFromSupervisor =
    | StartupDetails
    | DagFileParseRequest
    | ErrorResponse;

// -------- Frames from runtime --------

export interface SucceedTask {
    type: "SucceedTask";
    end_date: string;
    /** Empty list when no outlets; required (not null) by Execution API. */
    task_outlets: unknown[];
    /** Empty list when no events; required (not null) by Execution API. */
    outlet_events: unknown[];
}

export interface TaskStateMsg {
    type: "TaskState";
    state: "failed" | "skipped" | "removed" | "up_for_retry";
    end_date?: string;
}

export interface DagParsingResult {
    type: "DagParsingResult";
    fileloc: string;
    bundle_path: string;
    dags: Record<string, unknown>;
}

export type MsgFromRuntime = SucceedTask | TaskStateMsg | DagParsingResult;

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
