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

// Public types exposed to user task code.
// TODO(pr2): extend TaskContext with TIRunContext fields (`dagRunConf`,
//            `maxTries`, `taskRescheduleCount`, etc.) — requires `enterRunning`
//            to return the response body instead of `Promise<void>`.
// TODO(future): add `getConnection` to TaskClient.
// TODO(pr4): add `log` (forwarded to Edge API) to TaskHandlerArgs.

/** Per-task context delivered to every task handler invocation. */

import type { EdgeJobFetched } from "./edge/edge-client.js";
import type { TaskClient } from "./client.js";

export interface TaskContext {
    readonly dagId: string;
    readonly taskId: string;
    readonly runId: string;
    readonly tryNumber: number;
    /** -1 for non-mapped tasks, 0..N-1 for mapped instances. */
    readonly mapIndex: number;
    readonly taskInstanceId: string;
    /** AbortSignal that fires when the task should stop (SIGTERM drain or
     *  execution_timeout). User tasks should pass this to `fetch()`,
     *  `setTimeout` helpers, or any abortable API. */
    readonly signal: AbortSignal;
}

/** Arguments passed to every task handler. Adding fields is non-breaking
 *  for consumers that destructure by name.
 *
 *  `client` is always present — both coordinator mode (comm-socket RPC)
 *  and edge mode (Execution API HTTP) provide a `TaskClient`.
 *  `job` is present in Edge worker mode only (`startWorker`). */
export interface TaskHandlerArgs {
    ctx: TaskContext;
    readonly client: TaskClient;
    readonly job?: EdgeJobFetched;
}

/** User task handler function signature. Non-undefined return values are
 *  automatically pushed to XCom under the key `"return_value"` (matches
 *  Python's `@task` behaviour). Return `undefined` (or nothing) to skip. */
export type TaskHandler<TReturn = unknown> = (args: TaskHandlerArgs) => Promise<TReturn>;

/** Configuration for `startWorker()`. */
export interface StartWorkerOptions {
    /** Airflow api-server base URL, e.g. `"http://localhost:8080"`.
     *  Falls back to `process.env.AIRFLOW__EDGE__API_URL`. */
    baseUrl?: string;
    /** Secret for Edge API JWT signing.
     *  Falls back to `process.env.AIRFLOW__API_AUTH__JWT_SECRET`. */
    secret?: string;
    /** Edge queue name(s) this worker listens on. At least one required. */
    queues: string[];
    /** Unique worker name. Defaults to `${hostname}-${pid}-${random}`. */
    workerName?: string;
    /** Milliseconds between poll attempts when idle. Default 5000. */
    pollIntervalInMs?: number;
    /** Milliseconds between worker-level heartbeats. Default 30000. */
    heartbeatIntervalInMs?: number;
    /** Consecutive heartbeat failures before the worker self-terminates.
     *  Default 10. */
    heartbeatFailureThreshold?: number;
    /** JWT `iss` claim sent on Edge API requests. Must match the server's
     *  `[api_auth] jwt_issuer` config. Falls back to
     *  `process.env.AIRFLOW__API_AUTH__JWT_ISSUER`, then `"airflow"`. */
    jwtIssuer?: string;
}
