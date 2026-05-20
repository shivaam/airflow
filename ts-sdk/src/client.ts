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

// TaskClient — the cross-mode contract for mid-task data access.
//
// Both coordinator mode (comm-socket RPC) and edge mode (Execution API
// over HTTP) implement this interface. User task handlers receive it
// via `TaskHandlerArgs.client` and never know which transport backs it.

/** Airflow connection — credentials and configuration for an external
 *  system (database, API, cloud service, etc.). Stored centrally in
 *  Airflow and looked up by connection ID at runtime. */
export interface Connection {
    readonly connId: string;
    readonly connType: string;
    readonly host: string | null;
    readonly schema: string | null;
    readonly login: string | null;
    readonly password: string | null;
    readonly port: number | null;
    readonly extra: string | null;
}

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
     *  missing or stored with a null value. Throws on any other error.
     *
     *  Note: this is a deliberate JS-idiomatic divergence from Python's
     *  `Variable.get`, which raises on a missing key. Use
     *  `getVariableOrThrow` for the Python-parity behaviour. */
    getVariable(key: string): Promise<string | null>;

    /** Like `getVariable`, but throws `VariableNotFoundError` when the
     *  key is missing (matches Python `Variable.get` with no default). */
    getVariableOrThrow(key: string): Promise<string>;

    /** Pull an XCom value. Returns `null` when the row is missing.
     *  Locator fields default to the current task's context.
     *
     *  The generic `T` lets callers narrow the return type when the
     *  shape is known:
     *  ```ts
     *  const data = await client.getXCom<{ count: number }>({ key: "result" });
     *  // data is { count: number } | null
     *  ``` */
    getXCom<T = unknown>(opts: GetXComOpts): Promise<T | null>;

    /** Push an XCom value. Resolves once the value has been persisted.
     *  Target fields default to the current task's context. */
    setXCom(opts: SetXComOpts): Promise<void>;

    /** Look up an Airflow Connection by ID. Returns `null` when the
     *  connection doesn't exist. Throws on any other error. */
    getConnection(connId: string): Promise<Connection | null>;
}

/** Thrown by {@link TaskClient.getVariableOrThrow} on a missing key. */
export class VariableNotFoundError extends Error {
    constructor(public readonly key: string) {
        super(`Variable not found: ${key}`);
        this.name = "VariableNotFoundError";
    }
}
