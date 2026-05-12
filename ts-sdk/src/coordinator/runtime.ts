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

// Coordinator runtime entrypoint.
//
// Invoked by Airflow's `BaseCoordinator` (PR #65958) as a subprocess:
//
//     node my-bundle.mjs --comm=host:port --logs=host:port
//
// where `my-bundle.mjs` is a user-bundled Node script that imports
// the SDK, calls `registerTask(...)` for each handler, then calls
// `startCoordinatorRuntime()`.
//
// Lifecycle:
//   1. Parse --comm / --logs from argv
//   2. Connect both TCP sockets
//   3. Read the first frame from comm:
//        - DagFileParseRequest → respond with DagParsingResult, exit
//        - StartupDetails      → run task, respond Succeed or Fail, exit
//
// TODO(pr-followup): bundle scanner / `provideDags()` API for the
//   parse-mode path. Until then, parse mode emits an empty DAG list
//   (sufficient for the Python-stub-DAG workflow Java SDK uses).

import { createCoordinatorClient } from "./client.js";
import { CommChannel } from "./comm-channel.js";
import { LogChannel } from "./log-channel.js";
import { asMsgFromSupervisor, type StartupDetails } from "./protocol.js";
import { getRegisteredTask, listRegisteredTasks } from "../registry.js";
import type { TaskContext, TaskHandlerArgs } from "../types.js";

/** Options for `startCoordinatorRuntime()`. */
export interface StartCoordinatorRuntimeOptions {
    /** Comm socket address (host:port). Falls back to parsing `--comm=` from argv. */
    commAddr?: string;
    /** Logs socket address (host:port). Falls back to parsing `--logs=` from argv. */
    logsAddr?: string;
    /** Source argv. Defaults to `process.argv`. */
    argv?: readonly string[];
}

interface ParsedArgs {
    commAddr: string;
    logsAddr: string;
}

export function parseArgs(argv: readonly string[]): ParsedArgs {
    let commAddr: string | null = null;
    let logsAddr: string | null = null;
    for (const arg of argv) {
        if (arg.startsWith("--comm=")) {
            commAddr = arg.slice("--comm=".length);
        } else if (arg.startsWith("--logs=")) {
            logsAddr = arg.slice("--logs=".length);
        }
    }
    if (!commAddr) throw new Error("Missing --comm=host:port");
    if (!logsAddr) throw new Error("Missing --logs=host:port");
    return { commAddr, logsAddr };
}

/** Start the coordinator runtime. Resolves when the subprocess has
 *  delivered its terminal frame and closed both sockets. */
export async function startCoordinatorRuntime(
    opts: StartCoordinatorRuntimeOptions = {},
): Promise<void> {
    const argv = opts.argv ?? process.argv;
    const parsed = (opts.commAddr && opts.logsAddr)
        ? { commAddr: opts.commAddr, logsAddr: opts.logsAddr }
        : parseArgs(argv);

    // Connect log channel first so early failures are captured.
    const logs = await LogChannel.connect(parsed.logsAddr);
    logs.info("Coordinator runtime started", {
        registered_tasks: listRegisteredTasks().length,
    });

    const comm = await CommChannel.connect(parsed.commAddr);
    logs.debug("Connected comm socket", { commAddr: parsed.commAddr });

    try {
        const firstFrame = await comm.waitForFrame();
        const body = asMsgFromSupervisor(firstFrame.body);
        logs.debug("First frame received", { type: body.type });

        if (body.type === "DagFileParseRequest") {
            await handleParse(firstFrame.id, body, comm, logs);
        } else if (body.type === "StartupDetails") {
            await handleTask(firstFrame.id, body, comm, logs);
        } else {
            const errMsg = `First frame must be DagFileParseRequest or StartupDetails, got ${body.type}`;
            logs.error("Unexpected first frame", { type: body.type });
            await comm.sendResponse(firstFrame.id, null, {
                error: "protocol_error",
                detail: errMsg,
            });
        }
    } finally {
        await comm.close();
        await logs.close();
    }
}

async function handleParse(
    id: number,
    request: { file: string; bundle_path: string },
    comm: CommChannel,
    logs: LogChannel,
): Promise<void> {
    // PR-1 minimal: respond with the registered task list as a single
    // synthetic DAG. A real bundle scanner is deferred — the Java
    // provider's recommended path for non-Python languages is the
    // Python-stub DAG, which doesn't go through this pathway anyway.
    logs.info("Parse-mode response (minimal)", {
        registered_tasks: listRegisteredTasks(),
    });
    await comm.sendResponse(id, {
        type: "DagParsingResult",
        fileloc: request.file,
        bundle_path: request.bundle_path,
        dags: {},
    });
}

async function handleTask(
    id: number,
    details: StartupDetails,
    comm: CommChannel,
    logs: LogChannel,
): Promise<void> {
    const ti = details.ti;
    // Lookup precedence: namespaced ("dag_id.task_id") then bare task_id.
    // Lets users opt into per-DAG namespacing when running multiple DAGs
    // through the same bundle without colliding on shared task names.
    const handler =
        getRegisteredTask(`${ti.dag_id}.${ti.task_id}`) ??
        getRegisteredTask(ti.task_id);

    if (!handler) {
        logs.warning("No handler registered for task", {
            dag_id: ti.dag_id,
            task_id: ti.task_id,
            available: listRegisteredTasks(),
        });
        await comm.sendResponse(id, {
            type: "TaskState",
            state: "removed",
            end_date: new Date().toISOString(),
        });
        return;
    }

    const ctx = buildContext(details);
    const client = createCoordinatorClient(comm);
    const args: TaskHandlerArgs = { ctx, client };
    logs.info("Running task", {
        dag_id: ctx.dagId,
        task_id: ctx.taskId,
        run_id: ctx.runId,
    });

    try {
        await handler(args);
        // SucceedTask MUST include task_outlets and outlet_events as
        // empty lists — the Execution API's TISuccessStatePayload
        // tagged-union validator rejects null for these fields.
        await comm.sendResponse(id, {
            type: "SucceedTask",
            end_date: new Date().toISOString(),
            task_outlets: [],
            outlet_events: [],
        });
        logs.info("Task succeeded", { task_id: ctx.taskId });
    } catch (err) {
        const message = (err as Error).message ?? String(err);
        logs.error("Task failed", {
            task_id: ctx.taskId,
            error: message,
            stack: (err as Error).stack ?? null,
        });
        await comm.sendResponse(id, {
            type: "TaskState",
            state: "failed",
            end_date: new Date().toISOString(),
        });
    }
}

function buildContext(details: StartupDetails): TaskContext {
    // No AbortSignal in coordinator mode (no SIGTERM-drain story yet —
    // the coordinator forwards SIGTERM to us and Node exits before any
    // graceful handler runs). Provide a never-aborting signal for API
    // parity with Edge mode.
    const ac = new AbortController();
    return {
        dagId: details.ti.dag_id,
        taskId: details.ti.task_id,
        runId: details.ti.run_id,
        tryNumber: details.ti.try_number,
        mapIndex: details.ti.map_index ?? -1,
        taskInstanceId: details.ti.id,
        signal: ac.signal,
    };
}
