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

// Pure-Node integration test for the coordinator runtime.
//
// Stands up a minimal in-process "supervisor" (TCP server +
// length-prefixed msgpack frame codec) that mirrors what Airflow's
// real `BaseCoordinator._runtime_subprocess_entrypoint` does, then
// drives the runtime through three lifecycles: StartupDetails →
// SucceedTask, StartupDetails (handler throws) → TaskState{failed},
// and StartupDetails for an unregistered task → TaskState{removed}.
//
// No Python, no Airflow install — but exercises the same wire format
// the real coordinator speaks.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createServer, type Server, type Socket } from "node:net";
import { encode, decode } from "@msgpack/msgpack";
import { startCoordinatorRuntime } from "../../src/coordinator/runtime.js";
import { clearRegistry, registerTask } from "../../src/registry.js";

interface MockResult {
    firstResponse: { id: number; body: unknown; isResponse: boolean } | null;
    logRecords: Record<string, unknown>[];
    runtimeRequests: { type: string; body: Record<string, unknown> }[];
}

/** Callback used by `driveSupervisor` to answer runtime-initiated
 *  requests. Return `{ body, error? }` to reply with that arity-3
 *  frame, or `null` to ignore the request (the runtime will hang
 *  waiting for a response — only useful for negative tests). */
type Responder = (
    msgType: string,
    body: Record<string, unknown>,
) => { body: unknown; error?: unknown } | null;

function frameBytes(id: number, body: unknown, isResponse: boolean): Buffer {
    const arr = isResponse ? [id, body, null] : [id, body];
    const payload = Buffer.from(encode(arr));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length, 0);
    return Buffer.concat([header, payload]);
}

function readFrames(
    buf: Buffer,
): { frames: { id: number; body: unknown; arity: number }[]; rest: Buffer } {
    const out: { id: number; body: unknown; arity: number }[] = [];
    let rest: Buffer = buf;
    while (rest.length >= 4) {
        const len = rest.readUInt32BE(0);
        if (rest.length < 4 + len) break;
        const payload = rest.subarray(4, 4 + len);
        const arr = decode(Buffer.from(payload)) as unknown[];
        out.push({ id: arr[0] as number, body: arr[1], arity: arr.length });
        rest = Buffer.from(rest.subarray(4 + len));
    }
    return { frames: out, rest };
}

function makeStartupDetails(taskId: string, dagId = "test_dag", runId = "r1"): unknown {
    return {
        type: "StartupDetails",
        ti: {
            id: "ti-1",
            task_id: taskId,
            dag_id: dagId,
            run_id: runId,
            try_number: 1,
            map_index: -1,
            queue: "default",
        },
        dag_rel_path: "test.py",
        bundle_info: { name: "test", version: null },
        start_date: "2026-04-23T00:00:00Z",
        ti_context: {},
        sentry_integration: "",
    };
}

async function listen(): Promise<{ server: Server; port: number }> {
    return new Promise((resolve) => {
        const server = createServer();
        server.listen(0, "127.0.0.1", () => {
            const port = (server.address() as { port: number }).port;
            resolve({ server, port });
        });
    });
}

async function acceptOne(server: Server): Promise<Socket> {
    return new Promise((resolve) => server.once("connection", resolve));
}

function xcomKey(body: Record<string, unknown>): string {
    return [body["dag_id"], body["run_id"], body["task_id"], body["key"]].join("|");
}

async function driveSupervisor(
    initialFrame: unknown,
    responder?: Responder,
): Promise<MockResult> {
    const comm = await listen();
    const logs = await listen();

    const commAccept = acceptOne(comm.server);
    const logsAccept = acceptOne(logs.server);

    const runtimeDone = startCoordinatorRuntime({
        commAddr: `127.0.0.1:${comm.port}`,
        logsAddr: `127.0.0.1:${logs.port}`,
        argv: [],
    });

    const [commSock, logsSock] = await Promise.all([commAccept, logsAccept]);

    // Send the kickoff frame as a _ResponseFrame (arity 3) — matches what
    // Airflow's `_send_startup_details` actually emits on the wire.
    commSock.write(frameBytes(0, initialFrame, true));

    let firstResponse: MockResult["firstResponse"] = null;
    const runtimeRequests: MockResult["runtimeRequests"] = [];
    const logChunks: Buffer[] = [];
    let commBuf: Buffer = Buffer.from(new Uint8Array(0));

    commSock.on("data", (chunk: Buffer) => {
        commBuf = Buffer.from(Buffer.concat([commBuf, chunk]));
        const taken = readFrames(commBuf);
        commBuf = taken.rest;
        for (const f of taken.frames) {
            if (f.arity === 2) {
                // Runtime-initiated request (mid-task RPC). Forward to the
                // responder; reply on the same id with an arity-3 frame.
                const body = (f.body ?? {}) as Record<string, unknown>;
                const msgType = String(body["type"] ?? "");
                runtimeRequests.push({ type: msgType, body });
                if (responder) {
                    const reply = responder(msgType, body);
                    if (reply) {
                        commSock.write(frameBytes(f.id, reply.body, true));
                    }
                }
                continue;
            }
            // Arity-3: a response from the runtime. The terminal frame
            // (SucceedTask / TaskState) lands here.
            if (firstResponse === null) {
                firstResponse = { id: f.id, body: f.body, isResponse: true };
            }
        }
    });
    logsSock.on("data", (chunk: Buffer) => logChunks.push(chunk));

    // Wait for the runtime to finish AND for the comm socket to deliver
    // its final bytes (FIN signals that all preceding frames are flushed).
    const commEnd = new Promise<void>((resolve) => commSock.on("end", () => resolve()));
    await Promise.all([runtimeDone, commEnd]);

    comm.server.close();
    logs.server.close();

    const lines = Buffer.concat(logChunks).toString("utf8").split("\n").filter(Boolean);
    const logRecords = lines.map((l) => JSON.parse(l) as Record<string, unknown>);

    return { firstResponse, logRecords, runtimeRequests };
}

describe("coordinator runtime integration", () => {
    beforeEach(() => clearRegistry());
    afterEach(() => clearRegistry());

    it("dispatches StartupDetails to a registered handler and emits SucceedTask", async () => {
        let observedCtx: unknown = null;
        registerTask("say_hello", async ({ ctx }) => {
            observedCtx = ctx;
            return "ok";
        });

        const result = await driveSupervisor(makeStartupDetails("say_hello"));

        expect(result.firstResponse).not.toBeNull();
        expect(result.firstResponse!.body).toMatchObject({
            type: "SucceedTask",
            task_outlets: [],
            outlet_events: [],
        });
        expect(observedCtx).toMatchObject({
            taskId: "say_hello",
            dagId: "test_dag",
            runId: "r1",
        });
        expect(result.logRecords.some((r) => r["event"] === "Task succeeded")).toBe(true);
    });

    it("returns TaskState=failed when the handler throws", async () => {
        registerTask("boom", async () => {
            throw new Error("boom");
        });

        const result = await driveSupervisor(makeStartupDetails("boom"));

        expect(result.firstResponse!.body).toMatchObject({
            type: "TaskState",
            state: "failed",
        });
    });

    it("returns TaskState=removed when no handler is registered", async () => {
        const result = await driveSupervisor(makeStartupDetails("missing_task"));

        expect(result.firstResponse!.body).toMatchObject({
            type: "TaskState",
            state: "removed",
        });
    });

    it("exposes a client that round-trips GetVariable / SetXCom / GetXCom", async () => {
        const xcomStore = new Map<string, unknown>();
        let observedGreeting: string | null = "<unset>";

        registerTask("say_hello", async ({ ctx, client }) => {
            // The coordinator-mode handler MUST receive a client.
            if (!client) throw new Error("client missing in coordinator mode");

            observedGreeting = await client.getVariable("e6_greeting");
            await client.setXCom({
                key: "echo",
                value: `node says: ${observedGreeting}`,
                dagId: ctx.dagId,
                taskId: ctx.taskId,
                runId: ctx.runId,
            });
            const back = await client.getXCom({
                key: "echo",
                dagId: ctx.dagId,
                taskId: ctx.taskId,
                runId: ctx.runId,
            });
            if (back !== `node says: ${observedGreeting}`) {
                throw new Error(`xcom round-trip mismatch: ${back}`);
            }
        });

        const responder: Responder = (msgType, body) => {
            if (msgType === "GetVariable") {
                return {
                    body: {
                        type: "VariableResult",
                        key: body["key"],
                        value: "hello from airflow",
                    },
                };
            }
            if (msgType === "SetXCom") {
                const k = xcomKey(body);
                xcomStore.set(k, body["value"]);
                // Supervisor sends an empty arity-3 frame on success.
                return { body: null };
            }
            if (msgType === "GetXCom") {
                const k = xcomKey(body);
                const value = xcomStore.get(k) ?? null;
                return {
                    body: { type: "XComResult", key: body["key"], value },
                };
            }
            return null;
        };

        const result = await driveSupervisor(makeStartupDetails("say_hello"), responder);

        expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
        expect(observedGreeting).toBe("hello from airflow");

        const requestTypes = result.runtimeRequests.map((r) => r.type);
        expect(requestTypes).toEqual(["GetVariable", "SetXCom", "GetXCom"]);

        const setReq = result.runtimeRequests.find((r) => r.type === "SetXCom")!.body;
        expect(setReq).toMatchObject({
            key: "echo",
            value: "node says: hello from airflow",
            dag_id: "test_dag",
            task_id: "say_hello",
            run_id: "r1",
        });
    });

    it("returns null from getVariable when the supervisor signals NOT_FOUND", async () => {
        let observed: string | null = "<unset>";
        registerTask("say_hello", async ({ client }) => {
            observed = await client!.getVariable("missing_key");
        });

        const responder: Responder = (msgType) => {
            if (msgType === "GetVariable") {
                return {
                    body: {
                        type: "ErrorResponse",
                        error: "VARIABLE_NOT_FOUND",
                        detail: { key: "missing_key" },
                    },
                };
            }
            return null;
        };

        const result = await driveSupervisor(makeStartupDetails("say_hello"), responder);
        expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
        expect(observed).toBeNull();
    });

    it("looks up handler by namespaced 'dag_id.task_id' before bare task_id", async () => {
        let calledNamespaced = false;
        let calledBare = false;
        registerTask("test_dag.say_hello", async () => {
            calledNamespaced = true;
        });
        registerTask("say_hello", async () => {
            calledBare = true;
        });

        await driveSupervisor(makeStartupDetails("say_hello"));

        expect(calledNamespaced).toBe(true);
        expect(calledBare).toBe(false);
    });
});
