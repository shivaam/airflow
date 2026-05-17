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

// Comm socket client — length-prefixed msgpack frames over TCP.
// Mirrors the supervisor side of `task-sdk/.../comms.py` and the Kotlin
// SDK's `CoordinatorComm` in `Comms.kt`.
//
// The channel is the sole reader on the socket. The task sends
// requests and awaits id-correlated replies. The supervisor's only
// unprompted frame is the greeting (StartupDetails /
// DagFileParseRequest), pre-caught into the `greeting` promise that
// `connect()` awaits — the protocol sends nothing else
// supervisor-initiated (comms.py: "No messages are sent to task
// process except in response to a request").

import type { Socket } from "node:net";
import {
    encodeRequest,
    encodeResponse,
    FrameReader,
    type Frame,
} from "./frames.js";
import { connectTcp } from "./tcp-connect.js";
import { Deferred } from "./deferred.js";

/** What `CommChannel.connect` resolves to: the live channel plus the
 *  supervisor's first frame (StartupDetails / DagFileParseRequest),
 *  already in hand so the caller never has to manage a "frame arrived
 *  with no consumer" window. */
export interface CommConnection {
    channel: CommChannel;
    firstFrame: Frame;
}

export class CommChannel {
    private readonly sock: Socket;
    private readonly reader = new FrameReader();
    private nextId = 0;
    private pendingReplies = new Map<number, (frame: Frame) => void>();
    private closed = false;
    private closeError: Error | null = null;

    // The greeting (first supervisor-initiated frame). The promise is
    // its own buffer: arriving before `connect()` awaits is fine — it
    // stays settled with the value, so there is no race to handle.
    private readonly greeting = new Deferred<Frame>();

    private constructor(sock: Socket) {
        this.sock = sock;
        // A `new Socket()` (from `connectTcp`) starts paused: it buffers
        // inbound bytes and emits no `data` until a listener attaches
        // and flips it to flowing. Attaching synchronously here — same
        // tick as construction, before the event loop can deliver a
        // read, and as the only reader — loses nothing, double-reads
        // nothing.
        sock.on("data", (chunk) => this.handleData(chunk));
        sock.on("close", () => this.handleClose(null));
        sock.on("error", (err) => this.handleClose(err));
    }

    /** Connect and wait for the supervisor's greeting; rejects if the
     *  socket dies before it arrives. */
    static async connect(addr: string): Promise<CommConnection> {
        const sock = await connectTcp(addr);
        const channel = new CommChannel(sock);
        const firstFrame = await channel.greeting.promise;
        return { channel, firstFrame };
    }

    /** Send a request to the supervisor and await its matching response. */
    async request(body: unknown): Promise<Frame> {
        const id = this.nextId++;
        return new Promise<Frame>((resolve, reject) => {
            if (this.closed) {
                reject(this.closeError ?? new Error("Comm channel closed"));
                return;
            }
            this.pendingReplies.set(id, (frame) => resolve(frame));
            const buf = encodeRequest(id, body);
            this.sock.write(buf, (err) => {
                if (err) {
                    this.pendingReplies.delete(id);
                    reject(err);
                }
            });
        });
    }

    /** Send a response for an incoming supervisor request. */
    async sendResponse(id: number, body: unknown, error?: unknown): Promise<void> {
        const buf = encodeResponse(id, body, error);
        return new Promise((resolve, reject) => {
            this.sock.write(buf, (err) => (err ? reject(err) : resolve()));
        });
    }

    async close(): Promise<void> {
        return new Promise((resolve) => {
            if (this.closed) {
                resolve();
                return;
            }
            this.sock.end(() => resolve());
        });
    }

    // -- internals --

    private handleData(chunk: Buffer): void {
        for (const frame of this.reader.push(chunk)) {
            this.route(frame);
        }
    }

    private route(frame: Frame): void {
        // Arity-authoritative (per-direction id counters both start at
        // 0, so id alone can't discriminate): (1) an arity-3 response
        // whose id matches a pending request → that reply;
        // (2) anything else → supervisor-initiated.
        if (frame.isResponse) {
            const pending = this.pendingReplies.get(frame.id);
            if (pending) {
                this.pendingReplies.delete(frame.id);
                pending(frame);
                return;
            }
        }
        this.deliverSupervisorFrame(frame);
    }

    private deliverSupervisorFrame(frame: Frame): void {
        // The supervisor's only unprompted frame is the greeting.
        if (!this.greeting.settled) {
            this.greeting.resolve(frame);
            return;
        }
        // Anything else supervisor-initiated is a protocol anomaly —
        // comms.py guarantees "No messages are sent to task process
        // except in response to a request". Surface it; never buffer.
        process.stderr.write(
            `[comm-channel] unexpected supervisor-initiated frame id=${frame.id} after greeting\n`,
        );
    }

    private handleClose(err: Error | null): void {
        this.closed = true;
        this.closeError = err;
        // Before the greeting this rejects so `connect()` throws;
        // after it, a no-op — the Deferred settles at most once, so no
        // guard is needed here.
        this.greeting.reject(
            err ?? new Error("Comm channel closed before first frame"),
        );
        for (const [, resolver] of this.pendingReplies) {
            resolver({
                id: -1,
                body: null,
                error: err?.message ?? "closed",
                isResponse: true,
            });
        }
        this.pendingReplies.clear();
    }
}
