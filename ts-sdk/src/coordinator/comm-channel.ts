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

import { Socket } from "node:net";
import {
    encodeRequest,
    encodeResponse,
    FrameReader,
    type Frame,
} from "./frames.js";

type FrameHandler = (frame: Frame) => void | Promise<void>;

export class CommChannel {
    private readonly sock: Socket;
    private readonly reader = new FrameReader();
    private nextId = 0;
    private pendingReplies = new Map<number, (frame: Frame) => void>();
    private waitingForFrame: ((frame: Frame) => void) | null = null;
    private closed = false;
    private closeError: Error | null = null;
    private inbox: Frame[] = [];
    private onIncoming: FrameHandler | null = null;

    constructor(sock: Socket) {
        this.sock = sock;
        sock.on("data", (chunk) => this.handleData(chunk));
        sock.on("close", () => this.handleClose(null));
        sock.on("error", (err) => this.handleClose(err));
    }

    static async connect(addr: string): Promise<CommChannel> {
        const [host, portStr] = splitHostPort(addr);
        return new Promise((resolve, reject) => {
            const sock = new Socket();
            sock.once("connect", () => {
                sock.setNoDelay(true);
                resolve(new CommChannel(sock));
            });
            sock.once("error", reject);
            sock.connect(Number.parseInt(portStr, 10), host);
        });
    }

    /** Install a handler for incoming supervisor-initiated frames (e.g.
     *  the first StartupDetails / DagFileParseRequest). */
    setIncomingHandler(handler: FrameHandler): void {
        this.onIncoming = handler;
        while (this.inbox.length > 0) {
            const frame = this.inbox.shift()!;
            void this.dispatchIncoming(frame);
        }
    }

    /** Wait for exactly one incoming supervisor-initiated frame. */
    waitForFrame(): Promise<Frame> {
        if (this.inbox.length > 0) {
            return Promise.resolve(this.inbox.shift()!);
        }
        if (this.closed) {
            return Promise.reject(this.closeError ?? new Error("Comm channel closed"));
        }
        return new Promise((resolve, reject) => {
            this.waitingForFrame = (frame) => resolve(frame);
            this.sock.once("close", () => {
                if (this.waitingForFrame) {
                    reject(this.closeError ?? new Error("Comm channel closed"));
                }
            });
        });
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
        // Frame ARITY (response vs request) is authoritative. Supervisor
        // and runtime keep independent id counters that both start at 0,
        // so collision across directions is normal — never route by id alone.
        if (frame.isResponse) {
            const pending = this.pendingReplies.get(frame.id);
            if (pending) {
                this.pendingReplies.delete(frame.id);
                pending(frame);
                return;
            }
            // No pending request matches. The Airflow coordinator's
            // `_send_startup_details` emits a `_ResponseFrame` (arity 3)
            // to deliver the initial StartupDetails — semantically a
            // supervisor-initiated request. Fall through so the runtime
            // can still dispatch.
        }
        if (this.waitingForFrame) {
            const cb = this.waitingForFrame;
            this.waitingForFrame = null;
            cb(frame);
            return;
        }
        if (this.onIncoming) {
            void this.dispatchIncoming(frame);
            return;
        }
        this.inbox.push(frame);
    }

    private async dispatchIncoming(frame: Frame): Promise<void> {
        if (!this.onIncoming) return;
        try {
            await this.onIncoming(frame);
        } catch (err) {
            process.stderr.write(
                `[comm-channel] handler error for frame id=${frame.id}: ${
                    (err as Error).stack ?? err
                }\n`,
            );
        }
    }

    private handleClose(err: Error | null): void {
        this.closed = true;
        this.closeError = err;
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

function splitHostPort(addr: string): [string, string] {
    const idx = addr.lastIndexOf(":");
    if (idx < 0) throw new Error(`Address must be host:port, got ${addr}`);
    return [addr.slice(0, idx), addr.slice(idx + 1)];
}
