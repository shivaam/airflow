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

import type { Socket } from "node:net";
import {
    encodeRequest,
    encodeResponse,
    FrameReader,
    type Frame,
} from "./frames.js";
import { connectTcp } from "./tcp-connect.js";

type FrameHandler = (frame: Frame) => void | Promise<void>;

/** What `CommChannel.connect` resolves to: the live channel plus the
 *  supervisor's first frame (StartupDetails / DagFileParseRequest),
 *  already in hand so there is no "frame arrived with no consumer"
 *  window for the caller to mishandle. */
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
    // First-frame one-shot latch. The supervisor's opening frame can
    // arrive before `connect()` attaches its awaiter, so it is stashed
    // here rather than dropped (the only buffering this channel does —
    // exactly one frame, never an unbounded queue).
    private firstFrameSeen = false;
    private stashedFirstFrame: Frame | null = null;
    private onFirstFrame: ((frame: Frame) => void) | null = null;
    // Supervisor-initiated frames AFTER the first one. The current
    // protocol pushes none; a richer supervisor might.
    private onSupervisorFrame: FrameHandler | null = null;

    private constructor(sock: Socket) {
        this.sock = sock;
        sock.on("data", (chunk) => this.handleData(chunk));
        sock.on("close", () => this.handleClose(null));
        sock.on("error", (err) => this.handleClose(err));
    }

    /** Connect and wait for the supervisor's first frame. Resolving
     *  only once that frame is in hand collapses the old
     *  waitForFrame/inbox/setIncomingHandler tangle: the timing gap
     *  they patched no longer exists. */
    static async connect(addr: string): Promise<CommConnection> {
        const sock = await connectTcp(addr);
        const channel = new CommChannel(sock);
        const firstFrame = await channel.awaitFirstFrame();
        return { channel, firstFrame };
    }

    private awaitFirstFrame(): Promise<Frame> {
        if (this.stashedFirstFrame) {
            return Promise.resolve(this.stashedFirstFrame);
        }
        if (this.closed) {
            return Promise.reject(
                this.closeError ?? new Error("Comm channel closed before first frame"),
            );
        }
        return new Promise((resolve, reject) => {
            this.onFirstFrame = resolve;
            this.sock.once("close", () => {
                if (!this.firstFrameSeen) {
                    reject(
                        this.closeError ??
                            new Error("Comm channel closed before first frame"),
                    );
                }
            });
        });
    }

    /** Register a handler for supervisor-initiated frames that arrive
     *  after the first one. Without a handler such a frame is logged
     *  and dropped — never silently buffered. */
    onSupervisorInitiatedFrame(handler: FrameHandler): void {
        this.onSupervisorFrame = handler;
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
        // Two cases, arity-authoritative. (1) An answer to a question
        // we asked: an arity-3 response whose id matches a pending
        // request. (2) Everything else is supervisor-initiated —
        // including the arity-3 StartupDetails wart, which is
        // structurally a response but has no matching pending request.
        // Independent per-direction id counters (both start at 0) mean
        // id alone can never be the discriminator.
        if (frame.isResponse) {
            const pending = this.pendingReplies.get(frame.id);
            if (pending) {
                this.pendingReplies.delete(frame.id);
                pending(frame);
                return;
            }
        }
        if (!this.firstFrameSeen) {
            this.firstFrameSeen = true;
            if (this.onFirstFrame) this.onFirstFrame(frame);
            else this.stashedFirstFrame = frame;
            return;
        }
        if (this.onSupervisorFrame) {
            void this.dispatchSupervisorFrame(frame);
            return;
        }
        process.stderr.write(
            `[comm-channel] dropped supervisor-initiated frame id=${frame.id} (no handler)\n`,
        );
    }

    private async dispatchSupervisorFrame(frame: Frame): Promise<void> {
        if (!this.onSupervisorFrame) return;
        try {
            await this.onSupervisorFrame(frame);
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
