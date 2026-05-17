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
// Mental model: a phone call.
//   - You ask questions and await the matching answers
//     (`request()` → reply correlated by id).
//   - The other side can also say something unprompted
//     (`onSupervisorInitiatedFrame`).
//   - The first thing they say is the greeting — exposed as the
//     `greeting` promise, which `connect()` awaits.
//
// There is exactly ONE reader on the socket (this channel). The
// greeting is simply the first supervisor-initiated frame, pre-caught
// into a promise; no separate consumer, no pause/resume, no replay.

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

    // The greeting: the first supervisor-initiated frame, pre-caught.
    // A promise IS the buffer — if the frame arrives before `connect()`
    // awaits, it just settles and the value is retained. Resolver/
    // rejecter are nulled once the greeting is delivered (or the socket
    // dies first).
    private readonly greeting: Promise<Frame>;
    private resolveGreeting: ((frame: Frame) => void) | null = null;
    private rejectGreeting: ((err: Error) => void) | null = null;

    // Supervisor-initiated frames AFTER the greeting. The current
    // protocol pushes none; a richer supervisor might.
    private onSupervisorFrame: FrameHandler | null = null;

    private constructor(sock: Socket) {
        this.sock = sock;
        this.greeting = new Promise<Frame>((resolve, reject) => {
            this.resolveGreeting = resolve;
            this.rejectGreeting = reject;
        });
        // The single reader. `connectTcp` returns a raw paused socket;
        // attaching `data` here (synchronously, before any async data
        // event can fire) is what starts the flow — nothing is missed,
        // nothing is double-read.
        sock.on("data", (chunk) => this.handleData(chunk));
        sock.on("close", () => this.handleClose(null));
        sock.on("error", (err) => this.handleClose(err));
    }

    /** Connect and wait for the supervisor's greeting. The channel is
     *  the only socket reader; `greeting` settles as soon as the first
     *  supervisor-initiated frame arrives (or rejects if the socket
     *  dies first). */
    static async connect(addr: string): Promise<CommConnection> {
        const sock = await connectTcp(addr);
        const channel = new CommChannel(sock);
        const firstFrame = await channel.greeting;
        return { channel, firstFrame };
    }

    /** Register a handler for supervisor-initiated frames that arrive
     *  after the greeting. Without a handler such a frame is logged and
     *  dropped — never silently buffered. */
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
        // Arity-authoritative, two ways only: (1) an answer to a
        // request we sent — an arity-3 response whose id matches a
        // pending request; (2) everything else is supervisor-initiated.
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
        this.deliverSupervisorFrame(frame);
    }

    private deliverSupervisorFrame(frame: Frame): void {
        // The first supervisor-initiated frame is the greeting (this is
        // the only "first" logic anywhere — and it reads like the
        // mental model: the first thing they say).
        if (this.resolveGreeting) {
            const resolve = this.resolveGreeting;
            this.resolveGreeting = null;
            this.rejectGreeting = null;
            resolve(frame);
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
        // Socket died before the greeting → `connect()`'s await throws,
        // exactly as the old in-channel awaiter did.
        if (this.rejectGreeting) {
            const reject = this.rejectGreeting;
            this.resolveGreeting = null;
            this.rejectGreeting = null;
            reject(err ?? new Error("Comm channel closed before first frame"));
        }
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
