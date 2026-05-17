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
// Mental model: a phone call. You ask questions and await the matching
// answers (`request()` → reply correlated by id); the other side can
// also just say something unprompted (`onSupervisorInitiatedFrame`).
// The very first thing the supervisor says is the greeting — and that
// is read by the handshake (`connect()`) BEFORE this channel is built,
// so the channel itself has no notion of a "first frame": `route()` is
// a flat two-way decision forever.

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
 *  read by the handshake before the channel exists so there is never a
 *  "frame arrived with no consumer" window for the caller to
 *  mishandle. */
export interface CommConnection {
    channel: CommChannel;
    firstFrame: Frame;
}

export class CommChannel {
    private readonly sock: Socket;
    private readonly reader: FrameReader;
    private nextId = 0;
    private pendingReplies = new Map<number, (frame: Frame) => void>();
    private closed = false;
    private closeError: Error | null = null;
    // Supervisor-initiated frames (the supervisor starting a
    // conversation we didn't). The current protocol pushes none after
    // the greeting; a richer supervisor might.
    private onSupervisorFrame: FrameHandler | null = null;

    // Built only by `connect()`, AFTER the handshake has the greeting.
    // `reader` is handed over already primed: one TCP chunk can carry
    // the greeting plus the start of the next frame, so its buffered
    // remainder must survive the handshake→channel seam or that next
    // frame is corrupted. The socket is paused on entry and resumed by
    // `connect()` once listeners are attached (no reattach race).
    private constructor(sock: Socket, reader: FrameReader) {
        this.sock = sock;
        this.reader = reader;
        sock.on("data", (chunk) => this.handleData(chunk));
        sock.on("close", () => this.handleClose(null));
        sock.on("error", (err) => this.handleClose(err));
    }

    /** Connect, then run the opening handshake: read frames until the
     *  supervisor's greeting is complete, hand the primed reader +
     *  paused socket to a fresh channel, replay anything that arrived
     *  bundled with the greeting, and resume. "First frame" exists only
     *  here — never inside the channel. */
    static async connect(addr: string): Promise<CommConnection> {
        const sock = await connectTcp(addr);
        const reader = new FrameReader();
        const frames = await receiveGreeting(sock, reader);
        // A coalesced TCP chunk can deliver the greeting AND following
        // frame(s) at once; `FrameReader.push` returns them all in one
        // array, so the greeting alone is `frames[0]`. Keep it, and
        // replay any extras through the live channel exactly as the old
        // single-consumer receive loop did. None are expected in
        // today's protocol (the supervisor sends the greeting then
        // waits); this only preserves behaviour under TCP coalescing or
        // a future supervisor that pushes.
        const [firstFrame, ...rest] = frames;
        const channel = new CommChannel(sock, reader);
        for (const frame of rest) channel.route(frame);
        sock.resume();
        return { channel, firstFrame };
    }

    /** Register a handler for supervisor-initiated frames. Without a
     *  handler such a frame is logged and dropped — never silently
     *  buffered. */
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
        // The arity-3 StartupDetails greeting never reaches here — the
        // handshake consumes it before the channel exists. Independent
        // per-direction id counters (both start at 0) mean id alone can
        // never be the discriminator.
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

/** Read from a freshly-connected socket until the supervisor's first
 *  frame (the greeting) is complete, then PAUSE the socket and stop
 *  listening. Resolves with every frame the final chunk yielded (the
 *  greeting is `[0]`); rejects if the socket errors or closes before
 *  any frame arrives — `connect()` then throws, exactly as the old
 *  in-channel awaiter did. Leaving the socket paused lets the caller
 *  hand it (and the primed reader) to the channel with no
 *  listener-reattach race. */
function receiveGreeting(
    sock: Socket,
    reader: FrameReader,
): Promise<[Frame, ...Frame[]]> {
    return new Promise<[Frame, ...Frame[]]>((resolve, reject) => {
        const onData = (chunk: Buffer) => {
            const frames = reader.push(chunk);
            if (frames.length === 0) return; // partial greeting — keep reading
            detach();
            sock.pause();
            // length checked just above, so the list is non-empty here.
            resolve(frames as [Frame, ...Frame[]]);
        };
        const onError = (err: Error) => {
            detach();
            reject(err);
        };
        const onClose = () => {
            detach();
            reject(new Error("Comm channel closed before first frame"));
        };
        function detach() {
            sock.off("data", onData);
            sock.off("error", onError);
            sock.off("close", onClose);
        }
        sock.on("data", onData);
        sock.once("error", onError);
        sock.once("close", onClose);
    });
}
