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

// Log channel — newline-delimited JSON log records over TCP.
//
// The Airflow coordinator's `_bridge` reads lines from this socket,
// parses each as JSON, and re-emits through structlog using the same
// handler used for ordinary Python task logs
// (`process_log_messages_from_subprocess`). Required fields are
// `event`, `level`, `logger`, `timestamp`. Extra fields pass through
// as structured log keys.

import { Socket } from "node:net";

export type LogLevel = "debug" | "info" | "warning" | "error";

export interface LogRecord {
    event: string;
    level: LogLevel;
    logger: string;
    timestamp: string;
    [key: string]: unknown;
}

export class LogChannel {
    private readonly sock: Socket;

    private constructor(sock: Socket) {
        this.sock = sock;
    }

    static async connect(addr: string): Promise<LogChannel> {
        const [host, portStr] = splitHostPort(addr);
        return new Promise((resolve, reject) => {
            const sock = new Socket();
            sock.once("connect", () => {
                sock.setNoDelay(true);
                resolve(new LogChannel(sock));
            });
            sock.once("error", reject);
            sock.connect(Number.parseInt(portStr, 10), host);
        });
    }

    send(record: Omit<LogRecord, "timestamp"> & { timestamp?: string }): void {
        const line = JSON.stringify({
            ...record,
            timestamp: record.timestamp ?? new Date().toISOString(),
        });
        this.sock.write(Buffer.from(line + "\n", "utf8"));
    }

    debug(event: string, args: Record<string, unknown> = {}): void {
        this.send({ event, level: "debug", logger: "task", ...args });
    }

    info(event: string, args: Record<string, unknown> = {}): void {
        this.send({ event, level: "info", logger: "task", ...args });
    }

    warning(event: string, args: Record<string, unknown> = {}): void {
        this.send({ event, level: "warning", logger: "task", ...args });
    }

    error(event: string, args: Record<string, unknown> = {}): void {
        this.send({ event, level: "error", logger: "task", ...args });
    }

    async close(): Promise<void> {
        return new Promise((resolve) => {
            this.sock.end(() => resolve());
        });
    }
}

function splitHostPort(addr: string): [string, string] {
    const idx = addr.lastIndexOf(":");
    if (idx < 0) throw new Error(`Address must be host:port, got ${addr}`);
    return [addr.slice(0, idx), addr.slice(idx + 1)];
}
