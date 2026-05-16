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

// Unit tests for the not-found contract (DESIGN.md CD-1 / CD-2).
// A fake comm channel returns one queued frame per request — no
// sockets, no Python.

import { describe, it, expect } from "vitest";
import { createCoordinatorClient } from "../../src/coordinator/client.js";
import type { CommChannel } from "../../src/coordinator/comm-channel.js";

function fakeComm(frames: { body: unknown; error?: unknown }[]): CommChannel {
    let i = 0;
    return {
        request: async () => frames[i++],
    } as unknown as CommChannel;
}

describe("getVariable not-found contract (CD-1/CD-2)", () => {
    it("returns null for the exact VARIABLE_NOT_FOUND code", async () => {
        const c = createCoordinatorClient(
            fakeComm([{ body: { type: "ErrorResponse", error: "VARIABLE_NOT_FOUND" } }]),
        );
        expect(await c.getVariable("x")).toBeNull();
    });

    it("throws for a non-not-found ErrorResponse", async () => {
        const c = createCoordinatorClient(
            fakeComm([{ body: { type: "ErrorResponse", error: "API_SERVER_ERROR" } }]),
        );
        await expect(c.getVariable("x")).rejects.toThrow(/API_SERVER_ERROR/);
    });

    it("does NOT treat a value that merely contains 'NOT_FOUND' as absence", async () => {
        const c = createCoordinatorClient(
            fakeComm([{ body: { type: "VariableResult", key: "x", value: "NOT_FOUND_LOL" } }]),
        );
        expect(await c.getVariable("x")).toBe("NOT_FOUND_LOL");
    });

    it("does NOT treat an error code merely containing the substring as not-found", async () => {
        // "SOMETHING_NOT_FOUND_ISH" is not in the exact set → must throw.
        const c = createCoordinatorClient(
            fakeComm([{ body: { type: "ErrorResponse", error: "SOMETHING_NOT_FOUND_ISH" } }]),
        );
        await expect(c.getVariable("x")).rejects.toThrow();
    });
});

describe("getXCom not-found contract", () => {
    it("returns null for the exact XCOM_NOT_FOUND code", async () => {
        const c = createCoordinatorClient(
            fakeComm([{ body: { type: "ErrorResponse", error: "XCOM_NOT_FOUND" } }]),
        );
        expect(
            await c.getXCom({ key: "k", dagId: "d", taskId: "t", runId: "r" }),
        ).toBeNull();
    });
});
