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
import { describe, expect, it } from "vitest";

import type { TaskHoldResponse, TaskInstanceState } from "openapi/requests/types.gen";

import { getHoldScope, holdMatches, isHoldPending, validateHoldForm } from "./taskHolds";

const makeHold = (overrides: Partial<TaskHoldResponse> = {}): TaskHoldResponse => ({
  active: true,
  created_at: "2026-10-20T10:00:00Z",
  created_by: "admin",
  dag_id: "example_dag",
  expires_at: null,
  id: 1,
  reason: "Upstream data is wrong",
  release_reason: null,
  released_at: null,
  released_by: null,
  run_id: null,
  task_id: null,
  ...overrides,
});

const target = { dagId: "example_dag", runId: "run_1", taskId: "load" };

const makeTaskInstance = (state: TaskInstanceState | null) => ({
  dag_id: "example_dag",
  dag_run_id: "run_1",
  state,
  task_id: "load",
});

describe("getHoldScope", () => {
  it("is task when only task_id is set", () => {
    expect(getHoldScope(makeHold({ task_id: "load" }))).toBe("task");
  });

  it("is run when only run_id is set", () => {
    expect(getHoldScope(makeHold({ run_id: "run_1" }))).toBe("run");
  });

  it("is taskInstance when both are set", () => {
    expect(getHoldScope(makeHold({ run_id: "run_1", task_id: "load" }))).toBe("taskInstance");
  });
});

describe("holdMatches", () => {
  it("matches a task hold in every run of that task", () => {
    const hold = makeHold({ task_id: "load" });

    expect(holdMatches(hold, target)).toBe(true);
    expect(holdMatches(hold, { ...target, runId: "run_2" })).toBe(true);
  });

  it("does not match a task hold on another task", () => {
    expect(holdMatches(makeHold({ task_id: "extract" }), target)).toBe(false);
  });

  it("matches a run hold for every task in that run", () => {
    const hold = makeHold({ run_id: "run_1" });

    expect(holdMatches(hold, target)).toBe(true);
    expect(holdMatches(hold, { ...target, taskId: "extract" })).toBe(true);
  });

  it("does not match a run hold on another run", () => {
    expect(holdMatches(makeHold({ run_id: "run_2" }), target)).toBe(false);
  });

  it("matches a task and run hold only for that task instance", () => {
    const hold = makeHold({ run_id: "run_1", task_id: "load" });

    expect(holdMatches(hold, target)).toBe(true);
    expect(holdMatches(hold, { ...target, runId: "run_2" })).toBe(false);
    expect(holdMatches(hold, { ...target, taskId: "extract" })).toBe(false);
  });

  it("never matches a hold of another Dag", () => {
    expect(holdMatches(makeHold({ dag_id: "other_dag", task_id: "load" }), target)).toBe(false);
  });
});

describe("isHoldPending", () => {
  const holds = [makeHold({ task_id: "load" })];

  it.each<TaskInstanceState | null>(["scheduled", "up_for_retry", "deferred", "awaiting_input", null])(
    "is pending for a matching task instance in %s",
    (state) => {
      expect(isHoldPending(holds, makeTaskInstance(state))).toBe(true);
    },
  );

  it("is not pending once the task instance is held", () => {
    expect(isHoldPending(holds, makeTaskInstance("held"))).toBe(false);
  });

  it.each<TaskInstanceState>([
    "success",
    "failed",
    "skipped",
    "upstream_failed",
    "removed",
    "queued",
    "running",
  ])("is not pending for a task instance the hold cannot affect (%s)", (state) => {
    expect(isHoldPending(holds, makeTaskInstance(state))).toBe(false);
  });

  it("ignores holds that are released or expired", () => {
    expect(isHoldPending([makeHold({ active: false, task_id: "load" })], makeTaskInstance("scheduled"))).toBe(
      false,
    );
  });

  it("ignores holds that do not match", () => {
    expect(isHoldPending([makeHold({ task_id: "extract" })], makeTaskInstance("scheduled"))).toBe(false);
    expect(isHoldPending([], makeTaskInstance("scheduled"))).toBe(false);
  });

  it("is pending when any one of several holds matches", () => {
    expect(
      isHoldPending(
        [makeHold({ id: 1, task_id: "extract" }), makeHold({ id: 2, run_id: "run_1" })],
        makeTaskInstance("scheduled"),
      ),
    ).toBe(true);
  });
});

describe("validateHoldForm", () => {
  const now = new Date("2026-10-20T12:00:00Z");

  it("requires a reason", () => {
    expect(validateHoldForm({ expiresAt: "", reason: "" }, now)).toEqual({ reason: "required" });
  });

  it("treats a whitespace-only reason as missing", () => {
    expect(validateHoldForm({ expiresAt: "", reason: "   " }, now)).toEqual({ reason: "required" });
  });

  it("accepts a reason without an expiry", () => {
    expect(validateHoldForm({ expiresAt: "", reason: "Investigating" }, now)).toEqual({});
  });

  it("accepts an expiry in the future", () => {
    expect(validateHoldForm({ expiresAt: "2026-10-21T12:00:00Z", reason: "Investigating" }, now)).toEqual({});
  });

  it("rejects an expiry that has passed", () => {
    expect(validateHoldForm({ expiresAt: "2026-10-19T12:00:00Z", reason: "Investigating" }, now)).toEqual({
      expiresAt: "past",
    });
  });

  it("reports every problem at once", () => {
    expect(validateHoldForm({ expiresAt: "2026-10-19T12:00:00Z", reason: "" }, now)).toEqual({
      expiresAt: "past",
      reason: "required",
    });
  });
});
