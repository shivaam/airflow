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

import { getHeldNowLockKind, getHeldNowLockKinds } from "./taskHolds";

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
  task_id: "load",
  ...overrides,
});

const finishedStates: Array<TaskInstanceState> = [
  "success",
  "failed",
  "skipped",
  "upstream_failed",
  "removed",
];

const makeTaskInstance = (state: TaskInstanceState | null) => ({
  dag_id: "example_dag",
  dag_run_id: "run_1",
  state,
  task_id: "load",
});

describe("getHeldNowLockKind", () => {
  const holds = [makeHold({ task_id: "load" })];

  it.each([
    { kind: "waiting", state: null },
    { kind: "held", state: "scheduled" },
  ] as const)(
    "gives the %s lock to a task instance the hold is stopping in state $state",
    ({ kind, state }) => {
      expect(getHeldNowLockKind(holds, makeTaskInstance(state))).toBe(kind);
    },
  );

  it.each([
    "up_for_retry",
    "up_for_reschedule",
    "queued",
    "running",
    "restarting",
    "deferred",
    "awaiting_input",
    ...finishedStates,
  ] as const)("gives nothing to a covered task instance in state %s, where the hold is pending", (state) => {
    expect(getHeldNowLockKind(holds, makeTaskInstance(state))).toBeUndefined();
  });

  it("gives nothing to a task instance that no active hold covers", () => {
    expect(getHeldNowLockKind([makeHold({ task_id: "extract" })], makeTaskInstance("scheduled"))).toBe(
      undefined,
    );
    expect(
      getHeldNowLockKind([makeHold({ active: false, task_id: "load" })], makeTaskInstance("scheduled")),
    ).toBe(undefined);
  });

  it("gives the task lock to a task across its runs", () => {
    expect(getHeldNowLockKind(holds, { dag_id: "example_dag", task_id: "load" })).toBe("task");
  });
});

describe("getHeldNowLockKinds", () => {
  const taskIds = ["load", "extract", "publish", "report", "notify", "wait"];
  const states = new Map<string, TaskInstanceState | null>([
    ["extract", "running"],
    ["load", "scheduled"],
    ["notify", "up_for_retry"],
    ["publish", "success"],
    ["report", null],
    ["wait", "queued"],
  ]);
  // A task instance hold on every task of the run.
  const runHolds = taskIds.map((taskId, index) =>
    makeHold({ id: index + 1, run_id: "run_1", task_id: taskId }),
  );

  it("holds the tasks whose task instance the hold is stopping now, and not those it leaves pending", () => {
    expect(
      getHeldNowLockKinds(runHolds, { dagId: "example_dag", run: { id: "run_1", states }, taskIds }),
    ).toEqual(
      new Map([
        ["load", "held"],
        ["report", "waiting"],
      ]),
    );
  });

  it("counts a task with no task instance in the run yet as having no state", () => {
    expect(
      getHeldNowLockKinds(runHolds, {
        dagId: "example_dag",
        run: { id: "run_1", states: new Map() },
        taskIds: ["load"],
      }),
    ).toEqual(new Map([["load", "waiting"]]));
  });

  it("holds nothing until the states of the run have loaded", () => {
    expect(
      getHeldNowLockKinds(runHolds, {
        dagId: "example_dag",
        run: { id: "run_1", states: undefined },
        taskIds,
      }),
    ).toEqual(new Map());
  });

  it("holds only what a hold covers in that run", () => {
    const pairHolds = [makeHold({ run_id: "run_2", task_id: "load" }), makeHold({ task_id: "extract" })];

    expect(
      getHeldNowLockKinds(pairHolds, { dagId: "example_dag", run: { id: "run_1", states }, taskIds }),
    ).toEqual(new Map());
    expect(
      getHeldNowLockKinds([makeHold({ task_id: "load" })], {
        dagId: "example_dag",
        run: { id: "run_1", states },
        taskIds,
      }),
    ).toEqual(new Map([["load", "held"]]));
  });

  it("holds a task that a hold on it in every run covers, when no run is in view", () => {
    expect(getHeldNowLockKinds([makeHold({ task_id: "load" })], { dagId: "example_dag", taskIds })).toEqual(
      new Map([["load", "task"]]),
    );
  });

  it("does not hold a task for a task instance hold, when no run is in view", () => {
    expect(
      getHeldNowLockKinds(runHolds, {
        dagId: "example_dag",
        taskIds,
      }),
    ).toEqual(new Map());
  });

  it("holds nothing without an active hold on the Dag", () => {
    const run = { id: "run_1", states };

    expect(getHeldNowLockKinds([], { dagId: "example_dag", run, taskIds })).toEqual(new Map());
    expect(
      getHeldNowLockKinds(
        runHolds.map((hold) => ({ ...hold, active: false })),
        { dagId: "example_dag", run, taskIds },
      ),
    ).toEqual(new Map());
    expect(
      getHeldNowLockKinds(
        runHolds.map((hold) => ({ ...hold, dag_id: "other_dag" })),
        { dagId: "example_dag", run, taskIds },
      ),
    ).toEqual(new Map());
  });
});
