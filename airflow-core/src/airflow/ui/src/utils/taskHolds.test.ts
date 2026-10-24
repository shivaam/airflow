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

import { $TaskInstanceState } from "openapi/requests/schemas.gen";
import type { GridNodeResponse, TaskHoldResponse, TaskInstanceState } from "openapi/requests/types.gen";

import {
  getHoldLockKind,
  getHoldScope,
  getHoldScopeLink,
  getMappedTaskIds,
  holdMatches,
  validateHoldForm,
  type HoldLockKind,
} from "./taskHolds";

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

// A task instance response also carries a map index, which a hold never looks at.
const makeTaskInstance = (state: TaskInstanceState | null, mapIndex = -1) => ({
  dag_id: "example_dag",
  dag_run_id: "run_1",
  map_index: mapIndex,
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

describe("getHoldLockKind", () => {
  describe("for a task instance", () => {
    const holds = [makeHold({ task_id: "load" })];

    // The states of the lock design's table, grouped by what a hold does to a task instance in them.
    const waitingStates: Array<TaskInstanceState> = ["restarting"];
    const heldStates: Array<TaskInstanceState> = ["scheduled", "up_for_retry", "up_for_reschedule"];
    const carryingOnKinds = {
      awaiting_input: "awaitingInput",
      deferred: "deferred",
      queued: "queued",
      running: "running",
    } as const satisfies Partial<Record<TaskInstanceState, HoldLockKind>>;
    const finishedStates: Array<TaskInstanceState> = [
      "success",
      "failed",
      "skipped",
      "upstream_failed",
      "removed",
    ];

    it.each(heldStates)("is held while the task instance waits in %s", (state) => {
      expect(getHoldLockKind(holds, makeTaskInstance(state))).toBe("held");
    });

    it.each(waitingStates)("waits where it is while the task instance is %s", (state) => {
      expect(getHoldLockKind(holds, makeTaskInstance(state))).toBe("waiting");
    });

    it("waits where it is while the task instance has no state, whether it is null or left out", () => {
      expect(getHoldLockKind(holds, makeTaskInstance(null))).toBe("waiting");
      expect(getHoldLockKind(holds, { dag_id: "example_dag", dag_run_id: "run_1", task_id: "load" })).toBe(
        "waiting",
      );
    });

    it.each(Object.entries(carryingOnKinds))("locks a task instance in %s as %s", (state, kind) => {
      expect(getHoldLockKind(holds, makeTaskInstance(state as TaskInstanceState))).toBe(kind);
    });

    it.each(finishedStates)("shows nothing for a task instance that is already %s", (state) => {
      expect(getHoldLockKind(holds, makeTaskInstance(state))).toBeUndefined();
    });

    it("decides every task instance state", () => {
      const decided = [...heldStates, ...waitingStates, ...Object.keys(carryingOnKinds), ...finishedStates];

      expect(decided.sort()).toEqual([...$TaskInstanceState.enum].sort());
    });

    it("is covered by a hold on its task in every run", () => {
      expect(getHoldLockKind(holds, { ...makeTaskInstance("scheduled"), dag_run_id: "run_2" })).toBe("held");
    });

    it("is covered by a hold on its run, for every task in it", () => {
      const runHolds = [makeHold({ run_id: "run_1" })];

      expect(getHoldLockKind(runHolds, makeTaskInstance("scheduled"))).toBe("held");
      expect(getHoldLockKind(runHolds, { ...makeTaskInstance("scheduled"), task_id: "extract" })).toBe(
        "held",
      );
      expect(
        getHoldLockKind(runHolds, { ...makeTaskInstance("scheduled"), dag_run_id: "run_2" }),
      ).toBeUndefined();
    });

    it("is covered by a hold on its task and run only in that run", () => {
      const pairHolds = [makeHold({ run_id: "run_1", task_id: "load" })];

      expect(getHoldLockKind(pairHolds, makeTaskInstance("scheduled"))).toBe("held");
      expect(
        getHoldLockKind(pairHolds, { ...makeTaskInstance("scheduled"), dag_run_id: "run_2" }),
      ).toBeUndefined();
      expect(
        getHoldLockKind(pairHolds, { ...makeTaskInstance("scheduled"), task_id: "extract" }),
      ).toBeUndefined();
    });

    it.each([-1, 0, 1, 42])("is covered whatever its map index (%i)", (mapIndex) => {
      const pairHolds = [makeHold({ run_id: "run_1", task_id: "load" })];

      expect(getHoldLockKind(pairHolds, makeTaskInstance("scheduled", mapIndex))).toBe("held");
      expect(getHoldLockKind(pairHolds, makeTaskInstance("running", mapIndex))).toBe("running");
    });

    it("is never covered by a hold of another Dag", () => {
      expect(
        getHoldLockKind([makeHold({ dag_id: "other_dag", task_id: "load" })], makeTaskInstance("scheduled")),
      ).toBeUndefined();
    });

    it("ignores holds that are released or expired", () => {
      expect(
        getHoldLockKind([makeHold({ active: false, task_id: "load" })], makeTaskInstance("scheduled")),
      ).toBeUndefined();
    });

    it("ignores holds that do not match, and there being none", () => {
      expect(
        getHoldLockKind([makeHold({ task_id: "extract" })], makeTaskInstance("scheduled")),
      ).toBeUndefined();
      expect(getHoldLockKind([], makeTaskInstance("scheduled"))).toBeUndefined();
    });

    it("is covered when any one of several holds matches", () => {
      expect(
        getHoldLockKind(
          [makeHold({ id: 1, task_id: "extract" }), makeHold({ id: 2, run_id: "run_1" })],
          makeTaskInstance("scheduled"),
        ),
      ).toBe("held");
    });
  });

  describe("for a task across its runs", () => {
    const subject = { dag_id: "example_dag", task_id: "load" };

    it("is covered by a hold on the task in every run", () => {
      expect(getHoldLockKind([makeHold({ task_id: "load" })], subject)).toBe("task");
    });

    it("is not covered by a hold on a single run of the task, or on a run", () => {
      expect(getHoldLockKind([makeHold({ run_id: "run_1", task_id: "load" })], subject)).toBeUndefined();
      expect(getHoldLockKind([makeHold({ run_id: "run_1" })], subject)).toBeUndefined();
    });

    it("is not covered by a hold on another task, another Dag, or one that is no longer active", () => {
      expect(getHoldLockKind([makeHold({ task_id: "extract" })], subject)).toBeUndefined();
      expect(getHoldLockKind([makeHold({ dag_id: "other_dag", task_id: "load" })], subject)).toBeUndefined();
      expect(getHoldLockKind([makeHold({ active: false, task_id: "load" })], subject)).toBeUndefined();
    });
  });

  describe("for a run as a whole", () => {
    const subject = { dag_id: "example_dag", dag_run_id: "run_1" };

    it("is covered by a hold on the run", () => {
      expect(getHoldLockKind([makeHold({ run_id: "run_1" })], subject)).toBe("run");
    });

    it("is not covered by a hold on a task of the run, or on a task in every run", () => {
      expect(getHoldLockKind([makeHold({ run_id: "run_1", task_id: "load" })], subject)).toBeUndefined();
      expect(getHoldLockKind([makeHold({ task_id: "load" })], subject)).toBeUndefined();
    });

    it("is not covered by a hold on another run, another Dag, or one that is no longer active", () => {
      expect(getHoldLockKind([makeHold({ run_id: "run_2" })], subject)).toBeUndefined();
      expect(getHoldLockKind([makeHold({ dag_id: "other_dag", run_id: "run_1" })], subject)).toBeUndefined();
      expect(getHoldLockKind([makeHold({ active: false, run_id: "run_1" })], subject)).toBeUndefined();
    });
  });
});

const node = (overrides: Partial<GridNodeResponse> & Pick<GridNodeResponse, "id">): GridNodeResponse => ({
  is_mapped: null,
  label: overrides.id,
  ...overrides,
});

describe("getMappedTaskIds", () => {
  it("is empty before the structure has loaded", () => {
    expect(getMappedTaskIds(undefined).size).toBe(0);
  });

  it("collects the mapped tasks and leaves the others out", () => {
    expect([...getMappedTaskIds([node({ id: "extract" }), node({ id: "load", is_mapped: true })])]).toEqual([
      "load",
    ]);
  });

  it("finds mapped tasks inside task groups, including those inside a mapped group", () => {
    const nodes = [
      node({
        children: [node({ id: "group.plain" }), node({ id: "group.expanded", is_mapped: true })],
        id: "group",
      }),
      node({
        children: [node({ id: "mapped_group.inner", is_mapped: true })],
        id: "mapped_group",
        is_mapped: true,
      }),
    ];

    expect([...getMappedTaskIds(nodes)].sort()).toEqual([
      "group.expanded",
      "mapped_group",
      "mapped_group.inner",
    ]);
  });
});

describe("getHoldScopeLink", () => {
  const mappedTaskIds = new Set(["transform"]);

  it("links a hold on a run to the run", () => {
    expect(getHoldScopeLink(makeHold({ run_id: "run_1" }), mappedTaskIds)).toBe(
      "/dags/example_dag/runs/run_1",
    );
  });

  it("links a hold on a task in every run to the task, mapped or not", () => {
    expect(getHoldScopeLink(makeHold({ task_id: "load" }), mappedTaskIds)).toBe(
      "/dags/example_dag/tasks/load",
    );
    expect(getHoldScopeLink(makeHold({ task_id: "transform" }), mappedTaskIds)).toBe(
      "/dags/example_dag/tasks/transform",
    );
  });

  it("links a hold on a task and run to the task instance", () => {
    expect(getHoldScopeLink(makeHold({ run_id: "run_1", task_id: "load" }), mappedTaskIds)).toBe(
      "/dags/example_dag/runs/run_1/tasks/load",
    );
  });

  it("links a hold on a mapped task and run to the list of its instances, as it has no single one", () => {
    expect(getHoldScopeLink(makeHold({ run_id: "run_1", task_id: "transform" }), mappedTaskIds)).toBe(
      "/dags/example_dag/runs/run_1/tasks/transform/mapped",
    );
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
