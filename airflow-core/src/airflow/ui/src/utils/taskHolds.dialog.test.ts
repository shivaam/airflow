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
import type { TaskInstanceState } from "openapi/requests/types.gen";

import {
  getDefaultHoldScope,
  getHoldRequestScope,
  getOfferedHoldScopes,
  getScopeStateLineKey,
  isTaskInstanceFinished,
} from "./taskHolds";

const finishedStates: Array<TaskInstanceState> = [
  "success",
  "failed",
  "skipped",
  "upstream_failed",
  "removed",
];
const unfinishedStates = $TaskInstanceState.enum.filter((state) => !finishedStates.includes(state));

const taskInstancePage = { runId: "run_1", taskId: "load" };
const taskPage = { taskId: "load" };
const runPage = { runId: "run_1" };

describe("isTaskInstanceFinished", () => {
  it.each(finishedStates)("says a task instance in %s has finished", (state) => {
    expect(isTaskInstanceFinished(state)).toBe(true);
  });

  it.each(unfinishedStates)("says a task instance in %s has not", (state) => {
    expect(isTaskInstanceFinished(state)).toBe(false);
  });

  it("says a task instance with no state, or one not known, has not", () => {
    expect(isTaskInstanceFinished(null)).toBe(false);
    expect(isTaskInstanceFinished(undefined)).toBe(false);
  });
});

describe("getOfferedHoldScopes", () => {
  it("offers all three from a task instance page, the task instance first", () => {
    expect(getOfferedHoldScopes(taskInstancePage)).toEqual(["taskInstance", "task", "run"]);
  });

  it("offers only the task in every run from a task page", () => {
    expect(getOfferedHoldScopes(taskPage)).toEqual(["task"]);
  });

  it("offers only every task in the run from a run page", () => {
    expect(getOfferedHoldScopes(runPage)).toEqual(["run"]);
  });
});

describe("getDefaultHoldScope", () => {
  it.each([
    { origin: taskInstancePage, scope: "taskInstance", title: "a task instance page" },
    { origin: taskPage, scope: "task", title: "a task page" },
    { origin: runPage, scope: "run", title: "a run page" },
  ] as const)("starts on the scope of $title", ({ origin, scope }) => {
    expect(getDefaultHoldScope(origin)).toBe(scope);
  });

  it.each(unfinishedStates)("starts on the task instance while it is in %s", (state) => {
    expect(getDefaultHoldScope({ ...taskInstancePage, state })).toBe("taskInstance");
  });

  it("starts on the task instance while it has no state, or its state is not known", () => {
    expect(getDefaultHoldScope({ ...taskInstancePage, state: null })).toBe("taskInstance");
    expect(getDefaultHoldScope(taskInstancePage)).toBe("taskInstance");
  });

  it.each(finishedStates)(
    "starts on the task in every run once the task instance is %s, which cannot be stopped in its run",
    (state) => {
      expect(getDefaultHoldScope({ ...taskInstancePage, state })).toBe("task");
    },
  );

  it("leaves a task page and a run page on their own scope whatever the state", () => {
    expect(getDefaultHoldScope({ ...taskPage, state: "success" })).toBe("task");
    expect(getDefaultHoldScope({ ...runPage, state: "success" })).toBe("run");
  });
});

describe("getHoldRequestScope", () => {
  it.each([
    { scope: "taskInstance", sets: { run_id: "run_1", task_id: "load" } },
    { scope: "task", sets: { run_id: null, task_id: "load" } },
    { scope: "run", sets: { run_id: "run_1", task_id: null } },
  ] as const)("sets $sets for a hold on $scope from a task instance page", ({ scope, sets }) => {
    expect(getHoldRequestScope(scope, taskInstancePage)).toEqual(sets);
  });

  it("sets just the task from a task page, and just the run from a run page", () => {
    expect(getHoldRequestScope("task", taskPage)).toEqual({ run_id: null, task_id: "load" });
    expect(getHoldRequestScope("run", runPage)).toEqual({ run_id: "run_1", task_id: null });
  });
});

describe("getScopeStateLineKey", () => {
  it("says what the hold does to a task instance in its state, once the state is known", () => {
    expect(getScopeStateLineKey("taskInstance", { state: "running" })).toBe("running");
    expect(getScopeStateLineKey("taskInstance", { state: null })).toBe("notStarted");
    expect(getScopeStateLineKey("taskInstance", {})).toBeUndefined();
  });

  it.each(["success", "failed"] as const)("says a run that is %s has finished", (runState) => {
    expect(getScopeStateLineKey("run", { runState })).toBe("runFinished");
  });

  it.each(["queued", "running"] as const)("says nothing of a run that is %s", (runState) => {
    expect(getScopeStateLineKey("run", { runState })).toBeUndefined();
  });

  it("says nothing of a run whose state is not known", () => {
    expect(getScopeStateLineKey("run", {})).toBeUndefined();
  });

  it("reads the state of the task instance only for a task instance, and of the run only for a run", () => {
    expect(getScopeStateLineKey("run", { runState: "running", state: "success" })).toBeUndefined();
    expect(getScopeStateLineKey("taskInstance", { runState: "success", state: "running" })).toBe("running");
    expect(getScopeStateLineKey("task", { runState: "success", state: "success" })).toBeUndefined();
  });
});
