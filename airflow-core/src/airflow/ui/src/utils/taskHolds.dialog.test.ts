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
  getHoldTarget,
  getOfferedHoldScopes,
  getScopeStateLineKey,
  isRunFinished,
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

describe("isRunFinished", () => {
  it.each(["success", "failed"] as const)("says a run that is %s has finished", (runState) => {
    expect(isRunFinished(runState)).toBe(true);
  });

  it.each(["queued", "running", undefined] as const)("says a run that is %s has not", (runState) => {
    expect(isRunFinished(runState)).toBe(false);
  });
});

describe("getOfferedHoldScopes", () => {
  it("offers both kinds from a task instance page, the task instance first", () => {
    expect(getOfferedHoldScopes(taskInstancePage)).toEqual(["taskInstance", "task"]);
  });

  it.each(["queued", "running"] as const)("offers both kinds while the run is %s", (runState) => {
    expect(getOfferedHoldScopes({ ...taskInstancePage, runState })).toEqual(["taskInstance", "task"]);
  });

  // A task instance hold ends with its run, so on a run that has finished it would end at once.
  it.each(["success", "failed"] as const)(
    "offers only the task in every run once the run is %s",
    (runState) => {
      expect(getOfferedHoldScopes({ ...taskInstancePage, runState })).toEqual(["task"]);
    },
  );

  it("offers only the task in every run from a task page", () => {
    expect(getOfferedHoldScopes(taskPage)).toEqual(["task"]);
  });
});

describe("getDefaultHoldScope", () => {
  it.each([
    { origin: taskInstancePage, scope: "taskInstance", title: "a task instance page" },
    { origin: taskPage, scope: "task", title: "a task page" },
  ] as const)("starts on the kind of hold of $title", ({ origin, scope }) => {
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

  it.each(["success", "failed"] as const)(
    "starts on the task in every run once the run is %s",
    (runState) => {
      expect(getDefaultHoldScope({ ...taskInstancePage, runState, state: null })).toBe("task");
    },
  );

  it("leaves a task page on the task in every run whatever the state", () => {
    expect(getDefaultHoldScope({ ...taskPage, state: "success" })).toBe("task");
    expect(getDefaultHoldScope({ ...taskPage, state: "running" })).toBe("task");
  });
});

describe("getHoldTarget", () => {
  it.each([
    { scope: "taskInstance", target: { runId: "run_1", taskId: "load" } },
    { scope: "task", target: { taskId: "load" } },
  ] as const)("targets $target for a hold on $scope from a task instance page", ({ scope, target }) => {
    expect(getHoldTarget(scope, taskInstancePage)).toStrictEqual(target);
  });

  it("targets just the task from a task page", () => {
    expect(getHoldTarget("task", taskPage)).toStrictEqual({ taskId: "load" });
  });
});

describe("getScopeStateLineKey", () => {
  it("says what the hold does to a task instance in its state, once the state is known", () => {
    expect(getScopeStateLineKey("taskInstance", { state: "running" })).toBe("running");
    expect(getScopeStateLineKey("taskInstance", { state: null })).toBe("notStarted");
    expect(getScopeStateLineKey("taskInstance", {})).toBeUndefined();
  });

  it("reads the state of the task instance only for a task instance hold", () => {
    expect(getScopeStateLineKey("task", { state: "success" })).toBeUndefined();
    expect(getScopeStateLineKey("task", { state: null })).toBeUndefined();
  });
});
