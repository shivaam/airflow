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

import { getDefaultHoldScope, getHoldTarget, getOfferedHoldScopes, getScopeStateLineKey } from "./taskHolds";

const taskInstancePage = { runId: "run_1", taskId: "load" };
const taskPage = { taskId: "load" };

describe("getOfferedHoldScopes", () => {
  it("offers both kinds from a task instance page, the task instance first", () => {
    expect(getOfferedHoldScopes(taskInstancePage)).toEqual(["taskInstance", "task"]);
  });

  it("offers only the task in every run from a task page", () => {
    expect(getOfferedHoldScopes(taskPage)).toEqual(["task"]);
  });
});

describe("getDefaultHoldScope", () => {
  // It follows from the page alone, so it does not change while the dialog is open.
  it.each([
    { origin: taskInstancePage, scope: "taskInstance", title: "a task instance page" },
    { origin: taskPage, scope: "task", title: "a task page" },
  ] as const)("starts on the kind of hold of $title", ({ origin, scope }) => {
    expect(getDefaultHoldScope(origin)).toBe(scope);
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
