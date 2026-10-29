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
import "@testing-library/jest-dom/vitest";
import { render, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type {
  GridRunsResponse,
  GridTISummaries,
  TaskHoldResponse,
  TaskInstanceState,
} from "openapi/requests/types.gen";

import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { GroupsProvider } from "src/context/groups";
import { TimezoneProvider } from "src/context/timezone";
import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import { Grid } from "./Grid";

const buildRun = (runId: string): GridRunsResponse => ({
  dag_id: "example_dag",
  dag_versions: [],
  duration: 1,
  end_date: null,
  has_missed_deadline: false,
  has_note: false,
  queued_at: null,
  run_after: "2026-09-22T00:00:00Z",
  run_id: runId,
  run_type: "manual",
  start_date: null,
  state: "running",
});

const gridRuns = [buildRun("run_2"), buildRun("run_1")];

vi.mock("src/queries/useGridRuns.ts", () => ({
  useGridRuns: () => ({ data: gridRuns, isLoading: false }),
}));

vi.mock("src/queries/useGridStructure.ts", () => ({
  useGridStructure: () => ({
    data: [
      { id: "scheduled_task", label: "scheduled_task" },
      { id: "running_task", label: "running_task" },
      { id: "finished_task", label: "finished_task" },
      { id: "mapped_task", is_mapped: true, label: "mapped_task" },
      { id: "retry_task", label: "retry_task" },
      { children: [{ id: "task_group.child", label: "child" }], id: "task_group", label: "task_group" },
    ],
  }),
}));

const states: Record<string, TaskInstanceState | null> = {
  finished_task: "success",
  mapped_task: "scheduled",
  retry_task: "up_for_retry",
  running_task: "running",
  scheduled_task: "scheduled",
  task_group: "scheduled",
};

const summaries = (runId: string): GridTISummaries => ({
  dag_id: "example_dag",
  run_id: runId,
  task_instances: Object.entries(states).map(([taskId, state]) => ({
    child_states: null,
    dag_version_number: 1,
    max_end_date: null,
    min_start_date: null,
    state,
    task_display_name: taskId,
    task_id: taskId,
  })),
});

vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: () => ({
    summariesByRunId: new Map([
      ["run_1", summaries("run_1")],
      ["run_2", summaries("run_2")],
    ]),
  }),
}));

// happy-dom gives the scroll container no size, so the real virtualizer renders no rows.
vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getTotalSize: () => count * 20,
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({ index, size: 20, start: index * 20 })),
    scrollElement: undefined,
    scrollToIndex: () => undefined,
  }),
}));

const hold: TaskHoldResponse = {
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
  run_id: "run_1",
  task_id: null,
};

let holdsRequests = 0;

const serveHolds = (holds: Array<TaskHoldResponse>) =>
  server.use(
    http.get("/api/v2/dags/example_dag/holds", () => {
      holdsRequests += 1;

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );

const renderGrid = () =>
  render(
    <BaseWrapper>
      <TimezoneProvider>
        <MemoryRouter initialEntries={["/dags/example_dag"]}>
          <Routes>
            <Route
              element={
                <GroupsProvider dagId="example_dag">
                  <Grid
                    limit={10}
                    offset={0}
                    onJumpToLatest={vi.fn()}
                    setOffset={vi.fn()}
                    showVersionIndicatorMode={VersionIndicatorOptions.ALL}
                  />
                </GroupsProvider>
              }
              path="/dags/:dagId"
            />
          </Routes>
        </MemoryRouter>
      </TimezoneProvider>
    </BaseWrapper>,
  );

// The cells of a task, by run, that carry the lock.
const heldCells = (container: HTMLElement, runId: string) =>
  [...container.querySelectorAll(`[data-run-id="${runId}"][data-task-id]`)]
    .filter((cell) => cell.querySelector('[data-testid="grid-hold-lock"]') !== null)
    .map((cell) => cell.getAttribute("data-task-id"));

describe("Grid holds", () => {
  beforeEach(() => {
    holdsRequests = 0;
  });

  it("marks the task instances a hold is stopping now, and not one waiting out a retry delay, a running or finished one, or a group", async () => {
    serveHolds([hold]);
    const { container } = renderGrid();

    await waitFor(() => expect(heldCells(container, "run_1")).toEqual(["scheduled_task", "mapped_task"]));
  });

  it("marks only the run the hold covers", async () => {
    serveHolds([hold]);
    const { container } = renderGrid();

    await waitFor(() => expect(heldCells(container, "run_1")).not.toEqual([]));
    expect(heldCells(container, "run_2")).toEqual([]);
  });

  it("marks nothing without an active hold", async () => {
    serveHolds([{ ...hold, active: false }]);
    const { container } = renderGrid();

    // Absence is only meaningful once the holds have been asked for and the answer has been rendered.
    await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(container.querySelectorAll('[data-testid="task-state-badge"]').length).toBeGreaterThan(0);
    expect(container.querySelector('[data-testid="grid-hold-lock"]')).toBeNull();
  });
});
