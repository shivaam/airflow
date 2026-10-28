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
/*
 * Separate from Graph.test.tsx, which stubs the holds out: these read them from msw, and look at the
 * nodes and edges the graph hands to ReactFlow.
 */
import { render, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { GridTISummaries, TaskHoldResponse, TaskInstanceState } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { Graph } from "./Graph";

// testsSetup.ts globally mocks Graph to null so full-page tests don't need to stub ELK layout data.
vi.unmock("src/layouts/Details/Graph/Graph");

type FlowProps = {
  edges: Array<{ data?: { rest?: { isHeld?: boolean } }; id: string }>;
  nodes: Array<{ data: { holdLockKind?: string }; id: string }>;
};

const flow = vi.hoisted(() => ({ props: undefined as FlowProps | undefined }));

let mockParams: Record<string, string> = {};

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => mockParams,
    useSearchParams: () => [new URLSearchParams()],
  };
});

vi.mock("openapi/queries", async () => {
  const actual = await vi.importActual("openapi/queries");

  return {
    ...actual,
    useDagRunServiceGetDagRun: () => ({ data: { state: "running" }, isLoading: false }),
    useStructureServiceStructureData: () => ({ data: { edges: [], nodes: [] } }),
  };
});

vi.mock("@xyflow/react", async () => {
  const actual = await vi.importActual("@xyflow/react");

  return {
    ...actual,
    ReactFlow: (props: FlowProps) => {
      flow.props = props;

      return undefined;
    },
  };
});

vi.mock("src/queries/useDependencyGraph", () => ({ useDependencyGraph: () => ({ data: undefined }) }));
vi.mock("src/hooks/useSelectedVersion", () => ({ default: () => 1 }));
vi.mock("src/context/groups", () => ({
  useGroups: () => ({ allGroupIds: [], openGroupIds: [], setAllGroupIds: vi.fn() }),
}));

const layoutNode = (id: string, data: Record<string, unknown> = {}) => ({
  data: { id, isGroup: false, label: id, type: "task", ...data },
  id,
  position: { x: 0, y: 0 },
  type: "task",
});

const layoutEdge = (source: string, target: string) => ({
  data: { rest: { id: `${source}-${target}`, sources: [source], targets: [target] } },
  id: `${source}-${target}`,
  source,
  target,
  type: "custom",
});

vi.mock("src/components/Graph/useGraphLayout", () => ({
  useGraphLayout: () => ({
    data: {
      edges: [
        layoutEdge("scheduled_task", "running_task"),
        layoutEdge("running_task", "finished_task"),
        layoutEdge("finished_task", "task_group"),
      ],
      nodes: [
        layoutNode("scheduled_task"),
        layoutNode("running_task"),
        layoutNode("finished_task"),
        layoutNode("task_group", { isGroup: true }),
      ],
    },
    isPending: false,
  }),
}));

const states: Record<string, TaskInstanceState | null> = {
  finished_task: "success",
  running_task: "running",
  scheduled_task: "scheduled",
  task_group: "scheduled",
};

let mockSummaries: GridTISummaries | undefined;

vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: () => ({
    summariesByRunId: new Map(mockSummaries === undefined ? [] : [["run_1", mockSummaries]]),
  }),
}));

const hold: TaskHoldResponse = {
  active: true,
  created_at: "2026-10-20T10:00:00Z",
  created_by: "admin",
  dag_id: "test_dag",
  expires_at: null,
  id: 1,
  reason: "Upstream data is wrong",
  release_reason: null,
  released_at: null,
  released_by: null,
  run_id: null,
  task_id: null,
};

let holdsRequests = 0;

const serveHolds = (holds: Array<TaskHoldResponse>) =>
  server.use(
    http.get("/api/v2/dags/test_dag/holds", () => {
      holdsRequests += 1;

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );

const heldNodes = () =>
  Object.fromEntries(
    (flow.props?.nodes ?? [])
      .filter((node) => node.data.holdLockKind !== undefined)
      .map((node) => [node.id, node.data.holdLockKind]),
  );
const heldEdges = () =>
  (flow.props?.edges ?? []).filter((edge) => edge.data?.rest?.isHeld === true).map((edge) => edge.id);

// Absence is only meaningful once the holds have been asked for and the answer has been rendered.
const settle = async () => {
  await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
  await new Promise((resolve) => {
    setTimeout(resolve, 50);
  });
};

describe("Graph holds", () => {
  beforeEach(() => {
    holdsRequests = 0;
    flow.props = undefined;
    mockParams = { dagId: "test_dag", runId: "run_1" };
    mockSummaries = {
      dag_id: "test_dag",
      run_id: "run_1",
      task_instances: Object.entries(states).map(([taskId, state]) => ({
        child_states: null,
        max_end_date: null,
        min_start_date: null,
        state,
        task_display_name: taskId,
        task_id: taskId,
      })),
    };
  });

  it("marks the tasks a hold is stopping now, and not a running or finished one, or a group", async () => {
    serveHolds([{ ...hold, run_id: "run_1" }]);
    render(<Graph />, { wrapper: Wrapper });

    await waitFor(() => expect(heldNodes()).toEqual({ scheduled_task: "held" }));
  });

  it("draws the edges out of a held task as held, and no others", async () => {
    serveHolds([{ ...hold, run_id: "run_1" }]);
    render(<Graph />, { wrapper: Wrapper });

    await waitFor(() => expect(heldEdges()).toEqual(["scheduled_task-running_task"]));
  });

  it("marks a task with no task instance in the run yet, which the hold keeps from starting", async () => {
    mockSummaries = { dag_id: "test_dag", run_id: "run_1", task_instances: [] };
    serveHolds([{ ...hold, run_id: "run_1" }]);
    render(<Graph />, { wrapper: Wrapper });

    await waitFor(() =>
      expect(heldNodes()).toEqual({
        finished_task: "waiting",
        running_task: "waiting",
        scheduled_task: "waiting",
      }),
    );
  });

  it("marks nothing while the task instances of the run are loading", async () => {
    mockSummaries = undefined;
    serveHolds([{ ...hold, run_id: "run_1" }]);
    render(<Graph />, { wrapper: Wrapper });

    await settle();
    expect(heldNodes()).toEqual({});
    expect(heldEdges()).toEqual([]);
  });

  it("marks a task that a hold on it in every run covers, when no run is selected", async () => {
    mockParams = { dagId: "test_dag" };
    serveHolds([{ ...hold, task_id: "running_task" }]);
    render(<Graph />, { wrapper: Wrapper });

    await waitFor(() => expect(heldNodes()).toEqual({ running_task: "task" }));
    expect(heldEdges()).toEqual(["running_task-finished_task"]);
  });

  it("marks nothing when no active hold covers the tasks", async () => {
    serveHolds([
      { ...hold, task_id: "another_task" },
      { ...hold, active: false, run_id: "run_1" },
    ]);
    render(<Graph />, { wrapper: Wrapper });

    await settle();
    expect(heldNodes()).toEqual({});
    expect(heldEdges()).toEqual([]);
  });
});
