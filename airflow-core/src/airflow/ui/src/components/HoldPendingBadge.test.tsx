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
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import type { TaskHoldResponse, TaskInstanceState } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { HoldPendingBadge } from "./HoldPendingBadge";

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
  run_id: null,
  task_id: "load",
};

const taskInstance = (state: TaskInstanceState) => ({
  dag_id: "example_dag",
  dag_run_id: "run_1",
  state,
  task_id: "load",
});

let holdsRequests = 0;

const serveHolds = (holds: Array<TaskHoldResponse>) => {
  holdsRequests = 0;
  server.use(
    http.get("/api/v2/dags/example_dag/holds", () => {
      holdsRequests += 1;

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );
};

// Absence is only meaningful once the holds have been asked for and the answer has been rendered.
const settle = async () => {
  await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
  await new Promise((resolve) => {
    setTimeout(resolve, 50);
  });
};

describe("HoldPendingBadge", () => {
  it("shows while an active hold covers a task instance that is not held yet", async () => {
    serveHolds([hold]);
    render(<HoldPendingBadge taskInstance={taskInstance("scheduled")} />, { wrapper: Wrapper });

    expect(await screen.findByTestId("hold-pending-badge")).toBeInTheDocument();
  });

  it("is absent once the task instance is held", async () => {
    serveHolds([hold]);
    render(<HoldPendingBadge taskInstance={taskInstance("held")} />, { wrapper: Wrapper });

    await settle();
    expect(screen.queryByTestId("hold-pending-badge")).not.toBeInTheDocument();
  });

  it("is absent when no active hold covers the task instance", async () => {
    serveHolds([{ ...hold, task_id: "extract" }]);
    render(<HoldPendingBadge taskInstance={taskInstance("scheduled")} />, { wrapper: Wrapper });

    await settle();
    expect(screen.queryByTestId("hold-pending-badge")).not.toBeInTheDocument();
  });
});
