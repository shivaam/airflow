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
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { TaskHoldResponse, TaskResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

const task = {
  doc_md: null,
  is_mapped: false,
  operator_name: "PythonOperator",
  task_display_name: "load",
  task_id: "load",
  trigger_rule: "all_success",
} satisfies Partial<TaskResponse> as unknown as TaskResponse;

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

let holdsRequests = 0;

const renderHeader = (holds: Array<TaskHoldResponse>) => {
  holdsRequests = 0;
  server.use(
    http.get("/api/v2/dags/example_dag/holds", () => {
      holdsRequests += 1;

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );

  return render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/example_dag/tasks/load"]}>
        <Routes>
          <Route element={<Header task={task} />} path="/dags/:dagId/tasks/:taskId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );
};

describe("Header", () => {
  it("locks a task that an active hold holds in every run", async () => {
    renderHeader([hold]);

    expect(await screen.findByTestId("hold-lock-badge")).toBeInTheDocument();
  });

  it("does not lock a task that is held in one run only", async () => {
    renderHeader([{ ...hold, run_id: "run_1" }]);

    // Absence is only meaningful once the holds have been asked for and the answer has been rendered.
    await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(screen.queryByTestId("hold-lock-badge")).not.toBeInTheDocument();
  });
});
