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
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { useCreateTaskHold } from "./useCreateTaskHold";

const placed: TaskHoldResponse = {
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

// What each endpoint was asked: its path and the request body.
const requests: Array<{ body: unknown; path: string }> = [];

const record = async ({ request }: { request: Request }) => {
  requests.push({ body: await request.json(), path: new URL(request.url).pathname });

  return HttpResponse.json(placed, { status: 201 });
};

describe("useCreateTaskHold", () => {
  beforeEach(() => {
    requests.length = 0;
    server.use(
      http.post("/api/v2/dags/:dagId/dagRuns/:dagRunId/taskInstances/:taskId/holds", record),
      http.post("/api/v2/dags/:dagId/tasks/:taskId/holds", record),
    );
  });

  it("places a task instance hold through the endpoint of the task instance", async () => {
    const onSuccessConfirm = vi.fn();
    const { result } = renderHook(() => useCreateTaskHold({ onSuccessConfirm }), { wrapper: Wrapper });

    result.current.mutate({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Upstream data is wrong" },
      runId: "run_1",
      taskId: "load",
    });

    await waitFor(() => expect(onSuccessConfirm).toHaveBeenCalled());
    expect(requests).toEqual([
      {
        body: { expires_at: null, reason: "Upstream data is wrong" },
        path: "/api/v2/dags/example_dag/dagRuns/run_1/taskInstances/load/holds",
      },
    ]);
  });

  it("places a task hold through the endpoint of the task", async () => {
    const onSuccessConfirm = vi.fn();
    const { result } = renderHook(() => useCreateTaskHold({ onSuccessConfirm }), { wrapper: Wrapper });

    result.current.mutate({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Vendor outage" },
      taskId: "load",
    });

    await waitFor(() => expect(onSuccessConfirm).toHaveBeenCalled());
    expect(requests).toEqual([
      {
        body: { expires_at: null, reason: "Vendor outage" },
        path: "/api/v2/dags/example_dag/tasks/load/holds",
      },
    ]);
  });
});
