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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { useActiveTaskHolds } from "./useActiveTaskHolds";

// Auto-refresh is off unless a test turns it on, as it is when the setting is not served.
const mockConfig: Record<string, unknown> = {};

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
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
  run_id: null,
  task_id: "load",
};

let holdsRequests = 0;
let requestedUrl = "";

const serveHolds = (holds: Array<TaskHoldResponse>) => {
  holdsRequests = 0;
  requestedUrl = "";
  server.use(
    http.get("/api/v2/dags/example_dag/holds", ({ request }) => {
      holdsRequests += 1;
      requestedUrl = request.url;

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );
};

// Long enough for several refreshes at the interval the refresh tests set.
const advanceClock = async () => {
  await new Promise((resolve) => {
    setTimeout(resolve, 300);
  });
};

describe("useActiveTaskHolds", () => {
  it("gives the active holds of the Dag", async () => {
    serveHolds([hold]);
    const { result } = renderHook(() => useActiveTaskHolds({ dagId: "example_dag" }), { wrapper: Wrapper });

    await waitFor(() => expect(result.current).toEqual([hold]));
    expect(new URL(requestedUrl).searchParams.get("active")).toBe("true");
  });

  it("gives the same array for as long as there are no holds, so a prop made of it is not rebuilt", async () => {
    serveHolds([]);
    const { rerender, result } = renderHook(() => useActiveTaskHolds({ dagId: "example_dag" }), {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
    const first = result.current;

    rerender();
    expect(result.current).toBe(first);
  });

  it("does not ask without a Dag", async () => {
    serveHolds([hold]);
    renderHook(() => useActiveTaskHolds({ dagId: "" }), { wrapper: Wrapper });

    await advanceClock();
    expect(holdsRequests).toBe(0);
  });
});

describe("useActiveTaskHolds refreshing", () => {
  beforeEach(() => {
    // Seconds, as the setting is: short enough to see a few refreshes within a test.
    mockConfig.auto_refresh_interval = 0.05;
  });

  afterEach(() => {
    delete mockConfig.auto_refresh_interval;
  });

  it("keeps asking while a hold is active, which can be released or run out on its own", async () => {
    serveHolds([hold]);
    renderHook(() => useActiveTaskHolds({ dagId: "example_dag" }), { wrapper: Wrapper });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(2), { timeout: 3000 });
  });

  it("keeps asking while a run in view is pending, in which a hold can be placed", async () => {
    serveHolds([]);
    renderHook(() => useActiveTaskHolds({ dagId: "example_dag", isRunPending: true }), {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(2), { timeout: 3000 });
  });

  it("stops asking when there is no hold and no run is pending", async () => {
    serveHolds([]);
    renderHook(() => useActiveTaskHolds({ dagId: "example_dag" }), { wrapper: Wrapper });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
    await advanceClock();
    expect(holdsRequests).toBe(1);
  });
});
