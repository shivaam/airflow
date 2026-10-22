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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import { Holds } from "./Holds";

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => (key === "fallback_page_limit" ? 25 : undefined),
}));

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
  task_id: "load",
  ...overrides,
});

const activeHold = makeHold();
const releasedHold = makeHold({
  active: false,
  id: 2,
  reason: "Old maintenance window",
  release_reason: "Done",
  released_at: "2026-10-20T11:00:00Z",
  released_by: "operator",
  run_id: "run_1",
  task_id: null,
});

const requestedFilters: Array<string | null> = [];

const serveHolds = () =>
  server.use(
    http.get("/api/v2/dags/example_dag/holds", ({ request }) => {
      const active = new URL(request.url).searchParams.get("active");

      requestedFilters.push(active);
      const holds = active === "false" ? [activeHold, releasedHold] : [activeHold];

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );

const renderHolds = (initialEntry = "/dags/example_dag/holds") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <TimezoneProvider>
          <Routes>
            <Route element={<Holds />} path="/dags/:dagId/holds" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

const getLinkTargets = () => screen.getAllByRole("link").map((link) => link.getAttribute("href"));

afterEach(() => {
  requestedFilters.length = 0;
});

describe("Holds", () => {
  it("lists only active holds by default, each with a Release action", async () => {
    serveHolds();
    renderHolds();

    expect(await screen.findByText("Upstream data is wrong")).toBeInTheDocument();
    expect(requestedFilters[0]).toBe("true");
    expect(screen.queryByText("Old maintenance window")).not.toBeInTheDocument();
    expect(screen.getAllByTestId("release-hold")).toHaveLength(1);
    expect(screen.getByTestId("hold-status")).toHaveTextContent("dags:taskHold.status.active");
  });

  it("links a hold to the task, run or task instance it covers", async () => {
    serveHolds();
    renderHolds("/dags/example_dag/holds?active=all");

    expect(await screen.findByText("Old maintenance window")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /taskHold\.scope\.task$/u })).toHaveAttribute(
      "href",
      "/dags/example_dag/tasks/load",
    );
    expect(screen.getByRole("link", { name: /taskHold\.scope\.run$/u })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run_1",
    );
  });

  it("links a hold on a task and run to the task instance, or to the instances of a mapped task", async () => {
    server.use(
      http.get("/api/v2/dags/example_dag/holds", () =>
        HttpResponse.json({
          task_holds: [
            makeHold({ id: 3, run_id: "run_1", task_id: "load" }),
            makeHold({ id: 4, run_id: "run_1", task_id: "transform" }),
            makeHold({ id: 5, run_id: "run_1", task_id: "group.expand" }),
          ],
          total_entries: 3,
        }),
      ),
      // A task inside a mapped task group is marked as mapped too, but the group itself is no task.
      http.get("/ui/grid/structure/example_dag", () =>
        HttpResponse.json([
          { id: "load", is_mapped: null, label: "load" },
          { id: "transform", is_mapped: true, label: "transform" },
          {
            children: [{ id: "group.expand", is_mapped: true, label: "expand" }],
            id: "group",
            is_mapped: true,
            label: "group",
          },
        ]),
      ),
    );
    renderHolds();

    // The structure arrives after the holds, and the links follow it.
    await waitFor(() =>
      expect(getLinkTargets()).toEqual([
        "/dags/example_dag/runs/run_1/tasks/load",
        "/dags/example_dag/runs/run_1/tasks/transform/mapped",
        "/dags/example_dag/runs/run_1/tasks/group.expand/mapped",
      ]),
    );
  });

  it("shows released holds too under the All filter, without a Release action for them", async () => {
    serveHolds();
    renderHolds();

    await screen.findByText("Upstream data is wrong");
    fireEvent.click(screen.getByTestId("holds-filter-all"));

    expect(await screen.findByText("Old maintenance window")).toBeInTheDocument();
    expect(requestedFilters).toContain("false");
    expect(screen.getAllByTestId("hold-status").map((badge) => badge.textContent)).toEqual([
      "dags:taskHold.status.active",
      "dags:taskHold.status.released",
    ]);
    expect(screen.getAllByTestId("release-hold")).toHaveLength(1);
  });

  it("releases a hold with an optional reason", async () => {
    serveHolds();
    const releases: Array<{ body: unknown; holdId: ReadonlyArray<string> | string | undefined }> = [];

    server.use(
      http.post("/api/v2/dags/example_dag/holds/:holdId/release", async ({ params, request }) => {
        releases.push({ body: await request.json(), holdId: params.holdId });

        return HttpResponse.json({ ...activeHold, active: false });
      }),
    );
    renderHolds();

    fireEvent.click(await screen.findByTestId("release-hold"));
    fireEvent.change(await screen.findByPlaceholderText("taskHold.release.reasonPlaceholder"), {
      target: { value: "Data fixed" },
    });
    fireEvent.click(screen.getByRole("button", { name: /taskHold\.release\.confirm/u }));

    await waitFor(() => expect(releases).toHaveLength(1));
    expect(releases[0]).toEqual({ body: { reason: "Data fixed" }, holdId: "1" });
  });

  it("releases a hold without a reason", async () => {
    serveHolds();
    const bodies: Array<unknown> = [];

    server.use(
      http.post("/api/v2/dags/example_dag/holds/:holdId/release", async ({ request }) => {
        bodies.push(await request.json());

        return HttpResponse.json({ ...activeHold, active: false });
      }),
    );
    renderHolds();

    fireEvent.click(await screen.findByTestId("release-hold"));
    fireEvent.click(await screen.findByRole("button", { name: /taskHold\.release\.confirm/u }));

    await waitFor(() => expect(bodies).toEqual([{ reason: null }]));
  });
});
