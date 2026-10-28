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
import { render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import dagsLocale from "../../../../public/i18n/locales/en/dags.json";
import dashboardLocale from "../../../../public/i18n/locales/en/dashboard.json";
import { DagHolds } from "./DagHolds";

const hold = (overrides: Partial<TaskHoldResponse>): TaskHoldResponse => ({
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

let holdsRequests = 0;
let requestedUrl = "";

const serveHolds = (holds: Array<TaskHoldResponse>) =>
  server.use(
    http.get("/api/v2/dags/~/holds", ({ request }) => {
      holdsRequests += 1;
      requestedUrl = request.url;

      return HttpResponse.json({ task_holds: holds, total_entries: holds.length });
    }),
  );

const renderCard = () =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/home"]}>
        <DagHolds />
      </MemoryRouter>
    </BaseWrapper>,
  );

describe("DagHolds", () => {
  // Without the bundles i18n.t() echoes the keys, so the strings below would assert nothing.
  beforeEach(() => {
    holdsRequests = 0;
    requestedUrl = "";
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
    i18n.addResourceBundle("en", "dashboard", dashboardLocale, true, true);
  });

  it("asks for the active holds of every Dag", async () => {
    serveHolds([hold({})]);
    renderCard();

    await screen.findByTestId("dag-holds-card");
    const params = new URL(requestedUrl).searchParams;

    expect(params.get("active")).toBe("true");
    expect(params.get("limit")).toBe("100");
  });

  it("counts the Dags with active holds, and lists each one with its holds, the most held first", async () => {
    serveHolds([
      hold({ dag_id: "etl_dag", id: 1 }),
      hold({ dag_id: "report_dag", id: 2 }),
      hold({ dag_id: "etl_dag", id: 3, run_id: "run_1" }),
      hold({ dag_id: "etl_dag", id: 4, run_id: "run_2", task_id: null }),
    ]);
    renderCard();

    const card = await screen.findByTestId("dag-holds-card");

    expect(within(card).getByText("2")).toBeInTheDocument();
    expect(within(card).getByText("Dags with active holds")).toBeInTheDocument();

    const rows = within(card).getAllByTestId("dag-holds-row");

    expect(rows.map((row) => row.textContent)).toEqual(["etl_dag3 active holds", "report_dag1 active hold"]);
  });

  it("links each Dag to its Holds tab", async () => {
    serveHolds([hold({ dag_id: "etl_dag", id: 1 }), hold({ dag_id: "report_dag", id: 2 })]);
    renderCard();

    const rows = await screen.findAllByTestId("dag-holds-row");

    expect(rows.map((row) => row.getAttribute("href"))).toEqual([
      "/dags/etl_dag/holds",
      "/dags/report_dag/holds",
    ]);
  });

  it("says Dag in the singular for a single Dag", async () => {
    serveHolds([hold({ dag_id: "etl_dag" })]);
    renderCard();

    expect(await screen.findByText("Dag with active holds")).toBeInTheDocument();
  });

  it("is not there when no Dag has an active hold", async () => {
    serveHolds([hold({ active: false })]);
    const { container } = renderCard();

    // Absence is only meaningful once the holds have been asked for and the answer has been rendered.
    await waitFor(() => expect(holdsRequests).toBeGreaterThan(0));
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(screen.queryByTestId("dag-holds-card")).not.toBeInTheDocument();
    expect(container).toBeEmptyDOMElement();
  });
});
