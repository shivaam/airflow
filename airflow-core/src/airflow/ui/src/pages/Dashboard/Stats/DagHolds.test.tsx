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

import type { DagHoldCount } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import dagsLocale from "../../../../public/i18n/locales/en/dags.json";
import dashboardLocale from "../../../../public/i18n/locales/en/dashboard.json";
import { DagHolds } from "./DagHolds";

let countRequests = 0;

const serveCounts = (dags: Array<DagHoldCount>) =>
  server.use(
    http.get("/ui/dashboard/dag_hold_counts", () => {
      countRequests += 1;

      return HttpResponse.json({ dags, total_entries: dags.length });
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
    countRequests = 0;
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
    i18n.addResourceBundle("en", "dashboard", dashboardLocale, true, true);
  });

  it("counts the Dags with active holds, and lists each one with its holds in the order the server gives", async () => {
    serveCounts([
      { count: 3, dag_id: "etl_dag" },
      { count: 1, dag_id: "report_dag" },
    ]);
    renderCard();

    const card = await screen.findByTestId("dag-holds-card");

    expect(within(card).getByText("2")).toBeInTheDocument();
    expect(within(card).getByText("Dags with active holds")).toBeInTheDocument();

    const rows = within(card).getAllByTestId("dag-holds-row");

    expect(rows.map((row) => row.textContent)).toEqual(["etl_dag3 active holds", "report_dag1 active hold"]);
  });

  it("shows a count beyond one page of holds", async () => {
    serveCounts([{ count: 20_000, dag_id: "etl_dag" }]);
    renderCard();

    const row = await screen.findByTestId("dag-holds-row");

    expect(row.textContent).toBe("etl_dag20,000 active holds");
  });

  it("links each Dag to its Holds tab", async () => {
    serveCounts([
      { count: 1, dag_id: "etl_dag" },
      { count: 1, dag_id: "report_dag" },
    ]);
    renderCard();

    const rows = await screen.findAllByTestId("dag-holds-row");

    expect(rows.map((row) => row.getAttribute("href"))).toEqual([
      "/dags/etl_dag/holds",
      "/dags/report_dag/holds",
    ]);
  });

  it("says Dag in the singular for a single Dag", async () => {
    serveCounts([{ count: 1, dag_id: "etl_dag" }]);
    renderCard();

    expect(await screen.findByText("Dag with active holds")).toBeInTheDocument();
  });

  it("is not there when no Dag has an active hold", async () => {
    serveCounts([]);
    const { container } = renderCard();

    // Absence is only meaningful once the counts have been asked for and the answer has been rendered.
    await waitFor(() => expect(countRequests).toBeGreaterThan(0));
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(screen.queryByTestId("dag-holds-card")).not.toBeInTheDocument();
    expect(container).toBeEmptyDOMElement();
  });
});
