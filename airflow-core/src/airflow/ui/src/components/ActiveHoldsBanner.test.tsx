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

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { ActiveHoldsBanner } from "./ActiveHoldsBanner";

describe("ActiveHoldsBanner", () => {
  it("links to the Holds tab while the Dag has active holds", async () => {
    server.use(
      http.get("/api/v2/dags/example_dag/holds", () =>
        HttpResponse.json({ task_holds: [], total_entries: 2 }),
      ),
    );
    render(<ActiveHoldsBanner dagId="example_dag" />, { wrapper: Wrapper });

    expect(await screen.findByRole("link")).toHaveAttribute("href", "/dags/example_dag/holds");
    expect(screen.getByTestId("active-holds-banner")).toBeInTheDocument();
  });

  it("renders nothing when the Dag has no active holds", async () => {
    let requests = 0;

    server.use(
      http.get("/api/v2/dags/example_dag/holds", () => {
        requests += 1;

        return HttpResponse.json({ task_holds: [], total_entries: 0 });
      }),
    );
    render(<ActiveHoldsBanner dagId="example_dag" />, { wrapper: Wrapper });

    // Absence is only meaningful once the holds have been asked for and the answer has been rendered.
    await waitFor(() => expect(requests).toBeGreaterThan(0));
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(screen.queryByTestId("active-holds-banner")).not.toBeInTheDocument();
  });
});
