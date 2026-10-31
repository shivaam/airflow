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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it } from "vitest";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../../public/i18n/locales/en/common.json";
import dagsLocale from "../../../public/i18n/locales/en/dags.json";
import { HeldByList } from "./HeldByList";

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

const runHold: TaskHoldResponse = {
  ...hold,
  id: 2,
  reason: "Bad input file",
  run_id: "run_1",
  task_id: null,
};

// What the release endpoint was asked: the hold and the request body.
const released: Array<{ body: unknown; holdId: string }> = [];

const renderList = (holds: Array<TaskHoldResponse>) =>
  render(<HeldByList dagId="example_dag" holds={holds} />, { wrapper: Wrapper });

const openRelease = (index = 0) => {
  fireEvent.click(screen.getAllByTestId("held-by-release")[index] as HTMLElement);

  return screen.getByTestId("held-by-release-reason");
};

describe("HeldByList", () => {
  beforeEach(() => {
    // Without the bundles i18n.t() echoes the keys, so the strings below would assert nothing.
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
    released.length = 0;
    server.use(
      http.post("/api/v2/dags/example_dag/holds/:holdId/release", async ({ params, request }) => {
        released.push({ body: await request.json(), holdId: String(params.holdId) });

        return HttpResponse.json({ ...hold, active: false });
      }),
    );
  });

  it("opens a box for an optional reason in the list, ready to type in, and releases nothing yet", () => {
    renderList([hold]);

    const reason = openRelease();

    expect(reason).toHaveFocus();
    expect(screen.getByText("Reason (optional)")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("Why is this being released?")).toBe(reason);
    expect(screen.getByTestId("held-by-release-confirm")).toHaveTextContent("Release hold");
    expect(screen.queryByTestId("held-by-release")).not.toBeInTheDocument();
    expect(released).toEqual([]);
  });

  it("releases without a reason when the box is left empty", async () => {
    renderList([hold]);

    openRelease();
    fireEvent.click(screen.getByTestId("held-by-release-confirm"));

    await waitFor(() => expect(released).toEqual([{ body: { reason: null }, holdId: "1" }]));
  });

  it("releases with the reason typed in the box, trimmed", async () => {
    renderList([hold]);

    fireEvent.change(openRelease(), { target: { value: "  The vendor fixed the feed  " } });
    fireEvent.click(screen.getByTestId("held-by-release-confirm"));

    await waitFor(() =>
      expect(released).toEqual([{ body: { reason: "The vendor fixed the feed" }, holdId: "1" }]),
    );
  });

  it("keeps no reason that is only whitespace", async () => {
    renderList([hold]);

    fireEvent.change(openRelease(), { target: { value: "   " } });
    fireEvent.click(screen.getByTestId("held-by-release-confirm"));

    await waitFor(() => expect(released).toEqual([{ body: { reason: null }, holdId: "1" }]));
  });

  it("closes the box once the hold is released", async () => {
    renderList([hold]);

    openRelease();
    fireEvent.click(screen.getByTestId("held-by-release-confirm"));

    await waitFor(() => expect(screen.queryByTestId("held-by-release-form")).not.toBeInTheDocument());
    expect(released).toHaveLength(1);
  });

  it("releases nothing when cancelled, and forgets the reason", () => {
    renderList([hold]);

    fireEvent.change(openRelease(), { target: { value: "Not yet" } });
    fireEvent.click(screen.getByTestId("held-by-release-cancel"));

    expect(screen.queryByTestId("held-by-release-form")).not.toBeInTheDocument();
    expect(openRelease()).toHaveValue("");
    expect(released).toEqual([]);
  });

  it("opens the box under the hold whose Release was clicked, and releases that hold", async () => {
    renderList([hold, runHold]);

    openRelease(1);

    const [taskHoldItem, runHoldItem] = screen.getAllByTestId("held-by-hold");

    expect(within(runHoldItem as HTMLElement).getByTestId("held-by-release-form")).toBeInTheDocument();
    expect(within(taskHoldItem as HTMLElement).queryByTestId("held-by-release-form")).not.toBeInTheDocument();
    expect(within(taskHoldItem as HTMLElement).getByTestId("held-by-release")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("held-by-release-confirm"));

    await waitFor(() => expect(released).toEqual([{ body: { reason: null }, holdId: "2" }]));
  });

  it("moves the box to another hold without carrying the reason over", () => {
    renderList([hold, runHold]);

    fireEvent.change(openRelease(1), { target: { value: "For the run" } });
    const reason = openRelease(0);

    expect(screen.getAllByTestId("held-by-release-form")).toHaveLength(1);
    expect(
      within(screen.getAllByTestId("held-by-hold")[0] as HTMLElement).getByTestId("held-by-release-reason"),
    ).toBe(reason);
    expect(reason).toHaveValue("");
  });
});
