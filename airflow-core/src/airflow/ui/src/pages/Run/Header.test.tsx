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
import { fireEvent, render, screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { DAGRunResponse, TaskHoldResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

// Action buttons and note preview pull in mutation/permission wiring that is
// unrelated to the partition key label under test; stub them out so the test
// only depends on the stats rendered by Header itself.
vi.mock("src/components/Clear", () => ({ ClearRunButton: () => undefined }));
vi.mock("src/components/MarkAs", () => ({ MarkRunAsButton: () => undefined }));
vi.mock("src/components/NeedsReviewButton", () => ({ NeedsReviewButtonWithModal: () => undefined }));
vi.mock("src/pages/DagRuns/DeleteRunButton", () => ({ default: () => undefined }));
vi.mock("src/components/NotePreview", () => ({ NotePreview: () => undefined }));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDeadlinesServiceGetDagDeadlineAlerts: vi.fn(),
  };
});

const mockConfig: Record<string, unknown> = { multi_team: false };

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

const { useDeadlinesServiceGetDagDeadlineAlerts } = await import("openapi/queries");

const baseDagRun = {
  bundle_version: null,
  conf: {},
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_1",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: null,
  end_date: null,
  last_scheduling_decision: null,
  logical_date: null,
  note: null,
  partition_date: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_type: "manual",
  start_date: null,
  state: "success",
  triggered_by: null,
  triggering_user_name: null,
} satisfies Partial<DAGRunResponse> as unknown as DAGRunResponse;

describe("Header", () => {
  beforeEach(() => {
    mockConfig.multi_team = false;
    vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);
  });

  it("shows the partition key stat using the dagRun.partitionKey label when partition_key is set", () => {
    render(<Header dagRun={{ ...baseDagRun, partition_key: "2025-01-01" }} />, { wrapper: Wrapper });

    expect(screen.getByText(i18n.t("dagRun.partitionKey"))).toBeInTheDocument();
    expect(screen.getByText("2025-01-01")).toBeInTheDocument();
    expect(screen.queryByText(/mappedPartitionKey/iu)).not.toBeInTheDocument();
  });

  it("hides the partition key stat when partition_key is null", () => {
    render(<Header dagRun={{ ...baseDagRun, partition_key: null }} />, { wrapper: Wrapper });

    expect(screen.queryByText(i18n.t("dagRun.partitionKey"))).not.toBeInTheDocument();
  });

  it("shows the team stat when multi-team is enabled and a team is resolved", () => {
    mockConfig.multi_team = true;
    render(<Header dagRun={{ ...baseDagRun, team_name: "team-a" }} />, { wrapper: Wrapper });

    expect(screen.getByText(i18n.t("common:dagDetails.team"))).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "team-a" })).toHaveAttribute("href", "/dags?teams=team-a");
  });

  it("hides the team stat when multi-team is disabled", () => {
    render(<Header dagRun={{ ...baseDagRun, team_name: "team-a" }} />, { wrapper: Wrapper });

    expect(screen.queryByText(i18n.t("common:dagDetails.team"))).not.toBeInTheDocument();
  });
});

describe("Header hold lock", () => {
  const runHold: TaskHoldResponse = {
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
    run_id: "run_1",
    task_id: null,
  };

  const serveHolds = (holds: Array<TaskHoldResponse>) =>
    server.use(
      http.get("/api/v2/dags/test_dag/holds", () =>
        HttpResponse.json({ task_holds: holds, total_entries: holds.length }),
      ),
    );

  beforeEach(() => {
    vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);
  });

  it("locks a run an active hold covers whole", async () => {
    serveHolds([runHold]);
    render(<Header dagRun={baseDagRun} />, { wrapper: Wrapper });

    expect(await screen.findByTestId("hold-lock-badge")).toBeInTheDocument();
  });

  // Holding a run that has finished has no effect, so the dialog is told the state of the run.
  it("tells the hold dialog the state of the run", async () => {
    render(<Header dagRun={{ ...baseDagRun, state: "success" }} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: /taskHold\.button/u }));

    expect((await screen.findByTestId("hold-state-line")).textContent).toBe(
      "taskHold.dialog.stateLine.runFinished",
    );
  });

  it("has nothing to say of a run that has not finished", async () => {
    render(<Header dagRun={{ ...baseDagRun, state: "running" }} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: /taskHold\.button/u }));

    expect(await screen.findByTestId("hold-scope")).toBeInTheDocument();
    expect(screen.queryByTestId("hold-state-line")).not.toBeInTheDocument();
  });
});
