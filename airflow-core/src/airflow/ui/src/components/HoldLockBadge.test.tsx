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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskHoldResponse, TaskInstanceState } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../public/i18n/locales/en/common.json";
import dagsLocale from "../../public/i18n/locales/en/dags.json";
import { HoldLockBadge } from "./HoldLockBadge";

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

// A task instance response also carries a map index, which a hold never looks at.
const taskInstance = (state: TaskInstanceState | null, mapIndex = -1) => ({
  dag_id: "example_dag",
  dag_run_id: "run_1",
  map_index: mapIndex,
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

// Long enough for several refreshes at the interval the refresh tests set.
const advanceClock = async () => {
  await new Promise((resolve) => {
    setTimeout(resolve, 300);
  });
};

// Clicking the badge lists the holds behind it, under a line that says what they do to what is in view.
const getHeldByText = async (badge: HTMLElement) => {
  fireEvent.click(badge);

  return (await screen.findByTestId("held-by-list")).textContent;
};

// The ids of the holds the release endpoint was asked to release.
const released: Array<string> = [];

const recordRelease = ({ params }: { params: Record<string, unknown> }) => {
  released.push(String(params.holdId));

  return HttpResponse.json({ ...hold, active: false });
};

const expectReleased = (ids: Array<string>) => () => expect(released).toEqual(ids);

describe("HoldLockBadge", () => {
  // Without the bundles i18n.t() echoes the keys, so the strings below would assert nothing.
  beforeEach(() => {
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
  });

  describe("on a task instance", () => {
    const stopsNow = "Won't run until the hold is released or expires.";
    const carriesOn = "This attempt carries on. Its next start or resume is held.";

    // What a hold does to a task instance depends on its state, and so does what the badge says. It is
    // a solid "Held" where the hold is stopping the task instance now, and a dashed "Hold pending"
    // where there is nothing to stop yet.
    it.each([
      { label: "Held", pending: false, state: null, tooltip: stopsNow },
      { label: "Held", pending: false, state: "scheduled", tooltip: stopsNow },
      { label: "Held", pending: false, state: "up_for_retry", tooltip: stopsNow },
      { label: "Held", pending: false, state: "up_for_reschedule", tooltip: stopsNow },
      { label: "Held", pending: false, state: "restarting", tooltip: stopsNow },
      { label: "Hold pending", pending: true, state: "queued", tooltip: carriesOn },
      { label: "Hold pending", pending: true, state: "running", tooltip: carriesOn },
      {
        label: "Hold pending",
        pending: true,
        state: "deferred",
        tooltip: "The trigger keeps waiting. Held when it resumes.",
      },
      {
        label: "Hold pending",
        pending: true,
        state: "awaiting_input",
        tooltip: "You can still answer. Held when it resumes.",
      },
    ] as const)(
      "locks a $state task instance a hold covers as $label",
      async ({ label, pending, state, tooltip }) => {
        serveHolds([hold]);
        render(<HoldLockBadge subject={taskInstance(state)} />, { wrapper: Wrapper });

        const badge = await screen.findByTestId("hold-lock-badge");

        expect(badge.textContent).toBe(label);
        expect(badge).toHaveAttribute("data-hold-pending", String(pending));
        expect(await getHeldByText(badge)).toContain(tooltip);
      },
    );

    // Nothing is left to stop, but if it is cleared the new attempt waits.
    it.each(["success", "failed", "skipped", "upstream_failed", "removed"] as const)(
      "shows a pending hold on a task instance that is already %s",
      async (state) => {
        serveHolds([hold]);
        render(<HoldLockBadge subject={taskInstance(state)} />, { wrapper: Wrapper });

        const badge = await screen.findByTestId("hold-lock-badge");

        expect(badge.textContent).toBe("Hold pending");
        expect(badge).toHaveAttribute("data-hold-pending", "true");
        expect(await getHeldByText(badge)).toContain("Held if it's cleared.");
      },
    );

    // The look is what tells the two apart at a glance: a pending hold is a dashed outline.
    it("draws a pending hold dashed and a hold that is stopping the task now solid", async () => {
      serveHolds([hold]);
      const { rerender } = render(<HoldLockBadge subject={taskInstance("running")} />, { wrapper: Wrapper });

      expect(await screen.findByTestId("hold-lock-badge")).toHaveStyle({
        borderStyle: "dashed",
        borderWidth: "1px",
      });

      rerender(<HoldLockBadge subject={taskInstance("scheduled")} />);
      expect(screen.getByTestId("hold-lock-badge")).not.toHaveStyle({ borderStyle: "dashed" });
    });

    it.each(["scheduled", "success"] as const)(
      "is absent when no active hold covers a %s task instance",
      async (state) => {
        serveHolds([{ ...hold, task_id: "extract" }]);
        render(<HoldLockBadge subject={taskInstance(state)} />, { wrapper: Wrapper });

        await settle();
        expect(screen.queryByTestId("hold-lock-badge")).not.toBeInTheDocument();
      },
    );

    it("is absent for a hold that has been released", async () => {
      serveHolds([{ ...hold, active: false }]);
      render(<HoldLockBadge subject={taskInstance("scheduled")} />, { wrapper: Wrapper });

      await settle();
      expect(screen.queryByTestId("hold-lock-badge")).not.toBeInTheDocument();
    });

    it("locks every map index of a mapped task, which a hold covers whole", async () => {
      serveHolds([{ ...hold, run_id: "run_1" }]);
      render(<HoldLockBadge subject={taskInstance("scheduled", 3)} />, { wrapper: Wrapper });

      expect(await screen.findByTestId("hold-lock-badge")).toBeInTheDocument();
    });
  });

  describe("on a task across its runs", () => {
    it("is locked by a hold on the task in every run", async () => {
      serveHolds([hold]);
      render(<HoldLockBadge subject={{ dag_id: "example_dag", task_id: "load" }} />, { wrapper: Wrapper });

      const badge = await screen.findByTestId("hold-lock-badge");

      expect(badge.textContent).toBe("Held");
      expect(badge).toHaveAttribute("data-hold-pending", "false");
      expect(await getHeldByText(badge)).toContain(
        "This task won't start or resume in any run until the hold is released or expires.",
      );
    });

    it("is not locked by a hold on a single run of the task", async () => {
      serveHolds([{ ...hold, run_id: "run_1" }]);
      render(<HoldLockBadge subject={{ dag_id: "example_dag", task_id: "load" }} />, { wrapper: Wrapper });

      await settle();
      expect(screen.queryByTestId("hold-lock-badge")).not.toBeInTheDocument();
    });
  });

  describe("on a run", () => {
    it("is locked by a hold on the run", async () => {
      serveHolds([{ ...hold, run_id: "run_1", task_id: null }]);
      render(<HoldLockBadge subject={{ dag_id: "example_dag", dag_run_id: "run_1" }} />, {
        wrapper: Wrapper,
      });

      const badge = await screen.findByTestId("hold-lock-badge");

      expect(badge.textContent).toBe("Held");
      expect(badge).toHaveAttribute("data-hold-pending", "false");
      expect(await getHeldByText(badge)).toContain(
        "Tasks in this run won't start or resume until the hold is released or expires.",
      );
    });

    it("is not locked by a hold on a task of the run", async () => {
      serveHolds([{ ...hold, run_id: "run_1" }]);
      render(<HoldLockBadge subject={{ dag_id: "example_dag", dag_run_id: "run_1" }} />, {
        wrapper: Wrapper,
      });

      await settle();
      expect(screen.queryByTestId("hold-lock-badge")).not.toBeInTheDocument();
    });
  });
});

describe("HoldLockBadge refreshing the holds", () => {
  beforeEach(() => {
    // Seconds, as the setting is: short enough to see a few refreshes within a test.
    mockConfig.auto_refresh_interval = 0.05;
  });

  afterEach(() => {
    delete mockConfig.auto_refresh_interval;
  });

  // A hold placed from another tab does not change the state of a task instance, so the holds are
  // the only way to see it, whether or not a lock is shown yet.
  it.each([
    { holds: [hold], title: "locked" },
    { holds: [], title: "not locked yet" },
  ])("keeps asking while a task instance that can still change is $title", async ({ holds }) => {
    serveHolds(holds);
    render(<HoldLockBadge subject={taskInstance("scheduled")} />, { wrapper: Wrapper });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(2), { timeout: 3000 });
  });

  it("stops asking once the task instance has finished and shows no lock", async () => {
    serveHolds([{ ...hold, task_id: "extract" }]);
    render(<HoldLockBadge subject={taskInstance("success")} />, { wrapper: Wrapper });

    await settle();
    await advanceClock();
    expect(holdsRequests).toBe(1);
  });

  // Its state is settled, but the hold can be released or run out on its own, as one on a task can.
  it("keeps asking while a finished task instance shows a pending hold", async () => {
    serveHolds([hold]);
    render(<HoldLockBadge subject={taskInstance("success")} />, { wrapper: Wrapper });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(2), { timeout: 3000 });
  });

  it("keeps asking while a task or a run shows a lock, which can run out on its own", async () => {
    serveHolds([hold]);
    render(<HoldLockBadge subject={{ dag_id: "example_dag", task_id: "load" }} />, { wrapper: Wrapper });

    await waitFor(() => expect(holdsRequests).toBeGreaterThan(2), { timeout: 3000 });
  });

  it("does not keep asking while a task or a run shows no lock", async () => {
    serveHolds([]);
    render(<HoldLockBadge subject={{ dag_id: "example_dag", task_id: "load" }} />, { wrapper: Wrapper });

    await settle();
    await advanceClock();
    expect(holdsRequests).toBe(1);
  });

  describe("held by", () => {
    const runHold: TaskHoldResponse = {
      ...hold,
      id: 2,
      reason: "Bad input file",
      run_id: "run_1",
      task_id: null,
    };

    it("counts the holds and lists each of them", async () => {
      serveHolds([hold, runHold]);
      render(<HoldLockBadge subject={taskInstance("scheduled")} />, { wrapper: Wrapper });

      const badge = await screen.findByTestId("hold-lock-badge");

      expect(badge.textContent).toBe("Held · 2");

      const text = await getHeldByText(badge);

      expect(text).toContain("Held by 2 holds");
      expect(text).toContain("Upstream data is wrong");
      expect(text).toContain("Bad input file");
      expect(text).toContain("Releasing one keeps it held by the others.");
    });

    it("releases the hold its Release button sits on", async () => {
      released.length = 0;
      serveHolds([hold, runHold]);
      server.use(http.post("/api/v2/dags/example_dag/holds/:holdId/release", recordRelease));
      render(<HoldLockBadge subject={taskInstance("scheduled")} />, { wrapper: Wrapper });

      await getHeldByText(await screen.findByTestId("hold-lock-badge"));
      const [, releaseRunHold] = screen.getAllByTestId("held-by-release");

      fireEvent.click(releaseRunHold as HTMLElement);
      await waitFor(expectReleased(["2"]));
    });
  });
});
