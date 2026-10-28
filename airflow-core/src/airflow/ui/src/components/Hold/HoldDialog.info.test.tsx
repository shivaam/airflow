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
/*
 * Separate from HoldDialog.test.tsx: these read the English the notes resolve to, which needs the
 * bundle and the i18n config, and that file asserts on untranslated keys.
 */
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { EdgeResponse, NodeResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import dagsLocale from "../../../public/i18n/locales/en/dags.json";
import HoldDialog from "./HoldDialog";

vi.mock("src/queries/useCreateTaskHold", () => ({
  useCreateTaskHold: () => ({ isPending: false, mutate: vi.fn() }),
}));

const task = (id: string, overrides: Partial<NodeResponse> = {}): NodeResponse => ({
  id,
  label: id,
  type: "task",
  ...overrides,
});

const serveStructure = (nodes: Array<NodeResponse>, edges: Array<EdgeResponse>) =>
  server.use(http.get("/ui/structure/structure_data", () => HttpResponse.json({ edges, nodes })));

// load -> cleanup, with create as the setup of cleanup.
const withTeardown = () =>
  serveStructure(
    [
      task("create", { setup_teardown_type: "setup" }),
      task("load"),
      task("cleanup", { setup_teardown_type: "teardown" }),
    ],
    [
      { source_id: "load", target_id: "cleanup" },
      { is_setup_teardown: true, source_id: "create", target_id: "cleanup" },
    ],
  );

const teardownRun =
  "This run's teardowns (cleanup) are held too, so resources their setups created stay up until the hold ends.";

// Options are picked by their label, and the choice lands a moment later.
const choose = async (label: string) => {
  fireEvent.click(screen.getByText(label));
  await waitFor(() => expect(screen.getByText(label).closest("label")?.querySelector("input")).toBeChecked());
};

const capacity =
  "Each run waits here until the hold ends. Waiting runs count toward max_active_runs, and a run that hits its dagrun_timeout fails with this task skipped.";
const teardown =
  "cleanup won't run until the hold ends, so resources from create stay up. Hold create instead to stop new ones from being created.";

// The tooltip mounts when it opens, which takes a hover and the open delay.
const getNotes = async (icon: HTMLElement) => {
  vi.useFakeTimers();

  try {
    await act(async () => {
      fireEvent.focus(icon);
      fireEvent.pointerEnter(icon);
      await vi.advanceTimersByTimeAsync(500);
    });

    return [...screen.getByRole("tooltip", { hidden: true }).querySelectorAll("p")].map(
      (line) => line.textContent,
    );
  } finally {
    vi.useRealTimers();
  }
};

describe("HoldDialog notes", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
  });

  it("shows no icon for a task instance with no teardown downstream", async () => {
    serveStructure([task("load"), task("publish")], [{ source_id: "load", target_id: "publish" }]);
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    await screen.findByText("This task in every run");
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(screen.queryByTestId("hold-scope-info")).not.toBeInTheDocument();
  });

  it("says each run waits, for a task in every run with no teardown", async () => {
    serveStructure([task("load")], []);
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

    expect(await getNotes(await screen.findByTestId("hold-scope-info"))).toEqual([capacity]);
  });

  it("says the teardown stays up, for a task instance", async () => {
    withTeardown();
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    expect(await getNotes(await screen.findByTestId("hold-scope-info"))).toEqual([teardown]);
  });

  it("shows both notes as separate lines, for a task in every run", async () => {
    withTeardown();
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

    expect(await getNotes(await screen.findByTestId("hold-scope-info"))).toEqual([capacity, teardown]);
  });

  it("says the teardowns of the run are held too, for a run", async () => {
    withTeardown();
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" />, { wrapper: Wrapper });

    expect(await getNotes(await screen.findByTestId("hold-scope-info"))).toEqual([teardownRun]);
  });
});

describe("HoldDialog notes follow the choice of what to hold", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
  });

  it.each([
    { label: "This task in this run", notes: [teardown] },
    { label: "This task in every run", notes: [capacity, teardown] },
    { label: "Every task in this run", notes: [teardownRun] },
  ])("says what is held with $label, from a task instance page", async ({ label, notes }) => {
    withTeardown();
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    await choose(label);

    expect(await getNotes(await screen.findByTestId("hold-scope-info"))).toEqual(notes);
  });

  it("says each run waits, and nothing of teardowns, where the Dag has none", async () => {
    serveStructure([task("load")], []);
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    await choose("This task in every run");

    expect(await getNotes(await screen.findByTestId("hold-scope-info"))).toEqual([capacity]);
  });

  it.each(["This task in this run", "Every task in this run"])(
    "shows no icon with %s where the Dag has no teardown",
    async (label) => {
      serveStructure([task("load")], []);
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
        wrapper: Wrapper,
      });

      await choose(label);
      await new Promise((resolve) => {
        setTimeout(resolve, 50);
      });

      expect(screen.queryByTestId("hold-scope-info")).not.toBeInTheDocument();
    },
  );
});
