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
import type { ChangeEvent } from "react";

import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import HoldDialog from "./HoldDialog";

const { mutate } = vi.hoisted(() => ({ mutate: vi.fn() }));

vi.mock("src/queries/useCreateTaskHold", () => ({
  useCreateTaskHold: () => ({ isPending: false, mutate }),
}));

// The date picker has its own tests, and its popover is more than these need: a plain input stands in
// for it, so that a test can enter a time.
vi.mock("src/components/DateTimeInput", () => ({
  DateTimeInput: ({
    onChange,
    value,
  }: {
    readonly onChange?: (event: ChangeEvent<HTMLInputElement>) => void;
    readonly value: string;
  }) => <input aria-label="expires at" data-testid="expires-at-input" onChange={onChange} value={value} />,
}));

afterEach(() => {
  mutate.mockClear();
});

const confirm = () => fireEvent.click(screen.getByRole("button", { name: /taskHold\.dialog\.confirm/u }));
const typeReason = (reason: string) =>
  fireEvent.change(screen.getByPlaceholderText("taskHold.dialog.reason.placeholder"), {
    target: { value: reason },
  });
// Options are picked by their label, as in the other radio cards, and the choice lands a moment later.
const choose = async (label: string) => {
  fireEvent.click(screen.getByText(label));
  await waitFor(() => expect(screen.getByText(label).closest("label")?.querySelector("input")).toBeChecked());
};

const SCOPE_LABEL = /taskHold\.dialog\.scope\.\w+\.label/u;

const getOptionText = (radio: HTMLElement) => radio.closest("label")?.textContent;

const getScopeRadio = (scope: "run" | "task" | "taskInstance") =>
  screen.getByRole("radio", { name: new RegExp(`taskHold\\.dialog\\.scope\\.${scope}\\.label`, "u") });

const finishedStates = ["success", "failed", "skipped", "upstream_failed", "removed"] as const;

describe("HoldDialog what to hold", () => {
  describe("offers", () => {
    it("all three scopes from a task instance page, the task instance first", () => {
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
        wrapper: Wrapper,
      });

      expect(screen.getAllByRole("radio", { name: SCOPE_LABEL }).map(getOptionText)).toEqual([
        expect.stringContaining("taskHold.dialog.scope.taskInstance.label"),
        expect.stringContaining("taskHold.dialog.scope.task.label"),
        expect.stringContaining("taskHold.dialog.scope.run.label"),
      ]);
    });

    it("gives each its secondary text", () => {
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
        wrapper: Wrapper,
      });

      expect(getScopeRadio("taskInstance").closest("label")).toHaveTextContent(
        "taskHold.dialog.scope.taskInstance.detail",
      );
      expect(getScopeRadio("task").closest("label")).toHaveTextContent("taskHold.dialog.scope.task.detail");
      // A run is described by the words the lists of holds use for it.
      expect(getScopeRadio("run").closest("label")).toHaveTextContent("taskHold.scope.run");
    });

    it.each([
      { props: { taskId: "load" }, scope: "task", title: "a task page" },
      { props: { runId: "run_1" }, scope: "run", title: "a run page" },
    ] as const)("only its own scope from $title, as plain text rather than a choice", ({ props, scope }) => {
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open {...props} />, { wrapper: Wrapper });

      expect(screen.queryAllByRole("radio", { name: SCOPE_LABEL })).toHaveLength(0);
      expect(screen.getByTestId("hold-scope")).toHaveTextContent(`taskHold.scope.${scope}`);
      expect(screen.getByText("taskHold.dialog.scope.label")).toBeInTheDocument();
    });
  });

  describe("starts on", () => {
    it("the task instance, from a task instance page", () => {
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
        wrapper: Wrapper,
      });

      expect(getScopeRadio("taskInstance")).toBeChecked();
    });

    it.each(["scheduled", "running", "queued", "deferred", "up_for_retry", null] as const)(
      "the task instance while it is %s",
      (state) => {
        render(
          <HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" state={state} taskId="load" />,
          { wrapper: Wrapper },
        );

        expect(getScopeRadio("taskInstance")).toBeChecked();
      },
    );

    it.each(finishedStates)(
      "the task in every run once the task instance is %s, which cannot be stopped in its run",
      (state) => {
        render(
          <HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" state={state} taskId="load" />,
          { wrapper: Wrapper },
        );

        expect(getScopeRadio("task")).toBeChecked();
        expect(getScopeRadio("taskInstance")).not.toBeChecked();
      },
    );

    it("a finished task instance still lets the person hold it in its own run", async () => {
      render(
        <HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" state="success" taskId="load" />,
        { wrapper: Wrapper },
      );

      await choose("taskHold.dialog.scope.taskInstance.label");

      expect(getScopeRadio("taskInstance")).toBeChecked();
    });
  });

  describe("places a hold with the ids of the scope chosen", () => {
    it.each([
      { scope: "taskInstance", sets: { run_id: "run_1", task_id: "load" } },
      { scope: "task", sets: { run_id: null, task_id: "load" } },
      { scope: "run", sets: { run_id: "run_1", task_id: null } },
    ] as const)("for $scope, from a task instance page", async ({ scope, sets }) => {
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
        wrapper: Wrapper,
      });

      await choose(`taskHold.dialog.scope.${scope}.label`);
      typeReason("Upstream data is wrong");
      confirm();

      expect(mutate).toHaveBeenCalledWith({
        dagId: "example_dag",
        requestBody: { expires_at: null, reason: "Upstream data is wrong", ...sets },
      });
    });

    it("for the task in every run when a finished task instance starts on it", () => {
      render(
        <HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" state="failed" taskId="load" />,
        { wrapper: Wrapper },
      );

      typeReason("Fix first");
      confirm();

      expect(mutate).toHaveBeenCalledWith({
        dagId: "example_dag",
        requestBody: { expires_at: null, reason: "Fix first", run_id: null, task_id: "load" },
      });
    });
  });

  it("starts on the scope of the page again once the dialog has been closed", async () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    await choose("taskHold.dialog.scope.run.label");

    // Closing the dialog resets what was chosen in it; the test's `open` stays true.
    fireEvent.click(screen.getByRole("button", { name: /modal\.cancel/u }));

    await waitFor(() => expect(getScopeRadio("taskInstance")).toBeChecked());
  });
});
