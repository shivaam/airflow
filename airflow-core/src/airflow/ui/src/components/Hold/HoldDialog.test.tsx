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

describe("HoldDialog", () => {
  it("does not show an error before the hold is attempted", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

    expect(screen.queryByText("taskHold.dialog.reason.required")).not.toBeInTheDocument();
  });

  it("requires a reason", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

    confirm();

    expect(screen.getByText("taskHold.dialog.reason.required")).toBeInTheDocument();
    expect(mutate).not.toHaveBeenCalled();
  });

  it("treats a reason of only whitespace as missing", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

    typeReason("   ");
    confirm();

    expect(screen.getByText("taskHold.dialog.reason.required")).toBeInTheDocument();
    expect(mutate).not.toHaveBeenCalled();
  });

  it("places a hold on one task instance without an expiry", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    typeReason("  Upstream data is wrong  ");
    confirm();

    expect(mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Upstream data is wrong", run_id: "run_1", task_id: "load" },
    });
  });

  it("places a hold on a task in every run", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

    typeReason("Maintenance");
    confirm();

    expect(mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Maintenance", run_id: null, task_id: "load" },
    });
  });

  it("places a hold on a whole run", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" />, { wrapper: Wrapper });

    typeReason("Bad input file");
    confirm();

    expect(mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Bad input file", run_id: "run_1", task_id: null },
    });
  });
});

describe("HoldDialog mapped hint", () => {
  it.each([
    { props: { runId: "run_1", taskId: "load" }, title: "a task instance page" },
    { props: { taskId: "load" }, title: "a task page" },
  ])("says a hold covers every map index of a mapped task, from $title", ({ props }) => {
    render(<HoldDialog dagId="example_dag" isMapped onClose={vi.fn()} open {...props} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByTestId("hold-mapped-hint")).toHaveTextContent("taskHold.dialog.mappedHint");
  });

  it("says nothing of map indexes for a task that is not mapped", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    expect(screen.queryByTestId("hold-mapped-hint")).not.toBeInTheDocument();
  });

  it("says it for the task instance and the task, which a hold covers whole, and not for a run", async () => {
    render(<HoldDialog dagId="example_dag" isMapped onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    expect(screen.getByTestId("hold-mapped-hint")).toBeInTheDocument();

    await choose("taskHold.dialog.scope.task.label");
    expect(screen.getByTestId("hold-mapped-hint")).toBeInTheDocument();

    await choose("taskHold.dialog.scope.run.label");
    expect(screen.queryByTestId("hold-mapped-hint")).not.toBeInTheDocument();
  });

  it("says nothing of map indexes from a run page", () => {
    render(<HoldDialog dagId="example_dag" isMapped onClose={vi.fn()} open runId="run_1" />, {
      wrapper: Wrapper,
    });

    expect(screen.queryByTestId("hold-mapped-hint")).not.toBeInTheDocument();
  });
});

describe("HoldDialog description", () => {
  it.each([
    { props: { taskId: "load" }, title: "a task in every run" },
    { props: { runId: "run_1", taskId: "load" }, title: "a task instance" },
  ])("describes a hold on $title as stopping the task", ({ props }) => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open {...props} />, { wrapper: Wrapper });

    expect(screen.getByText("taskHold.dialog.description")).toBeInTheDocument();
    expect(screen.queryByText("taskHold.dialog.descriptionRun")).not.toBeInTheDocument();
  });

  it("describes a hold on a run as stopping the tasks in it", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" />, { wrapper: Wrapper });

    expect(screen.getByText("taskHold.dialog.descriptionRun")).toBeInTheDocument();
    expect(screen.queryByText("taskHold.dialog.description")).not.toBeInTheDocument();
  });

  it("follows the choice of what to hold", async () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("taskHold.dialog.description")).toBeInTheDocument();

    await choose("taskHold.dialog.scope.run.label");
    expect(screen.getByText("taskHold.dialog.descriptionRun")).toBeInTheDocument();
    expect(screen.queryByText("taskHold.dialog.description")).not.toBeInTheDocument();

    await choose("taskHold.dialog.scope.task.label");
    expect(screen.getByText("taskHold.dialog.description")).toBeInTheDocument();
    expect(screen.queryByText("taskHold.dialog.descriptionRun")).not.toBeInTheDocument();
  });
});

describe("HoldDialog state line", () => {
  // The states of a group get the same line. It is about the task instance, so it is read with that
  // chosen, which a finished one does not start on.
  it.each([
    { key: "notStarted", state: null },
    { key: "waitingToRun", state: "scheduled" },
    { key: "waitingToRun", state: "up_for_retry" },
    { key: "waitingToRun", state: "up_for_reschedule" },
    { key: "deferred", state: "deferred" },
    { key: "awaitingInput", state: "awaiting_input" },
    { key: "queued", state: "queued" },
    { key: "running", state: "running" },
    { key: "finished", state: "success" },
    { key: "finished", state: "failed" },
    { key: "finished", state: "skipped" },
    { key: "finished", state: "upstream_failed" },
    { key: "finished", state: "removed" },
  ] as const)("says what the hold does to a task instance in state $state", async ({ key, state }) => {
    render(
      <HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" state={state} taskId="load" />,
      { wrapper: Wrapper },
    );
    await choose("taskHold.dialog.scope.taskInstance.label");

    expect(screen.getByTestId("hold-state-line").textContent).toBe(`taskHold.dialog.stateLine.${key}`);
  });

  it.each([
    { props: { runId: "run_1", state: "restarting", taskId: "load" }, title: "a restarting task instance" },
    { props: { runId: "run_1", taskId: "load" }, title: "a task instance whose state is not given" },
    { props: { state: "running", taskId: "load" }, title: "a task in every run" },
    { props: { runId: "run_1", state: "running" }, title: "a run" },
  ] as const)("has no state line for $title", ({ props }) => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open {...props} />, { wrapper: Wrapper });

    expect(screen.queryByTestId("hold-state-line")).not.toBeInTheDocument();
  });

  it("is about the task instance only while that is what is chosen", async () => {
    render(
      <HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" state="running" taskId="load" />,
      { wrapper: Wrapper },
    );

    expect(screen.getByTestId("hold-state-line").textContent).toBe("taskHold.dialog.stateLine.running");

    await choose("taskHold.dialog.scope.task.label");
    expect(screen.queryByTestId("hold-state-line")).not.toBeInTheDocument();

    await choose("taskHold.dialog.scope.run.label");
    expect(screen.queryByTestId("hold-state-line")).not.toBeInTheDocument();
  });
});

describe("HoldDialog finished run", () => {
  it.each(["success", "failed"] as const)("says a run that is %s has finished, from its page", (runState) => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" runState={runState} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByTestId("hold-state-line").textContent).toBe("taskHold.dialog.stateLine.runFinished");
  });

  it.each(["queued", "running", undefined] as const)(
    "says nothing of a run that is %s, from its page",
    (runState) => {
      render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" runState={runState} />, {
        wrapper: Wrapper,
      });

      expect(screen.queryByTestId("hold-state-line")).not.toBeInTheDocument();
    },
  );

  it("says so only while every task in the run is what is chosen", async () => {
    render(
      <HoldDialog
        dagId="example_dag"
        onClose={vi.fn()}
        open
        runId="run_1"
        runState="success"
        state="running"
        taskId="load"
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByTestId("hold-state-line").textContent).toBe("taskHold.dialog.stateLine.running");

    await choose("taskHold.dialog.scope.run.label");
    expect(screen.getByTestId("hold-state-line").textContent).toBe("taskHold.dialog.stateLine.runFinished");

    await choose("taskHold.dialog.scope.task.label");
    expect(screen.queryByTestId("hold-state-line")).not.toBeInTheDocument();
  });
});
