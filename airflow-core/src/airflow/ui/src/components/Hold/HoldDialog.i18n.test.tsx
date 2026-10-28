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
 * Separate from HoldDialog.test.tsx: these read the English the dialog's choices and messages resolve
 * to, which needs the bundle and the i18n config, and that file asserts on untranslated keys.
 */
import type { ChangeEvent } from "react";

import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagsLocale from "../../../public/i18n/locales/en/dags.json";
import { HoldButton } from "./HoldButton";
import HoldDialog from "./HoldDialog";

vi.mock("src/queries/useCreateTaskHold", () => ({
  useCreateTaskHold: () => ({ isPending: false, mutate: vi.fn() }),
}));

// The date picker has its own tests: a plain input stands in for it, so that a time can be entered.
vi.mock("src/components/DateTimeInput", () => ({
  DateTimeInput: ({
    onChange,
    value,
  }: {
    readonly onChange?: (event: ChangeEvent<HTMLInputElement>) => void;
    readonly value: string;
  }) => <input aria-label="expires at" data-testid="expires-at-input" onChange={onChange} value={value} />,
}));

const getOptionText = (radio: HTMLElement) => radio.closest("label")?.textContent;

// Without the bundle i18n.t() echoes the keys, so the strings below would assert nothing.
beforeEach(() => {
  i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
});

const renderTaskPage = () =>
  render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

// Options are picked by their label, and the choice lands a moment later.
const choose = async (label: string) => {
  fireEvent.click(screen.getByText(label));
  await waitFor(() => expect(screen.getByText(label).closest("label")?.querySelector("input")).toBeChecked());
};

describe("HoldDialog what to hold, in English", () => {
  it("offers the three scopes from a task instance page, each with its secondary text", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" taskId="load" />, {
      wrapper: Wrapper,
    });

    const options = within(screen.getByRole("radiogroup", { name: "What to hold" })).getAllByRole("radio");

    expect(options.map(getOptionText)).toEqual([
      "This task in this runload in run run_1",
      "This task in every runload, in current and future runs",
      "Every task in this runAll tasks in run run_1",
    ]);
  });

  it.each([
    { props: { taskId: "load" }, text: "Task load in all runs", title: "a task page" },
    { props: { runId: "run_1" }, text: "All tasks in run run_1", title: "a run page" },
  ])("shows the one scope of $title as plain text", ({ props, text }) => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open {...props} />, { wrapper: Wrapper });

    expect(screen.getByText("What to hold")).toBeInTheDocument();
    expect(screen.getByTestId("hold-scope")).toHaveTextContent(text);
    expect(screen.queryByRole("radiogroup", { name: "What to hold" })).not.toBeInTheDocument();
  });

  it("says a run that has finished has nothing to start, and that clearing makes tasks wait", () => {
    render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open runId="run_1" runState="success" />, {
      wrapper: Wrapper,
    });

    expect(screen.getByTestId("hold-state-line")).toHaveTextContent(
      "This run has finished. Nothing starts unless its tasks are cleared; cleared tasks then wait.",
    );
  });
});

describe("HoldDialog how long, in English", () => {
  it("offers a hold until it is released, or until a date and time", () => {
    renderTaskPage();

    const group = within(screen.getByRole("radiogroup", { name: "How long" }));

    expect(group.getByRole("radio", { name: "Until I release it" })).toBeChecked();
    expect(group.getByRole("radio", { name: "Until a date and time" })).not.toBeChecked();
  });

  it("says what a date and time does, once it is chosen", async () => {
    renderTaskPage();

    await choose("Until a date and time");

    expect(screen.getByText("The hold ends by itself at this time.")).toBeInTheDocument();
  });

  it("asks for the time when none is entered, and flags one in the past", async () => {
    renderTaskPage();

    await choose("Until a date and time");
    fireEvent.change(screen.getByPlaceholderText("Why is this being held?"), { target: { value: "Fix" } });
    fireEvent.click(screen.getByRole("button", { name: /Place hold/u }));

    expect(screen.getByText("Choose when the hold ends.")).toBeInTheDocument();

    fireEvent.change(screen.getByTestId("expires-at-input"), {
      target: { value: "2020-01-01T12:00:00.000Z" },
    });

    expect(screen.getByText("The expiry must be in the future.")).toBeInTheDocument();
  });
});

describe("HoldButton, in English", () => {
  it.each([
    { props: { runId: "run_1", taskId: "load" }, title: "a task instance page" },
    { props: { taskId: "load" }, title: "a task page" },
    { props: { runId: "run_1" }, title: "a run page" },
  ])("says only Hold... on $title", ({ props }) => {
    render(<HoldButton dagId="example_dag" {...props} />, { wrapper: Wrapper });

    expect(screen.getByRole("button", { name: "Hold..." })).toBeInTheDocument();
  });
});
