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

const FUTURE = "2999-01-01T12:00:00.000Z";
const PAST = "2020-01-01T12:00:00.000Z";

const renderDialog = () =>
  render(<HoldDialog dagId="example_dag" onClose={vi.fn()} open taskId="load" />, { wrapper: Wrapper });

const getDuration = (name: "date" | "forever") =>
  screen.getByRole("radio", { name: new RegExp(`taskHold\\.dialog\\.expiresAt\\.${name}`, "u") });

const chooseDate = () => choose("taskHold.dialog.expiresAt.date");

const enterTime = (value: string) =>
  fireEvent.change(screen.getByTestId("expires-at-input"), { target: { value } });

describe("HoldDialog how long", () => {
  it("lasts until it is released, unless chosen otherwise, with no date to enter", () => {
    renderDialog();

    expect(getDuration("forever")).toBeChecked();
    expect(getDuration("date")).not.toBeChecked();
    expect(screen.queryByTestId("expires-at-input")).not.toBeInTheDocument();
  });

  it("sends no expiry for a hold that lasts until it is released", () => {
    renderDialog();

    typeReason("Maintenance");
    confirm();

    expect(mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Maintenance", run_id: null, task_id: "load" },
    });
  });

  it("asks for a date and time when that is chosen, and says what it does", async () => {
    renderDialog();

    await chooseDate();

    expect(screen.getByTestId("expires-at-input")).toBeInTheDocument();
    expect(screen.getByText("taskHold.dialog.expiresAt.helper")).toBeInTheDocument();
  });

  it("sends the time chosen as the expiry", async () => {
    renderDialog();

    await chooseDate();
    enterTime(FUTURE);
    typeReason("Maintenance");
    confirm();

    expect(mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { expires_at: FUTURE, reason: "Maintenance", run_id: null, task_id: "load" },
    });
  });

  it("does not place the hold when a date and time is chosen but none is entered", async () => {
    renderDialog();

    await chooseDate();
    typeReason("Maintenance");
    expect(screen.queryByText("taskHold.dialog.expiresAt.required")).not.toBeInTheDocument();
    confirm();

    expect(screen.getByText("taskHold.dialog.expiresAt.required")).toBeInTheDocument();
    expect(mutate).not.toHaveBeenCalled();
  });

  it("flags a time in the past as soon as it is entered, not only when the hold is placed", async () => {
    renderDialog();

    await chooseDate();
    expect(screen.queryByText("taskHold.dialog.expiresAt.past")).not.toBeInTheDocument();

    enterTime(PAST);
    expect(screen.getByText("taskHold.dialog.expiresAt.past")).toBeInTheDocument();

    typeReason("Maintenance");
    confirm();
    expect(mutate).not.toHaveBeenCalled();
  });

  it("clears the flag once a time in the future is entered", async () => {
    renderDialog();

    await chooseDate();
    enterTime(PAST);
    expect(screen.getByText("taskHold.dialog.expiresAt.past")).toBeInTheDocument();

    enterTime(FUTURE);
    await waitFor(() => expect(screen.queryByText("taskHold.dialog.expiresAt.past")).not.toBeInTheDocument());
  });

  it("sends no expiry after going back to a hold that lasts until it is released", async () => {
    renderDialog();

    await chooseDate();
    enterTime(FUTURE);
    await choose("taskHold.dialog.expiresAt.forever");
    typeReason("Maintenance");
    confirm();

    expect(mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { expires_at: null, reason: "Maintenance", run_id: null, task_id: "load" },
    });
  });

  it("does not flag a time in the past that no longer applies", async () => {
    renderDialog();

    await chooseDate();
    enterTime(PAST);
    await choose("taskHold.dialog.expiresAt.forever");

    expect(screen.queryByText("taskHold.dialog.expiresAt.past")).not.toBeInTheDocument();
    typeReason("Maintenance");
    confirm();
    expect(mutate).toHaveBeenCalled();
  });
});
