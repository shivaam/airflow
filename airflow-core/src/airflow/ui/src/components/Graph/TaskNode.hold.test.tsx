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
 * Separate from TaskNode.test.tsx: these read the English the lock's label and tooltips resolve to,
 * which needs the bundle and the i18n config.
 */
import type { ComponentProps } from "react";

import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagsLocale from "../../../public/i18n/locales/en/dags.json";
import { TaskNode } from "./TaskNode";
import type { CustomNodeProps } from "./reactflowUtils";

vi.mock("src/context/groups", () => ({
  useGroups: vi.fn(() => ({ toggleGroupId: vi.fn() })),
}));

const renderNode = (data: Partial<CustomNodeProps>) =>
  render(
    <ReactFlowProvider>
      <TaskNode
        {...({
          data: {
            height: 80,
            id: "t1",
            label: "t1",
            operator: "BashOperator",
            type: "task",
            width: 200,
            ...data,
          },
          id: "t1",
        } as unknown as ComponentProps<typeof TaskNode>)}
      />
    </ReactFlowProvider>,
    { wrapper: Wrapper },
  );

// The tooltip mounts when it opens, which takes a hover and the open delay.
const hover = async (element: HTMLElement) => {
  vi.useFakeTimers();

  try {
    await act(async () => {
      fireEvent.focus(element);
      fireEvent.pointerEnter(element);
      await vi.advanceTimersByTimeAsync(500);
    });
  } finally {
    vi.useRealTimers();
  }
};

describe("TaskNode hold lock", () => {
  // Without the bundle i18n.t() echoes the keys, so the strings below would assert nothing.
  beforeEach(() => {
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
  });

  it.each([
    { holdLockKind: "waiting", tooltip: "Won't run until the hold is released or expires." },
    { holdLockKind: "held", tooltip: "Won't run until the hold is released or expires." },
    {
      holdLockKind: "task",
      tooltip: "This task won't start or resume in any run until the hold is released or expires.",
    },
  ] as const)(
    "shows a lock beside the name of a task in a $holdLockKind lock",
    async ({ holdLockKind, tooltip }) => {
      renderNode({ holdLockKind });

      const lock = screen.getByTestId("graph-hold-lock");

      expect(within(lock).getByLabelText("Held")).toBeInTheDocument();
      await hover(lock);
      expect(screen.getByText(tooltip)).toBeInTheDocument();
    },
  );

  it("shows no lock for a task that no hold is stopping now", () => {
    renderNode({});

    expect(screen.queryByTestId("graph-hold-lock")).not.toBeInTheDocument();
  });
});
