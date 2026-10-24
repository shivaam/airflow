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
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { HoldButton } from "./HoldButton";

describe("HoldButton", () => {
  it("tells the dialog the state of the task instance", async () => {
    render(<HoldButton dagId="example_dag" runId="run_1" state="running" taskId="load" />, {
      wrapper: Wrapper,
    });

    fireEvent.click(screen.getByRole("button"));

    expect((await screen.findByTestId("hold-state-line")).textContent).toBe(
      "taskHold.dialog.stateLine.running",
    );
  });
});
