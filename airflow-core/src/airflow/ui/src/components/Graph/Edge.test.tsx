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
import { render } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import Edge from "./Edge";
import type { EdgeData } from "./reactflowUtils";

const renderEdge = (rest: Partial<EdgeData["rest"]>) => {
  const { container } = render(
    <ReactFlowProvider>
      <svg>
        <Edge
          data={{
            rest: {
              id: "a-b",
              sections: [
                {
                  endPoint: { x: 10, y: 10 },
                  id: "section",
                  incomingShape: "a",
                  outgoingShape: "b",
                  startPoint: { x: 0, y: 0 },
                },
              ],
              sources: ["a"],
              targets: ["b"],
              ...rest,
            },
          }}
          id="a-b"
          source="a"
          target="b"
        />
      </svg>
    </ReactFlowProvider>,
    { wrapper: Wrapper },
  );
  const path = container.querySelector("path");

  return {
    dash: path?.getAttribute("stroke-dasharray") ?? undefined,
    stroke: path?.getAttribute("stroke"),
    width: path?.getAttribute("stroke-width"),
  };
};

describe("Edge", () => {
  it("draws an edge out of a held task in its own colour, a little thicker than the others", () => {
    const plain = renderEdge({});
    const held = renderEdge({ isHeld: true });

    expect(plain.width).toBe("1");
    expect(held.width).toBe("2");
    expect(held.stroke).toBeTruthy();
    expect(held.stroke).not.toBe(plain.stroke);
  });

  it("keeps the dashes of a setup and teardown edge that is held too", () => {
    expect(renderEdge({ isSetupTeardown: true }).dash).toBe("10,5");
    expect(renderEdge({ isHeld: true, isSetupTeardown: true })).toMatchObject({ dash: "10,5", width: "2" });
  });

  it("keeps the colour of a held edge when it is selected, with the thickness of a selected one", () => {
    const held = renderEdge({ isHeld: true });
    const selected = renderEdge({ isSelected: true });
    const heldAndSelected = renderEdge({ isHeld: true, isSelected: true });

    expect(selected.width).toBe("3");
    expect(heldAndSelected).toEqual({ ...held, width: "3" });
    expect(heldAndSelected.stroke).not.toBe(selected.stroke);
  });
});
