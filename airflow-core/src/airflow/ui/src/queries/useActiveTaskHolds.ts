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
import { useTaskHoldServiceListTaskHolds } from "openapi/queries";
import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { useAutoRefresh } from "src/utils";

// Holds are matched on the client rather than asked of the API, so the Dag's active holds are fetched
// whole; a Dag with more than this many at once is beyond what the prototype shows. Everything that
// asks for them uses this limit, so that they all share one query.
export const ACTIVE_HOLDS_LIMIT = 100;

const NO_HOLDS: ReadonlyArray<TaskHoldResponse> = [];

/**
 * The active holds of a Dag, for marking what they are holding up in the graph and the grid. They are
 * refreshed while a run in view is pending, as its task instances change state, and while a hold is
 * active, as it can be released or expire on its own.
 */
export const useActiveTaskHolds = ({
  dagId,
  isRunPending = false,
}: {
  readonly dagId: string;
  readonly isRunPending?: boolean;
}): ReadonlyArray<TaskHoldResponse> => {
  // Not keyed to the Dag, whose paused state stops auto-refresh: holds can be placed, released and
  // expire on a paused Dag too.
  const refetchInterval = useAutoRefresh({});

  const { data } = useTaskHoldServiceListTaskHolds(
    { active: true, dagId, limit: ACTIVE_HOLDS_LIMIT },
    undefined,
    {
      enabled: dagId !== "",
      refetchInterval: (query) =>
        (isRunPending || (query.state.data?.task_holds.length ?? 0) > 0) && refetchInterval,
    },
  );

  // The same array while the holds are the same, so what is given it as a prop is not rebuilt.
  return data?.task_holds ?? NO_HOLDS;
};
