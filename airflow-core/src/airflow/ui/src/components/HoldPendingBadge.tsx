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
import { Badge } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuHand } from "react-icons/lu";

import { useTaskHoldServiceListTaskHolds } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import { isStatePending, useAutoRefresh } from "src/utils";
import { isHoldPending } from "src/utils/taskHolds";

// Holds are matched here rather than asked of the API, so the Dag's active holds are fetched
// whole; a Dag with more than this many at once is beyond what the prototype shows.
const ACTIVE_HOLDS_LIMIT = 100;

type Props = {
  readonly taskInstance: Pick<TaskInstanceResponse, "dag_id" | "dag_run_id" | "state" | "task_id">;
};

/**
 * Shown while an active hold covers a task instance that is not `held` yet: the scheduler parks
 * it on its next pass, so until then the state alone would hide that the hold is coming.
 */
export const HoldPendingBadge = ({ taskInstance }: Props) => {
  const { t: translate } = useTranslation("dags");
  const refetchInterval = useAutoRefresh({});

  const { data } = useTaskHoldServiceListTaskHolds(
    { active: true, dagId: taskInstance.dag_id, limit: ACTIVE_HOLDS_LIMIT },
    undefined,
    // The task instance view polls while it can still change; the holds follow it, so a hold
    // placed elsewhere turns up and the badge gives way once the instance is held.
    { refetchInterval: isStatePending(taskInstance.state) && refetchInterval },
  );

  if (!isHoldPending(data?.task_holds ?? [], taskInstance)) {
    return undefined;
  }

  return (
    <Tooltip content={translate("taskHold.pending.tooltip")}>
      <Badge colorPalette="held" data-testid="hold-pending-badge" variant="subtle">
        <LuHand size={12} />
        {translate("taskHold.pending.label")}
      </Badge>
    </Tooltip>
  );
};
