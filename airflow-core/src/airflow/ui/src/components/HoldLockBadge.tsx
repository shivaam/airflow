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
import { LuLock } from "react-icons/lu";

import { useTaskHoldServiceListTaskHolds } from "openapi/queries";

import { Tooltip } from "src/system-components";

import { isStatePending, useAutoRefresh } from "src/utils";
import { getHoldLockKind, type HoldLockSubject } from "src/utils/taskHolds";

// Holds are matched here rather than asked of the API, so the Dag's active holds are fetched
// whole; a Dag with more than this many at once is beyond what the prototype shows.
const ACTIVE_HOLDS_LIMIT = 100;

type Props = {
  readonly subject: HoldLockSubject;
};

/**
 * A lock, shown while an active hold covers what is in view. The hold never changes the state of a
 * task instance, so the badge is the only sign of it, and its tooltip says what the hold does to a
 * task instance in the state it is in.
 */
export const HoldLockBadge = ({ subject }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const refetchInterval = useAutoRefresh({});
  const isTaskInstance = subject.dag_run_id !== undefined && subject.task_id !== undefined;

  const { data } = useTaskHoldServiceListTaskHolds(
    { active: true, dagId: subject.dag_id, limit: ACTIVE_HOLDS_LIMIT },
    undefined,
    {
      enabled: subject.dag_id !== "",
      // The task instance view polls while it can still change, and the holds follow it, so a hold
      // placed or released elsewhere turns up. A task or a run has no state to follow, so it polls
      // only while it shows a lock, which can run out on its own.
      refetchInterval: (query) =>
        ((isTaskInstance && isStatePending(subject.state)) ||
          getHoldLockKind(query.state.data?.task_holds ?? [], subject) !== undefined) &&
        refetchInterval,
    },
  );

  const kind = getHoldLockKind(data?.task_holds ?? [], subject);

  if (kind === undefined) {
    return undefined;
  }

  return (
    <Tooltip content={translate(`dags:taskHold.lock.tooltip.${kind}`)}>
      <Badge colorPalette="indigo" data-testid="hold-lock-badge" variant="subtle">
        <LuLock size={12} />
        {translate("common:hold_one")}
      </Badge>
    </Tooltip>
  );
};
