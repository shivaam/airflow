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
import { Badge, type BadgeProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuLock } from "react-icons/lu";

import { useTaskHoldServiceListTaskHolds } from "openapi/queries";

import { Popover } from "src/system-components";

import { ACTIVE_HOLDS_LIMIT } from "src/queries/useActiveTaskHolds";
import { isStatePending, useAutoRefresh } from "src/utils";
import {
  getCoveringHolds,
  getHoldLockKind,
  isHoldLockPending,
  type HoldLockSubject,
} from "src/utils/taskHolds";

import { HeldByList } from "./Hold/HeldByList";

// A hold that is stopping the task now shows as a solid badge. A pending one, with nothing to stop yet,
// is drawn as a dashed outline with no fill, so the two are easy to tell apart.
const solidLook: BadgeProps = { variant: "subtle" };
const pendingLook: BadgeProps = {
  borderColor: "colorPalette.solid",
  borderStyle: "dashed",
  borderWidth: "1px",
  variant: "plain",
};

type Props = {
  readonly subject: HoldLockSubject;
};

/**
 * A lock, shown while an active hold covers what is in view. The hold never changes the state of a
 * task instance, so the badge is the only sign of it, and what it opens says what the hold does to a
 * task instance in the state it is in. The badge is solid while the hold is stopping the task now, and
 * dashed while it is pending, with nothing to stop yet. Clicking it lists the holds behind it, each
 * with a way to release it.
 */
export const HoldLockBadge = ({ subject }: Props) => {
  const { t: translate } = useTranslation("dags");
  const refetchInterval = useAutoRefresh({});
  const isTaskInstance = subject.dag_run_id !== undefined && subject.task_id !== undefined;

  const { data } = useTaskHoldServiceListTaskHolds(
    { active: true, dagId: subject.dag_id, limit: ACTIVE_HOLDS_LIMIT },
    undefined,
    {
      enabled: subject.dag_id !== "",
      // The task instance view polls while it can still change, and the holds follow it, so a hold
      // placed or released elsewhere turns up. A task or a run has no state to follow, so it polls
      // only while it shows a lock, which can run out on its own. A finished task instance does the
      // same: its state is settled, but the pending hold on it can be released or expire elsewhere.
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

  const isPending = isHoldLockPending(kind);
  const holds = getCoveringHolds(data?.task_holds ?? [], subject);

  return (
    <Popover.Root lazyMount positioning={{ placement: "bottom-start" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Badge
          as="button"
          colorPalette="indigo"
          cursor="pointer"
          data-hold-pending={String(isPending)}
          data-testid="hold-lock-badge"
          {...(isPending ? pendingLook : solidLook)}
        >
          <LuLock size={12} />
          {translate(`taskHold.lock.label.${isPending ? "pending" : "held"}`)}
          {holds.length > 1 ? ` · ${holds.length}` : undefined}
        </Badge>
      </Popover.Trigger>
      <Popover.Content maxW="420px" width="max-content">
        <Popover.Arrow />
        <Popover.Body>
          <HeldByList
            dagId={subject.dag_id}
            holds={holds}
            note={translate(`taskHold.lock.tooltip.${kind}`)}
          />
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
