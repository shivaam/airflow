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
import type { ButtonProps } from "@chakra-ui/react";
import { useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuHand, LuHandHelping } from "react-icons/lu";

import type { DagRunState, TaskInstanceState } from "openapi/requests/types.gen";

import { IconButton, Popover } from "src/system-components";

import { useActiveTaskHolds } from "src/queries/useActiveTaskHolds";
import { getCoveringHolds } from "src/utils/taskHolds";

import { HeldByList } from "./HeldByList";
import HoldDialog from "./HoldDialog";
import ReleaseHoldDialog from "./ReleaseHoldDialog";

type Props = {
  readonly dagId: string;
  readonly isMapped?: boolean;
  /** The run the page is about, or with `taskId` the run of that task instance. */
  readonly runId?: string;
  /** The state of the run, for the dialog to say when holding it has no effect. */
  readonly runState?: DagRunState;
  /** The state of the task instance, for the dialog to say what the hold does to it. */
  readonly state?: TaskInstanceState | null;
  /** The task the page is about, or with `runId` that task instance. */
  readonly taskId?: string;
} & ButtonProps;

/**
 * Opens the dialog that places a hold. It is given what the page it sits on knows, which decides what
 * the dialog offers to hold; the person chooses from that. While a hold already covers what the page
 * is about, it releases instead: one hold through the release dialog, several through the list of them,
 * as releasing one leaves the others in force.
 */
export const HoldButton = ({ dagId, isMapped, runId, runState, state, taskId, ...rest }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("dags");
  const holds = getCoveringHolds(useActiveTaskHolds({ dagId }), {
    dag_id: dagId,
    dag_run_id: runId,
    task_id: taskId,
  });
  const [onlyHold] = holds;

  if (holds.length > 1) {
    return (
      <Popover.Root lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
        <Popover.Trigger asChild>
          <IconButton {...rest} label={translate("taskHold.release.confirm")}>
            <LuHandHelping />
          </IconButton>
        </Popover.Trigger>
        <Popover.Content maxW="420px" width="max-content">
          <Popover.Arrow />
          <Popover.Body>
            <HeldByList dagId={dagId} holds={holds} />
          </Popover.Body>
        </Popover.Content>
      </Popover.Root>
    );
  }

  if (onlyHold !== undefined) {
    return (
      <>
        <IconButton {...rest} label={translate("taskHold.release.confirm")} onClick={onOpen}>
          <LuHandHelping />
        </IconButton>
        <ReleaseHoldDialog hold={onlyHold} onClose={onClose} open={open} />
      </>
    );
  }

  return (
    <>
      <IconButton {...rest} label={translate("taskHold.button")} onClick={onOpen}>
        <LuHand />
      </IconButton>
      <HoldDialog
        dagId={dagId}
        isMapped={isMapped}
        onClose={onClose}
        open={open}
        runId={runId}
        runState={runState}
        state={state}
        taskId={taskId}
      />
    </>
  );
};
