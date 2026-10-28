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
import { LuHand } from "react-icons/lu";

import type { DagRunState, TaskInstanceState } from "openapi/requests/types.gen";

import { IconButton } from "src/system-components";

import HoldDialog from "./HoldDialog";

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
 * the dialog offers to hold; the person chooses from that.
 */
export const HoldButton = ({ dagId, isMapped, runId, runState, state, taskId, ...rest }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("dags");

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
