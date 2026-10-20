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

import { IconButton } from "src/system-components";

import { getHoldScope } from "src/utils/taskHolds";

import HoldDialog from "./HoldDialog";

type Props = {
  readonly dagId: string;
  readonly isMapped?: boolean;
  /** Set to hold every task in this run, or with `taskId` just that task instance. */
  readonly runId?: string;
  /** Set to hold this task in every run, or with `runId` just that task instance. */
  readonly taskId?: string;
} & ButtonProps;

const typeKeys = {
  run: "dagRun_one",
  task: "task_one",
  taskInstance: "taskInstance_one",
} as const;

/** Opens the dialog that places a hold, its scope fixed by where the button sits. */
export const HoldButton = ({ dagId, isMapped, runId, taskId, ...rest }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();

  const scope = getHoldScope({ run_id: runId ?? null, task_id: taskId ?? null });

  return (
    <>
      <IconButton
        {...rest}
        label={translate("dags:taskHold.button", { type: translate(typeKeys[scope]) })}
        onClick={onOpen}
      >
        <LuHand />
      </IconButton>
      <HoldDialog
        dagId={dagId}
        isMapped={isMapped}
        onClose={onClose}
        open={open}
        runId={runId}
        taskId={taskId}
      />
    </>
  );
};
