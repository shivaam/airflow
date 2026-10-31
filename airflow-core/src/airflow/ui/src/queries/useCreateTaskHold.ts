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
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import {
  useDashboardServiceDagHoldCountsKey,
  useTaskHoldServiceCreateTaskHold,
  useTaskHoldServiceCreateTaskInstanceHold,
  useTaskHoldServiceListTaskHoldsKey,
} from "openapi/queries";
import type { TaskHoldBody } from "openapi/requests/types.gen";

import { toaster } from "src/system-components";

import { createErrorToaster } from "src/utils";

/** A hold to place: with `runId` a task instance hold, without it a task hold. */
export type NewTaskHold = {
  readonly dagId: string;
  readonly requestBody: TaskHoldBody;
  readonly runId?: string;
  readonly taskId: string;
};

/** Places a task instance hold or a task hold, each through its own endpoint. */
export const useCreateTaskHold = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("dags");

  const onError = (error: unknown) => {
    createErrorToaster(error, { titleKey: "dags:taskHold.dialog.error" }, translate);
  };

  const onSuccess = async () => {
    // A hold changes no task instance, so only the lists that show holds change.
    await Promise.all(
      [[useTaskHoldServiceListTaskHoldsKey], [useDashboardServiceDagHoldCountsKey]].map((queryKey) =>
        queryClient.invalidateQueries({ queryKey }),
      ),
    );

    toaster.create({
      description: translate("taskHold.dialog.success.description"),
      title: translate("taskHold.dialog.success.title"),
      type: "success",
    });

    onSuccessConfirm();
  };

  const taskInstanceHold = useTaskHoldServiceCreateTaskInstanceHold({ onError, onSuccess });
  const taskHold = useTaskHoldServiceCreateTaskHold({ onError, onSuccess });

  const mutate = ({ dagId, requestBody, runId, taskId }: NewTaskHold) => {
    if (runId === undefined) {
      taskHold.mutate({ dagId, requestBody, taskId });
    } else {
      taskInstanceHold.mutate({ dagId, dagRunId: runId, requestBody, taskId });
    }
  };

  return { isPending: taskInstanceHold.isPending || taskHold.isPending, mutate };
};
