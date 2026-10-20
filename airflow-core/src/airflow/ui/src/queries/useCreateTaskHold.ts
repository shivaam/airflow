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
  useTaskHoldServiceCreateTaskHold,
  useTaskHoldServiceListTaskHoldsKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { CreateTaskHoldData, TaskHoldResponse } from "openapi/requests/types.gen";

import { toaster } from "src/system-components";

import { createErrorToaster } from "src/utils";

import { gridQueryKeys } from "./gridViewQueryKeys";

export const useCreateTaskHold = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("dags");

  const onError = (error: unknown) => {
    createErrorToaster(error, { titleKey: "dags:taskHold.dialog.error" }, translate);
  };

  const onSuccess = async (_hold: TaskHoldResponse, { dagId }: CreateTaskHoldData) => {
    // The scheduler parks the matching task instances on its next pass, so the lists that show
    // holds refresh now and the task instance views follow through their own polling.
    await Promise.all(
      [
        [useTaskHoldServiceListTaskHoldsKey],
        [useTaskInstanceServiceGetTaskInstancesKey],
        ...gridQueryKeys(dagId),
      ].map((queryKey) => queryClient.invalidateQueries({ queryKey })),
    );

    toaster.create({
      description: translate("taskHold.dialog.success.description"),
      title: translate("taskHold.dialog.success.title"),
      type: "success",
    });

    onSuccessConfirm();
  };

  return useTaskHoldServiceCreateTaskHold({ onError, onSuccess });
};
