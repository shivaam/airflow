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
import { useState } from "react";

import { Button, Field, Text, Textarea, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuHandHelping } from "react-icons/lu";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { Modal } from "src/system-components";

import { useReleaseTaskHold } from "src/queries/useReleaseTaskHold";
import { getHoldScope } from "src/utils/taskHolds";

type Props = {
  readonly hold: TaskHoldResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

/** Releases a hold, with an optional reason that is kept on the hold. */
const ReleaseHoldDialog = ({ hold, onClose, open }: Props) => {
  const { t: translate } = useTranslation("dags");
  const [reason, setReason] = useState("");

  const handleClose = () => {
    setReason("");
    onClose();
  };

  const { isPending, mutate } = useReleaseTaskHold({ onSuccessConfirm: handleClose });

  return (
    <Modal
      footerActions={
        <Button
          loading={isPending}
          onClick={() =>
            mutate({
              dagId: hold.dag_id,
              holdId: hold.id,
              requestBody: { reason: reason.trim() === "" ? null : reason.trim() },
            })
          }
        >
          <LuHandHelping /> {translate("taskHold.release.confirm")}
        </Button>
      }
      lazyMount
      onOpenChange={(details) => {
        if (!details.open) {
          handleClose();
        }
      }}
      open={open}
      title={translate("taskHold.release.title")}
    >
      <VStack align="stretch" gap={4}>
        <Text>
          {translate(`taskHold.scope.${getHoldScope(hold)}`, { runId: hold.run_id, taskId: hold.task_id })}
        </Text>
        <Text color="fg.muted" fontSize="sm">
          {translate("taskHold.release.description")}
        </Text>
        <Field.Root>
          <Field.Label>{translate("taskHold.release.reasonLabel")}</Field.Label>
          <Textarea
            onChange={(event) => setReason(event.target.value)}
            placeholder={translate("taskHold.release.reasonPlaceholder")}
            rows={3}
            value={reason}
          />
        </Field.Root>
      </VStack>
    </Modal>
  );
};

export default ReleaseHoldDialog;
