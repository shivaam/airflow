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

import { Box, Button, Field, Text, Textarea, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuHand } from "react-icons/lu";

import { Modal } from "src/system-components";

import { DateTimeInput } from "src/components/DateTimeInput";

import { useCreateTaskHold } from "src/queries/useCreateTaskHold";
import { getHoldScope, validateHoldForm } from "src/utils/taskHolds";

type Props = {
  readonly dagId: string;
  /** The hold covers every map index of a mapped task instance; the dialog says so. */
  readonly isMapped?: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
  /** Set to hold every task in this run, or with `taskId` just that task instance. */
  readonly runId?: string;
  /** Set to hold this task in every run, or with `runId` just that task instance. */
  readonly taskId?: string;
};

const HoldDialog = ({ dagId, isMapped = false, onClose, open, runId, taskId }: Props) => {
  const { t: translate } = useTranslation("dags");

  const [reason, setReason] = useState("");
  const [expiresAt, setExpiresAt] = useState("");
  // Errors stay hidden until the first attempt to place the hold, so an untouched dialog is not red.
  const [showErrors, setShowErrors] = useState(false);

  const scope = getHoldScope({ run_id: runId ?? null, task_id: taskId ?? null });
  const errors = validateHoldForm({ expiresAt, reason });
  const isReasonInvalid = showErrors && errors.reason !== undefined;
  const isExpiresAtInvalid = showErrors && errors.expiresAt !== undefined;

  const handleClose = () => {
    setReason("");
    setExpiresAt("");
    setShowErrors(false);
    onClose();
  };

  const { isPending, mutate } = useCreateTaskHold({ onSuccessConfirm: handleClose });

  const onConfirm = () => {
    setShowErrors(true);

    if (Object.keys(errors).length > 0) {
      return;
    }

    mutate({
      dagId,
      requestBody: {
        expires_at: expiresAt === "" ? null : expiresAt,
        reason: reason.trim(),
        run_id: runId ?? null,
        task_id: taskId ?? null,
      },
    });
  };

  return (
    <Modal
      footerActions={
        <Button loading={isPending} onClick={onConfirm}>
          <LuHand /> {translate("taskHold.dialog.confirm")}
        </Button>
      }
      lazyMount
      onOpenChange={(details) => {
        if (!details.open) {
          handleClose();
        }
      }}
      open={open}
      title={translate("taskHold.dialog.title")}
    >
      <VStack align="stretch" gap={4}>
        <Text color="fg.muted" fontSize="sm">
          {translate("taskHold.dialog.description")}
        </Text>
        <Box>
          <Text fontWeight="bold" mb={1}>
            {translate("taskHold.dialog.scope")}
          </Text>
          <Text data-testid="hold-scope">
            {translate(`taskHold.scope.${scope}`, { runId, taskId })}
            {isMapped ? ` ${translate("taskHold.dialog.mappedHint")}` : undefined}
          </Text>
        </Box>
        <Field.Root invalid={isReasonInvalid} required>
          <Field.Label>
            {translate("taskHold.dialog.reason.label")} <Field.RequiredIndicator />
          </Field.Label>
          <Textarea
            onChange={(event) => setReason(event.target.value)}
            placeholder={translate("taskHold.dialog.reason.placeholder")}
            rows={3}
            value={reason}
          />
          <Field.ErrorText>{translate("taskHold.dialog.reason.required")}</Field.ErrorText>
        </Field.Root>
        <Field.Root invalid={isExpiresAtInvalid}>
          <Field.Label>{translate("taskHold.dialog.expiresAt.label")}</Field.Label>
          <DateTimeInput onChange={(event) => setExpiresAt(event.target.value)} value={expiresAt} />
          <Field.HelperText>{translate("taskHold.dialog.expiresAt.helper")}</Field.HelperText>
          <Field.ErrorText>{translate("taskHold.dialog.expiresAt.past")}</Field.ErrorText>
        </Field.Root>
      </VStack>
    </Modal>
  );
};

export default HoldDialog;
