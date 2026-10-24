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

import { Box, Button, Field, HStack, Text, Textarea, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuHand, LuInfo } from "react-icons/lu";

import { useStructureServiceStructureData } from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests/types.gen";

import { Modal, Tooltip } from "src/system-components";

import { DateTimeInput } from "src/components/DateTimeInput";

import { useCreateTaskHold } from "src/queries/useCreateTaskHold";
import {
  getHoldScope,
  getHoldStateLineKey,
  getHoldTeardownNote,
  validateHoldForm,
} from "src/utils/taskHolds";

type Props = {
  readonly dagId: string;
  /** The hold covers every map index of a mapped task instance; the dialog says so. */
  readonly isMapped?: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
  /** Set to hold every task in this run, or with `taskId` just that task instance. */
  readonly runId?: string;
  /**
   * The state of the task instance, for the dialog to say what the hold does to it. Left out where it
   * is not known, and not read for a task in every run or a run as a whole.
   */
  readonly state?: TaskInstanceState | null;
  /** Set to hold this task in every run, or with `runId` just that task instance. */
  readonly taskId?: string;
};

const HoldDialog = ({ dagId, isMapped = false, onClose, open, runId, state, taskId }: Props) => {
  const { t: translate } = useTranslation("dags");

  const [reason, setReason] = useState("");
  const [expiresAt, setExpiresAt] = useState("");
  // Errors stay hidden until the first attempt to place the hold, so an untouched dialog is not red.
  const [showErrors, setShowErrors] = useState(false);

  const scope = getHoldScope({ run_id: runId ?? null, task_id: taskId ?? null });
  const stateLineKey =
    scope === "taskInstance" && state !== undefined ? getHoldStateLineKey(state) : undefined;
  // The structure of the latest version is only needed to word the notes, so it is asked for while the
  // dialog is open.
  const { data: structure } = useStructureServiceStructureData({ dagId }, undefined, { enabled: open });
  const teardownNote = getHoldTeardownNote(structure, scope, taskId);
  const infoNotes = [
    ...(scope === "task" ? [translate("taskHold.dialog.info.capacity")] : []),
    ...(teardownNote === undefined
      ? []
      : [
          translate(`taskHold.dialog.info.${teardownNote.key}`, {
            setups: teardownNote.setups.join(", "),
            teardowns: teardownNote.teardowns.join(", "),
          }),
        ]),
  ];
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
        <VStack align="stretch" gap={2}>
          <Text color="fg.muted" fontSize="sm">
            {translate(scope === "run" ? "taskHold.dialog.descriptionRun" : "taskHold.dialog.description")}
          </Text>
          {stateLineKey === undefined ? undefined : (
            <Text bg="bg.muted" borderRadius="md" data-testid="hold-state-line" fontSize="sm" px={3} py={2}>
              {translate(`taskHold.dialog.stateLine.${stateLineKey}`)}
            </Text>
          )}
        </VStack>
        <Box>
          <HStack gap={1} mb={1}>
            <Text fontWeight="bold">{translate("taskHold.dialog.scope")}</Text>
            {infoNotes.length === 0 ? undefined : (
              <Tooltip
                content={
                  <VStack align="start" gap={2}>
                    {infoNotes.map((note) => (
                      <Text key={note}>{note}</Text>
                    ))}
                  </VStack>
                }
                portalled
              >
                <Box as="span" color="fg.muted" cursor="pointer" data-testid="hold-scope-info">
                  <LuInfo />
                </Box>
              </Tooltip>
            )}
          </HStack>
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
