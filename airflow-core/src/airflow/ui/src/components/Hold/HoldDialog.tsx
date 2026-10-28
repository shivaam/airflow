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
import type { DagRunState, TaskInstanceState } from "openapi/requests/types.gen";

import { Modal, RadioCardItem, RadioCardLabel, RadioCardRoot, Tooltip } from "src/system-components";

import { DateTimeInput } from "src/components/DateTimeInput";

import { useCreateTaskHold } from "src/queries/useCreateTaskHold";
import {
  getDefaultHoldScope,
  getHoldRequestScope,
  getHoldTeardownNote,
  getOfferedHoldScopes,
  getScopeStateLineKey,
  validateHoldForm,
  type HoldScope,
} from "src/utils/taskHolds";

type Props = {
  readonly dagId: string;
  /** The hold covers every map index of a mapped task instance; the dialog says so. */
  readonly isMapped?: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
  /** The run of the page the dialog is opened from. With `taskId` it offers to hold just that task instance. */
  readonly runId?: string;
  /** The state of that run, for the dialog to say when holding it has no effect. */
  readonly runState?: DagRunState;
  /**
   * The state of the task instance, for the dialog to say what the hold does to it. Left out where it
   * is not known, and not read for a task in every run or a run as a whole.
   */
  readonly state?: TaskInstanceState | null;
  /** The task of the page the dialog is opened from. With `runId` it offers to hold just that task instance. */
  readonly taskId?: string;
};

/** How long a hold lasts: until it is released, or until a time it ends by itself. */
type HoldDuration = "forever" | "until";

// The label of each choice of what to hold. A run is described by the same words as in the lists of
// holds, so its secondary text reuses those.
const scopeOptionKeys = {
  run: { detail: "taskHold.scope.run", label: "taskHold.dialog.scope.run.label" },
  task: { detail: "taskHold.dialog.scope.task.detail", label: "taskHold.dialog.scope.task.label" },
  taskInstance: {
    detail: "taskHold.dialog.scope.taskInstance.detail",
    label: "taskHold.dialog.scope.taskInstance.label",
  },
} as const satisfies Record<HoldScope, { detail: string; label: string }>;

const HoldDialog = ({ dagId, isMapped = false, onClose, open, runId, runState, state, taskId }: Props) => {
  const { t: translate } = useTranslation("dags");

  const [reason, setReason] = useState("");
  // Left unset until a choice is made, so that what the dialog starts on follows the page it is opened from.
  const [chosenScope, setChosenScope] = useState<HoldScope | undefined>(undefined);
  const [duration, setDuration] = useState<HoldDuration>("forever");
  const [expiresAt, setExpiresAt] = useState("");
  // Errors stay hidden until the first attempt to place the hold, so an untouched dialog is not red.
  const [showErrors, setShowErrors] = useState(false);

  const offeredScopes = getOfferedHoldScopes({ runId, taskId });
  const scope =
    offeredScopes.find((candidate) => candidate === chosenScope) ??
    getDefaultHoldScope({ runId, state, taskId });
  const stateLineKey = getScopeStateLineKey(scope, { runState, state });
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
  const errors = validateHoldForm({ expiresAt: duration === "until" ? expiresAt : undefined, reason });
  const isReasonInvalid = showErrors && errors.reason !== undefined;
  // A time in the past is flagged as soon as it is entered. A missing one waits for the attempt to place
  // the hold, like a missing reason.
  const isExpiresAtInvalid = errors.expiresAt === "past" || (showErrors && errors.expiresAt !== undefined);

  const handleClose = () => {
    setReason("");
    setChosenScope(undefined);
    setDuration("forever");
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
        expires_at: duration === "until" ? expiresAt : null,
        reason: reason.trim(),
        ...getHoldRequestScope(scope, { runId, taskId }),
      },
    });
  };

  const infoIcon =
    infoNotes.length === 0 ? undefined : (
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
    );

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
          {offeredScopes.length > 1 ? (
            <RadioCardRoot
              onValueChange={({ value }) => {
                const next = offeredScopes.find((candidate) => candidate === value);

                if (next !== undefined) {
                  setChosenScope(next);
                }
              }}
              size="sm"
              value={scope}
            >
              <HStack gap={1} mb={2}>
                <RadioCardLabel fontSize="md" fontWeight="bold">
                  {translate("taskHold.dialog.scope.label")}
                </RadioCardLabel>
                {infoIcon}
              </HStack>
              <VStack align="stretch" gap={2}>
                {offeredScopes.map((option) => (
                  <RadioCardItem
                    description={translate(scopeOptionKeys[option].detail, { runId, taskId })}
                    indicatorPlacement="start"
                    key={option}
                    label={translate(scopeOptionKeys[option].label)}
                    value={option}
                  />
                ))}
              </VStack>
            </RadioCardRoot>
          ) : (
            <>
              <HStack gap={1} mb={1}>
                <Text fontWeight="bold">{translate("taskHold.dialog.scope.label")}</Text>
                {infoIcon}
              </HStack>
              <Text data-testid="hold-scope">{translate(`taskHold.scope.${scope}`, { runId, taskId })}</Text>
            </>
          )}
          {isMapped && (scope === "task" || scope === "taskInstance") ? (
            <Text color="fg.muted" data-testid="hold-mapped-hint" fontSize="sm" mt={2}>
              {translate("taskHold.dialog.mappedHint")}
            </Text>
          ) : undefined}
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
        <Box>
          <RadioCardRoot
            onValueChange={({ value }) => {
              if (value === "forever" || value === "until") {
                setDuration(value);
              }
            }}
            size="sm"
            value={duration}
          >
            <RadioCardLabel fontSize="md" fontWeight="bold" mb={2}>
              {translate("taskHold.dialog.expiresAt.label")}
            </RadioCardLabel>
            <VStack align="stretch" gap={2}>
              <RadioCardItem
                indicatorPlacement="start"
                label={translate("taskHold.dialog.expiresAt.forever")}
                value="forever"
              />
              <RadioCardItem
                indicatorPlacement="start"
                label={translate("taskHold.dialog.expiresAt.date")}
                value="until"
              />
            </VStack>
          </RadioCardRoot>
          {duration === "until" ? (
            <Field.Root invalid={isExpiresAtInvalid} mt={2}>
              <DateTimeInput onChange={(event) => setExpiresAt(event.target.value)} value={expiresAt} />
              <Field.HelperText>{translate("taskHold.dialog.expiresAt.helper")}</Field.HelperText>
              <Field.ErrorText>
                {translate(
                  errors.expiresAt === "required"
                    ? "taskHold.dialog.expiresAt.required"
                    : "taskHold.dialog.expiresAt.past",
                )}
              </Field.ErrorText>
            </Field.Root>
          ) : undefined}
        </Box>
      </VStack>
    </Modal>
  );
};

export default HoldDialog;
