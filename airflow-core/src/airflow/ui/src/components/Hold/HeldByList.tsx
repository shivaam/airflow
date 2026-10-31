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
import { useEffect, useRef, useState } from "react";

import { Button, Field, HStack, Stack, Text, Textarea } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import Time from "src/components/Time";

import { useReleaseTaskHold } from "src/queries/useReleaseTaskHold";
import { getHoldScope } from "src/utils/taskHolds";

type Props = {
  readonly dagId: string;
  /** The active holds that cover what is in view, each listed with a way to release it. */
  readonly holds: ReadonlyArray<TaskHoldResponse>;
  /** What the holds do to what is in view, said under the title. */
  readonly note?: string;
};

/**
 * Which holds are holding up what is in view, and a way to release each one. Several holds can cover
 * the same task instance, and it stays held until all of them are released, so each is listed on its own.
 * Release opens a box for an optional reason under that hold, inside the list: it is shown in a popover,
 * which a dialog on top would close. Releasing with the box left empty keeps no reason, and the toast
 * says it is done.
 */
export const HeldByList = ({ dagId, holds, note }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const [releasingId, setReleasingId] = useState<number | undefined>(undefined);
  const [reason, setReason] = useState("");
  const reasonRef = useRef<HTMLTextAreaElement>(null);

  const closeRelease = () => {
    setReleasingId(undefined);
    setReason("");
  };

  const openRelease = (holdId: number) => {
    setReleasingId(holdId);
    setReason("");
  };

  const { isPending, mutate } = useReleaseTaskHold({ onSuccessConfirm: closeRelease });

  // The box takes the focus as it opens, so a reason can be typed without another click.
  useEffect(() => {
    if (releasingId !== undefined) {
      reasonRef.current?.focus();
    }
  }, [releasingId]);

  return (
    <Stack data-testid="held-by-list" gap={3}>
      <Stack gap={0.5}>
        <Text fontWeight="semibold">{translate("taskHold.heldBy.title", { count: holds.length })}</Text>
        {note === undefined ? undefined : (
          <Text color="fg.muted" fontSize="sm">
            {note}
          </Text>
        )}
      </Stack>
      {holds.map((hold) => (
        <Stack data-testid="held-by-hold" gap={2} key={hold.id}>
          <HStack alignItems="flex-start" gap={3}>
            <Stack flex={1} gap={0.5} minW={0}>
              <Text fontSize="sm" fontWeight="medium">
                {translate(`taskHold.scope.${getHoldScope(hold)}`, {
                  runId: hold.run_id,
                  taskId: hold.task_id,
                })}
              </Text>
              <Text fontSize="sm" whiteSpace="pre-wrap">
                {hold.reason}
              </Text>
              <Text color="fg.muted" fontSize="xs">
                {hold.created_by} · <Time datetime={hold.created_at} /> ·{" "}
                {hold.expires_at === null ? (
                  translate("taskHold.heldBy.untilReleased")
                ) : (
                  <>
                    {translate("taskHold.heldBy.until")} <Time datetime={hold.expires_at} />
                  </>
                )}
              </Text>
            </Stack>
            {releasingId === hold.id ? undefined : (
              <Button
                colorPalette="indigo"
                data-testid="held-by-release"
                onClick={() => openRelease(hold.id)}
                size="xs"
                variant="outline"
              >
                {translate("taskHold.release.button")}
              </Button>
            )}
          </HStack>
          {releasingId === hold.id ? (
            <Stack data-testid="held-by-release-form" gap={2}>
              <Field.Root>
                <Field.Label fontSize="xs">{translate("taskHold.release.reasonLabel")}</Field.Label>
                <Textarea
                  data-testid="held-by-release-reason"
                  onChange={(event) => setReason(event.target.value)}
                  placeholder={translate("taskHold.release.reasonPlaceholder")}
                  ref={reasonRef}
                  rows={2}
                  size="sm"
                  value={reason}
                />
              </Field.Root>
              <HStack gap={2} justifyContent="flex-end">
                <Button
                  data-testid="held-by-release-cancel"
                  disabled={isPending}
                  onClick={closeRelease}
                  size="xs"
                  variant="ghost"
                >
                  {translate("common:modal.cancel")}
                </Button>
                <Button
                  colorPalette="indigo"
                  data-testid="held-by-release-confirm"
                  loading={isPending}
                  onClick={() =>
                    mutate({
                      dagId: hold.dag_id,
                      holdId: hold.id,
                      requestBody: { reason: reason.trim() === "" ? null : reason.trim() },
                    })
                  }
                  size="xs"
                >
                  {translate("taskHold.release.confirm")}
                </Button>
              </HStack>
            </Stack>
          ) : undefined}
        </Stack>
      ))}
      <HStack fontSize="xs" justifyContent="space-between">
        {holds.length > 1 ? (
          <Text color="fg.muted">{translate("taskHold.heldBy.othersRemain")}</Text>
        ) : (
          <span />
        )}
        <RouterLink to={`/dags/${dagId}/holds`}>{translate("taskHold.heldBy.viewAll")}</RouterLink>
      </HStack>
    </Stack>
  );
};
