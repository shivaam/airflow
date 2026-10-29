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
import { Button, HStack, Stack, Text } from "@chakra-ui/react";
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
 * Release acts at once, without asking for a reason: it is shown in a popover, which a dialog on top
 * would close, and the toast says it is done.
 */
export const HeldByList = ({ dagId, holds, note }: Props) => {
  const { t: translate } = useTranslation("dags");
  const { isPending, mutate, variables } = useReleaseTaskHold({ onSuccessConfirm: () => undefined });
  const releasingId = isPending ? variables.holdId : undefined;

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
        <HStack alignItems="flex-start" data-testid="held-by-hold" gap={3} key={hold.id}>
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
          <Button
            colorPalette="indigo"
            data-testid="held-by-release"
            loading={releasingId === hold.id}
            onClick={() => mutate({ dagId: hold.dag_id, holdId: hold.id, requestBody: { reason: null } })}
            size="xs"
            variant="outline"
          >
            {translate("taskHold.release.button")}
          </Button>
        </HStack>
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
