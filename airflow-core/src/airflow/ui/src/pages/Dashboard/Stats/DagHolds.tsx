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
import { Box, HStack, Skeleton, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight } from "react-icons/fi";
import { LuHand } from "react-icons/lu";
import { Link as RouterLink } from "react-router-dom";

import { useTaskHoldServiceListTaskHolds } from "openapi/queries";

import { StateBadge } from "src/components/StateBadge";

import { ACTIVE_HOLDS_LIMIT } from "src/queries/useActiveTaskHolds";
import { formatNumber, useAutoRefresh } from "src/utils";
import { countActiveHoldsByDag } from "src/utils/taskHolds";

/**
 * Lists the Dags that have active task holds, each with its count and a link to its Holds tab. Like
 * the import error cards, it is not there when there is nothing to report. The holds are asked for
 * whole, so a Dashboard with more active holds than the limit lists only the Dags of the first ones.
 */
export const DagHolds = () => {
  const { i18n, t: translate } = useTranslation("dashboard");
  const refetchInterval = useAutoRefresh({});

  const { data, isLoading } = useTaskHoldServiceListTaskHolds(
    { active: true, dagId: "~", limit: ACTIVE_HOLDS_LIMIT },
    undefined,
    {
      // Only poll while a hold is active, so an idle Dashboard does not tick against the holds endpoint.
      refetchInterval: (query) => (query.state.data?.task_holds.length ?? 0) > 0 && refetchInterval,
    },
  );

  const dags = countActiveHoldsByDag(data?.task_holds ?? []);

  const isRTL = i18n.dir() === "rtl";
  const Chevron = isRTL ? FiChevronLeft : FiChevronRight;

  if (isLoading) {
    return <Skeleton height="9" width="225px" />;
  }

  if (dags.length === 0) {
    return undefined;
  }

  return (
    <Box borderRadius="lg" borderWidth={1} color="fg.emphasized" data-testid="dag-holds-card" p={2}>
      <HStack alignItems="center">
        <StateBadge colorPalette="indigo" mr={2}>
          <LuHand />
          {formatNumber(dags.length, i18n.language)}
        </StateBadge>
        <Text color="fg" fontSize="sm" fontWeight="bold">
          {translate("stats.heldDags", { count: dags.length })}
        </Text>
      </HStack>
      <VStack align="stretch" as="ul" gap={1} listStyleType="none" maxH="200px" mt={2} overflowY="auto">
        {dags.map(({ count, dagId }) => (
          <li key={dagId}>
            <RouterLink data-testid="dag-holds-row" to={`/dags/${dagId}/holds`}>
              <HStack fontSize="sm" justifyContent="space-between">
                <Text>{dagId}</Text>
                <HStack color="fg.muted" gap={1}>
                  <Text>{translate("dags:taskHold.activeCount", { count })}</Text>
                  <Chevron size={16} />
                </HStack>
              </HStack>
            </RouterLink>
          </li>
        ))}
      </VStack>
    </Box>
  );
};
