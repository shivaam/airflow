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
import type { ComponentProps } from "react";

import { Badge, Box, Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuLock } from "react-icons/lu";
import { Link, useLocation, useParams, useSearchParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { StateIcon } from "src/components/StateIcon";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";

import { useColorMode } from "src/context/colorMode";
import { setExecutionCoordinates, stripExecutionParams } from "src/utils/executionParams";
import { buildTaskInstanceUrl } from "src/utils/links";
import { sortStateEntries } from "src/utils/stateUtils";

import { NOTE_GRADIENT, SELECTED_TASK_OUTLINE_COLOR, TASK_BAR_HEIGHT_PX } from "./constants";

const BACK_CARD_OFFSET_PX = 2.5;
const STACK_GAP_SHADOW = "0.5px 0.5px 0 0 var(--chakra-colors-bg)";
const buildBackCardShadow = (state: string) =>
  `${STACK_GAP_SHADOW}, ${BACK_CARD_OFFSET_PX}px ${BACK_CARD_OFFSET_PX}px 0 0 var(--chakra-colors-${state}-solid)`;

// Only a held cell asks for its label, so the cells of a Dag with no holds carry no translation hook.
const HeldMark = () => {
  const { t: translate } = useTranslation("dags");

  return (
    <Box
      alignItems="center"
      as="span"
      bg="indigo.solid"
      borderRadius="full"
      bottom="1px"
      color="indigo.contrast"
      data-testid="grid-hold-lock"
      display="flex"
      height="11px"
      justifyContent="center"
      pointerEvents="none"
      position="absolute"
      right="-2px"
      width="11px"
    >
      <LuLock aria-label={translate("taskHold.lock.label.held")} size={7} />
    </Box>
  );
};

// The tooltip of a held cell leads with the hold, so it is told apart from a task that is merely waiting.
const HeldTooltip = (props: ComponentProps<typeof TaskInstanceTooltip>) => {
  const { t: translate } = useTranslation("dags");

  return <TaskInstanceTooltip {...props} tooltip={translate("taskHold.lock.gridTooltip")} />;
};

type Props = {
  readonly dagId: string;
  readonly hasNote?: boolean;
  readonly instance: LightGridTaskInstanceSummary;
  readonly isGroup?: boolean;
  /** Set while an active hold is stopping this task instance now. */
  readonly isHeld?: boolean;
  readonly isMapped?: boolean | null;
  readonly label: string;
  readonly loopMaxIterations?: number | null;
  readonly onClick?: () => void;
  readonly runId: string;
  readonly taskId: string;
};

export const GridTI = ({
  dagId,
  hasNote = false,
  instance,
  isGroup,
  isHeld = false,
  isMapped,
  loopMaxIterations,
  onClick,
  runId,
  taskId,
}: Props) => {
  const { groupId: selectedGroupId, runId: selectedRunId, taskId: selectedTaskId } = useParams();
  const { colorMode = "light" } = useColorMode();
  const location = useLocation();

  const [searchParams] = useSearchParams();

  const hasTaskInstance = instance.dag_version_number !== null && instance.dag_version_number !== undefined;
  const taskUrl = hasTaskInstance
    ? buildTaskInstanceUrl({
        currentPathname: location.pathname,
        dagId,
        isGroup,
        isMapped: Boolean(isMapped),
        runId,
        taskId,
      })
    : `/dags/${dagId}/tasks/${isGroup ? "group/" : ""}${taskId}`;

  const redirectionSearch = setExecutionCoordinates(
    stripExecutionParams(searchParams),
    instance.latest_region_id,
    instance.latest_region_index,
  ).toString();

  const isSelectedRow = selectedTaskId === taskId || selectedGroupId === taskId;
  const isSelectedTaskInstance = selectedRunId === runId && isSelectedRow;
  const selectedOutlineColor = SELECTED_TASK_OUTLINE_COLOR[colorMode];
  const isStacked = Boolean(isMapped) || Boolean(isGroup);
  const frontCardState = instance.state ?? "none";
  const backCardState = isStacked
    ? (sortStateEntries(instance.child_states).find(([state]) => state !== frontCardState)?.[0] ??
      frontCardState)
    : undefined;
  const badgeSize = `${isStacked ? TASK_BAR_HEIGHT_PX - 1 : TASK_BAR_HEIGHT_PX}px`;
  const stackPadding = isStacked ? `${BACK_CARD_OFFSET_PX}px` : undefined;
  const CellTooltip = isHeld ? HeldTooltip : TaskInstanceTooltip;
  // A held cell is ringed, unless it is selected, whose outline takes the same place.
  const outlineColor = isSelectedTaskInstance ? selectedOutlineColor : isHeld ? "indigo.solid" : undefined;

  return (
    <Flex
      alignItems="center"
      bg={isSelectedRow ? "brand.emphasized" : undefined}
      data-run-id={runId}
      data-selected={isSelectedRow}
      data-task-id={taskId}
      height="20px"
      id={`task-${taskId.replaceAll(".", "-")}`}
      justifyContent="center"
      key={taskId}
      position="relative"
      py={0}
      transition="background-color 0.2s"
      width="18px"
    >
      <CellTooltip
        loopMaxIterations={loopMaxIterations}
        openDelay={500}
        positioning={{ placement: "bottom" }}
        runId={runId}
        taskInstance={instance}
      >
        <Box
          as="span"
          borderRadius={4}
          display="flex"
          outlineColor={outlineColor}
          outlineOffset={isStacked ? "0px" : "1px"}
          outlineStyle={outlineColor === undefined ? undefined : "solid"}
          outlineWidth={outlineColor === undefined ? undefined : "2px"}
          pb={stackPadding}
          pr={stackPadding}
          transition="outline-color 0.2s"
        >
          <Link
            data-testid={`grid-${runId}-${taskId}`}
            id={`grid-${runId}-${taskId}`}
            onClick={onClick}
            replace
            to={{
              pathname: taskUrl,
              search: redirectionSearch,
            }}
          >
            <Badge
              alignItems="center"
              borderRadius={4}
              colorPalette={frontCardState}
              data-selected={isSelectedTaskInstance || undefined}
              data-testid="task-state-badge"
              display="flex"
              height={badgeSize}
              justifyContent="center"
              minH={0}
              p={0}
              style={{
                background: hasNote ? NOTE_GRADIENT : undefined,
                boxShadow: backCardState === undefined ? undefined : buildBackCardShadow(backCardState),
              }}
              variant="solid"
              width={badgeSize}
            >
              <StateIcon size={10} state={instance.state} />
            </Badge>
          </Link>
        </Box>
      </CellTooltip>
      {isHeld ? <HeldMark /> : undefined}
    </Flex>
  );
};
