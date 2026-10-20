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

import { Badge, Button, HStack, Stack, Text, useDisclosure } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useTaskHoldServiceListTaskHolds } from "openapi/queries";
import type { TaskHoldResponse } from "openapi/requests/types.gen";

import { ButtonGroupToggle, RouterLink } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import ReleaseHoldDialog from "src/components/Hold/ReleaseHoldDialog";
import Time from "src/components/Time";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useAutoRefresh } from "src/utils";
import { getHoldScope, getHoldStatus, type HoldStatus } from "src/utils/taskHolds";

const statusPalettes = {
  active: "held",
  expired: "gray",
  released: "success",
} as const satisfies Record<HoldStatus, string>;

type HoldsFilter = "active" | "all";

const isHoldsFilter = (value: string | null): value is HoldsFilter => value === "active" || value === "all";

type ColumnProps = {
  readonly onRelease: (hold: TaskHoldResponse) => void;
  readonly translate: TFunction;
};

const getScopeLink = (hold: TaskHoldResponse): string => {
  const scope = getHoldScope(hold);

  return scope === "run"
    ? `/dags/${hold.dag_id}/runs/${hold.run_id}`
    : scope === "task"
      ? `/dags/${hold.dag_id}/tasks/${hold.task_id}`
      : `/dags/${hold.dag_id}/runs/${hold.run_id}/tasks/${hold.task_id}`;
};

const getColumns = ({
  onRelease,
  translate,
}: ColumnProps): Array<ColumnDef<DataTableFeatures, TaskHoldResponse>> => [
  {
    accessorKey: "scope",
    cell: ({ row: { original } }) => (
      <RouterLink to={getScopeLink(original)}>
        {translate(`dags:taskHold.scope.${getHoldScope(original)}`, {
          runId: original.run_id,
          taskId: original.task_id,
        })}
      </RouterLink>
    ),
    enableSorting: false,
    header: translate("dags:taskHold.columns.scope"),
  },
  {
    accessorKey: "reason",
    cell: ({ row: { original } }) => <Text whiteSpace="pre-wrap">{original.reason}</Text>,
    enableSorting: false,
    header: translate("dags:taskHold.columns.reason"),
  },
  {
    accessorKey: "created_by",
    enableSorting: false,
    header: translate("dags:taskHold.columns.createdBy"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row: { original } }) => <Time datetime={original.created_at} />,
    enableSorting: true,
    header: translate("dags:taskHold.columns.createdAt"),
  },
  {
    accessorKey: "expires_at",
    cell: ({ row: { original } }) =>
      original.expires_at === null ? (
        <Text color="fg.muted">{translate("dags:taskHold.columns.neverExpires")}</Text>
      ) : (
        <Time datetime={original.expires_at} />
      ),
    enableSorting: true,
    header: translate("dags:taskHold.columns.expiresAt"),
  },
  {
    accessorKey: "status",
    cell: ({ row: { original } }) => {
      const status = getHoldStatus(original);

      return (
        <Stack alignItems="flex-start" gap={1}>
          <Badge colorPalette={statusPalettes[status]} data-testid="hold-status" variant="subtle">
            {translate(`dags:taskHold.status.${status}`)}
          </Badge>
          {status === "released" ? (
            <Text color="fg.muted" fontSize="xs">
              {original.released_by}
              {original.release_reason === null ? undefined : `: ${original.release_reason}`}
            </Text>
          ) : undefined}
        </Stack>
      );
    },
    enableSorting: false,
    header: translate("dags:taskHold.columns.status"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) =>
      original.active ? (
        <HStack gap={1}>
          <Button
            colorPalette="held"
            data-testid="release-hold"
            onClick={() => onRelease(original)}
            size="xs"
            variant="outline"
          >
            {translate("dags:taskHold.release.button")}
          </Button>
        </HStack>
      ) : undefined,
    enableSorting: false,
    header: "",
  },
];

export const Holds = () => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { dagId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const refetchInterval = useAutoRefresh({});
  const { onClose, onOpen, open } = useDisclosure();
  // Kept after the dialog closes so it can finish animating out.
  const [releasing, setReleasing] = useState<TaskHoldResponse | undefined>(undefined);

  const filterParam = searchParams.get(SearchParamsKeys.ACTIVE);
  // Only holds still in force by default, the way the endpoint answers without a filter.
  const filter: HoldsFilter = isHoldsFilter(filterParam) ? filterParam : "active";

  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-created_at"];

  const { data, error, isLoading } = useTaskHoldServiceListTaskHolds(
    {
      active: filter === "active",
      dagId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
    undefined,
    { enabled: dagId !== "", refetchInterval },
  );

  const onFilterChange = (value: HoldsFilter) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        next.set(SearchParamsKeys.ACTIVE, value);
        next.delete(SearchParamsKeys.OFFSET);

        return next;
      },
      { replace: true },
    );
  };

  const columns = getColumns({
    onRelease: (hold) => {
      setReleasing(hold);
      onOpen();
    },
    translate,
  });

  return (
    <>
      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data ? data.task_holds : []}
        filterActions={
          <ButtonGroupToggle
            onChange={onFilterChange}
            options={[
              {
                dataTestId: "holds-filter-active",
                label: translate("dags:taskHold.filter.active"),
                value: "active",
              },
              { dataTestId: "holds-filter-all", label: translate("dags:taskHold.filter.all"), value: "all" },
            ]}
            value={filter}
          />
        }
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="common:hold"
        noRowsMessage={translate(
          filter === "active" ? "dags:taskHold.noActiveHolds" : "dags:taskHold.noHolds",
        )}
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
      {releasing === undefined ? undefined : (
        <ReleaseHoldDialog hold={releasing} onClose={onClose} open={open} />
      )}
    </>
  );
};
