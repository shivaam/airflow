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
import { Badge } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuHand } from "react-icons/lu";

import { useTaskHoldServiceListTaskHolds } from "openapi/queries";

import { RouterLink } from "src/system-components";

import { useAutoRefresh } from "src/utils";

type Props = {
  readonly dagId: string;
};

/** Reports that the Dag has active task holds, linking to its Holds tab. */
export const ActiveHoldsBanner = ({ dagId }: Props) => {
  const { t: translate } = useTranslation("dags");
  // Not keyed to the Dag, whose paused state stops auto-refresh: holds can be placed, released and
  // expire on a paused Dag too, so the banner has to keep up with them either way.
  const refetchInterval = useAutoRefresh({});

  const { data } = useTaskHoldServiceListTaskHolds({ active: true, dagId, limit: 1 }, undefined, {
    enabled: dagId !== "",
    // Only poll while a hold is active, so an idle Dag page does not tick against the holds
    // endpoint forever. Holds placed from this page refresh the banner through invalidation.
    refetchInterval: (query) => (query.state.data?.total_entries ?? 0) > 0 && refetchInterval,
  });

  const count = data?.total_entries ?? 0;

  if (count === 0) {
    return undefined;
  }

  return (
    <RouterLink to={`/dags/${dagId}/holds`}>
      <Badge colorPalette="indigo" cursor="pointer" data-testid="active-holds-banner" variant="subtle">
        <LuHand size={12} />
        {translate("taskHold.activeCount", { count })}
      </Badge>
    </RouterLink>
  );
};
