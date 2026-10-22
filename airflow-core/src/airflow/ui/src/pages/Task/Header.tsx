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
import { useTranslation } from "react-i18next";
import { FiBookOpen } from "react-icons/fi";
import { useParams } from "react-router-dom";

import type { TaskResponse } from "openapi/requests/types.gen";

import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import { HoldButton } from "src/components/Hold";
import { HoldLockBadge } from "src/components/HoldLockBadge";

import { TaskIcon } from "src/assets/TaskIcon";

export const Header = ({ task }: { readonly task: TaskResponse }) => {
  const { t: translate } = useTranslation();
  const { dagId = "" } = useParams();

  return (
    <HeaderCard
      actions={
        <>
          {task.doc_md === null ? undefined : (
            <DisplayMarkdownButton
              bg="bg"
              header={translate("task.documentation")}
              icon={<FiBookOpen />}
              mdContent={task.doc_md}
              text={translate("docs.documentation")}
              variant="outline"
            />
          )}
          {task.task_id === null ? undefined : (
            <HoldButton bg="bg" dagId={dagId} taskId={task.task_id} variant="outline" />
          )}
        </>
      }
      icon={<TaskIcon />}
      stats={[
        { label: translate("task.operator"), value: task.operator_name },
        { label: translate("task.triggerRule"), value: task.trigger_rule },
      ]}
      statusBadge={
        task.task_id === null ? undefined : (
          <HoldLockBadge subject={{ dag_id: dagId, task_id: task.task_id }} />
        )
      }
      title={`${task.task_display_name}${task.is_mapped ? " [ ]" : ""}`}
      type="task"
    />
  );
};
