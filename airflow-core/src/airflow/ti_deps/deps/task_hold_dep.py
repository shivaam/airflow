#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.base_ti_dep import TIDepStatus


class TaskHoldDep(BaseTIDep):
    """
    Determines whether no operator-placed task hold covers the task instance.

    This is the first of the two gates a task hold has in the scheduler. It sits among the dependencies the
    scheduler evaluates when it decides whether a task instance can move to ``scheduled``, so a covered
    task instance stays where it is (``none``, ``up_for_retry`` or ``up_for_reschedule``) and is
    evaluated again by every scheduling decision until the hold is released or expires. The task
    instance's state, its try number and its trigger rule are never touched by the hold, and the trigger
    rule is evaluated afresh on release.

    A hold covers every map index of the task. A hold without a ``task_id`` covers every task of its run,
    and a hold without a ``run_id`` covers its task in every run.

    A context that sets ``ignore_task_holds`` passes the dependency: the scheduler's deadlock check does,
    so a covered task instance counts as runnable later instead of as stuck.
    """

    NAME = "Task Hold"
    IGNORABLE = True

    def _get_dep_statuses(
        self, ti: TaskInstance, dep_context: DepContext, *, session: Session
    ) -> Iterator[TIDepStatus]:
        if dep_context.ignore_task_holds:
            yield self._passing_status(reason="The context specified that task holds can be ignored.")
            return

        holds = dep_context.covering_task_holds(ti, session=session)
        if not holds:
            yield self._passing_status(reason="No active task hold covers the task instance.")
            return

        for hold in holds:
            yield self._failing_status(reason=f"Task hold {hold.id} is active: {hold.reason}")
