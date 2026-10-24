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

import json
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Query, status
from pydantic import NonNegativeInt
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.task_holds import (
    TaskHoldBody,
    TaskHoldCollectionResponse,
    TaskHoldReleaseBody,
    TaskHoldResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, ReadableTaskHoldsFilterDep, requires_access_dag
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.models.taskhold import TaskHold

if TYPE_CHECKING:
    from datetime import datetime

task_holds_router = AirflowRouter(tags=["Task Hold"], prefix="/dags/{dag_id}/holds")


def _to_response(hold: TaskHold, now: datetime) -> TaskHoldResponse:
    return TaskHoldResponse(
        **{name: getattr(hold, name) for name in TaskHoldResponse.model_fields if name != "active"},
        active=hold.is_active(now),
    )


@task_holds_router.get(
    "",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def list_task_holds(
    dag_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "created_at", "expires_at", "released_at"], TaskHold).dynamic_depends()),
    ],
    readable_task_holds_filter: ReadableTaskHoldsFilterDep,
    session: SessionDep,
    active: Annotated[
        bool,
        Query(
            description="Only return holds that are neither released nor expired. "
            "Pass ``false`` to include released and expired holds as well."
        ),
    ] = True,
) -> TaskHoldCollectionResponse:
    """
    List the task holds of a Dag.

    This endpoint allows specifying `~` as the dag_id to retrieve task holds for all Dags.
    """
    now = timezone.utcnow()
    statement = select(TaskHold)
    if dag_id != "~":
        statement = statement.where(TaskHold.dag_id == dag_id)
    if active:
        statement = statement.where(TaskHold.active_clause(now))

    select_stmt, total_entries = paginated_select(
        statement=statement,
        filters=[readable_task_holds_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    return TaskHoldCollectionResponse(
        task_holds=[_to_response(hold, now) for hold in session.scalars(select_stmt)],
        total_entries=total_entries,
    )


@task_holds_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE)),
        Depends(action_logging(event="hold_created")),
    ],
)
def create_task_hold(
    dag_id: str,
    body: TaskHoldBody,
    user: GetUserDep,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> TaskHoldResponse:
    """
    Place a hold on a task or a run.

    A hold is a lock, not a task instance state: it never changes the state of a task instance, it only
    stops a new start. While it is active the scheduler does not schedule a task instance that has not
    started, which waits in `none`, and does not queue one that is `scheduled`.
    A hold on a task covers every map index of it in every current and future run, a hold on a run covers
    every task in that run, and a hold on both covers that one task instance.
    The current attempt of a queued or running task instance continues, and later attempts, such as a
    retry or a clear, are held. A retry, a sensor reschedule and a task resuming after a trigger or a human
    answer reach `scheduled` as usual and wait there, except that a retry or reschedule of a task that
    starts from its trigger (`start_from_trigger`) waits in its state. A task instance that is already
    `scheduled` keeps the decision it has. This endpoint changes no task instance.
    """
    now = timezone.utcnow()
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)

    if body.run_id is not None and (
        session.scalar(select(DagRun.id).where(DagRun.dag_id == dag_id, DagRun.run_id == body.run_id)) is None
    ):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Dag Run with dag_id: `{dag_id}` and run_id: `{body.run_id}` was not found",
        )
    if body.task_id is not None and not dag.has_task(body.task_id):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Task with ID: `{body.task_id}` not found in the latest version of Dag: `{dag_id}`",
        )
    if body.expires_at is not None and body.expires_at <= now:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "expires_at must be in the future")

    hold = TaskHold(
        dag_id=dag_id,
        task_id=body.task_id,
        run_id=body.run_id,
        reason=body.reason,
        created_by=user.get_name(),
        expires_at=body.expires_at,
    )
    session.add(hold)
    session.flush()
    return _to_response(hold, now)


@task_holds_router.post(
    "/{hold_id}/release",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def release_task_hold(
    dag_id: str,
    hold_id: NonNegativeInt,
    user: GetUserDep,
    session: SessionDep,
    body: TaskHoldReleaseBody | None = None,
) -> TaskHoldResponse:
    """
    Release a task hold.

    The scheduler schedules and queues the task instances that no active hold covers any more on its
    next loops. The release itself changes no task instance. The request body is optional.
    """
    hold = session.scalar(
        select(TaskHold).where(TaskHold.id == hold_id, TaskHold.dag_id == dag_id).with_for_update()
    )
    if hold is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Hold with id: `{hold_id}` was not found for Dag: `{dag_id}`",
        )

    now = timezone.utcnow()
    if not hold.is_active(now):
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"The Task Hold with id: `{hold_id}` is already released or expired"
        )

    hold.released_by = user.get_name()
    hold.released_at = now
    hold.release_reason = body.reason if body else None
    # Logged here rather than by ``action_logging``: the hold's scope is not in the request, and the row
    # must carry it like the ``hold_expired`` row the scheduler writes.
    session.add(
        Log(
            event="hold_released",
            dag_id=dag_id,
            task_id=hold.task_id,
            run_id=hold.run_id,
            owner=hold.released_by,
            owner_display_name=user.get_display_name(),
            extra=json.dumps({"hold_id": str(hold.id), "reason": hold.release_reason}),
        )
    )
    return _to_response(hold, now)
