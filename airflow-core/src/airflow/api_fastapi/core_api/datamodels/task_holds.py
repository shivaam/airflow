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

from datetime import datetime
from typing import Annotated

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.models.base import ID_LEN


class TaskHoldBody(StrictBaseModel):
    """Object used for create task hold request."""

    task_id: Annotated[str, StringConstraints(min_length=1, max_length=ID_LEN)] | None = Field(
        default=None,
        description="Hold this task in every current and future run. "
        "Combined with ``run_id``, holds only that task in that run.",
    )
    run_id: Annotated[str, StringConstraints(min_length=1, max_length=ID_LEN)] | None = Field(
        default=None,
        description="Hold every task in this run. "
        "Combined with ``task_id``, holds only that task in that run.",
    )
    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    expires_at: AwareDatetime | None = Field(
        default=None,
        description="When the hold stops applying. The hold lasts until it is released if not set.",
    )

    @model_validator(mode="after")
    def validate_scope(self):
        if self.task_id is None and self.run_id is None:
            raise ValueError("At least one of task_id or run_id must be set")
        return self


class TaskHoldReleaseBody(StrictBaseModel):
    """Object used for release task hold request."""

    reason: str | None = None


class TaskHoldResponse(BaseModel):
    """Task hold serializer for responses."""

    id: int
    dag_id: str
    task_id: str | None
    run_id: str | None
    reason: str
    created_by: str | None
    created_at: datetime
    expires_at: datetime | None
    released_by: str | None
    released_at: datetime | None
    release_reason: str | None
    active: bool = Field(description="Whether the hold is neither released nor expired.")


class TaskHoldCollectionResponse(BaseModel):
    """Task hold collection serializer for responses."""

    task_holds: list[TaskHoldResponse]
    total_entries: int
