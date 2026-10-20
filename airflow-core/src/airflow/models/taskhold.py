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

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, Text, and_, or_
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement


class TaskHold(Base):
    """
    An operator-placed hold that stops matching task instances from being queued.

    A hold matches task instances by coordinates, never by task instance id, so it keeps applying
    across retries and clears. ``task_id`` set and ``run_id`` unset holds that task in every current
    and future run; ``run_id`` set and ``task_id`` unset holds the whole run; both set hold one task
    instance (all map indexes). The scheduler moves matching ``scheduled`` task instances to ``held``
    just before queueing, and moves ``held`` task instances back to ``scheduled`` once no active hold
    matches them.
    """

    __tablename__ = "task_hold"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dag_id: Mapped[str] = mapped_column(
        StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    run_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    released_by: Mapped[str | None] = mapped_column(String(512), nullable=True)
    released_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    release_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_task_hold_released_at_dag_id", released_at, dag_id),
        CheckConstraint("task_id IS NOT NULL OR run_id IS NOT NULL", name="task_hold_scope_check"),
    )

    def __repr__(self) -> str:
        return f"<TaskHold {self.id} dag_id={self.dag_id} task_id={self.task_id} run_id={self.run_id}>"

    @classmethod
    def active_clause(cls, now: datetime) -> ColumnElement[bool]:
        """Return a SQL clause selecting holds that are neither released nor expired at ``now``."""
        return and_(
            cls.released_at.is_(None),
            or_(cls.expires_at.is_(None), cls.expires_at > now),
        )

    def is_active(self, now: datetime) -> bool:
        """Whether this hold is neither released nor expired at ``now``."""
        return self.released_at is None and (self.expires_at is None or self.expires_at > now)

    def matches(self, *, dag_id: str, task_id: str, run_id: str) -> bool:
        """Whether this hold covers the task instance with the given coordinates."""
        return (
            self.dag_id == dag_id
            and (self.task_id is None or self.task_id == task_id)
            and (self.run_id is None or self.run_id == run_id)
        )
