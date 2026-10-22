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

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, Text, and_, exists, or_, select
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import ColumnElement


class TaskHold(Base):
    """
    An operator-placed lock that stops matching task instances from being scheduled and queued.

    A hold is a lock, not a task state: it never changes the state of a task instance it covers. A hold
    matches task instances by coordinates, never by task instance id, so it keeps applying across retries
    and clears. ``task_id`` set and ``run_id`` unset holds that task in every current and future run;
    ``run_id`` set and ``task_id`` unset holds the whole run; both set hold one task instance (all map
    indexes). A hold is *active* until it is released or expires.

    The scheduler reads the table at two gates. The ``TaskHoldDep`` dependency keeps a covered task
    instance from moving to ``scheduled``, whatever state it waits in, and the statement that moves a
    ``scheduled`` task instance to ``queued`` refuses a covered one. The attempt of a task instance that is
    already queued or running continues; later attempts are held.
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

    @classmethod
    def covering_clause(cls, now: datetime) -> ColumnElement[bool]:
        """
        Return an ``EXISTS`` clause that is true for the task instances an active hold covers at ``now``.

        The clause is correlated with the ``task_instance`` table of the statement it is used in, so
        ``~TaskHold.covering_clause(now)`` in the ``WHERE`` of a ``SELECT`` or an ``UPDATE`` leaves out the
        covered rows, checked by the same statement that reads or writes them. It is the SQL counterpart
        of :meth:`matches`: a hold without a ``task_id`` or a ``run_id`` matches any.
        """
        from airflow.models.taskinstance import TaskInstance

        return exists().where(
            cls.dag_id == TaskInstance.dag_id,
            or_(cls.task_id.is_(None), cls.task_id == TaskInstance.task_id),
            or_(cls.run_id.is_(None), cls.run_id == TaskInstance.run_id),
            cls.active_clause(now),
        )

    @classmethod
    def active_for_run(
        cls, dag_id: str, run_id: str, *, now: datetime, session: Session
    ) -> Sequence[TaskHold]:
        """Load the holds that are active at ``now`` and can cover a task instance of the given Dag run."""
        return session.scalars(
            select(cls)
            .where(cls.dag_id == dag_id, or_(cls.run_id.is_(None), cls.run_id == run_id))
            .where(cls.active_clause(now))
            .order_by(cls.id)
        ).all()

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
