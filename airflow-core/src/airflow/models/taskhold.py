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

from sqlalchemy import (
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    and_,
    exists,
    literal,
    or_,
    select,
    union_all,
)
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import ColumnElement, Select


class TaskHold(Base):
    """
    An operator-placed lock that stops matching task instances from starting.

    A hold is a lock, not a task state: it never changes the state of a task instance it covers. A hold
    matches task instances by coordinates, never by task instance id, so it keeps applying across retries
    and clears. It always names a task. A *task instance hold* also names a run and holds that task in
    that run only; a *task hold* has no ``run_id`` and holds the task in every current and future run.
    Either covers every map index of the task. A hold is *active* until it is released or expires.

    The scheduler reads the table at two gates. The ``TaskHoldDep`` dependency keeps a covered task
    instance that has not started in ``none``, and the statement that moves a ``scheduled`` task instance
    to ``queued`` refuses a covered one. Any other task instance that is about to start again, such as a
    retry, a reschedule or one resuming from a trigger, reaches ``scheduled`` as usual and waits there,
    except a ``start_from_trigger`` retry or reschedule: scheduling it skips ``scheduled``, so the
    dependency keeps it where it is. The attempt of a task instance that is already queued or running
    continues; later attempts are held.
    """

    __tablename__ = "task_hold"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dag_id: Mapped[str] = mapped_column(
        StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    run_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    released_by: Mapped[str | None] = mapped_column(String(512), nullable=True)
    released_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    release_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # The scheduler's checks match every column of this index exactly (``released_at IS NULL`` for an
        # active hold), so they cost the same however many holds a Dag has. Leading with ``released_at``
        # keeps the holds that ended out of everything the scheduler reads.
        Index("idx_task_hold_released_at_dag_id_run_id_task_id", released_at, dag_id, run_id, task_id),
        # The holds that have not ended, across Dags: expiry, the scheduler's per-loop list of the Dags
        # that have a hold and the Dashboard's counts per Dag, which read ``dag_id`` from the index alone.
        # ``airflow db clean`` finds the holds that ended before a date by its first column.
        Index("idx_task_hold_released_at_expires_at_dag_id", released_at, expires_at, dag_id),
        # A Dag's whole hold history, and deleting the Dag, read only that Dag's holds. MySQL also needs
        # an index that starts with ``dag_id`` for the foreign key.
        Index("idx_task_hold_dag_id", dag_id),
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
        Return a clause that is true for the task instances an active hold covers at ``now``.

        The clause is correlated with the ``task_instance`` table of the statement it is used in, so
        ``~TaskHold.covering_clause(now)`` in the ``WHERE`` of a ``SELECT`` or an ``UPDATE`` leaves out the
        covered rows, checked by the same statement that reads or writes them. It is the SQL counterpart
        of :meth:`matches`: a hold without a ``run_id`` matches its task in every run.

        Each kind of hold is looked up exactly: a task instance hold by its task in its run, and a task
        hold by its task with no run. The two lookups are one ``EXISTS`` over their ``UNION ALL``, so the
        check is two probes of ``idx_task_hold_released_at_dag_id_run_id_task_id`` per task instance,
        however many holds there are, how they are spread over Dags, and how many have ended. One
        ``EXISTS`` with ``run_id IS NULL OR run_id = ...`` would instead compare every active hold of the
        task.
        """
        from airflow.models.taskinstance import TaskInstance

        def covered_by(run_match: ColumnElement[bool]) -> Select[tuple[int]]:
            return (
                select(literal(1))
                .where(
                    cls.dag_id == TaskInstance.dag_id,
                    run_match,
                    cls.task_id == TaskInstance.task_id,
                    cls.active_clause(now),
                )
                .correlate(TaskInstance)
            )

        return exists(
            union_all(
                covered_by(cls.run_id == TaskInstance.run_id),
                covered_by(cls.run_id.is_(None)),
            )
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
            and self.task_id == task_id
            and (self.run_id is None or self.run_id == run_id)
        )
