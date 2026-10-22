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

from datetime import datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, OperationalError

from airflow._shared.timezones import timezone
from airflow.api.common.mark_tasks import set_dag_run_state_to_failed, set_state
from airflow.models.taskhold import TaskHold
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState

from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

NOW = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)

# (id, released_at, expires_at, whether the hold is active at NOW)
ACTIVITY = [
    ("open_ended", None, None, True),
    ("not_yet_expired", None, NOW + HOUR, True),
    ("expired", None, NOW - HOUR, False),
    ("expiring_now", None, NOW, False),
    ("released", NOW - HOUR, None, False),
    ("released_before_expiry", NOW - HOUR, NOW + HOUR, False),
]


@pytest.mark.parametrize(
    ("released_at", "expires_at", "expected"),
    [
        pytest.param(released_at, expires_at, expected, id=name)
        for name, released_at, expires_at, expected in ACTIVITY
    ],
)
def test_is_active(released_at, expires_at, expected):
    hold = TaskHold(
        dag_id="dag", task_id="task", reason="test", released_at=released_at, expires_at=expires_at
    )

    assert hold.is_active(NOW) is expected


def test_active_clause_selects_the_holds_that_are_active(dag_maker, session):
    with dag_maker("task_hold_active_clause", schedule=None, session=session):
        EmptyOperator(task_id="task")
    session.flush()
    expected_active = set()
    for name, released_at, expires_at, expected in ACTIVITY:
        session.add(
            TaskHold(
                dag_id="task_hold_active_clause",
                task_id=name,
                reason="test",
                released_at=released_at,
                expires_at=expires_at,
            )
        )
        if expected:
            expected_active.add(name)
    session.flush()

    active = set(session.scalars(select(TaskHold.task_id).where(TaskHold.active_clause(NOW))))

    assert active == expected_active


@pytest.mark.parametrize(
    ("hold_task_id", "hold_run_id", "dag_id", "task_id", "run_id", "expected"),
    [
        pytest.param("t1", None, "dag", "t1", "r1", True, id="task_hold_matches_its_task"),
        pytest.param("t1", None, "dag", "t1", "r2", True, id="task_hold_matches_every_run"),
        pytest.param("t1", None, "dag", "t2", "r1", False, id="task_hold_ignores_other_tasks"),
        pytest.param("t1", None, "other", "t1", "r1", False, id="task_hold_ignores_other_dags"),
        pytest.param(None, "r1", "dag", "t1", "r1", True, id="run_hold_matches_its_run"),
        pytest.param(None, "r1", "dag", "t2", "r1", True, id="run_hold_matches_every_task"),
        pytest.param(None, "r1", "dag", "t1", "r2", False, id="run_hold_ignores_other_runs"),
        pytest.param(None, "r1", "other", "t1", "r1", False, id="run_hold_ignores_other_dags"),
        pytest.param("t1", "r1", "dag", "t1", "r1", True, id="task_in_run_hold_matches_its_task_instance"),
        pytest.param("t1", "r1", "dag", "t2", "r1", False, id="task_in_run_hold_ignores_other_tasks"),
        pytest.param("t1", "r1", "dag", "t1", "r2", False, id="task_in_run_hold_ignores_other_runs"),
    ],
)
def test_matches(hold_task_id, hold_run_id, dag_id, task_id, run_id, expected):
    hold = TaskHold(dag_id="dag", task_id=hold_task_id, run_id=hold_run_id, reason="test")

    assert hold.matches(dag_id=dag_id, task_id=task_id, run_id=run_id) is expected


def test_hold_must_name_a_task_or_a_run(dag_maker, session):
    with dag_maker("task_hold_scope_check", schedule=None, session=session):
        EmptyOperator(task_id="task")
    session.flush()
    session.add(TaskHold(dag_id="task_hold_scope_check", reason="matches nothing in particular"))

    # MySQL reports CHECK violations as OperationalError (3819) rather than IntegrityError.
    with pytest.raises((IntegrityError, OperationalError)):
        session.flush()
    session.rollback()


def _two_dags_with_two_runs_and_two_tasks(dag_maker, session) -> set[tuple[str, str, str]]:
    """Create ``dag`` and ``other`` with runs ``r1`` and ``r2`` of tasks ``t1`` and ``t2``; return their keys."""
    for dag_id in ("dag", "other"):
        with dag_maker(dag_id, schedule=None, session=session):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
        for offset, run_id in enumerate(("r1", "r2")):
            dag_maker.create_dagrun(
                run_id=run_id, logical_date=DEFAULT_DATE + timedelta(days=offset), session=session
            )
    session.flush()
    return {
        (dag_id, run_id, task_id)
        for dag_id in ("dag", "other")
        for run_id in ("r1", "r2")
        for task_id in ("t1", "t2")
    }


@pytest.mark.parametrize(
    ("hold_task_id", "hold_run_id"),
    [
        pytest.param("t1", None, id="task_hold"),
        pytest.param(None, "r1", id="run_hold"),
        pytest.param("t1", "r1", id="task_in_run_hold"),
    ],
)
@pytest.mark.parametrize(
    ("released_at", "expires_at", "active"),
    [
        pytest.param(None, None, True, id="open_ended"),
        pytest.param(None, NOW - HOUR, False, id="expired"),
        pytest.param(NOW - HOUR, None, False, id="released"),
    ],
)
def test_covering_clause_selects_the_task_instances_matches_accepts(
    dag_maker, session, hold_task_id, hold_run_id, released_at, expires_at, active
):
    """The SQL clause and the Python rule are the same rule: an active hold covers what ``matches`` says."""
    keys = _two_dags_with_two_runs_and_two_tasks(dag_maker, session)
    hold = TaskHold(
        dag_id="dag",
        task_id=hold_task_id,
        run_id=hold_run_id,
        reason="test",
        released_at=released_at,
        expires_at=expires_at,
    )
    session.add(hold)
    session.flush()

    covered = set(
        session.execute(
            select(TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.task_id).where(
                TaskHold.covering_clause(NOW)
            )
        )
    )

    assert covered == {
        (dag_id, run_id, task_id)
        for dag_id, run_id, task_id in keys
        if active and hold.matches(dag_id=dag_id, task_id=task_id, run_id=run_id)
    }
    # Every scope covers at least this task instance, so an empty result is not a vacuous pass.
    assert (("dag", "r1", "t1") in covered) is active


def test_covering_clause_covers_a_task_instance_until_every_hold_that_matches_it_is_over(dag_maker, session):
    _two_dags_with_two_runs_and_two_tasks(dag_maker, session)
    task_hold = TaskHold(dag_id="dag", task_id="t1", reason="task")
    run_hold = TaskHold(dag_id="dag", run_id="r1", reason="run")
    session.add_all([task_hold, run_hold])
    session.flush()

    def covered() -> set[tuple[str, str]]:
        return set(
            session.execute(
                select(TaskInstance.run_id, TaskInstance.task_id).where(
                    TaskInstance.dag_id == "dag", TaskHold.covering_clause(NOW)
                )
            )
        )

    assert covered() == {("r1", "t1"), ("r2", "t1"), ("r1", "t2")}

    task_hold.released_at = NOW - HOUR
    session.flush()
    # (r1, t1) is still covered by the run hold; (r2, t1) was only covered by the task hold.
    assert covered() == {("r1", "t1"), ("r1", "t2")}

    run_hold.expires_at = NOW - HOUR
    session.flush()
    assert covered() == set()


def test_active_for_run_loads_only_the_active_holds_that_can_cover_the_run(dag_maker, session):
    _two_dags_with_two_runs_and_two_tasks(dag_maker, session)
    holds = {
        "task": TaskHold(dag_id="dag", task_id="t1", reason="task"),
        "this_run": TaskHold(dag_id="dag", run_id="r1", reason="this run"),
        "task_in_this_run": TaskHold(dag_id="dag", task_id="t2", run_id="r1", reason="task in this run"),
        "other_run": TaskHold(dag_id="dag", run_id="r2", reason="other run"),
        "other_dag": TaskHold(dag_id="other", task_id="t1", reason="other dag"),
        "released": TaskHold(dag_id="dag", task_id="t1", reason="released", released_at=NOW - HOUR),
        "expired": TaskHold(dag_id="dag", task_id="t1", reason="expired", expires_at=NOW - HOUR),
    }
    session.add_all(holds.values())
    session.flush()

    loaded = TaskHold.active_for_run("dag", "r1", now=NOW, session=session)

    assert [hold.id for hold in loaded] == sorted(
        holds[name].id for name in ("task", "this_run", "task_in_this_run")
    )


def test_a_cleared_task_instance_is_covered_by_the_hold_that_covered_its_earlier_attempt(dag_maker, session):
    with dag_maker("task_hold_clear", schedule=None, session=session):
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun(session=session)
    ti = dr.task_instances[0]
    ti.state = TaskInstanceState.FAILED
    ti.try_number = 1
    session.add(TaskHold(dag_id=dr.dag_id, task_id="task", reason="test"))
    session.flush()
    old_id = ti.id

    clear_task_instances([ti], session)
    session.flush()

    covered_ids = set(session.scalars(select(TaskInstance.id).where(TaskHold.covering_clause(NOW))))
    live = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.run_id == dr.run_id,
            TaskInstance.working_set.is_(True),
        )
    ).one()
    # The new attempt has an id of its own and no state; the hold goes by coordinates, so it covers it.
    assert live.id != old_id
    assert live.state is None
    assert live.id in covered_ids


@pytest.mark.parametrize(
    "state", [TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.SKIPPED]
)
def test_a_task_instance_a_hold_covers_can_be_marked(dag_maker, session, state):
    with dag_maker("task_hold_mark", schedule=None, session=session) as dag:
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun(session=session)
    dr.task_instances[0].state = TaskInstanceState.SCHEDULED
    hold = TaskHold(dag_id=dr.dag_id, task_id="task", reason="test")
    session.add(hold)
    session.flush()
    task = dag.get_task("task")
    task.dag = dag

    altered = set_state(tasks=[task], run_id=dr.run_id, state=state, commit=True, session=session)

    assert [ti.task_id for ti in altered] == ["task"]
    assert altered[0].state == state
    assert hold.is_active(timezone.utcnow())


def test_marking_a_run_failed_skips_the_unfinished_task_instances_a_hold_covers(dag_maker, session):
    with dag_maker("task_hold_mark_run_failed", schedule=None, session=session) as dag:
        EmptyOperator(task_id="held")
        EmptyOperator(task_id="running")
        EmptyOperator(task_id="finished")
    dr = dag_maker.create_dagrun(session=session)
    states = {
        "held": TaskInstanceState.SCHEDULED,
        "running": TaskInstanceState.RUNNING,
        "finished": TaskInstanceState.SUCCESS,
    }
    for ti in dr.task_instances:
        ti.state = states[ti.task_id]
    session.add(TaskHold(dag_id=dr.dag_id, run_id=dr.run_id, reason="test"))
    session.flush()

    set_dag_run_state_to_failed(dag=dag, run_id=dr.run_id, commit=True, session=session)
    session.flush()

    assert dr.state == DagRunState.FAILED
    final_states = {
        task_id: state
        for task_id, state in session.execute(
            select(TaskInstance.task_id, TaskInstance.state).where(
                TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id
            )
        )
    }
    assert final_states == {
        "held": TaskInstanceState.SKIPPED,
        "running": TaskInstanceState.FAILED,
        "finished": TaskInstanceState.SUCCESS,
    }
