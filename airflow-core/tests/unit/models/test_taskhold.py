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
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.api.common.mark_tasks import set_dag_run_state_to_failed, set_state
from airflow.models.taskhold import TaskHold
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState

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

    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


def test_clearing_a_held_task_instance_resets_it_to_none(dag_maker, session):
    with dag_maker("task_hold_clear", schedule=None, session=session):
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun(session=session)
    ti = dr.task_instances[0]
    ti.state = TaskInstanceState.HELD
    session.flush()

    clear_task_instances([ti], session)
    session.flush()

    state = session.scalar(
        select(TaskInstance.state).where(
            TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id, TaskInstance.task_id == "task"
        )
    )
    assert state is None


@pytest.mark.parametrize(
    "state", [TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.SKIPPED]
)
def test_a_held_task_instance_can_be_marked(dag_maker, session, state):
    with dag_maker("task_hold_mark", schedule=None, session=session) as dag:
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun(session=session)
    dr.task_instances[0].state = TaskInstanceState.HELD
    session.flush()
    task = dag.get_task("task")
    task.dag = dag

    altered = set_state(tasks=[task], run_id=dr.run_id, state=state, commit=True, session=session)

    assert [ti.task_id for ti in altered] == ["task"]
    assert altered[0].state == state


def test_marking_a_run_failed_skips_its_held_task_instances(dag_maker, session):
    with dag_maker("task_hold_mark_run_failed", schedule=None, session=session) as dag:
        EmptyOperator(task_id="held")
        EmptyOperator(task_id="running")
        EmptyOperator(task_id="finished")
    dr = dag_maker.create_dagrun(session=session)
    states = {
        "held": TaskInstanceState.HELD,
        "running": TaskInstanceState.RUNNING,
        "finished": TaskInstanceState.SUCCESS,
    }
    for ti in dr.task_instances:
        ti.state = states[ti.task_id]
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
