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

from datetime import timedelta

import attrs
import pytest

from airflow._shared.timezones import timezone
from airflow.models.taskhold import TaskHold
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import BaseOperator
from airflow.serialization.definitions.baseoperator import DEFAULT_OPERATOR_DEPS
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.task_hold_dep import TaskHoldDep
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

HOUR = timedelta(hours=1)


@pytest.fixture
def task_instances(dag_maker, session):
    """
    Task instances of two Dags, as ``{(dag_id, run_id, task_id, map_index): task_instance}``.

    ``dag`` and ``other`` both have runs ``r1`` and ``r2`` of tasks ``t1`` and ``t2``; ``dag`` also has a task
    ``mapped`` expanded into three task instances.
    """
    result = {}
    for dag_id in ("dag", "other"):
        with dag_maker(dag_id, schedule=None, session=session):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
            if dag_id == "dag":
                BashOperator.partial(task_id="mapped", bash_command="true").expand(
                    env=[{"n": str(n)} for n in range(3)]
                )
        serialized_dag = dag_maker.serialized_dag
        for offset, run_id in enumerate(("r1", "r2")):
            dr = dag_maker.create_dagrun(
                run_id=run_id, logical_date=DEFAULT_DATE + timedelta(days=offset), session=session
            )
            for ti in dr.task_instances:
                ti.task = serialized_dag.get_task(ti.task_id)
                result[(dag_id, run_id, ti.task_id, ti.map_index)] = ti
    session.flush()
    return result


class StartsFromTrigger(BaseOperator):
    """An operator the scheduler sends straight to the triggerer, past ``scheduled``."""

    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        timeout=None,
    )
    start_from_trigger = True

    def execute_complete(self):
        pass


@pytest.fixture
def start_from_trigger_ti(dag_maker, session):
    """The task instance of a ``start_from_trigger`` task, with a hold on its task."""
    with dag_maker("trigger_dag", schedule=None, session=session):
        StartsFromTrigger(task_id="trigger_task")
    serialized_dag = dag_maker.serialized_dag
    ti = dag_maker.create_dagrun(run_id="r1", session=session).task_instances[0]
    ti.task = serialized_dag.get_task("trigger_task")
    session.add(TaskHold(dag_id="trigger_dag", task_id="trigger_task", reason="test"))
    session.flush()
    return ti


def failing_hold_ids(ti, session, **context_args) -> list[int]:
    """Return the ids of the holds the dependency fails ``ti`` for; none if it passes."""
    statuses = TaskHoldDep().get_dep_statuses(ti, DepContext(**context_args), session=session)
    # The reason of a failing status reads "Task hold <id> is active: <reason>".
    return [int(status.reason.split()[2]) for status in statuses if not status.passed]


def covered_keys(task_instances, session, **context_args) -> set[tuple[str, str, str, int]]:
    """Return the keys of the task instances the dependency fails, evaluated with one shared context."""
    context = DepContext(**context_args)
    return {
        key for key, ti in task_instances.items() if not TaskHoldDep().is_met(ti, context, session=session)
    }


class TestTaskHoldDep:
    def test_is_among_the_default_dependencies_of_every_operator(self, task_instances):
        assert TaskHoldDep() in DEFAULT_OPERATOR_DEPS
        for ti in task_instances.values():
            assert TaskHoldDep() in ti.task.deps

    def test_passes_when_no_hold_exists(self, task_instances, session):
        assert covered_keys(task_instances, session) == set()
        statuses = list(
            TaskHoldDep().get_dep_statuses(
                task_instances[("dag", "r1", "t1", -1)], DepContext(), session=session
            )
        )
        assert [status.passed for status in statuses] == [True]

    def test_task_hold_covers_the_task_in_every_run_of_its_dag(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", task_id="t1", reason="test"))
        session.flush()

        assert covered_keys(task_instances, session) == {("dag", "r1", "t1", -1), ("dag", "r2", "t1", -1)}

    def test_run_hold_covers_every_task_of_its_run(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", run_id="r1", reason="test"))
        session.flush()

        assert covered_keys(task_instances, session) == {
            ("dag", "r1", "t1", -1),
            ("dag", "r1", "t2", -1),
            ("dag", "r1", "mapped", 0),
            ("dag", "r1", "mapped", 1),
            ("dag", "r1", "mapped", 2),
        }

    def test_task_in_run_hold_covers_the_one_task_instance(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", task_id="t2", run_id="r2", reason="test"))
        session.flush()

        assert covered_keys(task_instances, session) == {("dag", "r2", "t2", -1)}

    @pytest.mark.parametrize("with_run_id", [False, True], ids=["task_hold", "task_in_run_hold"])
    def test_a_hold_covers_every_map_index_of_a_mapped_task(self, task_instances, session, with_run_id):
        session.add(
            TaskHold(dag_id="dag", task_id="mapped", run_id="r1" if with_run_id else None, reason="test")
        )
        session.flush()

        expected_runs = ["r1"] if with_run_id else ["r1", "r2"]
        assert covered_keys(task_instances, session) == {
            ("dag", run_id, "mapped", map_index) for run_id in expected_runs for map_index in range(3)
        }

    def test_a_hold_never_covers_another_dag_with_the_same_ids(self, task_instances, session):
        session.add(TaskHold(dag_id="other", task_id="t1", reason="test"))
        session.flush()

        covered = covered_keys(task_instances, session)

        assert covered == {("other", "r1", "t1", -1), ("other", "r2", "t1", -1)}

    @pytest.mark.parametrize(
        ("released_at", "expires_at"),
        [
            pytest.param(timezone.utcnow() - HOUR, None, id="released"),
            pytest.param(None, timezone.utcnow() - HOUR, id="expired"),
        ],
    )
    def test_a_released_or_expired_hold_covers_nothing(
        self, task_instances, session, released_at, expires_at
    ):
        session.add(
            TaskHold(
                dag_id="dag", task_id="t1", reason="test", released_at=released_at, expires_at=expires_at
            )
        )
        session.flush()

        assert covered_keys(task_instances, session) == set()

    def test_a_hold_that_has_not_expired_yet_covers(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", run_id="r2", reason="test", expires_at=timezone.utcnow() + HOUR))
        session.flush()

        assert {key[:2] for key in covered_keys(task_instances, session)} == {("dag", "r2")}

    @pytest.mark.parametrize(
        "state",
        [
            pytest.param(None, id="none"),
            *(pytest.param(state, id=state.value) for state in TaskInstanceState),
        ],
    )
    def test_a_hold_keeps_only_a_task_instance_that_has_not_started(self, task_instances, session, state):
        """A hold only delays a start: a task instance in any other state is left to the scheduler's decisions."""
        session.add(TaskHold(dag_id="dag", task_id="t1", reason="test"))
        ti = task_instances[("dag", "r1", "t1", -1)]
        ti.state = state
        session.flush()

        assert TaskHoldDep().is_met(ti, DepContext(), session=session) is (state is not None)

    def test_a_task_instance_that_has_started_does_not_read_the_hold_table(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", run_id="r1", reason="test"))
        ti = task_instances[("dag", "r1", "t1", -1)]
        ti.state = TaskInstanceState.UP_FOR_RETRY
        session.flush()

        with assert_queries_count(0, session=session):
            assert TaskHoldDep().is_met(ti, DepContext(), session=session)

    @pytest.mark.parametrize(
        "state",
        [
            pytest.param(None, id="none"),
            *(pytest.param(state, id=state.value) for state in TaskInstanceState),
        ],
    )
    def test_a_hold_keeps_a_start_from_trigger_task_instance_wherever_scheduling_would_defer_it(
        self, start_from_trigger_ti, session, state
    ):
        """Scheduling such a task instance sends it straight to the triggerer, past ``scheduled``."""
        start_from_trigger_ti.state = state
        session.flush()

        kept_in_place = state in (None, TaskInstanceState.UP_FOR_RETRY, TaskInstanceState.UP_FOR_RESCHEDULE)
        assert TaskHoldDep().is_met(start_from_trigger_ti, DepContext(), session=session) is not kept_in_place

    def test_every_covering_hold_fails_the_dependency_until_the_last_one_is_over(
        self, task_instances, session
    ):
        ti = task_instances[("dag", "r1", "t1", -1)]
        task_hold = TaskHold(dag_id="dag", task_id="t1", reason="the task")
        run_hold = TaskHold(dag_id="dag", run_id="r1", reason="the run")
        unrelated = TaskHold(dag_id="dag", task_id="t2", reason="another task")
        session.add_all([task_hold, run_hold, unrelated])
        session.flush()

        assert failing_hold_ids(ti, session) == [task_hold.id, run_hold.id]

        task_hold.released_at = timezone.utcnow()
        session.flush()
        assert failing_hold_ids(ti, session) == [run_hold.id]

        run_hold.expires_at = timezone.utcnow() - HOUR
        session.flush()
        assert failing_hold_ids(ti, session) == []

    def test_the_failing_status_names_the_hold_and_its_reason(self, task_instances, session):
        hold = TaskHold(dag_id="dag", task_id="t1", reason="incident INC-42")
        session.add(hold)
        session.flush()

        [status] = TaskHoldDep().get_dep_statuses(
            task_instances[("dag", "r1", "t1", -1)], DepContext(), session=session
        )

        assert (status.dep_name, status.passed) == ("Task Hold", False)
        assert status.reason == f"Task hold {hold.id} is active: incident INC-42"

    def test_a_context_can_ignore_task_holds(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", run_id="r1", reason="test"))
        session.flush()
        ti = task_instances[("dag", "r1", "t1", -1)]

        assert failing_hold_ids(ti, session)
        assert failing_hold_ids(ti, session, ignore_task_holds=True) == []

    def test_a_context_that_ignores_all_dependencies_passes(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", run_id="r1", reason="test"))
        session.flush()

        assert failing_hold_ids(task_instances[("dag", "r1", "t1", -1)], session, ignore_all_deps=True) == []

    def test_an_ignoring_context_does_not_read_the_hold_table(self, task_instances, session):
        session.add(TaskHold(dag_id="dag", run_id="r1", reason="test"))
        session.flush()
        context = DepContext(ignore_task_holds=True)

        with assert_queries_count(0, session=session):
            for ti in task_instances.values():
                assert TaskHoldDep().is_met(ti, context, session=session)

    def test_the_hold_table_is_read_once_per_dag_run(self, task_instances, session):
        session.add_all(
            [
                TaskHold(dag_id="dag", task_id="t1", reason="test"),
                TaskHold(dag_id="dag", run_id="r2", reason="test"),
            ]
        )
        session.flush()
        context = DepContext()
        runs = {(dag_id, run_id) for dag_id, run_id, _, _ in task_instances}

        # Every task instance of the Dag runs is evaluated, and the table is read once for each Dag run.
        with assert_queries_count(len(runs), session=session):
            for ti in task_instances.values():
                TaskHoldDep().is_met(ti, context, session=session)

        # A second evaluation with the same context reads nothing.
        with assert_queries_count(0, session=session):
            for ti in task_instances.values():
                TaskHoldDep().is_met(ti, context, session=session)

    def test_a_copy_of_the_context_reads_the_memo_of_the_original(self, task_instances, session):
        """``are_dependencies_met`` copies the context for a rescheduling task instance; the copy must not re-read."""
        session.add(TaskHold(dag_id="dag", task_id="t1", reason="test"))
        session.flush()
        ti = task_instances[("dag", "r1", "t1", -1)]
        context = DepContext()
        assert not TaskHoldDep().is_met(ti, context, session=session)

        copy = attrs.evolve(context, deps=set())

        with assert_queries_count(0, session=session):
            assert not TaskHoldDep().is_met(ti, copy, session=session)

    def test_a_covered_task_instance_does_not_have_its_dependencies_met(self, task_instances, session):
        """The dependency is among the ones ``are_dependencies_met`` evaluates for every task."""
        session.add(TaskHold(dag_id="dag", task_id="t1", reason="test"))
        session.flush()
        covered = task_instances[("dag", "r1", "t1", -1)]
        free = task_instances[("dag", "r1", "t2", -1)]

        assert not covered.are_dependencies_met(dep_context=DepContext(), session=session)
        assert [
            status.dep_name for status in covered.get_failed_dep_statuses(DepContext(), session=session)
        ] == ["Task Hold"]
        assert free.are_dependencies_met(dep_context=DepContext(), session=session)
        # Holds are the only thing that stops it, so a context that ignores them lets the task instance through.
        assert covered.are_dependencies_met(dep_context=DepContext(ignore_task_holds=True), session=session)
