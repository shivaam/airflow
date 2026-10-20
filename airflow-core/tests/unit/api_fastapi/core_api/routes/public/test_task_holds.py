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
from datetime import datetime
from unittest import mock

import pytest
from sqlalchemy import delete, func, select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models.log import Log
from airflow.models.taskhold import TaskHold
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

DAG_ID = "hold_dag"
OTHER_DAG_ID = "hold_other_dag"
TASK_ID = "hold_task"
OTHER_TASK_ID = "hold_other_task"
RUN_ID = "hold_run"
OTHER_DAG_RUN_ID = "hold_other_dag_run"
LOGICAL_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
FUTURE = datetime(2099, 1, 1, tzinfo=timezone.utc)
PAST = datetime(2000, 1, 1, tzinfo=timezone.utc)
EXPECTED_STATES = {
    (RUN_ID, TASK_ID): TaskInstanceState.SCHEDULED,
    (RUN_ID, OTHER_TASK_ID): TaskInstanceState.RUNNING,
}


def _clean_db(session):
    session.execute(delete(TaskHold))
    session.commit()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()
    clear_db_logs()


@pytest.fixture(autouse=True)
def setup(dag_maker, session):
    _clean_db(session)
    with dag_maker(DAG_ID, schedule=None, start_date=LOGICAL_DATE, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID)
        EmptyOperator(task_id=OTHER_TASK_ID)
    dag_maker.create_dagrun(run_id=RUN_ID, logical_date=LOGICAL_DATE, session=session)
    with dag_maker(OTHER_DAG_ID, schedule=None, start_date=LOGICAL_DATE, serialized=True, session=session):
        EmptyOperator(task_id=TASK_ID)
    dag_maker.create_dagrun(run_id=OTHER_DAG_RUN_ID, logical_date=LOGICAL_DATE, session=session)
    session.commit()
    yield
    _clean_db(session)


@pytest.fixture
def dag_reader_headers(test_client):
    """A caller who may read the Dags but not edit them: viewer is below the role edits require."""
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username="reader", role="viewer"))
    )
    return {"Authorization": f"Bearer {token}"}


def _add_hold(
    session,
    *,
    dag_id=DAG_ID,
    task_id=TASK_ID,
    run_id=None,
    expires_at=None,
    released_at=None,
    released_by=None,
) -> TaskHold:
    hold = TaskHold(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        reason="Snowflake outage",
        created_by="someone",
        expires_at=expires_at,
        released_at=released_at,
        released_by=released_by,
    )
    session.add(hold)
    session.commit()
    return hold


def _task_instance_states(session) -> dict[tuple[str, str], TaskInstanceState | None]:
    session.expire_all()
    tis = session.scalars(select(TaskInstance).where(TaskInstance.dag_id == DAG_ID))
    return {(ti.run_id, ti.task_id): ti.state for ti in tis}


def _set_task_instance_states(session) -> None:
    for ti in session.scalars(select(TaskInstance).where(TaskInstance.dag_id == DAG_ID)):
        ti.state = EXPECTED_STATES[(ti.run_id, ti.task_id)]
    session.commit()


class TestCreateTaskHold:
    @pytest.mark.parametrize(
        ("scope", "expected_task_id", "expected_run_id"),
        [
            pytest.param({"task_id": TASK_ID}, TASK_ID, None, id="task"),
            pytest.param({"run_id": RUN_ID}, None, RUN_ID, id="run"),
            pytest.param({"task_id": TASK_ID, "run_id": RUN_ID}, TASK_ID, RUN_ID, id="task_instance"),
        ],
    )
    def test_should_respond_201(self, test_client, session, scope, expected_task_id, expected_run_id):
        response = test_client.post(f"/dags/{DAG_ID}/holds", json={**scope, "reason": "Snowflake outage"})

        assert response.status_code == 201
        assert response.json() == {
            "id": mock.ANY,
            "dag_id": DAG_ID,
            "task_id": expected_task_id,
            "run_id": expected_run_id,
            "reason": "Snowflake outage",
            "created_by": "test",
            "created_at": mock.ANY,
            "expires_at": None,
            "released_by": None,
            "released_at": None,
            "release_reason": None,
            "active": True,
        }
        hold = session.get(TaskHold, response.json()["id"])
        assert hold is not None
        assert (hold.dag_id, hold.task_id, hold.run_id, hold.reason, hold.created_by) == (
            DAG_ID,
            expected_task_id,
            expected_run_id,
            "Snowflake outage",
            "test",
        )
        assert hold.released_at is None

        log = session.scalar(select(Log).where(Log.event == "hold_created"))
        assert log is not None
        assert (log.dag_id, log.task_id, log.run_id, log.owner) == (
            DAG_ID,
            expected_task_id,
            expected_run_id,
            "test",
        )
        assert json.loads(log.extra)["reason"] == "Snowflake outage"

    def test_should_store_expires_at(self, test_client, session):
        response = test_client.post(
            f"/dags/{DAG_ID}/holds",
            json={"task_id": TASK_ID, "reason": "Snowflake outage", "expires_at": "2099-01-01T00:00:00Z"},
        )

        assert response.status_code == 201
        assert response.json()["expires_at"] == "2099-01-01T00:00:00Z"
        assert response.json()["active"] is True
        hold = session.get(TaskHold, response.json()["id"])
        assert hold is not None
        assert hold.expires_at == FUTURE

    def test_should_not_change_task_instance_states(self, test_client, session):
        _set_task_instance_states(session)

        response = test_client.post(f"/dags/{DAG_ID}/holds", json={"run_id": RUN_ID, "reason": "pause"})

        assert response.status_code == 201
        assert _task_instance_states(session) == EXPECTED_STATES

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param({"reason": "r"}, id="no_scope"),
            pytest.param({"task_id": TASK_ID}, id="no_reason"),
            pytest.param({"task_id": TASK_ID, "reason": ""}, id="empty_reason"),
            pytest.param({"task_id": TASK_ID, "reason": "   "}, id="blank_reason"),
            pytest.param({"task_id": "", "reason": "r"}, id="empty_task_id"),
            pytest.param({"run_id": "", "reason": "r"}, id="empty_run_id"),
            pytest.param(
                {"task_id": TASK_ID, "reason": "r", "expires_at": "2099-01-01T00:00:00"},
                id="expires_at_without_timezone",
            ),
            pytest.param({"task_id": TASK_ID, "reason": "r", "unknown": 1}, id="unknown_field"),
        ],
    )
    def test_should_respond_422(self, test_client, session, body):
        response = test_client.post(f"/dags/{DAG_ID}/holds", json=body)

        assert response.status_code == 422
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0

    def test_should_respond_400_for_unknown_task(self, test_client, session):
        response = test_client.post(f"/dags/{DAG_ID}/holds", json={"task_id": "nope", "reason": "r"})

        assert response.status_code == 400
        assert response.json()["detail"] == (
            f"Task with ID: `nope` not found in the latest version of Dag: `{DAG_ID}`"
        )
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0

    def test_should_respond_400_for_expires_at_in_the_past(self, test_client, session):
        response = test_client.post(
            f"/dags/{DAG_ID}/holds",
            json={"task_id": TASK_ID, "reason": "r", "expires_at": "2000-01-01T00:00:00Z"},
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "expires_at must be in the future"
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0

    def test_should_respond_404_for_unknown_dag(self, test_client, session):
        response = test_client.post("/dags/nope/holds", json={"task_id": TASK_ID, "reason": "r"})

        assert response.status_code == 404
        assert response.json()["detail"] == "The Dag with ID: `nope` was not found"
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0

    @pytest.mark.parametrize("run_id", ["nope", OTHER_DAG_RUN_ID], ids=["unknown_run", "run_of_other_dag"])
    def test_should_respond_404_for_run_not_in_dag(self, test_client, session, run_id):
        response = test_client.post(f"/dags/{DAG_ID}/holds", json={"run_id": run_id, "reason": "r"})

        assert response.status_code == 404
        assert response.json()["detail"] == (
            f"The Dag Run with dag_id: `{DAG_ID}` and run_id: `{run_id}` was not found"
        )
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            f"/dags/{DAG_ID}/holds", json={"task_id": TASK_ID, "reason": "r"}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, session):
        response = unauthorized_test_client.post(
            f"/dags/{DAG_ID}/holds", json={"task_id": TASK_ID, "reason": "r"}
        )

        assert response.status_code == 403
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0

    def test_should_respond_403_for_reader(self, test_client, dag_reader_headers, session):
        response = test_client.post(
            f"/dags/{DAG_ID}/holds",
            json={"task_id": TASK_ID, "reason": "r"},
            headers=dag_reader_headers,
        )

        assert response.status_code == 403
        assert session.scalar(select(func.count()).select_from(TaskHold)) == 0


class TestListTaskHolds:
    @pytest.fixture
    def holds(self, session) -> dict[str, TaskHold]:
        return {
            "task": _add_hold(session),
            "run": _add_hold(session, task_id=None, run_id=RUN_ID, expires_at=FUTURE),
            "released": _add_hold(session, task_id=OTHER_TASK_ID, released_at=PAST, released_by="alice"),
            "expired": _add_hold(session, task_id=OTHER_TASK_ID, expires_at=PAST),
            "other_dag": _add_hold(session, dag_id=OTHER_DAG_ID),
        }

    @staticmethod
    def _ids(response) -> list[int]:
        return [hold["id"] for hold in response.json()["task_holds"]]

    def test_should_return_active_holds_of_the_dag_by_default(self, test_client, holds):
        response = test_client.get(f"/dags/{DAG_ID}/holds")

        assert response.status_code == 200
        assert self._ids(response) == [holds["task"].id, holds["run"].id]
        assert response.json()["total_entries"] == 2
        assert [hold["active"] for hold in response.json()["task_holds"]] == [True, True]
        assert response.json()["task_holds"][0] == {
            "id": holds["task"].id,
            "dag_id": DAG_ID,
            "task_id": TASK_ID,
            "run_id": None,
            "reason": "Snowflake outage",
            "created_by": "someone",
            "created_at": mock.ANY,
            "expires_at": None,
            "released_by": None,
            "released_at": None,
            "release_reason": None,
            "active": True,
        }

    def test_should_include_inactive_holds_when_active_is_false(self, test_client, holds):
        response = test_client.get(f"/dags/{DAG_ID}/holds", params={"active": False})

        assert response.status_code == 200
        assert self._ids(response) == [
            holds["task"].id,
            holds["run"].id,
            holds["released"].id,
            holds["expired"].id,
        ]
        assert response.json()["total_entries"] == 4
        assert [hold["active"] for hold in response.json()["task_holds"]] == [True, True, False, False]
        released = response.json()["task_holds"][2]
        assert released["released_by"] == "alice"
        assert released["released_at"] == "2000-01-01T00:00:00Z"

    def test_should_list_holds_of_all_dags_with_tilde(self, test_client, holds):
        response = test_client.get("/dags/~/holds")

        assert response.status_code == 200
        assert self._ids(response) == [holds["task"].id, holds["run"].id, holds["other_dag"].id]
        assert response.json()["total_entries"] == 3

        response = test_client.get("/dags/~/holds", params={"active": False})

        assert response.json()["total_entries"] == 5

    def test_should_only_list_holds_of_the_requested_dag(self, test_client, holds):
        response = test_client.get(f"/dags/{OTHER_DAG_ID}/holds")

        assert response.status_code == 200
        assert self._ids(response) == [holds["other_dag"].id]

    def test_should_return_empty_collection_without_holds(self, test_client):
        response = test_client.get(f"/dags/{DAG_ID}/holds")

        assert response.status_code == 200
        assert response.json() == {"task_holds": [], "total_entries": 0}

    def test_should_paginate_and_order(self, test_client, holds):
        response = test_client.get(f"/dags/{DAG_ID}/holds", params={"limit": 1, "offset": 1})

        assert self._ids(response) == [holds["run"].id]
        assert response.json()["total_entries"] == 2

        response = test_client.get(f"/dags/{DAG_ID}/holds", params={"order_by": "-id"})

        assert self._ids(response) == [holds["run"].id, holds["task"].id]

    def test_should_only_list_holds_of_readable_dags(self, test_client, holds):
        with mock.patch.object(SimpleAuthManager, "get_authorized_dag_ids", return_value={DAG_ID}):
            response = test_client.get("/dags/~/holds", params={"active": False})

        assert response.status_code == 200
        assert {hold["dag_id"] for hold in response.json()["task_holds"]} == {DAG_ID}
        assert response.json()["total_entries"] == 4

    def test_reader_should_be_able_to_list(self, test_client, dag_reader_headers, holds):
        response = test_client.get(f"/dags/{DAG_ID}/holds", headers=dag_reader_headers)

        assert response.status_code == 200
        assert response.json()["total_entries"] == 2

    @pytest.mark.parametrize("dag_id", [DAG_ID, "~"])
    def test_should_respond_401(self, unauthenticated_test_client, dag_id):
        response = unauthenticated_test_client.get(f"/dags/{dag_id}/holds")
        assert response.status_code == 401

    @pytest.mark.parametrize("dag_id", [DAG_ID, "~"])
    def test_should_respond_403(self, unauthorized_test_client, dag_id):
        response = unauthorized_test_client.get(f"/dags/{dag_id}/holds")
        assert response.status_code == 403


class TestReleaseTaskHold:
    @staticmethod
    def _url(hold: TaskHold, dag_id: str = DAG_ID) -> str:
        return f"/dags/{dag_id}/holds/{hold.id}/release"

    def test_should_respond_200(self, test_client, session):
        hold = _add_hold(session)

        response = test_client.post(self._url(hold), json={"reason": "fix deployed"})

        assert response.status_code == 200
        assert response.json() == {
            "id": hold.id,
            "dag_id": DAG_ID,
            "task_id": TASK_ID,
            "run_id": None,
            "reason": "Snowflake outage",
            "created_by": "someone",
            "created_at": mock.ANY,
            "expires_at": None,
            "released_by": "test",
            "released_at": mock.ANY,
            "release_reason": "fix deployed",
            "active": False,
        }
        session.expire_all()
        released = session.get(TaskHold, hold.id)
        assert released is not None
        assert (released.released_by, released.release_reason) == ("test", "fix deployed")
        assert released.released_at is not None

        log = session.scalar(select(Log).where(Log.event == "hold_released"))
        assert log is not None
        assert (log.dag_id, log.task_id, log.run_id, log.owner) == (DAG_ID, TASK_ID, None, "test")
        assert json.loads(log.extra) == {"hold_id": str(hold.id), "reason": "fix deployed"}

    def test_should_log_the_run_scope_of_the_hold(self, test_client, session):
        hold = _add_hold(session, task_id=None, run_id=RUN_ID)

        response = test_client.post(self._url(hold))

        assert response.status_code == 200
        log = session.scalar(select(Log).where(Log.event == "hold_released"))
        assert log is not None
        assert (log.dag_id, log.task_id, log.run_id) == (DAG_ID, None, RUN_ID)

    @pytest.mark.parametrize(
        "kwargs",
        [pytest.param({}, id="no_body"), pytest.param({"json": {}}, id="empty_json_body")],
    )
    def test_should_accept_empty_body(self, test_client, session, kwargs):
        hold = _add_hold(session)

        response = test_client.post(self._url(hold), **kwargs)

        assert response.status_code == 200
        assert response.json()["release_reason"] is None
        assert response.json()["active"] is False
        session.expire_all()
        released = session.get(TaskHold, hold.id)
        assert released is not None
        assert released.released_by == "test"
        assert released.released_at is not None

    def test_should_not_change_task_instance_states(self, test_client, session):
        hold = _add_hold(session, task_id=None, run_id=RUN_ID)
        _set_task_instance_states(session)

        response = test_client.post(self._url(hold))

        assert response.status_code == 200
        assert _task_instance_states(session) == EXPECTED_STATES

    def test_should_respond_409_when_already_released(self, test_client, session):
        hold = _add_hold(session)
        assert test_client.post(self._url(hold), json={"reason": "first"}).status_code == 200

        response = test_client.post(self._url(hold), json={"reason": "second"})

        assert response.status_code == 409
        assert response.json()["detail"] == (
            f"The Task Hold with id: `{hold.id}` is already released or expired"
        )
        session.expire_all()
        released = session.get(TaskHold, hold.id)
        assert released is not None
        assert released.release_reason == "first"
        # Only the release that took effect is logged.
        assert session.scalar(select(func.count()).select_from(Log).where(Log.event == "hold_released")) == 1

    def test_should_respond_409_when_expired(self, test_client, session):
        hold = _add_hold(session, expires_at=PAST)

        response = test_client.post(self._url(hold))

        assert response.status_code == 409
        session.expire_all()
        expired = session.get(TaskHold, hold.id)
        assert expired is not None
        assert expired.released_at is None

    def test_should_respond_404_for_missing_hold(self, test_client):
        response = test_client.post(f"/dags/{DAG_ID}/holds/12345/release")

        assert response.status_code == 404
        assert (
            response.json()["detail"] == f"The Task Hold with id: `12345` was not found for Dag: `{DAG_ID}`"
        )

    @pytest.mark.parametrize("dag_id", [OTHER_DAG_ID, "~"])
    def test_should_respond_404_for_hold_of_another_dag(self, test_client, session, dag_id):
        hold = _add_hold(session)

        response = test_client.post(self._url(hold, dag_id))

        assert response.status_code == 404
        session.expire_all()
        untouched = session.get(TaskHold, hold.id)
        assert untouched is not None
        assert untouched.released_at is None

    def test_should_respond_422_for_unknown_field(self, test_client, session):
        hold = _add_hold(session)

        response = test_client.post(self._url(hold), json={"unknown": 1})

        assert response.status_code == 422

    def test_should_respond_401(self, unauthenticated_test_client, session):
        hold = _add_hold(session)

        response = unauthenticated_test_client.post(self._url(hold))

        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, session):
        hold = _add_hold(session)

        response = unauthorized_test_client.post(self._url(hold))

        assert response.status_code == 403
        session.expire_all()
        untouched = session.get(TaskHold, hold.id)
        assert untouched is not None
        assert untouched.released_at is None

    def test_should_respond_403_for_reader(self, test_client, dag_reader_headers, session):
        hold = _add_hold(session)

        response = test_client.post(self._url(hold), headers=dag_reader_headers)

        assert response.status_code == 403
        session.expire_all()
        untouched = session.get(TaskHold, hold.id)
        assert untouched is not None
        assert untouched.released_at is None
