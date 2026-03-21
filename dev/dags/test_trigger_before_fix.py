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
"""
Test DAG: Simulates BEFORE-FIX behavior — GitHub discussion #63706.

Demonstrates the bug: when a trigger's run() raises an exception instead of
yielding a TriggerEvent, the triggerer catches it generically and the task
fails with "Trigger failure". execute_complete() is never called.

Run in Breeze:
    breeze run airflow dags trigger test_trigger_before_fix

What to watch for in logs:
    - on_failure_callback fires with exception type "TaskDeferralError"
    - exception message is just "Trigger failure" — no Glue error details
    - execute_complete() is NEVER called (you won't see its log line)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

logger = logging.getLogger(__name__)

SIMULATED_ERROR = (
    "AWS Glue job failed.: FAILED - "
    "Script failed with exit code 1: "
    "java.lang.RuntimeException: Error in Spark transformation\n"
    "Waiter job_complete failed: Waiter encountered a terminal failure state"
)


class FailingTriggerBeforeFix(BaseTrigger):
    """
    Simulates AwsBaseWaiterTrigger.run() BEFORE the fix.

    async_wait() raises AirflowException on terminal failure.
    Before the fix, this propagated uncaught — the triggerer catches
    it generically and sets next_method="__fail__".
    """

    def __init__(self, run_id: str, **kwargs):
        super().__init__(**kwargs)
        self.run_id = run_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (self.__class__.__module__ + "." + self.__class__.__qualname__, {"run_id": self.run_id})

    async def run(self):
        await asyncio.sleep(2)
        # BEFORE FIX: exception propagates uncaught from run()
        raise AirflowException(SIMULATED_ERROR)
        yield TriggerEvent({})


class DeferredOperatorBeforeFix(BaseOperator):
    """Defers to FailingTriggerBeforeFix. execute_complete is never called."""

    def __init__(self, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def execute(self, context):
        self.log.info("Deferring to trigger (BEFORE-FIX simulation)...")
        self.defer(trigger=FailingTriggerBeforeFix(run_id="jr_abc123"), method_name="execute_complete")

    def execute_complete(self, context, **kwargs):
        self.log.info("execute_complete called — THIS SHOULD NOT APPEAR IN BEFORE-FIX")
        event = kwargs
        if event.get("status") != "success":
            raise AirflowException(f"Glue job failed: {event.get('message', 'unknown')}")


def failure_callback_before(context):
    exception = context.get("exception")
    logger.error("=" * 60)
    logger.error("BEFORE-FIX CALLBACK")
    logger.error("Exception type : %s", type(exception).__name__ if exception else "None")
    logger.error("Exception message: %s", str(exception))
    logger.error("-" * 60)
    logger.error("BUG: Message is 'Trigger failure' — actual Glue error is lost!")
    logger.error("execute_complete() was never called.")
    logger.error("=" * 60)


with DAG(
    dag_id="test_trigger_before_fix",
    schedule=None,
    catchup=False,
    tags=["test", "glue-fix", "issue-63706"],
) as dag:
    DeferredOperatorBeforeFix(
        task_id="glue_before_fix",
        job_name="my-etl-job",
        on_failure_callback=failure_callback_before,
    )
