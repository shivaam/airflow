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
Test DAG: Simulates AFTER-FIX behavior — GitHub discussion #63706.

Demonstrates the fix: when async_wait() raises, run() catches it and yields
a TriggerEvent with the error details. execute_complete() IS called, and
on_failure_callback gets the actual Glue error message.

Run in Breeze:
    breeze run airflow dags trigger test_trigger_after_fix

What to watch for in logs:
    - execute_complete() IS called (you'll see its log line)
    - on_failure_callback fires with exception type "AirflowException"
    - exception message contains the actual Glue error text
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


class FailingTriggerAfterFix(BaseTrigger):
    """
    Simulates AwsBaseWaiterTrigger.run() AFTER the fix.

    async_wait() raises AirflowException on terminal failure.
    After the fix, run() catches it and yields TriggerEvent(status="error").
    """

    def __init__(self, run_id: str, **kwargs):
        super().__init__(**kwargs)
        self.run_id = run_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (self.__class__.__module__ + "." + self.__class__.__qualname__, {"run_id": self.run_id})

    async def run(self):
        await asyncio.sleep(2)
        # AFTER FIX: catch exception, yield error event
        try:
            raise AirflowException(SIMULATED_ERROR)
        except AirflowException as e:
            yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})


class DeferredOperatorAfterFix(BaseOperator):
    """Defers to FailingTriggerAfterFix. execute_complete IS called."""

    def __init__(self, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def execute(self, context):
        self.log.info("Deferring to trigger (AFTER-FIX simulation)...")
        self.defer(trigger=FailingTriggerAfterFix(run_id="jr_abc123"), method_name="execute_complete")

    def execute_complete(self, context, **kwargs):
        self.log.info("execute_complete called with: %s", kwargs)
        if kwargs.get("status") != "success":
            raise AirflowException(f"Error in glue job: {kwargs}")


def failure_callback_after(context):
    exception = context.get("exception")
    logger.error("=" * 60)
    logger.error("AFTER-FIX CALLBACK")
    logger.error("Exception type : %s", type(exception).__name__ if exception else "None")
    logger.error("Exception message: %s", str(exception))
    logger.error("-" * 60)
    logger.error("FIX: Message contains the actual Glue error details!")
    logger.error("execute_complete() was called, error routed properly.")
    logger.error("=" * 60)


with DAG(
    dag_id="test_trigger_after_fix",
    schedule=None,
    catchup=False,
    tags=["test", "glue-fix", "issue-63706"],
) as dag:
    DeferredOperatorAfterFix(
        task_id="glue_after_fix",
        job_name="my-etl-job",
        on_failure_callback=failure_callback_after,
    )
