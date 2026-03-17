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
Test DAG: Demonstrates the AFTER-FIX behavior (Path A) from GitHub discussion #63706.

After the fix, AwsBaseWaiterTrigger.run() catches AirflowException from async_wait()
and yields a TriggerEvent with status="error" and the actual error message.
This ensures execute_complete() is always called, and the operator can surface
meaningful error messages to on_failure_callback.

To run in Breeze:
    breeze run airflow dags trigger test_trigger_after_fix

Expected behavior:
    - Task defers to the trigger
    - Trigger catches the exception and yields TriggerEvent(status="error", message="...")
    - Triggerer sends the event to the scheduler normally
    - Worker calls execute_complete(event={"status": "error", "message": "..."})
    - execute_complete() raises AirflowException with the ACTUAL Glue error
    - on_failure_callback sees context["exception"] with the real error message
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

logger = logging.getLogger(__name__)


class GlueJobFailureTriggerAfterFix(BaseTrigger):
    """
    Simulates the AFTER-FIX behavior of AwsBaseWaiterTrigger.

    When the Glue job fails, async_wait() raises AirflowException.
    After the fix, run() catches the exception and yields a TriggerEvent
    with the error details, ensuring execute_complete() is called.
    """

    def __init__(self, job_run_id: str, **kwargs):
        super().__init__(**kwargs)
        self.job_run_id = job_run_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "dev.dags.test_trigger_after_fix.GlueJobFailureTriggerAfterFix",
            {"job_run_id": self.job_run_id},
        )

    async def run(self):
        """
        Simulate what AwsBaseWaiterTrigger.run() does AFTER the fix.

        The trigger polls for a while, then the waiter detects a terminal
        failure. AFTER THE FIX: The exception is caught and a TriggerEvent
        is yielded with the error details.
        """
        from airflow.exceptions import AirflowException

        # Simulate a few seconds of polling
        await asyncio.sleep(2)

        # Simulate what async_wait() does when it detects terminal failure
        try:
            raise AirflowException(
                f"Glue job {self.job_run_id} FAILED: "
                f"JobRunState: FAILED, ErrorMessage: Script failed with exit code 1 - "
                f"java.lang.RuntimeException: Error in Spark transformation"
            )
        except AirflowException as e:
            # AFTER THE FIX: Catch the exception and yield a TriggerEvent
            # with error details. This ensures execute_complete() is called.
            yield TriggerEvent({"status": "error", "message": str(e), "return_value": self.job_run_id})


class GlueOperatorAfterFix(BaseOperator):
    """
    Simulates GlueJobOperator in deferred mode AFTER the fix.

    Defers to GlueJobFailureTriggerAfterFix, which catches exceptions and
    yields a TriggerEvent. execute_complete() IS called with the error details.
    """

    def __init__(self, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def execute(self, context):
        job_run_id = "jr_abc123def456"
        self.log.info("Started Glue job %s, run ID: %s", self.job_name, job_run_id)
        self.log.info("Deferring to trigger to wait for job completion...")

        self.defer(
            trigger=GlueJobFailureTriggerAfterFix(job_run_id=job_run_id),
            method_name="execute_complete",
        )

    def execute_complete(self, context, **kwargs):
        """
        This method IS called after the fix.

        The trigger yields a TriggerEvent with the error details,
        so this method receives the actual error message and can
        raise a meaningful exception.
        """
        from airflow.exceptions import AirflowException

        status = kwargs.get("status")
        message = kwargs.get("message")
        job_run_id = kwargs.get("return_value")

        self.log.info("execute_complete called with status=%s, message=%s", status, message)

        if status != "success":
            # This is the key difference: the actual error message is available!
            raise AirflowException(f"Glue job {job_run_id} failed with status '{status}': {message}")

        self.log.info("Glue job %s completed successfully", job_run_id)


def failure_callback(context):
    """
    The on_failure_callback that the user sets up for alerting.

    AFTER THE FIX: context["exception"] contains AirflowException with the
    actual Glue error message — useful for alerting and monitoring.
    """
    exception = context.get("exception")
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"

    logger.error("=" * 70)
    logger.error("FAILURE CALLBACK FIRED for task: %s", task_id)
    logger.error("Exception type: %s", type(exception).__name__ if exception else "None")
    logger.error("Exception message: %s", str(exception))
    logger.error("")
    logger.error("FIX: The actual Glue error IS in the exception message above!")
    logger.error("The user can now extract meaningful error details in their callback")
    logger.error("and send them to Slack/Teams/PagerDuty for immediate triage.")
    logger.error("=" * 70)


with DAG(
    dag_id="test_trigger_after_fix",
    schedule=None,
    catchup=False,
    description="Demonstrates the AFTER-FIX behavior: trigger yields error event → meaningful exception",
    tags=["test", "glue-fix", "after-fix", "issue-63706"],
) as dag:
    GlueOperatorAfterFix(
        task_id="glue_job_after_fix",
        job_name="my-etl-job",
        on_failure_callback=failure_callback,
    )
