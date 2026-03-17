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
Test DAG: Demonstrates the BEFORE-FIX behavior (Path B) from GitHub discussion #63706.

When a trigger's run() raises an exception instead of yielding a TriggerEvent,
the triggerer framework catches it generically and marks the task failed with
"Trigger failure". The on_failure_callback only sees TaskDeferralError("Trigger failure")
instead of the actual error message.

This replicates the GlueJobOperator bug where deferred mode loses detailed failure status.

To run in Breeze:
    breeze run airflow dags trigger test_trigger_before_fix

Expected behavior:
    - Task defers to the trigger
    - Trigger raises AirflowException("Glue job FAILED: Script failed with exit code 1")
    - Triggerer catches it generically, sets next_method="__fail__"
    - Worker raises TaskDeferralError("Trigger failure") — GENERIC, no details
    - on_failure_callback sees context["exception"] = TaskDeferralError("Trigger failure")
    - execute_complete() is NEVER called
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

logger = logging.getLogger(__name__)


class GlueJobFailureTriggerBeforeFix(BaseTrigger):
    """
    Simulates the BEFORE-FIX behavior of AwsBaseWaiterTrigger.

    When the Glue job fails, async_wait() raises AirflowException.
    Before the fix, this exception propagates uncaught from run(),
    causing the triggerer to catch it generically and mark the task
    failed with "Trigger failure".
    """

    def __init__(self, job_run_id: str, **kwargs):
        super().__init__(**kwargs)
        self.job_run_id = job_run_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "dev.dags.test_trigger_before_fix.GlueJobFailureTriggerBeforeFix",
            {"job_run_id": self.job_run_id},
        )

    async def run(self):
        """
        Simulate what AwsBaseWaiterTrigger.run() does BEFORE the fix.

        The trigger polls for a while, then the waiter detects a terminal
        failure and raises AirflowException. This exception propagates
        uncaught — the triggerer framework catches it generically.
        """
        from airflow.exceptions import AirflowException

        # Simulate a few seconds of polling
        await asyncio.sleep(2)

        # Simulate what async_wait() does when it detects terminal failure:
        # It raises AirflowException with the failure details.
        # BEFORE THE FIX: This propagates uncaught from run()!
        raise AirflowException(
            f"Glue job {self.job_run_id} FAILED: "
            f"JobRunState: FAILED, ErrorMessage: Script failed with exit code 1 - "
            f"java.lang.RuntimeException: Error in Spark transformation"
        )

        # This yield is never reached, but Python needs it to make this an async generator
        yield TriggerEvent({})


class GlueOperatorBeforeFix(BaseOperator):
    """
    Simulates GlueJobOperator in deferred mode BEFORE the fix.

    Defers to GlueJobFailureTriggerBeforeFix, which raises an exception
    instead of yielding a TriggerEvent. execute_complete() is never called.
    """

    def __init__(self, job_name: str, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name

    def execute(self, context):
        job_run_id = "jr_abc123def456"
        self.log.info("Started Glue job %s, run ID: %s", self.job_name, job_run_id)
        self.log.info("Deferring to trigger to wait for job completion...")

        self.defer(
            trigger=GlueJobFailureTriggerBeforeFix(job_run_id=job_run_id),
            method_name="execute_complete",
        )

    def execute_complete(self, context, **kwargs):
        """This method is NEVER called when the trigger raises an exception."""
        event = kwargs.get("event", kwargs)
        self.log.info("execute_complete called with event: %s", event)

        if event.get("status") != "success":
            from airflow.exceptions import AirflowException

            raise AirflowException(f"Glue job failed: {event.get('message', 'unknown error')}")

        self.log.info("Glue job completed successfully")


def failure_callback(context):
    """
    The on_failure_callback that the user sets up for alerting.

    BEFORE THE FIX: context["exception"] is TaskDeferralError("Trigger failure")
    — the actual Glue error message is lost.
    """
    exception = context.get("exception")
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"

    logger.error("=" * 70)
    logger.error("FAILURE CALLBACK FIRED for task: %s", task_id)
    logger.error("Exception type: %s", type(exception).__name__ if exception else "None")
    logger.error("Exception message: %s", str(exception))
    logger.error("")
    logger.error("BUG: The actual Glue error is NOT in the exception message above!")
    logger.error("The user only sees 'Trigger failure' — useless for alerting/monitoring.")
    logger.error("The real error is buried in the task logs, not accessible to the callback.")
    logger.error("=" * 70)


with DAG(
    dag_id="test_trigger_before_fix",
    schedule=None,
    catchup=False,
    description="Demonstrates the BEFORE-FIX behavior: trigger raises → generic 'Trigger failure'",
    tags=["test", "glue-fix", "before-fix", "issue-63706"],
) as dag:
    GlueOperatorBeforeFix(
        task_id="glue_job_before_fix",
        job_name="my-etl-job",
        on_failure_callback=failure_callback,
    )
