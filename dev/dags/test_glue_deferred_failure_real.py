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
Test DAG: Tests the ACTUAL GlueJobOperator in deferred mode against real AWS.

This DAG uses the real GlueJobOperator with deferrable=True to run a Glue job
that is designed to fail. After the fix, the failure callback should contain
the actual Glue error message instead of the generic "Trigger failure".

Prerequisites:
    1. AWS connection 'aws_default' configured in Airflow
    2. A Glue job named 'test-deferred-failure-job' that will FAIL
       (e.g., a PySpark script that raises an exception)
    3. IAM role with Glue permissions

To create a failing Glue job for testing:
    See dev/testing-glue-deferred-fix.md for instructions.

Run in Breeze:
    breeze run airflow dags trigger test_glue_deferred_failure_real

What to check:
    - Task should fail with "Error in glue job: {'status': 'error', 'message': '...'}"
    - The 'message' field should contain the actual Glue error (JobRunState + ErrorMessage)
    - on_failure_callback should receive the full error text
"""

from __future__ import annotations

import logging

from airflow.sdk import DAG

logger = logging.getLogger(__name__)


def failure_callback(context):
    """Log the full exception details from the failure."""
    exception = context.get("exception")
    ti = context.get("task_instance")

    logger.error("=" * 70)
    logger.error("GLUE DEFERRED FAILURE TEST — on_failure_callback")
    logger.error("Task: %s", ti.task_id if ti else "unknown")
    logger.error("Exception type: %s", type(exception).__name__ if exception else "None")
    logger.error("Exception message: %s", str(exception))
    logger.error("-" * 70)

    exc_str = str(exception) if exception else ""

    # Check if the fix is working
    if "Trigger failure" in exc_str:
        logger.error("RESULT: BUG — still seeing generic 'Trigger failure'")
        logger.error("The fix may not be applied, or something else went wrong.")
    elif "Error in glue job" in exc_str:
        logger.error("RESULT: FIX WORKING — error message contains actual Glue details")
        # Try to extract the message field
        try:
            # The exception message looks like: Error in glue job: {'status': 'error', 'message': '...'}
            dict_str = exc_str.split("Error in glue job: ", 1)[1]
            # ast.literal_eval would be safer but this is a test DAG
            logger.error("Event payload: %s", dict_str)
        except (IndexError, ValueError):
            pass
    else:
        logger.error("RESULT: UNEXPECTED — exception doesn't match expected patterns")
        logger.error("Full exception repr: %r", exception)

    logger.error("=" * 70)


def success_callback(context):
    """If the job succeeds, log a reminder that it should have failed."""
    logger.warning("=" * 70)
    logger.warning("GLUE DEFERRED FAILURE TEST — Job SUCCEEDED unexpectedly!")
    logger.warning("This test DAG expects the Glue job to FAIL.")
    logger.warning("Check that the job 'test-deferred-failure-job' is configured to fail.")
    logger.warning("=" * 70)


with DAG(
    dag_id="test_glue_deferred_failure_real",
    schedule=None,
    catchup=False,
    tags=["test", "glue-fix", "issue-63706", "requires-aws"],
) as dag:
    from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

    GlueJobOperator(
        task_id="glue_deferred_fail",
        job_name="test-deferred-failure-job",
        script_location="s3://NOT-USED/already-created",
        deferrable=True,
        wait_for_completion=True,
        verbose=False,
        waiter_delay=10,
        waiter_max_attempts=30,
        aws_conn_id="aws_default",
        on_failure_callback=failure_callback,
        on_success_callback=success_callback,
    )
