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
Test DAG for ECS executor callback support.

Verifies that ExecuteCallback workloads are correctly dispatched to ECS
containers when a deadline is missed.

Deployment (on EC2):
    Copy this file to the team_alpha DAG folder and sync to S3:

        cp dev/test_dags/test_deadline_callback.py /tmp/dags/team_alpha/
        source /opt/airflow-scripts/env.sh
        aws s3 sync /tmp/dags/ s3://${DAG_BUCKET}/ --delete

    Or run deploy-dags.sh if this file has been added to it.

How to test:
    1. Trigger the DAG manually from the UI or CLI
    2. The task sleeps for 120s, but the deadline is 30s
    3. After ~30s, the scheduler detects the missed deadline
    4. Scheduler creates an ExecuteCallback workload and queues it to the ECS executor
    5. ECS executor launches a new container that runs the callback
    6. The callback prints a message and exits
    7. Check the scheduler logs for "Executing callback workload" messages
    8. Check ECS console for a second task (the callback container)

What success looks like:
    - Two ECS tasks launched: one for slow_task, one for the callback
    - Callback ECS task exits with code 0
    - Scheduler logs show callback state: PENDING -> QUEUED -> RUNNING -> SUCCESS
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import DAG, task
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference


def deadline_missed_alert(**kwargs):
    """Simple callback function that prints when a deadline is missed."""
    import socket
    from datetime import datetime

    print("=" * 60)
    print("DEADLINE MISSED — CALLBACK EXECUTED SUCCESSFULLY")
    print(f"  Host:      {socket.gethostname()}")
    print(f"  Timestamp: {datetime.now().isoformat()}")
    print(f"  Context:   {kwargs}")
    print("=" * 60)


with DAG(
    dag_id="test_deadline_callback",
    schedule=None,
    catchup=False,
    tags=["ecs-test", "callback", "deadline"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=SyncCallback(deadline_missed_alert),
    ),
):

    @task
    def slow_task():
        """Task that runs longer than the deadline, triggering the callback."""
        import socket
        import time

        print(f"Starting slow_task on host {socket.gethostname()}")
        print("Sleeping for 120s (deadline is 30s, so callback will fire)...")
        time.sleep(120)
        print("slow_task completed")
        return "done"

    slow_task()
