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
Test DAG for reproducing Airflow 3 backfill race condition (GitHub #61375).

Issue: When running `airflow backfill create`, the backfill job is marked as
"completed" almost immediately, even though the triggered DAG runs are still
executing. This happens because the scheduler checks for unfinished DagRuns
associated with the backfill before they've been created in the DB.

Setup:
    Requires Airflow 3.x (tested on 3.1.6+).

Usage:
    airflow backfill create \
        --dag-id test_backfill_race_condition \
        --from-date 2025-01-01 \
        --to-date 2025-01-10 \
        --max-active-runs 1 \
        --run-backwards

What to observe:
    1. Run `airflow backfill list` immediately after creating the backfill.
       If the bug is present, the backfill status will be "completed" even
       though DAG runs are still executing.
    2. Check the Airflow UI — DAG runs should still be in "running" state.
    3. Each task sleeps for SLEEP_SECONDS so the race window is obvious.
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_ID = "test_backfill_race_condition"
SLEEP_SECONDS = 5  # per task, ~15s total per DAG run


def slow_task(task_name: str, sleep_seconds: int, **context):
    """Simulates work with a sleep so the race condition window is observable."""
    logical_date = context.get("logical_date")
    print(f"[{task_name}] Starting for logical_date={logical_date}")
    print(f"[{task_name}] Sleeping {sleep_seconds}s...")
    time.sleep(sleep_seconds)
    print(f"[{task_name}] Completed.")
    return f"{task_name} done for {logical_date}"


def log_run_metadata(**context):
    """Logs DAG run metadata to help diagnose backfill behavior."""
    dag_run = context["dag_run"]
    print(f"Run ID:        {dag_run.run_id}")
    print(f"Run type:      {dag_run.run_type}")
    print(f"Logical date:  {dag_run.logical_date}")
    print(f"State:         {dag_run.state}")
    if dag_run.run_type == "backfill":
        backfill_id = getattr(dag_run, "backfill_id", "N/A")
        print(f"Backfill ID:   {backfill_id}")
        print(">>> This run was triggered by a backfill job.")
    else:
        print(f">>> Normal trigger (run_type={dag_run.run_type})")


with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 2, 1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args={"owner": "test", "retries": 0},
    description="Reproduce Airflow 3 backfill race condition (GH #61375)",
    tags=["test", "backfill", "debug"],
) as dag:
    log_info = PythonOperator(
        task_id="log_run_metadata",
        python_callable=log_run_metadata,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=slow_task,
        op_kwargs={"task_name": "extract", "sleep_seconds": SLEEP_SECONDS},
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=slow_task,
        op_kwargs={"task_name": "transform", "sleep_seconds": SLEEP_SECONDS},
    )

    load = PythonOperator(
        task_id="load",
        python_callable=slow_task,
        op_kwargs={"task_name": "load", "sleep_seconds": SLEEP_SECONDS},
    )

    log_info >> extract >> transform >> load
