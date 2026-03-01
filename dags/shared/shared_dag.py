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
"""Shared DAG (no team) — should use the global LocalExecutor, not ECS."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["multi-team", "shared", "ecs-test"],
)
def shared_simple_dag():
    @task
    def shared_hello():
        print("Hello from shared DAG running on LocalExecutor!")
        return "shared_done"

    shared_hello()


shared_simple_dag()
