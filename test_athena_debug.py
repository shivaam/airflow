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
"""Local regression test for #55678 — SQLValueCheckOperator with Athena connection extras."""

from __future__ import annotations

from unittest.mock import patch

from airflow.models.connection import Connection
from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator


def test_sql_value_check_operator_with_athena_extras():
    """Reproduces #55678: s3_staging_dir in connection extras caused TypeError.

    BaseSQLOperator.get_hook() passes all connection extras as kwargs to the
    hook constructor. Before the fix, extras like s3_staging_dir would cause:
    TypeError: AwsGenericHook.__init__() got an unexpected keyword argument 's3_staging_dir'
    """
    athena_conn = Connection(
        conn_id="athena_conn",
        conn_type="athena",
        schema="test_schema",
        extra={"s3_staging_dir": "s3://mybucket/athena/", "region_name": "eu-west-1"},
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=athena_conn):
        # This line would raise TypeError before the fix
        hook = SQLValueCheckOperator.get_hook(conn_id="athena_conn")
        assert hook.__class__.__name__ == "AthenaSQLHook"
