#
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
Add task_hold table.

Revision ID: d8efa8f98142
Revises: 38c8df3fcb9c
Create Date: 2026-10-20 09:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "d8efa8f98142"
down_revision = "38c8df3fcb9c"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Create the task_hold table."""
    op.create_table(
        "task_hold",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("task_id", StringID(), nullable=True),
        sa.Column("run_id", StringID(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_by", sa.String(length=512), nullable=True),
        sa.Column("created_at", UtcDateTime(), nullable=False),
        sa.Column("expires_at", UtcDateTime(), nullable=True),
        sa.Column("released_by", sa.String(length=512), nullable=True),
        sa.Column("released_at", UtcDateTime(), nullable=True),
        sa.Column("release_reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("task_hold_pkey")),
        sa.ForeignKeyConstraint(
            ["dag_id"],
            ["dag.dag_id"],
            name="task_hold_dag_id_fkey",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("task_id IS NOT NULL OR run_id IS NOT NULL", name="task_hold_scope_check"),
    )
    op.create_index("idx_task_hold_released_at_dag_id", "task_hold", ["released_at", "dag_id"])


def downgrade():
    """Drop the task_hold table."""
    op.drop_table("task_hold")
