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
Reproduces the expunge_all bug in MetastoreBackend (issue #62244).

Run with: breeze run pytest dev/test_expunge_all_bug.py -xvs
"""
from __future__ import annotations

import pytest

from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = pytest.mark.db_test


class TestExpungeAllBug:
    """Demonstrates that MetastoreBackend.expunge_all() destroys unrelated pending session objects."""

    def setup_method(self):
        clear_db_connections()
        clear_db_variables()

    def teardown_method(self):
        clear_db_connections()
        clear_db_variables()

    def test_get_connection_expunges_pending_objects(self):
        """
        Bug reproduction: expunge_all in get_connection removes unrelated pending objects.

        This simulates what happens in sync_bundles_to_db:
        1. A function adds a pending object to the session (like DagBundleModel)
        2. MetastoreBackend.get_connection is called on the SAME session
        3. expunge_all() nukes the pending object

        This test should FAIL with the current code (expunge_all) and
        PASS after the fix (expunge only the queried object).
        """
        # First, create a connection in the DB that we can look up
        with create_session() as session:
            session.add(Connection(conn_id="test_lookup_conn", conn_type="aws"))
            session.commit()

        with create_session() as session:
            # Simulate what sync_bundles_to_db does: add a pending object
            pending_conn = Connection(conn_id="pending_bundle_conn", conn_type="http")
            session.add(pending_conn)

            # Verify it's in session.new
            assert pending_conn in session.new, "pending_conn should be in session.new before get_connection"

            # Now call MetastoreBackend.get_connection with the SAME session
            # (this is what happens when scoped sessions are shared)
            backend = MetastoreBackend()
            result = backend.get_connection("test_lookup_conn", session=session)

            # The looked-up connection should be returned
            assert result is not None, "get_connection should find test_lookup_conn"
            assert result.conn_id == "test_lookup_conn"

            # THE BUG: pending_conn was expunged from the session by expunge_all()
            assert pending_conn in session.new, (
                "BUG: expunge_all() removed the pending object from session.new! "
                "This is the root cause of issue #62244 — pending DagBundleModel objects "
                "get destroyed when MetastoreBackend.get_connection is called on a shared session."
            )

    def test_get_variable_expunges_pending_objects(self):
        """Same bug but for get_variable — expunge_all destroys unrelated pending objects."""
        # Create a variable in the DB
        Variable.set(key="test_key", value="test_value")

        with create_session() as session:
            # Add a pending object
            pending_conn = Connection(conn_id="pending_conn", conn_type="http")
            session.add(pending_conn)

            assert pending_conn in session.new

            # Call get_variable on the same session
            backend = MetastoreBackend()
            result = backend.get_variable("test_key", session=session)

            assert result == "test_value"

            # THE BUG: pending_conn was expunged
            assert pending_conn in session.new, (
                "BUG: expunge_all() in get_variable removed the pending object from session.new!"
            )

    def test_get_connection_preserves_persistent_objects(self):
        """expunge_all also removes persistent (already-committed) objects from the identity map."""
        with create_session() as session:
            session.add(Connection(conn_id="lookup_conn_3", conn_type="aws"))
            session.add(Connection(conn_id="other_conn_3", conn_type="http"))
            session.commit()

        with create_session() as session:
            # Load other_conn into the session's identity map (make it persistent)
            other = session.scalar(
                Connection.__table__.select().where(Connection.conn_id == "other_conn_3")
            )
            # Actually load it as an ORM object
            from sqlalchemy import select

            other = session.scalar(select(Connection).where(Connection.conn_id == "other_conn_3"))
            assert other is not None

            identity_map_size_before = len(session.identity_map)

            # Call get_connection — expunge_all will nuke the identity map
            backend = MetastoreBackend()
            result = backend.get_connection("lookup_conn_3", session=session)

            assert result is not None

            # After expunge_all, the identity map should be empty
            # (other_conn was kicked out even though it had nothing to do with the query)
            identity_map_size_after = len(session.identity_map)

            # This assertion shows the collateral damage — even persistent objects get removed
            assert identity_map_size_after >= identity_map_size_before, (
                f"BUG: Identity map shrank from {identity_map_size_before} to {identity_map_size_after}. "
                "expunge_all() removed persistent objects that were unrelated to the query."
            )
