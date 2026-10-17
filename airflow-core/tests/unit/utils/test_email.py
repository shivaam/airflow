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
from __future__ import annotations

import pytest

from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.providers.smtp.utils import emailer
from airflow.utils import email
from airflow.utils.deprecation_tools import DeprecatedImportWarning


@pytest.mark.parametrize(
    "name",
    ["send_email", "send_email_smtp", "build_mime_message", "send_mime_email", "get_email_address_list"],
)
def test_deprecated_names_resolve_to_provider(name):
    with pytest.warns(DeprecatedImportWarning, match=f"airflow.providers.smtp.utils.emailer.{name}"):
        assert getattr(email, name) is getattr(emailer, name)


def test_previous_default_backend_still_imports():
    with pytest.warns(DeprecatedImportWarning):
        assert import_string("airflow.utils.email.send_email_smtp") is emailer.send_email_smtp


def test_default_backend_is_provider():
    assert conf.get_default_value("email", "email_backend") == (
        "airflow.providers.smtp.utils.emailer.send_email_smtp"
    )


def test_unknown_name_raises_attribute_error():
    with pytest.raises(AttributeError):
        email.does_not_exist
