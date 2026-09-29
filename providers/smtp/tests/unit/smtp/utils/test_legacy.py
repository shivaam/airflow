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

import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from unittest import mock

import pytest


def test_plain_delivery_interface(mocker):
    from airflow.providers.smtp.utils.legacy import send_mime_email

    constructor = mocker.patch("airflow.providers.smtp.utils.legacy.smtplib.SMTP", autospec=True)
    message = MIMEMultipart()
    factory = mocker.Mock(spec=ssl.create_default_context)
    assert (
        send_mime_email(
            "from@fixture.invalid",
            ["to@fixture.invalid"],
            message,
            host="localhost",
            port=2525,
            timeout=7,
            use_ssl=False,
            starttls=False,
            retry_limit=0,
            username=None,
            password=None,
            ssl_context_factory=factory,
        )
        is None
    )
    constructor.return_value.sendmail.assert_called_once_with(
        "from@fixture.invalid", ["to@fixture.invalid"], message.as_string()
    )
    factory.assert_not_called()


@pytest.fixture
def delivery(mocker):
    from airflow.providers.smtp.utils import legacy

    plain = mocker.patch.object(legacy.smtplib, "SMTP", autospec=True)
    secure = mocker.patch.object(legacy.smtplib, "SMTP_SSL", autospec=True)
    factory = mocker.Mock(spec=ssl.create_default_context, return_value=None)
    message = MIMEMultipart(boundary="fixed-test-boundary")

    def send(**overrides):
        options = dict(
            host="smtp.fixture.invalid",
            port=2525,
            timeout=7,
            use_ssl=False,
            starttls=False,
            retry_limit=0,
            username=None,
            password=None,
            ssl_context_factory=factory,
        )
        options.update(overrides)
        return legacy.send_mime_email("from@fixture.invalid", ["to@fixture.invalid"], message, **options)

    return send, plain, secure, factory, message


def test_plain_delivery_calls_constructor_sendmail_and_quit(delivery):
    send, plain, secure, factory, message = delivery
    assert send() is None
    plain.assert_called_once_with(host="smtp.fixture.invalid", port=2525, timeout=7)
    assert plain.return_value.method_calls == [
        mock.call.sendmail("from@fixture.invalid", ["to@fixture.invalid"], message.as_string()),
        mock.call.quit(),
    ]
    secure.assert_not_called()
    factory.assert_not_called()


@pytest.mark.parametrize("limit", [-1, 0, 1, 2])
@pytest.mark.parametrize("failure", [smtplib.SMTPServerDisconnected, ConnectionRefusedError, TimeoutError])
def test_constructor_retry_boundary(delivery, limit, failure):
    send, plain, _, factory, _ = delivery
    error = failure("synthetic")
    plain.side_effect = error
    if limit < 0:
        assert send(retry_limit=limit) is None
        plain.assert_not_called()
    else:
        with pytest.raises(failure) as caught:
            send(retry_limit=limit)
        assert caught.value is error
        assert plain.call_count == (limit + 1 if failure is smtplib.SMTPServerDisconnected else 1)
    factory.assert_not_called()


@pytest.mark.parametrize("use_ssl", [False, True])
@pytest.mark.parametrize("starttls", [False, True])
def test_tls_context_timing(delivery, use_ssl, starttls):
    send, plain, secure, factory, message = delivery
    send(use_ssl=use_ssl, starttls=starttls)
    constructor, unused = (secure, plain) if use_ssl else (plain, secure)
    kwargs = dict(host="smtp.fixture.invalid", port=2525, timeout=7)
    if use_ssl:
        kwargs["context"] = None
    constructor.assert_called_once_with(**kwargs)
    unused.assert_not_called()
    assert factory.call_count == int(use_ssl) + int(starttls)
    expected = [mock.call.starttls(context=None)] if starttls else []
    expected += [
        mock.call.sendmail("from@fixture.invalid", ["to@fixture.invalid"], message.as_string()),
        mock.call.quit(),
    ]
    assert constructor.return_value.method_calls == expected


@pytest.mark.parametrize("use_ssl", [False, True])
def test_constructor_recovers_without_duplicate_send(delivery, use_ssl):
    send, plain, secure, factory, _ = delivery
    constructor = secure if use_ssl else plain
    client = constructor.return_value
    constructor.side_effect = [smtplib.SMTPServerDisconnected("synthetic"), client]
    send(use_ssl=use_ssl, retry_limit=2)
    assert constructor.call_count == 2
    assert factory.call_count == (2 if use_ssl else 0)
    client.sendmail.assert_called_once()
    client.quit.assert_called_once()


@pytest.mark.parametrize("use_ssl", [False, True])
@pytest.mark.parametrize("error_type", [smtplib.SMTPServerDisconnected, ValueError])
def test_factory_failure_retry_scope(delivery, use_ssl, error_type):
    send, plain, secure, factory, _ = delivery
    error = error_type("synthetic")
    factory.side_effect = error
    with pytest.raises(error_type) as caught:
        send(use_ssl=use_ssl, starttls=not use_ssl, retry_limit=2)
    assert caught.value is error
    assert factory.call_count == (3 if use_ssl and error_type is smtplib.SMTPServerDisconnected else 1)
    secure.assert_not_called()
    assert plain.call_count == (0 if use_ssl else 1)
    plain.return_value.sendmail.assert_not_called()


@pytest.mark.parametrize(
    "credentials",
    [
        (None, None),
        ("", ""),
        ("user", None),
        (None, "secret"),
        ("user", ""),
        ("", "secret"),
        ("user", "secret"),
    ],
)
def test_credentials_control_login(delivery, credentials):
    send, plain, _, _, _ = delivery
    send(username=credentials[0], password=credentials[1])
    if all(credentials):
        plain.return_value.login.assert_called_once_with(*credentials)
    else:
        plain.return_value.login.assert_not_called()


@pytest.mark.parametrize("stage", ["starttls", "login", "as_string", "sendmail", "quit"])
def test_post_connect_failure_is_not_retried(delivery, mocker, stage):
    send, plain, _, _, message = delivery
    error = smtplib.SMTPServerDisconnected("synthetic")
    if stage == "as_string":
        mocker.patch.object(message, "as_string", autospec=True, side_effect=error)
    else:
        getattr(plain.return_value, stage).side_effect = error
    with pytest.raises(smtplib.SMTPServerDisconnected) as caught:
        send(starttls=True, username="user", password="secret", retry_limit=2)
    assert caught.value is error
    plain.assert_called_once()
    assert plain.return_value.sendmail.call_count == (1 if stage in {"sendmail", "quit"} else 0)
    assert plain.return_value.quit.call_count == (1 if stage == "quit" else 0)


@pytest.mark.parametrize("recipients", ["to@fixture.invalid", ["to@fixture.invalid", "to@fixture.invalid"]])
@pytest.mark.parametrize("refused", [False, True])
def test_envelope_and_partial_refusal_are_preserved(delivery, recipients, refused):
    from airflow.providers.smtp.utils.legacy import send_mime_email

    _, plain, _, factory, message = delivery
    plain.return_value.sendmail.return_value = {"bad@fixture.invalid": (550, b"refused")} if refused else {}
    assert (
        send_mime_email(
            "from@fixture.invalid",
            recipients,
            message,
            host="localhost",
            port=2525,
            timeout=7,
            use_ssl=False,
            starttls=False,
            retry_limit=0,
            username=None,
            password=None,
            ssl_context_factory=factory,
        )
        is None
    )
    assert plain.return_value.sendmail.call_args.args[1] is recipients
    plain.return_value.quit.assert_called_once()


def test_all_refused_propagates(delivery):
    send, plain, _, _, _ = delivery
    error = smtplib.SMTPRecipientsRefused({"to@fixture.invalid": (550, b"refused")})
    plain.return_value.sendmail.side_effect = error
    with pytest.raises(smtplib.SMTPRecipientsRefused) as caught:
        send(retry_limit=2)
    assert caught.value is error
    plain.assert_called_once()
    plain.return_value.quit.assert_not_called()


@pytest.mark.parametrize("supplied", [False, True])
def test_logger_selection_and_attempts(delivery, mocker, supplied):
    from airflow.providers.smtp.utils import legacy

    send, plain, _, _, _ = delivery
    default = mocker.patch.object(legacy, "log", autospec=True)
    custom = mocker.Mock(spec=logging.Logger)
    plain.side_effect = [smtplib.SMTPServerDisconnected("synthetic"), plain.return_value]
    send(retry_limit=1, logger=custom if supplied else None, username="user", password="secret")
    selected, unused = (custom, default) if supplied else (default, custom)
    assert [call.args[1] for call in selected.info.call_args_list] == ["1", "2", ["to@fixture.invalid"]]
    unused.info.assert_not_called()
    assert all("secret" not in str(call) for call in selected.method_calls)
