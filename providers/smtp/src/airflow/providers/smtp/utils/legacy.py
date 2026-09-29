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
"""Transport compatibility for callers that resolve legacy email settings."""

from __future__ import annotations

import logging
import smtplib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ssl
    from collections.abc import Callable
    from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)


def send_mime_email(
    e_from: str,
    e_to: str | list[str],
    mime_msg: MIMEMultipart,
    *,
    host: str,
    port: int,
    timeout: int,
    use_ssl: bool,
    starttls: bool,
    retry_limit: int,
    username: str | None,
    password: str | None,
    ssl_context_factory: Callable[[], ssl.SSLContext | None],
    logger: logging.Logger | None = None,
) -> None:
    """
    Deliver a prebuilt message using the legacy core SMTP policy.

    Settings and credentials must already be resolved by the caller. Unlike
    :class:`~airflow.providers.smtp.hooks.smtp.SmtpHook`, this function does not
    read Connections or configure delivery from their extras.

    Only construction-time SMTP disconnections are retried. Partial recipient
    refusals are ignored, and failures after construction propagate without a
    resend or additional cleanup, preserving the legacy delivery contract.

    :param ssl_context_factory: Resolve TLS configuration lazily for each SSL
        connection attempt and each STARTTLS upgrade.
    :param logger: Optional caller logger to preserve legacy log attribution.
    """
    delivery_log = logger if logger is not None else log
    smtp_conn: smtplib.SMTP
    for attempt in range(retry_limit + 1):
        delivery_log.info("Email alerting: attempt %s", str(attempt + 1))
        try:
            if use_ssl:
                smtp_conn = smtplib.SMTP_SSL(
                    host=host, port=port, timeout=timeout, context=ssl_context_factory()
                )
            else:
                smtp_conn = smtplib.SMTP(host=host, port=port, timeout=timeout)
        except smtplib.SMTPServerDisconnected:
            if attempt == retry_limit:
                raise
        else:
            if starttls:
                smtp_conn.starttls(context=ssl_context_factory())
            if username and password:
                smtp_conn.login(username, password)
            delivery_log.info("Sent an alert email to %s", e_to)
            smtp_conn.sendmail(e_from, e_to, mime_msg.as_string())
            smtp_conn.quit()
            break
