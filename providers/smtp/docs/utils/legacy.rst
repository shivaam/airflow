.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Legacy delivery compatibility
=============================

:func:`airflow.providers.smtp.utils.legacy.send_mime_email` provides the transport
policy used by legacy core email callers. Callers supply a prebuilt MIME message,
resolved settings and credentials, and a callable for lazy TLS context creation.
The function does not read Airflow configuration or Connections.

This is not a replacement configured email backend and is not interchangeable
with :class:`airflow.providers.smtp.hooks.smtp.SmtpHook`. New provider users should
use the hook, operator or notifier rather than adopting legacy delivery semantics.

Only SMTP disconnections during connection construction are retried, up to
``retry_limit + 1`` attempts. STARTTLS, authentication, serialization, delivery
and QUIT failures propagate without another attempt. A partial recipient refusal
is not reported to the caller. Successful delivery returns ``None``. A failure
after the server accepted a message does not imply that it was not delivered;
retrying at a higher level may produce duplicate mail.
