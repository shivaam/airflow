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

Why a separate compatibility interface?
---------------------------------------

Legacy core settings and modern hook Connections are different contracts. Sending
existing core callers through ``SmtpHook`` would combine an ownership move with
changes to configuration, authentication, retries and failure handling. Keeping
the compatibility transport separate allows those policies to be evaluated in
later changes without requiring users to migrate their configuration first.

The intended consumer is core's existing public email wrapper, not new Dag code.
The provider interface is nevertheless a cross-distribution dependency: a private
module name would not remove the need to coordinate releases or preserve behavior
for supported core callers. The compatibility cost is intentional, and any later
policy change must be evaluated against those callers rather than silently adopted
from the modern hook. Legacy refusal, retry and failure-path cleanup behavior is
documented here as a compatibility constraint, not as recommended delivery policy.

Release ordering
----------------

.. warning::

   A core change that delegates to this function must not ship while its declared
   minimum SMTP provider version lacks this capability. Dry runs do not exercise
   the transport import and cannot prove installed-package compatibility.

Release this provider capability first, then coordinate core's dependency floor
with the release managers using the actual released version. Validate the core
cutover against installed provider artifacts, including the minimum supported
version, before merging it. A combined source-checkout test run is not sufficient.
Do not retain a second legacy core transport as an import-error fallback: that
would hide an invalid dependency combination and leave two implementations to
maintain. The modern hook, operator and notifier remain unchanged.

The compatibility boundary is the public core email functions, not incidental
module attributes. External tests that patch ``airflow.utils.email.smtplib`` or
the private ``_get_smtp_connection`` helper need to patch the transport's owning
module after the cutover; those private patch targets are not retained aliases.
