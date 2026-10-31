/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { http, HttpResponse, type HttpHandler } from "msw";

import type { DagRunState } from "openapi/requests/types.gen";

// No holds by default, so pages that look for them render as they always did.
export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/dags/:dagId/holds", () => HttpResponse.json({ task_holds: [], total_entries: 0 })),
];

/**
 * Serves every Dag run in the given state. The hold dialog reads the state of the run of a task instance
 * to decide whether to offer a task instance hold, which ends with its run.
 */
export const serveRunsInState = (state: DagRunState): HttpHandler =>
  http.get("/api/v2/dags/:dagId/dagRuns/:dagRunId", ({ params }) =>
    HttpResponse.json({ dag_id: params.dagId, dag_run_id: params.dagRunId, state }),
  );
