<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Airflow TypeScript SDK

A Node.js SDK for [Apache Airflow](https://airflow.apache.org/). Author task
handlers in TypeScript; run them in one of two modes:

- **Edge mode** — long-running Node worker polls the
  [Edge API](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/index.html)
  ([AIP-69](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-69)).
  Workers can run anywhere with HTTPS reach to Airflow.
- **Coordinator mode** — Airflow's `TypescriptCoordinator`
  ([PR #65958](https://github.com/apache/airflow/pull/65958)) spawns a
  one-shot Node subprocess per task and bridges it to the supervisor over
  TCP + msgpack. Works with any executor (Local, Celery, K8s).

Both modes share `registerTask()` and the same `TaskClient` interface for
Variables / XCom / Connections — handlers don't know which mode is running them.

**Status:** alpha · API will change · Node 22+ · ESM-only
**Supervisor wire schema:** pinned to api_version `2026-06-16` (vendored
via [PR #67235](https://github.com/apache/airflow/pull/67235)'s Cadwyn
framework — exported as `SUPERVISOR_API_VERSION`).

## Build

```bash
pnpm install
pnpm test         # vitest
pnpm run typecheck
pnpm run build
```

Uses pnpm via Node 22's corepack — no separate install needed.

## Edge-mode quickstart

```ts
import { registerTask, startWorker } from "@apache-airflow/ts-sdk";

registerTask("hello_typescript", async ({ ctx, client }) => {
  const greeting = await client.getVariable("greeting");
  return { message: `Hello from ${ctx.taskId}: ${greeting}` };
});

await startWorker({ queues: ["ts-tasks"] });
// baseUrl + secret default to env:
//   AIRFLOW__EDGE__API_URL, AIRFLOW__API_AUTH__JWT_SECRET
```

Run: `node worker.ts` (Node 23.6+) or `npx tsx worker.ts` (any Node 22+).

## Coordinator-mode quickstart

```ts
import { registerTask, startCoordinatorRuntime } from "@apache-airflow/ts-sdk";

registerTask("say_hello", async ({ ctx, client }) => {
  return { message: `Hello from ${ctx.taskId}` };
});

await startCoordinatorRuntime();
```

Bundle this to a single `.mjs` (esbuild) and configure Airflow with the
`TypescriptCoordinator` — see [`COORDINATOR.md`](COORDINATOR.md).

## Deeper docs

- [`COORDINATOR.md`](COORDINATOR.md) — coordinator-mode architecture + wire protocol.
- [`TESTING.md`](TESTING.md) — end-to-end recipe against a real Airflow.
- [`SDK_COMPARISON.md`](SDK_COMPARISON.md) — feature table vs Python/Java SDKs.
- [`REVIEW_OBSERVATIONS.md`](REVIEW_OBSERVATIONS.md) — concept catalogue + open fixes.
- [`REFACTOR_NOTES.md`](REFACTOR_NOTES.md) — author's working notes.
