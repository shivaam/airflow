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

Public TypeScript interfaces for writing Apache Airflow task handlers.

**Status:** alpha · API will change · Node 22+ · ESM-only

This package defines the user-facing task handler contract and the coordinator
runtime used to execute registered TypeScript handlers from Airflow.

## Install

```bash
pnpm add @apache-airflow/ts-sdk
```

## Task Handlers

```ts
import { registerTask } from "@apache-airflow/ts-sdk";

registerTask({ dagId: "example_dag", taskId: "say_hello" }, async ({ ctx, client }) => {
  const greeting = await client.getVariable("greeting");
  return { message: `Hello from ${ctx.taskId}: ${greeting}` };
});
```

Non-`undefined` return values are pushed to XCom under the `"return_value"`
key by the active runtime, matching Python `@task` behavior.

## Coordinator Usage

Coordinator mode follows the same shape as the other non-Python SDKs: a Python
Dag declares the scheduling shape with stub tasks, and the TypeScript module
registers handlers with matching task IDs.

Python Dag:

```python
from airflow.sdk import dag, task


@dag
def sales_pipeline():
    @task.stub(queue="typescript")
    def extract(): ...

    @task.stub(queue="typescript")
    def transform(extracted): ...

    transform(extract())


sales_pipeline()
```

TypeScript handlers:

```ts
import { registerTask } from "@apache-airflow/ts-sdk";
import { startCoordinatorRuntime } from "@apache-airflow/ts-sdk/coordinator";

registerTask({ dagId: "sales_pipeline", taskId: "extract" }, async ({ client }) => {
  const connection = await client.getConnection("sales_db");
  const rowCount = Number((await client.getVariable("daily_row_count")) ?? "0");

  return {
    connectionId: connection?.connId ?? null,
    rowCount,
  };
});

registerTask({ dagId: "sales_pipeline", taskId: "transform" }, async ({ client }) => {
  const extracted = await client.getXCom<{ rowCount: number }>({
    key: "return_value",
    taskId: "extract",
  });

  return {
    transformedRows: extracted?.rowCount ?? 0,
  };
});

await startCoordinatorRuntime();
```

The Python stub defines the Dag dependency graph. The TypeScript handler does
the work and uses `TaskClient` for task-time Airflow data access. Register each
handler with the Python Dag's `dag_id` and the stub task's `task_id`. The
coordinator launches Node.js, finds the registered handler for that Dag/task
pair, and runs it.

Bundle the TypeScript module as a single ESM file, for example:

```bash
npx esbuild src/tasks.ts --bundle --platform=node --format=esm --outfile=/opt/airflow/ts-bundles/bundle.mjs
```

Configure Airflow to route a queue to the TypeScript coordinator and point it at
the bundle root:

```toml
[sdk]
coordinators = {
  "ts": {
    "classpath": "airflow.sdk.coordinators.typescript.TypescriptCoordinator",
    "kwargs": {"bundles_root": ["/opt/airflow/ts-bundles"]},
  },
}
queue_to_coordinator = {"typescript": "ts"}
```

## TaskClient

Every task handler receives a `TaskClient` for task-time Airflow data access:

| Method                                    | Description         |
| ----------------------------------------- | ------------------- |
| `getVariable(key)` / `getVariableOrThrow` | Airflow Variables   |
| `getXCom(opts)` / `setXCom(opts)`         | XCom read/write     |
| `getConnection(connId)`                   | Airflow Connections |

Locator fields such as `dagId`, `runId`, and `taskId` default to the
current task context when omitted.

## Cancellation

`ctx.signal` is an `AbortSignal` controlled by the active runtime. Pass it to
`fetch()`, timers, database clients, child processes, or other abortable APIs
so tasks can clean up cooperatively when Airflow terminates the task attempt.

## Development

```bash
pnpm install
pnpm test
pnpm run typecheck
pnpm run build
```
