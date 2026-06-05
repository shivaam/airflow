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

TypeScript SDK
==============

|experimental|

The TypeScript SDK lets a Python Dag declare task scheduling with
``@task.stub`` while the task implementation runs in Node.js.
:class:`task-sdk:airflow.sdk.coordinators.typescript.TypescriptCoordinator`
launches a bundled ESM file for each task attempt and communicates with it
through the Airflow supervisor protocol.

Python Dag
----------

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def sales_pipeline():
        @task.stub(queue="typescript")
        def extract(): ...

        @task.stub(queue="typescript")
        def transform(extracted): ...

        transform(extract())


    sales_pipeline()

TypeScript handlers
-------------------

.. code-block:: ts

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

Bundle
------

Bundle the TypeScript entrypoint as a single ESM file:

.. code-block:: bash

    npx esbuild src/tasks.ts --bundle --platform=node --format=esm --outfile=/opt/airflow/ts-bundles/bundle.mjs

Coordinator configuration
-------------------------

Configure Airflow to route the TypeScript queue to the coordinator:

.. code-block:: ini

    [sdk]
    coordinators = {
        "ts": {
            "classpath": "airflow.sdk.coordinators.typescript.TypescriptCoordinator",
            "kwargs": {"bundles_root": ["/opt/airflow/ts-bundles"]}
        }
    }
    queue_to_coordinator = {"typescript": "ts"}

The coordinator scans ``bundles_root`` for exactly one ``.mjs`` file and
launches ``node <bundle>`` with the supervisor socket arguments.
