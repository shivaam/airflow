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

// DAG builder and registry. Coordinator-mode only — defines DAG
// structure (tasks, dependencies, schedule) that handleParse()
// serializes to Airflow's DagSerialization v3 format.

import type { TaskHandler } from "../task.js";
import { registerTask } from "../registry.js";

/** Options for DAG construction. All optional — sensible defaults match
 *  Python's DAG constructor and the Java SDK's Dag class. */
export interface DagOptions {
    schedule?: string | null;
    description?: string;
    startDate?: Date;
    endDate?: Date;
    catchup?: boolean;
    tags?: string[];
    maxActiveTasks?: number;
    maxActiveRuns?: number;
    maxConsecutiveFailedDagRuns?: number;
    dagrunTimeout?: number;
    docMd?: string;
    isPausedUponCreation?: boolean;
    dagDisplayName?: string;
    failFast?: boolean;
    renderTemplateAsNativeObj?: boolean;
    params?: Record<string, unknown>;
    defaultArgs?: Record<string, unknown>;
    ownerLinks?: Record<string, string>;
    accessControl?: Record<string, Record<string, string[]>>;
}

/** Per-task options within a DAG definition. */
export interface TaskOpts {
    downstream?: string[];
    /** Queue name for coordinator routing. When set, the scheduler uses
     *  `[sdk] queue_to_coordinator` to dispatch this task to the matching
     *  coordinator (e.g. `"java-runtime"` routes to `JavaCoordinator`).
     *  Omit for tasks that run in the current runtime (TypeScript). */
    queue?: string;
    /** Language hint serialized into the task. Defaults to `"typescript"`.
     *  Set to `"java"` for tasks delegated to the Java coordinator. */
    language?: string;
}

/** A task entry in the DAG: its ID, dependencies, and routing metadata. */
export interface DagTask {
    readonly taskId: string;
    readonly downstream: string[];
    readonly queue?: string;
    readonly language: string;
}

/** Fluent DAG builder. Created by the `dag()` function. */
export class DagBuilder {
    readonly dagId: string;
    readonly options: DagOptions;
    private readonly _tasks = new Map<string, DagTask>();

    constructor(dagId: string, options: DagOptions = {}) {
        if (!dagId || typeof dagId !== "string") {
            throw new Error("dag: dagId must be a non-empty string");
        }
        this.dagId = dagId;
        this.options = options;
    }

    /** All tasks in insertion order. */
    get tasks(): ReadonlyMap<string, DagTask> {
        return this._tasks;
    }

    /** Add a task with an inline handler. Auto-registers as `dag_id.task_id`. */
    task(taskId: string, handler: TaskHandler, opts?: TaskOpts): this;
    /** Add a task referencing a handler registered via `registerTask()`. */
    task(taskId: string, handlerRef: string, opts?: TaskOpts): this;
    /** Add a task with no handler (handler must be registered separately). */
    task(taskId: string, opts?: TaskOpts): this;
    task(
        taskId: string,
        handlerOrRefOrOpts?: TaskHandler | string | TaskOpts,
        maybeOpts?: TaskOpts,
    ): this {
        if (this._tasks.has(taskId)) {
            throw new Error(`dag "${this.dagId}": task "${taskId}" is already defined`);
        }

        let opts: TaskOpts | undefined;
        if (typeof handlerOrRefOrOpts === "function") {
            // Inline handler — auto-register as dag_id.task_id
            registerTask(`${this.dagId}.${taskId}`, handlerOrRefOrOpts);
            opts = maybeOpts;
        } else if (typeof handlerOrRefOrOpts === "string") {
            // String reference — no registration, resolved at execution time
            opts = maybeOpts;
        } else {
            // No handler, just opts
            opts = handlerOrRefOrOpts;
        }

        this._tasks.set(taskId, {
            taskId,
            downstream: opts?.downstream ? [...opts.downstream] : [],
            queue: opts?.queue,
            language: opts?.language ?? "typescript",
        });
        return this;
    }
}

// ---- Module-level DAG registry ----

const dagRegistry = new Map<string, DagBuilder>();

/** Define a DAG and register it for parse-mode serialization.
 *  Returns a fluent builder for adding tasks. */
export function dag(dagId: string, options?: DagOptions): DagBuilder {
    if (dagRegistry.has(dagId)) {
        throw new Error(`DAG "${dagId}" is already registered`);
    }
    const builder = new DagBuilder(dagId, options);
    dagRegistry.set(dagId, builder);
    return builder;
}

/** List all registered DAG builders. */
export function listRegisteredDags(): DagBuilder[] {
    return [...dagRegistry.values()];
}

/** Clear the DAG registry. Primarily for testing. */
export function clearDagRegistry(): void {
    dagRegistry.clear();
}
