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

// DAG serialization — produces Airflow's DagSerialization v3 format.
// Port of the Java SDK's Serde.kt. The output must byte-match what
// Python's DagSerialization.from_dict() expects to deserialize.

import type { DagBuilder, DagTask } from "./dag.js";
import { relative } from "node:path";

// ---- Defaults matching Airflow's DAG constructor ----

const DEFAULT_MAX_ACTIVE_TASKS = 16;
const DEFAULT_MAX_ACTIVE_RUNS = 16;
const DEFAULT_MAX_CONSECUTIVE_FAILED_DAG_RUNS = 0;

// ---- Value serialization (__type/__var encoding) ----

/** Recursively serialize a value with Airflow's type encoding.
 *  Primitives pass through; complex types are wrapped in
 *  `{ __type, __var }`. Matches Python's BaseSerialization.serialize(). */
export function serializeValue(value: unknown): unknown {
    if (value === null || value === undefined) return null;
    if (typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
        return value;
    }
    if (value instanceof Date) {
        return { __type: "datetime", __var: value.getTime() / 1000 };
    }
    if (value instanceof Set) {
        const items = [...value].map(serializeValue);
        try {
            items.sort((a, b) => String(a).localeCompare(String(b)));
        } catch { /* safe fallback — keep insertion order */ }
        return { __type: "set", __var: items };
    }
    if (value instanceof Map) {
        const obj: Record<string, unknown> = {};
        for (const [k, v] of value) obj[String(k)] = serializeValue(v);
        return { __type: "dict", __var: obj };
    }
    if (Array.isArray(value)) {
        return value.map(serializeValue);
    }
    if (typeof value === "object") {
        // Plain object → dict encoding
        const obj: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(value)) {
            obj[k] = serializeValue(v);
        }
        return { __type: "dict", __var: obj };
    }
    return String(value);
}

/** Unwrap a single level of __type/__var encoding. Non-decorated fields
 *  in Airflow's serialization format are serialized then unwrapped. */
function unwrap(value: unknown): unknown {
    if (
        value !== null &&
        typeof value === "object" &&
        !Array.isArray(value) &&
        "__type" in value &&
        "__var" in value
    ) {
        return (value as Record<string, unknown>).__var;
    }
    return value;
}

// ---- Timetable serialization ----

function serializeTimetable(schedule: string | null | undefined): Record<string, unknown> {
    if (schedule === null || schedule === undefined) {
        return { __type: "airflow.timetables.simple.NullTimetable", __var: {} };
    }
    if (schedule === "@once") {
        return { __type: "airflow.timetables.simple.OnceTimetable", __var: {} };
    }
    if (schedule === "@continuous") {
        return { __type: "airflow.timetables.simple.ContinuousTimetable", __var: {} };
    }
    // Everything else is a cron expression (including @daily, @hourly, etc.)
    return {
        __type: "airflow.timetables.trigger.CronTriggerTimetable",
        __var: {
            expression: schedule,
            timezone: "UTC",
            interval: 0.0,
            run_immediately: false,
        },
    };
}

// ---- Task serialization ----

function serializeTask(task: DagTask): Record<string, unknown> {
    const data: Record<string, unknown> = {
        task_id: task.taskId,
        task_type: "TypeScriptTask",
        _task_module: "airflow.ts_sdk",
        language: task.language,
    };
    if (task.queue) {
        data.queue = task.queue;
    }
    if (task.downstream.length > 0) {
        data.downstream_task_ids = [...task.downstream].sort();
    }
    return { __type: "operator", __var: data };
}

// ---- Task group serialization (flat root group) ----

function serializeTaskGroup(taskIds: string[]): Record<string, unknown> {
    const children: Record<string, [string, string]> = {};
    for (const id of taskIds) {
        children[id] = ["operator", id];
    }
    return {
        _group_id: null,
        group_display_name: "",
        prefix_group_id: true,
        tooltip: "",
        ui_color: "CornflowerBlue",
        ui_fgcolor: "#000",
        children,
        upstream_group_ids: [],
        downstream_group_ids: [],
        upstream_task_ids: [],
        downstream_task_ids: [],
    };
}

// ---- Params serialization ----

function serializeParams(params: Record<string, unknown>): unknown[] {
    return Object.entries(params).map(([k, v]) => [
        k,
        {
            __class: "airflow.sdk.definitions.param.Param",
            default: serializeValue(v),
            description: null,
            schema: serializeValue({}),
            source: null,
        },
    ]);
}

// ---- DAG serialization ----

/** Serialize a single DAG to the Airflow v3 format. */
export function serializeDag(
    dag: DagBuilder,
    fileloc: string,
    relativeFileloc: string,
): Record<string, unknown> {
    const opts = dag.options;
    const taskIds = [...dag.tasks.keys()];

    const result: Record<string, unknown> = {
        // Required fields (always present)
        dag_id: dag.dagId,
        fileloc,
        relative_fileloc: relativeFileloc,
        timezone: "UTC",
        timetable: serializeTimetable(opts.schedule),
        tasks: [...dag.tasks.values()].map(serializeTask),
        dag_dependencies: [],
        task_group: serializeTaskGroup(taskIds),
        edge_info: {},
        params: opts.params ? serializeParams(opts.params) : [],
        deadline: null,
        allowed_run_types: null,
    };

    // Optional fields — only include if non-null/non-default
    if (opts.description != null) result.description = opts.description;
    if (opts.startDate != null) result.start_date = unwrap(serializeValue(opts.startDate));
    if (opts.endDate != null) result.end_date = unwrap(serializeValue(opts.endDate));
    if (opts.dagrunTimeout != null) result.dagrun_timeout = opts.dagrunTimeout;
    if (opts.docMd != null) result.doc_md = opts.docMd;
    if (opts.isPausedUponCreation != null) result.is_paused_upon_creation = opts.isPausedUponCreation;

    // Decorated fields (full __type/__var encoding, NOT unwrapped)
    if (opts.defaultArgs && Object.keys(opts.defaultArgs).length > 0) {
        result.default_args = serializeValue(opts.defaultArgs);
    }
    if (opts.accessControl != null) {
        result.access_control = serializeValue(opts.accessControl);
    }

    // Boolean flags — excluded when matching schema defaults
    if (opts.catchup) result.catchup = true;
    if (opts.failFast) result.fail_fast = true;
    if (opts.renderTemplateAsNativeObj) result.render_template_as_native_obj = true;

    // Numeric fields — excluded when matching schema defaults
    if (opts.maxActiveTasks != null && opts.maxActiveTasks !== DEFAULT_MAX_ACTIVE_TASKS) {
        result.max_active_tasks = opts.maxActiveTasks;
    }
    if (opts.maxActiveRuns != null && opts.maxActiveRuns !== DEFAULT_MAX_ACTIVE_RUNS) {
        result.max_active_runs = opts.maxActiveRuns;
    }
    if (opts.maxConsecutiveFailedDagRuns != null && opts.maxConsecutiveFailedDagRuns !== DEFAULT_MAX_CONSECUTIVE_FAILED_DAG_RUNS) {
        result.max_consecutive_failed_dag_runs = opts.maxConsecutiveFailedDagRuns;
    }

    // dag_display_name — excluded when it equals dag_id (the default)
    if (opts.dagDisplayName != null && opts.dagDisplayName !== dag.dagId) {
        result.dag_display_name = opts.dagDisplayName;
    }

    // Collection fields — serialized then unwrapped; excluded when empty
    if (opts.tags && opts.tags.length > 0) {
        result.tags = unwrap(serializeValue(new Set(opts.tags)));
    }
    if (opts.ownerLinks && Object.keys(opts.ownerLinks).length > 0) {
        result.owner_links = unwrap(serializeValue(opts.ownerLinks));
    }

    return result;
}

// ---- Top-level parsing result ----

function computeRelativeFileloc(fileloc: string, bundlePath: string): string {
    if (!fileloc) return "";
    if (!bundlePath) return ".";
    const rel = relative(bundlePath, fileloc);
    return rel || ".";
}

/** Serialize the full DagFileParsingResult response. */
export function serializeParsingResult(
    dags: DagBuilder[],
    fileloc: string,
    bundlePath: string,
): Record<string, unknown> {
    const relativeFileloc = computeRelativeFileloc(fileloc, bundlePath);
    return {
        type: "DagFileParsingResult",
        fileloc,
        serialized_dags: dags.map((d) => ({
            data: {
                __version: 3,
                dag: serializeDag(d, fileloc, relativeFileloc),
            },
        })),
    };
}
