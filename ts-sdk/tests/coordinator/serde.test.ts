import { describe, it, expect, beforeEach } from "vitest";
import { DagBuilder } from "../../src/coordinator/dag.js";
import { serializeDag, serializeValue, serializeParsingResult } from "../../src/coordinator/serde.js";

describe("serializeValue", () => {
    it("passes primitives through", () => {
        expect(serializeValue(null)).toBe(null);
        expect(serializeValue(undefined)).toBe(null);
        expect(serializeValue("hello")).toBe("hello");
        expect(serializeValue(42)).toBe(42);
        expect(serializeValue(true)).toBe(true);
    });

    it("encodes Date as datetime __type/__var", () => {
        const d = new Date("2026-01-01T00:00:00Z");
        expect(serializeValue(d)).toEqual({
            __type: "datetime",
            __var: d.getTime() / 1000,
        });
    });

    it("encodes Set with sorted items", () => {
        const result = serializeValue(new Set(["c", "a", "b"]));
        expect(result).toEqual({
            __type: "set",
            __var: ["a", "b", "c"],
        });
    });

    it("encodes plain object as dict", () => {
        expect(serializeValue({ key: "val" })).toEqual({
            __type: "dict",
            __var: { key: "val" },
        });
    });

    it("encodes Array by mapping items", () => {
        expect(serializeValue([1, "two", null])).toEqual([1, "two", null]);
    });

    it("recursively encodes nested structures", () => {
        const result = serializeValue({ nested: { deep: true } });
        expect(result).toEqual({
            __type: "dict",
            __var: {
                nested: { __type: "dict", __var: { deep: true } },
            },
        });
    });
});

describe("serializeDag", () => {
    it("produces minimal DAG with required fields", () => {
        const d = new DagBuilder("minimal_dag");
        const result = serializeDag(d, "/dags/test.mjs", "test.mjs");

        expect(result.dag_id).toBe("minimal_dag");
        expect(result.fileloc).toBe("/dags/test.mjs");
        expect(result.relative_fileloc).toBe("test.mjs");
        expect(result.timezone).toBe("UTC");
        expect(result.tasks).toEqual([]);
        expect(result.dag_dependencies).toEqual([]);
        expect(result.edge_info).toEqual({});
        expect(result.params).toEqual([]);
        expect(result.deadline).toBeNull();
        expect(result.allowed_run_types).toBeNull();
        // Timetable defaults to NullTimetable
        expect(result.timetable).toEqual({
            __type: "airflow.timetables.simple.NullTimetable",
            __var: {},
        });
    });

    it("serializes tasks with dependencies", () => {
        const d = new DagBuilder("task_dag");
        d.task("extract", { downstream: ["transform"] });
        d.task("transform", { downstream: ["load"] });
        d.task("load");

        const result = serializeDag(d, "", "");
        const tasks = result.tasks as { __type: string; __var: Record<string, unknown> }[];

        expect(tasks).toHaveLength(3);
        expect(tasks[0]!.__type).toBe("operator");
        expect(tasks[0]!.__var.task_id).toBe("extract");
        expect(tasks[0]!.__var.language).toBe("typescript");
        expect(tasks[0]!.__var.downstream_task_ids).toEqual(["transform"]);
        expect(tasks[2]!.__var.downstream_task_ids).toBeUndefined(); // no downstream
    });

    it("serializes task_group with children", () => {
        const d = new DagBuilder("tg_dag");
        d.task("a").task("b");

        const result = serializeDag(d, "", "");
        const tg = result.task_group as Record<string, unknown>;

        expect(tg._group_id).toBeNull();
        expect(tg.children).toEqual({
            a: ["operator", "a"],
            b: ["operator", "b"],
        });
    });

    it("includes optional fields when set", () => {
        const d = new DagBuilder("full_dag", {
            description: "A test DAG",
            startDate: new Date("2026-01-01T00:00:00Z"),
            catchup: true,
            tags: ["test", "alpha"],
            maxActiveTasks: 8,
            docMd: "# My DAG",
            isPausedUponCreation: true,
            dagDisplayName: "Full DAG (Display)",
        });

        const result = serializeDag(d, "", "");

        expect(result.description).toBe("A test DAG");
        expect(result.start_date).toBe(new Date("2026-01-01T00:00:00Z").getTime() / 1000);
        expect(result.catchup).toBe(true);
        expect(result.tags).toEqual(expect.arrayContaining(["alpha", "test"]));
        expect(result.max_active_tasks).toBe(8);
        expect(result.doc_md).toBe("# My DAG");
        expect(result.is_paused_upon_creation).toBe(true);
        expect(result.dag_display_name).toBe("Full DAG (Display)");
    });

    it("excludes fields matching schema defaults", () => {
        const d = new DagBuilder("default_dag", {
            catchup: false,
            maxActiveTasks: 16,
            maxActiveRuns: 16,
            maxConsecutiveFailedDagRuns: 0,
            dagDisplayName: "default_dag", // same as dagId — excluded
        });

        const result = serializeDag(d, "", "");

        expect(result.catchup).toBeUndefined();
        expect(result.max_active_tasks).toBeUndefined();
        expect(result.max_active_runs).toBeUndefined();
        expect(result.max_consecutive_failed_dag_runs).toBeUndefined();
        expect(result.dag_display_name).toBeUndefined();
    });

    it("serializes params with Param schema", () => {
        const d = new DagBuilder("param_dag", {
            params: { batch_size: 100, env: "prod" },
        });

        const result = serializeDag(d, "", "");
        const params = result.params as unknown[][];

        expect(params).toHaveLength(2);
        const [name, schema] = params[0] as [string, Record<string, unknown>];
        expect(name).toBe("batch_size");
        expect(schema.__class).toBe("airflow.sdk.definitions.param.Param");
        expect(schema.default).toBe(100);
    });
});

describe("timetable serialization", () => {
    function timetableOf(schedule?: string | null) {
        const d = new DagBuilder("tt_test", { schedule });
        return serializeDag(d, "", "").timetable as Record<string, unknown>;
    }

    it("null → NullTimetable", () => {
        expect(timetableOf(null).__type).toBe("airflow.timetables.simple.NullTimetable");
    });

    it("undefined → NullTimetable", () => {
        expect(timetableOf(undefined).__type).toBe("airflow.timetables.simple.NullTimetable");
    });

    it("@once → OnceTimetable", () => {
        expect(timetableOf("@once").__type).toBe("airflow.timetables.simple.OnceTimetable");
    });

    it("@continuous → ContinuousTimetable", () => {
        expect(timetableOf("@continuous").__type).toBe("airflow.timetables.simple.ContinuousTimetable");
    });

    it("cron expression → CronTriggerTimetable", () => {
        const tt = timetableOf("0 0 * * *");
        expect(tt.__type).toBe("airflow.timetables.trigger.CronTriggerTimetable");
        const vars = tt.__var as Record<string, unknown>;
        expect(vars.expression).toBe("0 0 * * *");
        expect(vars.timezone).toBe("UTC");
    });

    it("@daily → CronTriggerTimetable", () => {
        const tt = timetableOf("@daily");
        expect(tt.__type).toBe("airflow.timetables.trigger.CronTriggerTimetable");
        expect((tt.__var as Record<string, unknown>).expression).toBe("@daily");
    });
});

describe("serializeParsingResult", () => {
    it("produces DagFileParsingResult envelope", () => {
        const d = new DagBuilder("envelope_test", { schedule: "@once" });
        d.task("t1");

        const result = serializeParsingResult([d], "/dags/test.mjs", "/dags");

        expect(result.type).toBe("DagFileParsingResult");
        expect(result.fileloc).toBe("/dags/test.mjs");

        const serializedDags = result.serialized_dags as { data: Record<string, unknown> }[];
        expect(serializedDags).toHaveLength(1);
        expect(serializedDags[0]!.data.__version).toBe(3);

        const dag = serializedDags[0]!.data.dag as Record<string, unknown>;
        expect(dag.dag_id).toBe("envelope_test");
        expect(dag.relative_fileloc).toBe("test.mjs");
    });

    it("supports multiple DAGs", () => {
        const d1 = new DagBuilder("dag_a");
        const d2 = new DagBuilder("dag_b");

        const result = serializeParsingResult([d1, d2], "/f", "/");
        const serializedDags = result.serialized_dags as unknown[];
        expect(serializedDags).toHaveLength(2);
    });
});
