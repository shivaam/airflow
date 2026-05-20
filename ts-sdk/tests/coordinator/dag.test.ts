import { describe, it, expect, beforeEach } from "vitest";
import { dag, DagBuilder, listRegisteredDags, clearDagRegistry } from "../../src/coordinator/dag.js";
import { clearRegistry, getRegisteredTask } from "../../src/registry.js";

describe("DagBuilder", () => {
    beforeEach(() => {
        clearDagRegistry();
        clearRegistry();
    });

    it("creates a DAG with fluent .task() chaining", () => {
        const d = new DagBuilder("test_dag", { schedule: "@daily" });
        d.task("a", { downstream: ["b"] }).task("b");

        expect(d.dagId).toBe("test_dag");
        expect(d.options.schedule).toBe("@daily");
        expect(d.tasks.size).toBe(2);
        expect(d.tasks.get("a")?.downstream).toEqual(["b"]);
        expect(d.tasks.get("b")?.downstream).toEqual([]);
    });

    it("rejects duplicate task IDs", () => {
        const d = new DagBuilder("test_dag");
        d.task("a");
        expect(() => d.task("a")).toThrow('task "a" is already defined');
    });

    it("auto-registers inline handlers as dag_id.task_id", () => {
        const handler = async () => "ok";
        const d = new DagBuilder("my_dag");
        d.task("extract", handler);

        expect(getRegisteredTask("my_dag.extract")).toBe(handler);
    });

    it("accepts string references without registering", () => {
        const d = new DagBuilder("my_dag");
        d.task("cleanup", "shared_cleanup");

        expect(d.tasks.has("cleanup")).toBe(true);
        // No auto-registration — the reference is resolved at execution time
        expect(getRegisteredTask("my_dag.cleanup")).toBeUndefined();
    });

    it("accepts tasks with no handler or reference", () => {
        const d = new DagBuilder("my_dag");
        d.task("work", { downstream: ["done"] });
        d.task("done");

        expect(d.tasks.size).toBe(2);
    });
});

describe("dag() registry", () => {
    beforeEach(() => {
        clearDagRegistry();
        clearRegistry();
    });

    it("registers and lists DAGs", () => {
        dag("dag_a", { schedule: "@hourly" }).task("t1");
        dag("dag_b").task("t2");

        const dags = listRegisteredDags();
        expect(dags).toHaveLength(2);
        expect(dags.map((d) => d.dagId).sort()).toEqual(["dag_a", "dag_b"]);
    });

    it("rejects duplicate DAG IDs", () => {
        dag("my_dag");
        expect(() => dag("my_dag")).toThrow('DAG "my_dag" is already registered');
    });

    it("clearDagRegistry removes all DAGs", () => {
        dag("a");
        dag("b");
        clearDagRegistry();
        expect(listRegisteredDags()).toHaveLength(0);
    });

    it("rejects empty dagId", () => {
        expect(() => dag("")).toThrow("dagId must be a non-empty string");
    });
});

describe("DagBuilder edge cases", () => {
    beforeEach(() => {
        clearDagRegistry();
        clearRegistry();
    });

    it("inline handler with downstream deps registers and wires deps", () => {
        const handler = async () => "done";
        const d = new DagBuilder("wired_dag");
        d.task("step1", handler, { downstream: ["step2"] });
        d.task("step2", async () => {});

        expect(getRegisteredTask("wired_dag.step1")).toBe(handler);
        expect(d.tasks.get("step1")?.downstream).toEqual(["step2"]);
    });

    it("supports many tasks in a chain", () => {
        const d = new DagBuilder("chain_dag");
        for (let i = 0; i < 10; i++) {
            const next = i < 9 ? [`t${i + 1}`] : undefined;
            d.task(`t${i}`, next ? { downstream: next } : undefined);
        }
        expect(d.tasks.size).toBe(10);
        expect(d.tasks.get("t0")?.downstream).toEqual(["t1"]);
        expect(d.tasks.get("t9")?.downstream).toEqual([]);
    });

    it("dag() returns the builder for immediate chaining", () => {
        const builder = dag("chained").task("a").task("b", { downstream: ["a"] });
        expect(builder).toBeInstanceOf(DagBuilder);
        expect(builder.tasks.size).toBe(2);
    });

    it("tasks default to language=typescript", () => {
        const d = new DagBuilder("lang_dag");
        d.task("ts_task");
        expect(d.tasks.get("ts_task")?.language).toBe("typescript");
    });

    it("supports queue and language for cross-runtime routing", () => {
        const d = new DagBuilder("polyglot");
        d.task("fetch_data", async () => "data", { downstream: ["java_etl"] });
        d.task("java_etl", { queue: "java-runtime", language: "java", downstream: ["summarize"] });
        d.task("summarize", async () => "done");

        const javaTask = d.tasks.get("java_etl")!;
        expect(javaTask.queue).toBe("java-runtime");
        expect(javaTask.language).toBe("java");

        const tsTask = d.tasks.get("fetch_data")!;
        expect(tsTask.queue).toBeUndefined();
        expect(tsTask.language).toBe("typescript");
    });
});
