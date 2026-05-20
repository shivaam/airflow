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
