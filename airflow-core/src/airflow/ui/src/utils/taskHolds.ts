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
import type {
  GridNodeResponse,
  NodeResponse,
  StructureDataResponse,
  TaskHoldResponse,
  TaskInstanceState,
} from "openapi/requests/types.gen";

// What the scheduler records as `released_by` when it marks a hold that has expired.
const EXPIRY_RELEASED_BY = "expiry";

/** The scope a hold covers, from which of `task_id` and `run_id` it sets. */
export type HoldScope = "run" | "task" | "taskInstance";

/** The coordinates a hold is matched against; `map_index` is deliberately absent, a hold covers every index. */
export type HoldTarget = {
  readonly dagId: string;
  readonly runId: string;
  readonly taskId: string;
};

export const getHoldScope = (hold: Pick<TaskHoldResponse, "run_id" | "task_id">): HoldScope =>
  hold.task_id !== null && hold.run_id !== null ? "taskInstance" : hold.task_id === null ? "run" : "task";

/**
 * Whether a hold covers a task instance: the same Dag, and the same task and run wherever the hold
 * sets them. Mirrors the backend match, so a hold on a task covers every run of it and a hold on a
 * run covers every task in it.
 */
export const holdMatches = (
  hold: Pick<TaskHoldResponse, "dag_id" | "run_id" | "task_id">,
  { dagId, runId, taskId }: HoldTarget,
): boolean =>
  hold.dag_id === dagId &&
  (hold.task_id === null || hold.task_id === taskId) &&
  (hold.run_id === null || hold.run_id === runId);

/**
 * What an active hold means where it is shown. A hold only stops a start or a resume and never changes
 * the state of a task instance, so what the lock does to it depends on how far along it is. Either the hold
 * is stopping the task now, or it is pending, with nothing to stop yet (see `isHoldLockPending`). A task
 * across its runs, and a run as a whole, have no state of their own and are simply held.
 */
export type HoldLockKind =
  "awaitingInput" | "deferred" | "finished" | "held" | "queued" | "run" | "running" | "task" | "waiting";

// The scheduler checks the lock before it moves a task instance out of `none`, so one that has not
// started waits where it is, and again before it queues a `scheduled` one. Every other task instance
// that is about to start again waits once it is `scheduled`: a retry or a reschedule moves there as
// usual when its time comes, and a deferred or awaiting-input task instance moves there when it
// resumes. An attempt that is already queued or running has passed both checks and carries on, so the
// hold stops its next start or resume: a retry, a clear, or resuming after it defers. One that has
// finished has nothing to stop yet, but if it is cleared the new attempt waits, so it shows a pending
// hold as well.
const lockKindByState: Record<TaskInstanceState, HoldLockKind> = {
  awaiting_input: "awaitingInput",
  deferred: "deferred",
  failed: "finished",
  queued: "queued",
  removed: "finished",
  restarting: "waiting",
  running: "running",
  scheduled: "held",
  skipped: "finished",
  success: "finished",
  up_for_reschedule: "held",
  up_for_retry: "held",
  upstream_failed: "finished",
};

// A lock is pending where the hold has nothing to stop yet: the attempt carries on or has finished,
// and the hold takes effect at its next start or resume. Everywhere else it is stopping the task now.
const pendingByLockKind: Record<HoldLockKind, boolean> = {
  awaitingInput: true,
  deferred: true,
  finished: true,
  held: false,
  queued: true,
  run: false,
  running: true,
  task: false,
  waiting: false,
};

/** Whether a lock is pending, with nothing to stop yet, rather than stopping the task now. */
export const isHoldLockPending = (kind: HoldLockKind): boolean => pendingByLockKind[kind];

/** Which line of `taskHold.dialog.stateLine` says what a hold does to a task instance in its state. */
export type HoldStateLineKey =
  "awaitingInput" | "deferred" | "finished" | "notStarted" | "queued" | "running" | "waitingToRun";

// A restarting task instance has no line. One with no state is told apart in `getHoldStateLineKey`.
const stateLineKeyByState: Record<TaskInstanceState, HoldStateLineKey | undefined> = {
  awaiting_input: "awaitingInput",
  deferred: "deferred",
  failed: "finished",
  queued: "queued",
  removed: "finished",
  restarting: undefined,
  running: "running",
  scheduled: "waitingToRun",
  skipped: "finished",
  success: "finished",
  up_for_reschedule: "waitingToRun",
  up_for_retry: "waitingToRun",
  upstream_failed: "finished",
};

/**
 * The line the hold dialog shows for a task instance in this state, or nothing for a state it has no
 * line for. A task instance with no state has not started.
 */
export const getHoldStateLineKey = (state: TaskInstanceState | null): HoldStateLineKey | undefined =>
  state === null ? "notStarted" : stateLineKeyByState[state];

/**
 * What a lock is shown for: a task instance, or, with `task_id` or `dag_run_id` left out, a task across
 * all of its runs or a run as a whole. A task instance response is one.
 */
export type HoldLockSubject = {
  readonly dag_id: string;
  /** Left out for a task across all of its runs. */
  readonly dag_run_id?: string;
  /** What a hold does to a task instance depends on it. Not read for a task or a run as a whole. */
  readonly state?: TaskInstanceState | null;
  /** Left out for a run as a whole. */
  readonly task_id?: string;
};

/**
 * The kind of lock to show for a subject, or nothing when no active hold covers it. A task instance is
 * covered by a hold that matches its Dag, its task and its run wherever the hold sets them, whatever its
 * map index, and a finished one is covered too: if it is cleared, the new attempt waits. A task or a run
 * as a whole is covered only by a hold set at exactly that scope, not by one on a single run of the task
 * or a single task of the run.
 */
export const getHoldLockKind = (
  holds: ReadonlyArray<TaskHoldResponse>,
  { dag_id: dagId, dag_run_id: runId, state, task_id: taskId }: HoldLockSubject,
): HoldLockKind | undefined => {
  const active = holds.filter((hold) => hold.active && hold.dag_id === dagId);

  if (taskId === undefined) {
    return active.some((hold) => hold.task_id === null && hold.run_id === runId) ? "run" : undefined;
  }

  if (runId === undefined) {
    return active.some((hold) => hold.run_id === null && hold.task_id === taskId) ? "task" : undefined;
  }

  const kind = state === null || state === undefined ? "waiting" : lockKindByState[state];

  return active.some((hold) => holdMatches(hold, { dagId, runId, taskId })) ? kind : undefined;
};

/**
 * The kind of lock to show for a subject when an active hold is stopping it now. It is the lock
 * `getHoldLockKind` gives, left out when that lock is pending, with nothing to stop yet: a task
 * instance that is running or has finished is not held up by it.
 */
export const getHeldNowLockKind = (
  holds: ReadonlyArray<TaskHoldResponse>,
  subject: HoldLockSubject,
): HoldLockKind | undefined => {
  const kind = getHoldLockKind(holds, subject);

  return kind === undefined || isHoldLockPending(kind) ? undefined : kind;
};

/** The run in view, with the state of each of its task instances by task id once they have loaded. */
export type HeldNowRun = {
  readonly id: string;
  readonly states: ReadonlyMap<string, TaskInstanceState | null> | undefined;
};

/**
 * The kind of lock of each task that an active hold is stopping now, by task id. With a run, a task is
 * judged by its task instance in that run, and one that has none yet counts as having no state; until
 * the states have loaded nothing can be said, so no task is held. Without a run, a task is held when a
 * hold on it in every run covers it. Which tasks to ask about is left to the caller, so that task groups
 * and other nodes can be left out.
 */
export const getHeldNowLockKinds = (
  holds: ReadonlyArray<TaskHoldResponse>,
  {
    dagId,
    run,
    taskIds,
  }: { readonly dagId: string; readonly run?: HeldNowRun; readonly taskIds: ReadonlyArray<string> },
): Map<string, HoldLockKind> => {
  const kinds = new Map<string, HoldLockKind>();

  if (holds.length === 0 || (run !== undefined && run.states === undefined)) {
    return kinds;
  }

  for (const taskId of taskIds) {
    const kind = getHeldNowLockKind(
      holds,
      run?.states === undefined
        ? { dag_id: dagId, task_id: taskId }
        : { dag_id: dagId, dag_run_id: run.id, state: run.states.get(taskId) ?? null, task_id: taskId },
    );

    if (kind !== undefined) {
      kinds.set(taskId, kind);
    }
  }

  return kinds;
};

export type DagHoldCount = { readonly count: number; readonly dagId: string };

/** The Dags that have active holds, with how many each has: the Dag with the most first, then by Dag id. */
export const countActiveHoldsByDag = (
  holds: ReadonlyArray<Pick<TaskHoldResponse, "active" | "dag_id">>,
): Array<DagHoldCount> => {
  const counts = new Map<string, number>();

  for (const hold of holds) {
    if (hold.active) {
      counts.set(hold.dag_id, (counts.get(hold.dag_id) ?? 0) + 1);
    }
  }

  return [...counts]
    .map(([dagId, count]) => ({ count, dagId }))
    .sort((first, second) => second.count - first.count || first.dagId.localeCompare(second.dagId));
};

/** The ids of the tasks the structure marks as mapped, including those inside a mapped task group. */
export const getMappedTaskIds = (nodes: ReadonlyArray<GridNodeResponse> | undefined): Set<string> => {
  const ids = new Set<string>();
  const collect = (list: ReadonlyArray<GridNodeResponse>) => {
    for (const node of list) {
      if (node.is_mapped === true) {
        ids.add(node.id);
      }
      collect(node.children ?? []);
    }
  };

  collect(nodes ?? []);

  return ids;
};

/**
 * Where to follow the scope of a hold to. A hold on a task and a run covers every map index, so for a
 * mapped task it links to the list of its instances in that run: the page of a single task instance
 * does not exist for it.
 */
export const getHoldScopeLink = (
  hold: Pick<TaskHoldResponse, "dag_id" | "run_id" | "task_id">,
  mappedTaskIds: ReadonlySet<string>,
): string => {
  const { dag_id: dagId, run_id: runId, task_id: taskId } = hold;

  if (taskId === null) {
    return `/dags/${dagId}/runs/${runId}`;
  }

  if (runId === null) {
    return `/dags/${dagId}/tasks/${taskId}`;
  }

  return `/dags/${dagId}/runs/${runId}/tasks/${taskId}${mappedTaskIds.has(taskId) ? "/mapped" : ""}`;
};

const flattenNodes = (nodes: ReadonlyArray<NodeResponse>): Array<NodeResponse> =>
  nodes.flatMap((node) => [node, ...flattenNodes(node.children ?? [])]);

/**
 * The teardowns downstream of a task, other than the task itself, as task ids. They are found through
 * all edges, transitively, and through join and group nodes: a group stands for the tasks in it.
 */
export const getDownstreamTeardownIds = (
  { edges, nodes }: Pick<StructureDataResponse, "edges" | "nodes">,
  taskId: string,
): Array<string> => {
  const all = flattenNodes(nodes);
  const byId = new Map(all.map((node) => [node.id, node]));
  const reached = new Set<string>();
  const pending = [taskId];

  for (let id = pending.pop(); id !== undefined; id = pending.pop()) {
    if (!reached.has(id)) {
      reached.add(id);
      pending.push(
        ...edges.filter((edge) => edge.source_id === id).map((edge) => edge.target_id),
        ...flattenNodes(byId.get(id)?.children ?? []).map((child) => child.id),
      );
    }
  }

  return all
    .filter((node) => node.setup_teardown_type === "teardown" && node.id !== taskId && reached.has(node.id))
    .map((node) => node.id);
};

/** The setups of the given teardowns: the sources of the setup-and-teardown edges that end in them. */
export const getSetupIds = (
  { edges }: Pick<StructureDataResponse, "edges">,
  teardownIds: ReadonlyArray<string>,
): Array<string> => [
  ...new Set(
    edges
      .filter((edge) => edge.is_setup_teardown === true && teardownIds.includes(edge.target_id))
      .map((edge) => edge.source_id),
  ),
];

/** Which `taskHold.dialog.info` note says what a hold does to teardowns, and the task ids it names. */
export type HoldTeardownNote = {
  readonly key: "teardown" | "teardownNoSetup" | "teardownRun";
  readonly setups: Array<string>;
  readonly teardowns: Array<string>;
};

/**
 * The note on the teardowns a hold holds back, or nothing when there are none. A hold on a task, or on
 * a task instance, holds the teardowns downstream of it. A hold on a run holds every teardown of the
 * Dag in it. Where the Dag structure is not known yet there is no note.
 */
export const getHoldTeardownNote = (
  structure: Pick<StructureDataResponse, "edges" | "nodes"> | undefined,
  scope: HoldScope,
  taskId: string | undefined,
): HoldTeardownNote | undefined => {
  if (structure === undefined) {
    return undefined;
  }

  if (scope === "run") {
    const runTeardowns = flattenNodes(structure.nodes)
      .filter((node) => node.setup_teardown_type === "teardown")
      .map((node) => node.id);

    return runTeardowns.length === 0
      ? undefined
      : { key: "teardownRun", setups: [], teardowns: runTeardowns };
  }

  const teardowns = taskId === undefined ? [] : getDownstreamTeardownIds(structure, taskId);

  if (teardowns.length === 0) {
    return undefined;
  }

  const setups = getSetupIds(structure, teardowns);

  return { key: setups.length === 0 ? "teardownNoSetup" : "teardown", setups, teardowns };
};

export type HoldStatus = "active" | "expired" | "released";

/** An inactive hold with no release time has run out its expiry before the scheduler recorded it. */
export const getHoldStatus = (
  hold: Pick<TaskHoldResponse, "active" | "released_at" | "released_by">,
): HoldStatus =>
  hold.active
    ? "active"
    : hold.released_at === null || hold.released_by === EXPIRY_RELEASED_BY
      ? "expired"
      : "released";

export type HoldFormErrors = {
  readonly expiresAt?: "past";
  readonly reason?: "required";
};

/** Validates the hold dialog's fields. `expiresAt` is an ISO datetime, or empty for a hold with no expiry. */
export const validateHoldForm = (
  { expiresAt, reason }: { readonly expiresAt: string; readonly reason: string },
  now: Date = new Date(),
): HoldFormErrors => ({
  ...(reason.trim() === "" ? { reason: "required" as const } : {}),
  ...(expiresAt !== "" && new Date(expiresAt).getTime() <= now.getTime()
    ? { expiresAt: "past" as const }
    : {}),
});
