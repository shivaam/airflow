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
import type { TaskHoldResponse, TaskInstanceState } from "openapi/requests/types.gen";

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

// A hold only parks a task instance that is still waiting to be queued. One that already finished,
// or that is queued or running, is left alone, so a matching hold changes nothing for it.
const unaffectedByHoldStates: ReadonlySet<TaskInstanceState> = new Set<TaskInstanceState>([
  "failed",
  "queued",
  "removed",
  "running",
  "skipped",
  "success",
  "upstream_failed",
]);

/**
 * Whether an active hold covers a task instance that the scheduler has not parked yet, so it shows
 * "hold pending" until the scheduler's next pass moves it to `held`.
 */
export const isHoldPending = (
  holds: ReadonlyArray<TaskHoldResponse>,
  taskInstance: {
    readonly dag_id: string;
    readonly dag_run_id: string;
    readonly state?: TaskInstanceState | null;
    readonly task_id: string;
  },
): boolean => {
  const { state } = taskInstance;

  if (state === "held" || (state !== null && state !== undefined && unaffectedByHoldStates.has(state))) {
    return false;
  }

  const target = {
    dagId: taskInstance.dag_id,
    runId: taskInstance.dag_run_id,
    taskId: taskInstance.task_id,
  };

  return holds.some((hold) => hold.active && holdMatches(hold, target));
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
