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

# ts-sdk wire-contract tracking — design

**Status:** draft
**Date:** 2026-05-23
**Phase:** A (second to implement — depends on [spec B](2026-05-23-ts-sdk-ci-workflow-design.md) being in place to gate the prek hook)
**Related:** [spec B — CI workflow](2026-05-23-ts-sdk-ci-workflow-design.md), [spec C — e2e integration tests](2026-05-23-ts-sdk-e2e-integration-tests-design.md)
**Upstream context:**
- [PR #65956](https://github.com/apache/airflow/pull/65956) — Java SDK (merge 1 of 3)
- [PR #65958](https://github.com/apache/airflow/pull/65958) — Coordinator layer + Java coordinator (merge 2 of 3)
- [PR #65959](https://github.com/apache/airflow/pull/65959) — Java CI + e2e + prek hooks (merge 3 of 3)
- [PR #67235](https://github.com/apache/airflow/pull/67235) — **merged 2026-05-22** — supervisor-side Cadwyn migration framework

## Goal

Keep our ts-sdk's wire types in lockstep with the upstream supervisor schema, and surface drift before it ships. Establish a documented convention by which the `TypescriptCoordinator` (Python side) learns which schema version our bundle was built against, so the supervisor's [Cadwyn migration framework](https://github.com/apache/airflow/pull/67235) can upgrade/downgrade frames bidirectionally.

## Why

PR #67235 (already merged) ships a JSON schema file describing the supervisor's wire models plus a Cadwyn migration structure. The deal it offers SDKs is:

> "Tell me which schema version you were built against. I'll upgrade your incoming messages to my current shape and downgrade outgoing messages to yours."

The Java SDK communicates its version via the `Airflow-Java-SDK-Version` header in the JAR manifest, which `JavaCoordinator` reads and passes to `_JavaActivitySubprocess.subprocess_schema_version`. That value is then plumbed through `WatchedSubprocess.send_msg` / `handle_requests` to drive the migrator per-frame.

The ts-sdk has no equivalent today. Without it:
- Frame migration doesn't run for our bundles — we silently rely on byte-matching whatever the supervisor sends.
- When upstream changes a frame shape, our generated types silently drift from what arrives on the wire — failures show up only at runtime.
- Multiple TS bundles in one Airflow install all have to ship against the same supervisor version.

## What's already done (commit `f54fd43a10`)

Substantial foundation landed in the most recent commit:

- ✅ **Vendored** [`ts-sdk/schema/supervisor-schema.json`](../../../ts-sdk/schema/supervisor-schema.json) from upstream `task-sdk/src/airflow/sdk/execution_time/schema/schema.json`.
- ✅ **Codegen script** [`ts-sdk/scripts/generate-supervisor.mjs`](../../../ts-sdk/scripts/generate-supervisor.mjs) uses `json-schema-to-typescript` to emit `src/generated/supervisor.ts` with one interface per message type and `SUPERVISOR_API_VERSION` constant.
- ✅ **Wrapper layer** in [`ts-sdk/src/coordinator/protocol.ts`](../../../ts-sdk/src/coordinator/protocol.ts) narrows the generated atoms to the SDK-facing shapes (discriminator narrowing, pass-through field extensions, string→literal-union narrowing, optional→required for downstream API validators).
- ✅ **Version exposure**: `SUPERVISOR_API_VERSION` re-exported from `coordinator/index.ts` and `src/index.ts`, logged in [`runtime.ts`](../../../ts-sdk/src/coordinator/runtime.ts) on startup.
- ✅ **Build hook**: `pnpm run generate:supervisor` regenerates types from the vendored snapshot.

This spec covers what remains.

## What's left

Three concrete deliverables, each in its own PR:

| # | Deliverable | Where | Upstream or local |
|---|---|---|---|
| A1 | Prek hook detecting drift between vendored snapshot and upstream `schema.json` | `scripts/ci/prek/check_ts_sdk_supervisor_schema.py` + `.pre-commit-config.yaml` | Local |
| A2 | `airflow-metadata.yaml` convention for TS bundles | Documented in [`ts-sdk/COORDINATOR.md`](../../../ts-sdk/COORDINATOR.md); example in [spec C](2026-05-23-ts-sdk-e2e-integration-tests-design.md)'s fixture bundles | Local doc + convention |
| A3 | `TypescriptCoordinator` learns to read `airflow-metadata.yaml` and pass `subprocess_schema_version` to supervisor | `task-sdk/src/airflow/sdk/coordinators/typescript/coordinator.py` (upstream) | **Upstream PR** — depends on #65958 merging first |

A1 is the highest-leverage and lowest-risk piece. A2 is documentation. A3 is the upstream PR that completes the loop.

## A1 — Prek hook: schema-drift detection {#prek-hook-schema-drift-detection}

### Architecture

A Python prek hook at `scripts/ci/prek/check_ts_sdk_supervisor_schema.py` that:

1. Reads the canonical upstream schema from `task-sdk/src/airflow/sdk/execution_time/schema/schema.json` (the source of truth maintained by `generate-supervisor-schemas-snapshot` from upstream).
2. Reads our vendored copy from `ts-sdk/schema/supervisor-schema.json`.
3. Deep-diffs (`json.dumps(sort_keys=True)` byte compare suffices — both files are deterministic).
4. If they differ:
   - Fail the hook.
   - Print the diff (truncated to first 50 lines for readability).
   - Print the remediation: `cd ts-sdk && cp ../task-sdk/src/airflow/sdk/execution_time/schema/schema.json schema/supervisor-schema.json && pnpm run generate:supervisor && git add schema/ src/generated/`.

This mirrors the structure of upstream's [`check_supervisor_schemas_versions.py`](https://github.com/apache/airflow/pull/65958/files) prek hook from PR #65958.

### Files

| Path | Change |
|---|---|
| `scripts/ci/prek/check_ts_sdk_supervisor_schema.py` | New file, ~80 lines |
| [`.pre-commit-config.yaml`](../../../.pre-commit-config.yaml) | Add hook registration in the appropriate section (manual stage if regen is heavy; pre-commit stage if cheap) |

### Hook registration

```yaml
- id: check-ts-sdk-supervisor-schema
  name: Check ts-sdk vendored supervisor schema against upstream
  description: >-
    Verify that ts-sdk/schema/supervisor-schema.json matches the
    canonical task-sdk/src/airflow/sdk/execution_time/schema/schema.json.
    If they differ, copy the upstream file and regenerate
    ts-sdk/src/generated/supervisor.ts.
  entry: ./scripts/ci/prek/check_ts_sdk_supervisor_schema.py
  language: python
  pass_filenames: false
  files: >
    (?x)
    ^task-sdk/src/airflow/sdk/execution_time/schema/schema\.json$|
    ^ts-sdk/schema/supervisor-schema\.json$|
    ^ts-sdk/scripts/generate-supervisor\.mjs$|
    ^ts-sdk/src/generated/supervisor\.ts$
```

**Stage decision**: leave at the default `pre-commit` stage (not `manual`). The check is pure Python, milliseconds — no Node required, no gradle. Cheap enough to run every commit. (Contrast: Java's `check-java-serialization-compatibility` is `manual` because it runs gradle.)

### Why a Python hook, not a Node hook

Consistency with the rest of `scripts/ci/prek/`. Node isn't part of every contributor's setup; Python is. The hook only reads two JSON files — Python's `json` stdlib is sufficient.

### What this hook does NOT do

- It does **not** regenerate `src/generated/supervisor.ts` automatically. That requires Node. We surface the regeneration command in the error message and leave the user to run it. (We could write a follow-up hook that does this if it becomes a friction point.)
- It does **not** verify the wrapping in `protocol.ts` is up-to-date for new message types. That requires reading the generated TS and comparing to the wrapper exports — a TypeScript-only operation, awkward to do in a Python hook. **Tracked as a follow-up**: we may eventually want a lint rule (or just a periodic manual sweep) confirming wrapper coverage.

## A2 — `airflow-metadata.yaml` convention {#airflow-metadata-yaml-convention}

### Convention

Each TS bundle ships an `airflow-metadata.yaml` file alongside its `bundle.mjs` (or alongside its directory if there are multiple bundles per folder). The file declares:

```yaml
# airflow-metadata.yaml — shipped next to bundle.mjs by the user's build step
supervisor_schema_version: "0.1"   # value of SUPERVISOR_API_VERSION at build time
dags:
  my_dag:           {entry: bundle.mjs}      # dag_id → entry .mjs (relative to this file)
  daily_pipeline:   {entry: bundle.mjs}
```

**Why YAML and not JSON or TOML.** Matches the Java convention (`Airflow-Java-SDK-Metadata` points to a YAML file embedded in the JAR). Lower cognitive cost for cross-SDK contributors.

**Why two top-level keys.** `supervisor_schema_version` is what the supervisor needs for the migrator. `dags` is what the `TypescriptCoordinator` needs to route a task to the right bundle when one `bundles_folder` contains multiple bundle subdirectories — analogous to Java's `BundleScanner` matching dag_id against per-JAR metadata.

**Why `entry:` as an object value instead of a bare string.** Forward-compat. Future per-DAG metadata (worker concurrency hints, timeout overrides, etc.) can be added under the same key without breaking the read path.

### How bundles author this file

Two options for users, documented in `ts-sdk/COORDINATOR.md`:

1. **Manual** — write the YAML by hand. Fine for one DAG, one bundle.
2. **Programmatic** — emit it from the build script alongside `esbuild`. Recommended pattern (sketch, not part of this spec to implement):

   ```ts
   // build.mjs
   import { writeFileSync } from "node:fs";
   import { SUPERVISOR_API_VERSION } from "@apache-airflow/ts-sdk";
   import { stringify } from "yaml";

   writeFileSync("dist/airflow-metadata.yaml", stringify({
     supervisor_schema_version: SUPERVISOR_API_VERSION,
     dags: { my_dag: { entry: "bundle.mjs" } },
   }));
   ```

   The `SUPERVISOR_API_VERSION` re-export from `@apache-airflow/ts-sdk` is what makes this trivially correct — users never hand-type the version.

### Documentation home

Add a new section to [`ts-sdk/COORDINATOR.md`](../../../ts-sdk/COORDINATOR.md) titled "Bundle metadata" between "Wire protocol" and "Task lookup precedence". It explains the file shape, the version-pinning purpose, and shows the programmatic build snippet above.

No code change in `ts-sdk` source — this is convention + docs only.

### What this convention does NOT cover

- Per-DAG configuration (concurrency, retry hints, etc.) — the `entry:` object structure leaves room for this but we don't define any extra keys yet.
- Pure-`.mjs` DAGs (no Python stub): the metadata file is unused in that case because the `.mjs` *is* the bundle, and the dag_id mapping is inferred elsewhere. Documented as an explicit non-applicable case.
- Schema validation of `airflow-metadata.yaml` itself: defer until misuse becomes a concrete problem.

## A3 — Upstream PR: `TypescriptCoordinator` reads metadata {#upstream-coordinator-pr}

### Scope

A separate PR against `apache/airflow` (depends on PR #65958 merging first). It changes [`task-sdk/src/airflow/sdk/coordinators/typescript/coordinator.py`](https://github.com/apache/airflow/pull/65958/files) — currently in PR #65958 itself, mirroring `JavaCoordinator`'s `BundleScanner` pattern at a smaller scale.

Two behavior changes:

1. **Read `airflow-metadata.yaml` from each subdirectory of `bundles_folder`.** Build a `dag_id → (bundle_dir, entry_mjs, supervisor_schema_version)` map at scan time, cached.
2. **Resolve the entry per task** using the dag_id. Currently the coordinator hardcodes `"bundle.mjs"` per the TODO at lines 113-120 of the testing branch. After this change, the entry comes from the metadata file.
3. **Pass `supervisor_schema_version` to `_TypescriptActivitySubprocess`** (analogous to how `JavaCoordinator` passes the JAR's manifest version), which the supervisor uses as `_subprocess_schema_version` to drive the migrator.

### Scope boundary

This PR does NOT belong in *our* repository. It belongs in `apache/airflow`. We track it here because:
- We are the team who needs it to work for our spec C integration tests.
- Without it, our `SUPERVISOR_API_VERSION` constant is purely cosmetic — the supervisor never learns it.
- It is the bridge that connects our local A1/A2 work to a real production benefit.

### Dependencies

- PR #65958 must merge first (defines the `TypescriptCoordinator` class to edit).
- This spec's A2 convention must be agreed (defines the file format the coordinator reads).

### Risk

The upstream PR may face design pushback (e.g. reviewers may prefer a single registry-style metadata file instead of per-bundle ones). That's fine — the local A1/A2 work is independently useful (drift detection + version exposure) regardless of A3's final shape.

## Files added / changed (this spec, local only)

| Path | Change | Size |
|---|---|---|
| `scripts/ci/prek/check_ts_sdk_supervisor_schema.py` | New file | ~80 lines |
| [`.pre-commit-config.yaml`](../../../.pre-commit-config.yaml) | Add hook registration | ~15 lines |
| [`ts-sdk/COORDINATOR.md`](../../../ts-sdk/COORDINATOR.md) | New "Bundle metadata" section | ~40 lines |

A3 is tracked separately and PR'd against `apache/airflow`. No file changes in this repo for A3.

## Acceptance criteria

1. The prek hook is registered and runs on `pre-commit` stage.
2. Manually corrupting `ts-sdk/schema/supervisor-schema.json` (e.g. adding a stray key) and committing fails the hook with an actionable error message including the regen command.
3. Updating the upstream `task-sdk/src/airflow/sdk/execution_time/schema/schema.json` and *not* updating our vendored copy fails the hook.
4. After running the regen command in the hook's error message, the hook passes.
5. `ts-sdk/COORDINATOR.md` has a "Bundle metadata" section showing the YAML shape and the programmatic build snippet.
6. The build snippet in COORDINATOR.md compiles when extracted to a test file (sanity check only — no formal test).

## Risks and open questions

### Risk: hook fires on unrelated PRs

The `files:` filter triggers on any change to the listed paths. A PR that touches `schema.json` upstream for unrelated reasons (a typo fix in a description field) will require us to bump the vendored copy and regenerate. That's the **point** — we want every schema change visible — but it does mean small upstream tweaks cause our PR queue to need rebases. Acceptable cost.

### Risk: `airflow-metadata.yaml` convention churn

The Java side may rev its metadata format in a future release; we'd want to stay aligned. Mitigation: the convention lives in [`ts-sdk/COORDINATOR.md`](../../../ts-sdk/COORDINATOR.md), a single doc to update. The format is intentionally minimal (two keys) so churn is unlikely.

### Open question: should `SUPERVISOR_API_VERSION` be sent on the wire too?

Today it's exposed at runtime but not sent. The plan is: out-of-band via `airflow-metadata.yaml` → coordinator → supervisor. An alternative would be: send it in a `Ready` handshake frame before `StartupDetails`. That requires an upstream protocol change (a new frame type), discussed in the broader DESIGN-LIMITATIONS doc but deferred to a future AIP-level discussion. Not in scope for this spec.

### Open question: does the prek hook need to compare anything beyond byte-equal JSON?

For now no — both files should be deterministic JSON output from the upstream generator. If we ever pretty-print one differently from the other, we'll need to normalize (`json.loads → json.dumps(indent=2, sort_keys=True)`) before comparing. Add when needed.

## Implementation order

1. **A1 first** — write and register the prek hook. Verify it catches drift on a manually-corrupted vendored file. Open PR, merge.
2. **A2 second** — add the `airflow-metadata.yaml` section to `COORDINATOR.md`. Probably the same PR as A1 if both are small; otherwise a follow-up.
3. **A3 third** — file the upstream PR after #65958 lands. Track via a GitHub issue if #65958 takes time.

A1 + A2 estimated: 1 PR, ~half a day.
A3 estimated: 1 upstream PR, ~half a day plus review cycle.

## Decision log

- **Vendor schema.json instead of fetching at build time** — already chosen by `f54fd43a10`. Reason: builds offline, vendored copy is the contract, drift is detectable.
- **`SUPERVISOR_API_VERSION` not sent on the wire** — already chosen by `f54fd43a10`. Reason: avoids upstream protocol change; mirrors Java's manifest-header pattern (Java doesn't send the version on the wire either).
- **Pre-commit stage for the drift hook** (not manual) — chose pre-commit. Reason: cheap pure-Python diff; should run on every commit so drift surfaces immediately.
- **Bundle metadata is a YAML file**, not a `package.json` field — chose YAML. Reason: matches Java; bundles aren't always associated with an npm `package.json` (esbuild can emit just a `.mjs` + sibling YAML); per-DAG entries don't fit cleanly in package.json semantics.
- **Per-DAG entry as `{entry: ...}` object** (not bare string) — chose object. Reason: forward-compat.

## Follow-ups (not in this spec)

- **Wrapper-coverage check**: detect when a new generated type lacks a `protocol.ts` wrapper. Likely a TypeScript script invoked as a prek hook.
- **Auto-regenerate hook**: a Node-based hook that regenerates `src/generated/supervisor.ts` automatically when the vendored snapshot changes (instead of erroring and asking the user to do it). Adds a Node dep to prek; defer until friction is real.
- **Wire-level `Ready` handshake** for schema version (DESIGN-LIMITATIONS doc item 1, recommendation 2). AIP-level discussion.
- **Multi-bundle airflow-metadata.yaml** (single top-level file at `bundles_folder` instead of per-bundle) — only if a real user asks for it.
