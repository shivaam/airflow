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

# ts-sdk CI workflow — design

**Status:** draft
**Date:** 2026-05-23
**Phase:** B (first to implement — unblocks A and C)
**Related:** [spec A — wire-contract tracking](2026-05-23-ts-sdk-wire-contract-tracking-design.md), [spec C — e2e integration tests](2026-05-23-ts-sdk-e2e-integration-tests-design.md)

## Goal

Catch ts-sdk regressions (type errors, broken tests, broken build) on every PR that touches `ts-sdk/**`, before reviewers see them. Sets the polyglot-SDK CI precedent that a future `java-sdk.yml` can mirror.

Out of scope: cross-SDK serialization compatibility (lives in [spec A](2026-05-23-ts-sdk-wire-contract-tracking-design.md)), end-to-end against real Airflow (lives in [spec C](2026-05-23-ts-sdk-e2e-integration-tests-design.md)), npm publishing (deferred until the package has traction).

## Why

Today the ts-sdk has [vitest tests](../../../ts-sdk/tests/), a [strict typecheck](../../../ts-sdk/tsconfig.json), and a [tsc build](../../../ts-sdk/package.json), but no automation runs any of them on push or PR. Regressions only surface when a contributor remembers to run `pnpm test` locally — and contributors who change unrelated files won't think to.

The Java SDK in [PR #65959](https://github.com/apache/airflow/pull/65959) (Java CI/E2E template) does not have a dedicated unit-test workflow either — its tests run inside the e2e docker image. That works for Java because gradle test invocation is part of the e2e bootstrap. For the ts-sdk we want a separate fast feedback loop: vitest finishes in seconds and shouldn't be gated on docker image builds.

## Non-goals

- **Lint (eslint/prettier).** No lint config exists in `ts-sdk/` today. Adding one is a separate discussion. Tracked as a follow-up.
- **Cross-language serialization compat check.** That is the [Phase A prek hook](2026-05-23-ts-sdk-wire-contract-tracking-design.md#prek-hook-schema-drift-detection).
- **Coverage reporting.** Vitest can produce coverage but we have no baseline yet; adding it now risks gating PRs on noisy thresholds.
- **Multi-Node matrix.** The package declares `"engines": { "node": ">=22" }` and uses `corepack`-managed pnpm. A matrix across Node 20/22/24 is overkill for an alpha SDK with one supported runtime.

## Architecture

Single GitHub Actions workflow file at `.github/workflows/ts-sdk.yml`. One job, `ts-sdk-checks`, runs three steps sequentially: typecheck, test, build. Caching of the pnpm store via `actions/setup-node`'s built-in cache key (lockfile hash).

```
on:
  pull_request:
    paths: ['ts-sdk/**', '.github/workflows/ts-sdk.yml']
  push:
    branches: [main]
    paths: ['ts-sdk/**', '.github/workflows/ts-sdk.yml']

concurrency:
  group: ts-sdk-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  ts-sdk-checks:
    runs-on: ubuntu-22.04
    defaults:
      run:
        working-directory: ts-sdk
    steps:
      - checkout
      - corepack enable                 # pnpm comes from package.json packageManager
      - setup-node@v4 (node 22, cache: pnpm, cache-dependency-path: ts-sdk/pnpm-lock.yaml)
      - pnpm install --frozen-lockfile
      - pnpm typecheck
      - pnpm test
      - pnpm build
```

### Why a single job (not three parallel jobs)

The three steps share the same `node_modules` install, which takes longer than any single step. Splitting into parallel jobs means three installs in series of setup work — slower in wall-clock time even though more parallel on paper. Keep it one job until install caching makes a split worthwhile.

### Why path-filter on `ts-sdk/**`

We are setting the polyglot precedent. Non-ts-sdk PRs (Python, providers, docs) shouldn't pay the GH Actions minute cost. Including `.github/workflows/ts-sdk.yml` in the trigger paths ensures the workflow re-runs when we change the workflow itself (otherwise a workflow edit is impossible to validate without a `ts-sdk/**` change tagging along).

### Why `cancel-in-progress`

PR iteration produces many force-pushes. Without cancellation, each push enqueues a fresh run while the previous one wastes a runner. Concurrency-cancel is standard practice in the repo's other workflows (`grep cancel-in-progress .github/workflows/*.yml`).

### Why ubuntu-22.04 and not ubuntu-latest

Pin the runner to avoid surprise Ubuntu upgrades silently changing the Node toolchain or package availability. Match the repo's existing pinned runners. (Mechanical — bump when the wider repo bumps.)

## Files added / changed

| Path | Change | Size |
|---|---|---|
| [`.github/workflows/ts-sdk.yml`](../../../.github/workflows/ts-sdk.yml) | New file | ~60 lines |
| [`ts-sdk/package.json`](../../../ts-sdk/package.json) | Confirm `typecheck`, `test`, `build` scripts exist (already do) | 0 lines |

## Acceptance criteria

1. Workflow file passes [`actionlint`](https://github.com/rhysd/actionlint) (run via prek as part of opening the implementation PR — `prek run actionlint --files .github/workflows/ts-sdk.yml`).
2. Touching any file under `ts-sdk/` triggers the workflow in CI.
3. Touching any file outside `ts-sdk/` (and not the workflow itself) does **not** trigger it.
4. Workflow is green on the branch HEAD as of the implementation commit: `pnpm typecheck`, `pnpm test`, `pnpm build` all pass with the current source tree.
5. Intentionally breaking a test in a follow-up PR (then reverting) fails the workflow as expected.
6. PR runs from forks succeed without leaking secrets — the workflow uses no secrets, so this is automatic, but confirm in the first external-fork PR.

## Risks and open questions

### Risk: pnpm version pinning

`package.json`'s `packageManager` field pins pnpm to a specific version. If `corepack enable` doesn't honor that field as expected (it should, but quirks happen), we'd silently get a different pnpm. Mitigation: log `pnpm --version` as the first step after install and verify it matches the pinned version. If it diverges, switch to explicit `pnpm/action-setup@v4` with `version: 10`.

### Risk: setup-node cache miss on lockfile change

`actions/setup-node`'s pnpm cache is keyed on lockfile hash. Every lockfile change causes a cold cache — this is expected and correct (different deps need a fresh install). No mitigation needed; just don't be surprised when a deps-bump PR runs slower.

### Open question: should we run `pnpm build` last or in parallel with tests?

Build (`tsc`) is independent of test (`vitest`). Could parallelize. But: total wall-clock saved is small (each is fast), and serial output is easier to debug when a step fails. Keep serial for now; revisit if combined time exceeds 2 minutes.

### Open question: should the workflow include `pnpm run generate:supervisor` as a guard?

If a contributor edits `ts-sdk/schema/supervisor-schema.json` by hand without regenerating `src/generated/supervisor.ts`, the generated file goes stale. We could `pnpm run generate:supervisor && git diff --exit-code src/generated/supervisor.ts` as a step. **Defer** — this overlaps with the [Phase A schema-drift prek hook](2026-05-23-ts-sdk-wire-contract-tracking-design.md#prek-hook-schema-drift-detection), which is the better home for it.

## Implementation order

1. Create `.github/workflows/ts-sdk.yml` matching the architecture above.
2. Run `prek run actionlint --files .github/workflows/ts-sdk.yml` locally — fix any issues.
3. Push branch, open PR.
4. Verify all five acceptance criteria pass.
5. Merge.

Estimated effort: 1 PR, 2-4 hours including verification.

## Decision log

- **Standalone workflow vs. fold into existing static-checks/ci.yml** — chose standalone. Reason: isolates iteration speed, doesn't gate Python PRs, sets a clean precedent for `java-sdk.yml`. Tradeoff: doesn't run on Python-only PRs that happen to touch shared infrastructure. Acceptable — path-filter is intentional.
- **Single job vs. parallel jobs** — chose single. Reason: install cost dominates step cost.
- **Node version matrix** — chose single Node 22. Reason: package declares `engines.node >= 22`; matrix would just verify nothing.
- **Including prek/manual hooks here** — chose to defer to Phase A. Reason: serialization drift detection is a wire-contract concern, not a unit-test concern.

## Follow-ups (not in this spec)

- Lint config (eslint + prettier) — open as a separate proposal once we have opinions on style rules. Could be a follow-up PR after B merges.
- Coverage reporting — once we have ~70%+ tests and a stable baseline.
- Java-sdk parallel workflow — when someone picks up the Java side, model after this.
- Multi-Node matrix — only if we ever support Node < 22 (unlikely given ESM-only).
