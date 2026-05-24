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

# ts-sdk e2e integration tests — design

**Status:** draft
**Date:** 2026-05-23
**Phase:** C (third to implement — depends on spec A's upstream PR #65958 landing, and on this repo's spec A2 metadata convention being documented)
**Related:** [spec B — CI workflow](2026-05-23-ts-sdk-ci-workflow-design.md), [spec A — wire-contract tracking](2026-05-23-ts-sdk-wire-contract-tracking-design.md)
**Upstream template:** [PR #65959 — Java SDK CI + E2E + prek hooks](https://github.com/apache/airflow/pull/65959)

## Goal

Validate end-to-end that a real Airflow install can execute TypeScript task bodies via the `TypescriptCoordinator`, with bidirectional XCom and Variable/Connection access working through the supervisor's RPC bridge. Catch wire-protocol drift that unit tests can't see — the supervisor is real, the migrator runs, and the bundle is built the way a user would build it.

## Why

Spec B's CI runs vitest against an in-process mock supervisor. That proves our frame codec is internally consistent but says nothing about whether real Airflow can drive us. PR #65959 demonstrates the canonical pattern for the Java SDK; we mirror it 1:1 so reviewers don't have to learn two layouts.

The cost of *not* doing this: the `protocol.ts` wrappers from `f54fd43a10` could silently disagree with what `comms.py` actually sends (e.g. a field we marked required is sometimes omitted) and only fail in production. E2E catches that class of bug before users do.

## Non-goals

- **Load testing.** A handful of scenarios proving the happy path and named failures. No throughput benchmarks.
- **Coverage of every message type.** ~85 supervisor message types exist; we currently wrap ~13. E2E covers the ones we actively use.
- **Coverage of pure-`.mjs` DAGs.** Defer until the `TypescriptCoordinator` has firm support for the `.mjs` file-extension fallback. Python stub DAGs are the primary integration pattern.
- **Cross-version compat testing.** Migrator round-trip with older/newer schema versions deserves its own spec; this one validates current-version round-trip only.

## Architecture

Mirror PR #65959's `java_sdk_tests/` structure under `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/`. Drive the test via `testcontainers.compose.DockerCompose` (the pattern already in [conftest.py](../../../airflow-e2e-tests/tests/airflow_e2e_tests/conftest.py)). Provision the ts-sdk into the running Airflow container via a docker-compose overlay and a `breeze start-airflow --sdk ts` flag (mirror of `--sdk java`).

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  airflow-e2e-tests/                                      │
│                                                                          │
│  docker/                                                                 │
│    ts-sdk.yml          ◀── docker-compose overlay (Node 22, ts-sdk      │
│                            mounted, bundle directory mapped)             │
│                                                                          │
│  tests/airflow_e2e_tests/                                                │
│    ts_sdk_tests/                                                         │
│      conftest.py       ◀── ts-sdk-specific fixtures (build bundle,      │
│                            wait for ts-runtime coordinator ready)        │
│      dags/                                                               │
│        stub_dag.py     ◀── Python stub DAG with queue="ts-runtime"      │
│      bundle-src/                                                         │
│        bundle.ts       ◀── TS source: registerTask + handlers            │
│        package.json                                                      │
│      test_ts_sdk.py    ◀── pytest scenarios that trigger DAGs and       │
│                            assert outcomes via Airflow API               │
│                                                                          │
│  conftest.py           ◀── existing; extend with ts-sdk fixtures        │
│  constants.py          ◀── existing; add TS-SDK paths/constants         │
└─────────────────────────────────────────────────────────────────────────┘

                                +
┌─────────────────────────────────────────────────────────────────────────┐
│  scripts/in_container/                                                   │
│    ts_sdk_build.sh     ◀── runs pnpm install + pnpm build + esbuild     │
│                            bundle inside the breeze container           │
│    ts_sdk_setup.sh     ◀── installs Node 22 if missing; calls            │
│                            ts_sdk_build.sh; copies bundle to             │
│                            /opt/airflow/ts-bundles                       │
│  docker/entrypoint_ci.sh                                                 │
│                        ◀── extend to invoke ts_sdk_setup.sh when         │
│                            SDK_LANG=ts                                   │
│                                                                          │
│  dev/breeze/.../developer_commands.py                                    │
│                        ◀── extend --sdk option to accept "ts"            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why mirror Java's layout exactly

Reviewer cognitive load. Anyone who understood [PR #65959](https://github.com/apache/airflow/pull/65959) will read this PR in half the time. The Java structure is itself a mirror of how `testcontainers` patterns elsewhere in the repo are laid out, so it's already idiomatic for the codebase.

### Why use the existing `airflow-e2e-tests/` package (not a new one)

PR #65959 established `airflow-e2e-tests/` as the home for cross-language SDK e2e. Creating `ts-sdk-integration-tests/` would split the universe; mounting `--sdk ts` and `--sdk java` overlays from the same harness makes polyglot DAGs (Python stub → TS task → Java task) trivially testable in future.

### Why docker-compose overlay (not a new Dockerfile)

The base `airflow-e2e-tests` Docker image already has Airflow + workers. We add Node + a mounted bundle directory through a compose overlay, the way PR #65959 adds JVM + jars folder. Keeps the base image lean and lets us toggle SDKs per test run.

## Files added / changed

### New files (in this repo)

| Path | Purpose | Approx size |
|---|---|---|
| `airflow-e2e-tests/docker/ts-sdk.yml` | Compose overlay: Node 22, ts-sdk bundle mount, env vars | ~30 lines |
| `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/__init__.py` | Package marker | 16 lines (license header) |
| `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/conftest.py` | Fixtures: build bundle, copy to mount, wait-for-coordinator | ~80 lines |
| `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/dags/stub_dag.py` | Python stub DAG with `queue="ts-runtime"` tasks | ~60 lines |
| `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/bundle-src/bundle.ts` | TS source with `registerTask` handlers exercising getVariable/getConnection/XCom | ~80 lines |
| `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/bundle-src/package.json` | Local-build manifest pinning `@apache-airflow/ts-sdk` to file path | ~20 lines |
| `airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk.py` | pytest scenarios | ~120 lines |
| `scripts/in_container/ts_sdk_build.sh` | Build bundle inside container | ~30 lines |
| `scripts/in_container/ts_sdk_setup.sh` | Install Node + invoke build + place bundle | ~40 lines |

### Modified files

| Path | Change |
|---|---|
| [`airflow-e2e-tests/tests/airflow_e2e_tests/conftest.py`](../../../airflow-e2e-tests/tests/airflow_e2e_tests/conftest.py) | Add ts-sdk path constants; extend the SDK-overlay fixture |
| [`airflow-e2e-tests/tests/airflow_e2e_tests/constants.py`](../../../airflow-e2e-tests/tests/airflow_e2e_tests/constants.py) | Add `TS_SDK_BUNDLES_PATH`, `TS_SDK_OVERLAY_PATH` |
| `scripts/docker/entrypoint_ci.sh` | Invoke `ts_sdk_setup.sh` when `SDK_LANG=ts` |
| `dev/breeze/src/airflow_breeze/commands/developer_commands.py` | Extend `--sdk` option to accept `ts` |
| `dev/breeze/src/airflow_breeze/global_constants.py` | Add `ts` to the supported-SDK list |

## Test scenarios

Each scenario is one pytest function in `test_ts_sdk.py`. Triggered via the Airflow REST API, polled for terminal state, asserted on final state plus side-effects (XCom values, log content).

### S1 — happy path (`test_ts_sdk_happy_path`)

**DAG:** `ts_smoke` — single-task Python stub that delegates to TS.

```python
@stub(queue="ts-runtime")
def smoke(): ...
```

**TS handler:** returns `{"ok": True}`. Auto-pushed as `return_value` XCom.

**Asserts:**
1. DAG run finishes with state `success`.
2. XCom `return_value` for `smoke` task equals `{"ok": True}`.
3. Log line "Coordinator runtime started" appears with the expected `supervisor_api_version` value.

### S2 — Variable + Connection + XCom round-trip (`test_ts_sdk_client_apis`)

**DAG:** `ts_client_apis` — Python stub task chain `seed → ts_task → verify`.

- `seed`: writes a Variable + Connection via Airflow Variable/Connection API as test setup.
- `ts_task`: TS handler reads `getVariable("test_var")`, `getConnection("test_conn")`, calls `setXCom({key: "out", value: ...})`, returns a summary object.
- `verify`: Python downstream task pulls `ts_task`'s XCom values and asserts.

**Asserts:**
1. DAG run state `success`.
2. XCom `out` from `ts_task` contains a structure derived from the Variable + Connection values.
3. `verify` task succeeds.

### S3 — handler failure (`test_ts_sdk_handler_failure`)

**DAG:** `ts_failure` — single-task stub.

**TS handler:** throws `new Error("intentional failure for test")`.

**Asserts:**
1. Task state `failed`.
2. Task log includes the error message.
3. DAG run state `failed`.
4. Subsequent run starts clean (no zombie process from the prior failure).

### S4 — unknown task (`test_ts_sdk_unknown_task`)

**DAG:** `ts_unknown` — Python stub with `task_id="not_in_bundle"`.

**TS bundle:** does NOT register a handler named `not_in_bundle`.

**Asserts:**
1. Task state `removed` (from `TaskState{state: "removed"}` frame — see [`runtime.ts:164`](../../../ts-sdk/src/coordinator/runtime.ts)).
2. Task log includes a clear "no handler registered" message.

### S5 — schema-version handshake (`test_ts_sdk_schema_version_advertised`)

**Setup:** the bundle ships with `airflow-metadata.yaml` declaring `supervisor_schema_version: <SUPERVISOR_API_VERSION>`.

**Asserts:**
1. After the `TypescriptCoordinator` reads the metadata, the supervisor logs the SDK's advertised schema version (specific log assertion against `WatchedSubprocess` log output).
2. Migrator runs without errors (no `MigrationError` log).

**Dependency:** this scenario requires spec A's A3 (upstream `TypescriptCoordinator` reads `airflow-metadata.yaml`). Until that lands, this test is marked `@pytest.mark.skip(reason="waiting on upstream PR for airflow-metadata.yaml support")`.

### What's intentionally NOT a scenario

- Concurrent tasks on the same comm channel — not a real production case (one subprocess per task).
- Mapped tasks — not yet supported by ts-sdk (see [SDK_COMPARISON.md](../../../ts-sdk/SDK_COMPARISON.md)).
- Deferred tasks — not yet supported.
- Asset/outlet propagation — ts-sdk sends `[]` placeholders; nothing to verify.
- Edge-mode tests — covered separately by [TESTING.md](../../../ts-sdk/TESTING.md)'s manual recipe and the existing edge unit tests.

## Bundle build approach

The TS bundle ships as source (`bundle.ts` + `package.json`) and is built **inside the container** by `ts_sdk_build.sh`. Two reasons:

1. **No pre-built artifact in the repo.** Avoids checking in compiled JS, which pollutes diffs and goes stale.
2. **Real-world fidelity.** A user building a bundle on their laptop uses the same `pnpm install + esbuild` flow.

`ts_sdk_build.sh` does:

```bash
cd /opt/airflow/airflow-e2e-tests/tests/airflow_e2e_tests/ts_sdk_tests/bundle-src
pnpm install --frozen-lockfile
pnpm exec esbuild bundle.ts --bundle --platform=node --format=esm --outfile=/opt/airflow/ts-bundles/bundle.mjs
# Emit airflow-metadata.yaml alongside
node -e "
  import('@apache-airflow/ts-sdk').then(sdk => {
    require('fs').writeFileSync('/opt/airflow/ts-bundles/airflow-metadata.yaml',
      'supervisor_schema_version: \"' + sdk.SUPERVISOR_API_VERSION + '\"\n' +
      'dags:\n  ts_smoke:        {entry: bundle.mjs}\n  ts_client_apis:  {entry: bundle.mjs}\n' +
      '  ts_failure:      {entry: bundle.mjs}\n  ts_unknown:      {entry: bundle.mjs}\n');
  });
"
```

The mount `/opt/airflow/ts-bundles` is what `[sdk] coordinators` points its `bundles_folder` at (set via env in the compose overlay).

## Breeze integration

Extend `breeze start-airflow --sdk` to accept `ts`. Today (per PR #65959) it accepts `java`. After this spec:

```bash
breeze start-airflow --backend postgres --executor local --sdk ts
```

What `--sdk ts` does:
1. Sets `SDK_LANG=ts` env in the container.
2. `entrypoint_ci.sh` sees `SDK_LANG=ts` → invokes `ts_sdk_setup.sh`.
3. Setup script installs Node 22 (if not in image), runs `ts_sdk_build.sh`, writes `airflow.cfg` [sdk] config (coordinators + queue_to_coordinator pointed at the ts bundle).
4. Airflow starts ready to dispatch `queue="ts-runtime"` tasks to the `TypescriptCoordinator`.

The breeze flag is also what powers the e2e tests' fixture — `conftest.py` spawns docker-compose with the `ts-sdk.yml` overlay, which sets `SDK_LANG=ts` and triggers the same path.

## Acceptance criteria

1. `cd airflow-e2e-tests && uv run pytest tests/airflow_e2e_tests/ts_sdk_tests/ -v` passes on a fresh checkout.
2. Each of S1, S2, S3, S4 passes individually (`pytest -k test_ts_sdk_happy_path`, etc.).
3. S5 either passes (if upstream PR has merged) or is properly skipped with a clear reason.
4. `breeze start-airflow --sdk ts` brings up an Airflow with the bundle ready to dispatch, verified by triggering `ts_smoke` manually and seeing it succeed.
5. The test suite runs in CI (existing `airflow-distributions-tests.yml` or similar — wire in as a path-triggered job).
6. Total wall-clock for the full ts_sdk_tests suite under 5 minutes on a clean Docker cache.

## Risks and open questions

### Risk: Node not in the base Airflow image

Today's `ghcr.io/apache/airflow/main/prod/python3.10` doesn't ship Node. `ts_sdk_setup.sh` has to install it. Options: (a) `apt-get install nodejs` (gets an old version); (b) use the official Node tarball from `nodejs.org`; (c) bake Node into the e2e image as a layer.

**Recommendation: (b) — official tarball.** Pinned version, no apt index dependency, matches what users would install. ~15-second setup cost on cold cache.

### Risk: pnpm install network flakiness in CI

`pnpm install --frozen-lockfile` hits npm registry. Flaky on CI. Mitigations: (1) cache the pnpm store at the runner level; (2) consider vendoring the `node_modules` for the test bundle (uglier but bulletproof). Start with (1), escalate if flakes are real.

### Risk: docker-compose overlay collision with java-sdk.yml

If a future test wants both Java and TS SDKs in the same Airflow (polyglot DAG), the two overlays need to coexist. They likely will (different `SDK_LANG`, different mount paths, no conflicting env keys), but worth verifying once both exist. Add a smoke test `test_polyglot_smoke` as a follow-up when both are in place.

### Risk: upstream A3 doesn't land

If the `TypescriptCoordinator` never gains `airflow-metadata.yaml` support, scenario S5 stays skipped indefinitely. S1-S4 still validate the wire contract via the existing `TypescriptCoordinator`'s hardcoded `bundle.mjs` fallback. Acceptable — the test suite still has value, just doesn't validate the migrator handshake.

### Open question: which CI workflow runs this?

PR #65959 wires Java e2e into the existing `airflow-distributions-tests.yml`. Simplest: do the same for ts — add a `ts-sdk` matrix entry. Alternative: separate workflow file. **Recommendation: piggyback on `airflow-distributions-tests.yml`** because docker-compose harness is shared; running it twice is wasteful.

### Open question: should the bundle source be inside `airflow-e2e-tests/` or under `ts-sdk/integration/`?

Java keeps test bundles under `airflow-e2e-tests/tests/airflow_e2e_tests/java_sdk_tests/`. Mirror that.

## Implementation order

1. **Land spec B** so unit-test CI is in place to catch issues this spec might introduce.
2. **Land spec A's A1+A2** so the schema-drift hook is active and the metadata convention is documented (S5 depends on the convention).
3. **C: docker overlay + breeze flag + setup scripts** — get `breeze start-airflow --sdk ts` working manually before any pytest. Verify by curling Airflow API to trigger a test DAG.
4. **C: bundle source + airflow-metadata.yaml** — confirm `TypescriptCoordinator` picks it up.
5. **C: scenarios S1, S3, S4** — minimal scenarios first (no Variable/Connection prerequisites).
6. **C: scenario S2** — adds Variable/Connection seeding complexity.
7. **C: scenario S5** — skipped until upstream A3 lands; un-skip when it does.
8. **C: CI wiring** — add the test path to `airflow-distributions-tests.yml`.

Estimated effort: 1 PR per group above (4-5 PRs total), ~1-2 weeks elapsed including review cycles.

## Decision log

- **`airflow-e2e-tests/` vs new `ts-sdk-integration-tests/` package** — chose existing. Reason: PR #65959 established the pattern; polyglot DAGs become trivially testable when both SDKs share a harness; reviewer cognitive load minimized.
- **Build bundle in-container vs check in pre-built `.mjs`** — chose in-container. Reason: real-world fidelity; no committed compiled JS.
- **One scenario file vs split per scenario** — chose one `test_ts_sdk.py`. Reason: matches Java's `test_java_sdk.py`; scenarios are short.
- **Skip S5 vs hard-fail until upstream lands** — chose skip. Reason: rest of the suite is independently useful; un-skip is a one-line change when A3 lands.
- **Piggyback on `airflow-distributions-tests.yml` vs new workflow** — chose piggyback. Reason: docker-compose infrastructure is shared.

## Follow-ups (not in this spec)

- **Polyglot test** — `test_polyglot_smoke` exercising Python → TS → Java in one DAG once both SDKs are wired in.
- **Migrator round-trip test** — pin our `SUPERVISOR_API_VERSION` to a version older than upstream's, verify the supervisor migrator upgrades our frames correctly. Requires multi-version snapshots of `schema.json`.
- **Pure-`.mjs` DAG scenario** — once the `TypescriptCoordinator`'s `.mjs` file-extension fallback is firm.
- **Performance baseline** — measure cold-start time of the Node subprocess; assert under threshold. Useful regression guard.
- **Bundle multi-tenancy** — multiple bundles in one `bundles_folder`, routed by dag_id via `airflow-metadata.yaml`. Validates A2 convention end-to-end.
