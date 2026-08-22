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

# Celery and Kombu batch-publishing research handoff

Updated: 2026-08-22

This branch preserves the Airflow-side prototype and the conclusions from the
follow-up Kombu investigation. It is a research branch, not a proposed Airflow
pull request. It intentionally has a neutral branch name and does not use issue
closing keywords or link itself to an issue.

## Executive summary

Batch publishing is both feasible and useful for Celery producers backed by
Redis. The measurable problem is network round-trip latency: a normal
`Producer.publish()` ultimately performs one Redis `LPUSH` for each queue
message. When a scheduler publishes hundreds or thousands of tasks, latency is
paid once per task even though the messages are already available as a group.

The Airflow-local prototype on this branch proved the performance benefit, but
it has to intercept Kombu's private Redis `_put()` and `_put_fanout()` methods.
That is too fragile for a durable Airflow implementation because those methods
own queue naming, priority selection, wire encoding, expiry, and fanout details.

The maintainable solution belongs upstream in Kombu. Kombu is Celery's
transport and messaging library; it is not Redis-only. It provides the common
producer/channel abstraction used with Redis, AMQP brokers such as RabbitMQ,
Amazon SQS, and other transports. The public API should therefore be
transport-neutral while each transport owns its batching implementation and
delivery semantics.

An upstream implementation is open as
[celery/kombu PR 2572](https://github.com/celery/kombu/pull/2572). It adds a
public `Producer.batch()` context and a Redis implementation based on
`client.pipeline(transaction=False)`. The design and performance case are
sound, but a later code review reproduced several edge cases that should be
fixed before the API is relied on by Airflow.

## What was explored

### Why Celery `group()` is not sufficient

The earlier Airflow draft implementation used Celery canvas groups. A group
reuses a producer, but Celery still loops over the child signatures and calls
`apply_async()` for each one. Each call reaches Kombu separately, and Redis
still receives one immediate `LPUSH` per task. Reusing the connection reduces
setup overhead but does not collapse broker round trips.

### Where batching belongs in the stack

The publication path is:

```text
Airflow workload loop
    -> Celery Task.apply_async()
    -> Kombu Producer.publish()
    -> message serialization, properties, declarations, and routing
    -> virtual transport direct/topic/fanout routing
    -> Redis Channel._put() or Channel._put_fanout()
    -> LPUSH / PUBLISH
```

Celery and Kombu must continue to perform normal message construction and
routing. The safe optimization point is the final transport operation: queue
the already-prepared Redis commands in a pipeline and execute them together.

This separation keeps the public API convenient while preserving transport
ownership:

- `Producer` owns the public context manager because applications already
  publish through a producer.
- `Channel` exposes a transport hook that creates a publish-batch session.
- Redis owns the pipeline, priority queue key selection, encoding, expiry, and
  fanout commands.
- Transports without an implementation retain immediate publication and expose
  that batching is unsupported.

### API designs compared

#### 1. Producer-only batch context

Example:

```python
with producer.batch():
    producer.publish(message_one, ...)
    producer.publish(message_two, ...)
```

This is the best caller experience, but it cannot be implemented entirely in
`Producer`. The producer does not own Redis priority keys, fanout channel names,
or the final encoded transport command.

#### 2. Transport/channel batch exposed through Producer

This is the selected design. `Producer.batch()` is the stable public facade,
and the active channel creates the transport-specific session. It keeps Redis
details out of Celery and Airflow while letting future transports implement
their own semantics.

#### 3. `publish_many()` accepting complete requests

This would duplicate the large `Producer.publish()` argument surface and force
callers to construct publication-request collections. It also fits Celery's
ordinary `Task.apply_async()` loop poorly. A context lets every call retain the
normal code path and minimizes compatibility risk.

## Selected public API

The proposed usage is:

```python
with producer.batch(max_size=500) as batch:
    producer.publish(message_one, exchange=exchange, routing_key="queue.one")
    producer.publish(message_two, exchange=exchange, routing_key="queue.two")
    batch.flush()  # optional; the context remains usable
    producer.publish(message_three, exchange=exchange, routing_key="queue.one")
```

The implementation in Kombu PR 2572 has these intended semantics:

- Existing publishing is unchanged unless `batch()` is explicitly entered.
- A successful outer context exit flushes buffered commands.
- `flush()` explicitly executes the current commands and keeps the context
  active for later publishes.
- An empty batch performs no Redis pipeline operation.
- Nested contexts share the outer session and its `max_size`.
- An exception in a nested context aborts the shared unflushed batch.
- `max_size` bounds final Redis commands, not logical messages. One logical
  publish may enqueue more than one command when expiry is involved.
- Redis uses `pipeline(transaction=False)`. Atomic transactions are unnecessary
  for this optimization and add protocol work and deployment constraints.
- The configured Redis socket timeout bounds a blocking `execute()` call. The
  proposal does not create a background timer or a global buffer.
- Batch state is scoped to one producer/channel and was initially isolated with
  `threading.local()`.

## Airflow-local prototype preserved by this branch

The parent commit on this branch implements the original proof of concept in
the Celery provider. It:

- shares a normal Celery/Kombu producer across a workload batch;
- lets `Task.apply_async()` construct and route each message normally;
- uses a non-transactional Redis pipeline for final direct/topic and fanout
  operations;
- keeps the non-Redis path and its process-pool behavior available;
- restores the channel methods after the scoped batch exits;
- converts batch flush failures into per-workload executor results;
- adds provider configuration and focused unit coverage.

The prototype was important evidence, but it should not be merged in its
current form. It overrides private Kombu methods and copies behavior that Kombu
should own. Once the public Kombu API is stable and released, Airflow can replace
the private interception with `producer.batch()`.

## Validation completed

### Airflow prototype

- Celery executor unit module: 57 passed, 14 skipped for unrelated
  backend/version conditions.
- Breeze provider integration: 10 of 10 passed using PostgreSQL, Redis,
  RabbitMQ, real Airflow executor workload objects, and Celery workers.
- Linux multiprocessing regression: 250 repeated dispatches completed; 1 test
  passed with a 0.11-second test body.
- Formatting, Ruff, mypy, `git diff --check`, and applicable pre-commit hooks
  passed.
- A real-daemon load test published 1,000 real BashOperator workloads through
  the selected Redis pipeline in 3.195 seconds. All 1,000 completed successfully
  in 155.345 seconds with worker concurrency 4.
- The unoptimized real-daemon path stalled on its first publication batch for
  more than 180 seconds; only one Redis `LPUSH` was observed while ten publisher
  child processes were blocked.

Airflow benchmark with 30 messages and 20 ms injected latency:

| Mode | Samples (seconds) | Median |
|---|---|---:|
| Ordinary | 0.7626, 0.7537, 0.8101, 0.8071, 0.6732 | 0.7626 |
| Redis pipeline | 0.0364, 0.0322, 0.0306, 0.0304, 0.0327 | 0.0322 |

Median speedup: 23.7x. Queue length was verified as 30 after every run.

### Kombu implementation

- Full unit suite: 1,611 passed, 178 skipped.
- Real Redis 8.6.1 integration suite: 64 passed.
- A later focused run remained green: 253 passed.
- Direct and topic routing, queue priorities and FIFO behavior, fanout,
  key-prefix behavior, nesting, empty batches, explicit flush, construction
  failure, pipeline failure, cleanup, and producer/channel/thread isolation are
  covered.
- Celery 5.6.3 smoke test: three ordinary `Task.apply_async()` calls were absent
  from Redis inside the context and became visible after successful exit.
- Applicable pre-commit hooks passed locally. The live PR currently has green
  Read the Docs and pre-commit checks.

Kombu benchmark with 100 messages, 10 ms simulated request latency, and seven
runs:

| Mode | Median |
|---|---:|
| Ordinary publication | 1.2290 s |
| Batched publication | 0.0164 s |

Median speedup: 75.1x.

These latency-injection benchmarks intentionally measure the problem that
pipelining addresses. Results on a local, zero-latency Redis server will show a
smaller relative improvement.

## Failure semantics

A Redis pipeline reduces round trips; it does not make publication exactly
once. If `pipeline.execute()` fails before a response is received, the caller
cannot know whether Redis accepted none, some, or all commands. Commands in a
non-transactional pipeline may also have partially executed.

The proposed public contract is therefore:

- raise a dedicated `BatchPublishError` for an ambiguous flush outcome;
- never replay the whole pipeline automatically at the Kombu batch layer;
- let the application explicitly decide whether at-least-once retry is safe;
- discard only commands that have not been executed;
- never imply that already-flushed commands can be rolled back.

Airflow task workloads usually have deterministic task-instance-derived IDs and
duplicate-running protections, which reduce duplicate execution risk. Callback
workloads may not have equivalent identifiers, so including callbacks in an
automatically retried batch needs a deliberate policy.

## Code-review findings and edge cases

The implementation passed its existing tests, but adversarial review found four
important gaps.

### 1. redis-py can replay the pipeline internally

With `retry_on_timeout=True`, redis-py may retry `pipeline.execute()` below
Kombu. A real Redis fault injection raised a timeout after Redis had accepted
the first execution. One logical flush made two low-level attempts and left two
messages in the queue.

This contradicts a strict "Kombu never replays" promise even if Kombu itself has
no retry loop. Before landing, the batch connection must disable redis-py's
internal replay without mutating a shared connection pool, or the API must
explicitly document at-least-once behavior. Disabling replay is the safer
default for an ambiguous multi-command flush.

### 2. channel revival can break the unsupported-transport fallback

The immediate fallback currently remembers the channel active when the context
starts. If a normal `retry=True` publish revives the producer onto a new channel,
the fallback rejects the new channel with `RuntimeError` instead of retaining
ordinary publication behavior. Redis failures before the flush can encounter a
similar channel-change question.

Unsupported transports should behave exactly as before. The fallback must not
pin a stale channel, and the Redis path needs a defined distinction between a
safe pre-buffer/pre-flush retry and an unsafe ambiguous post-send retry.

### 3. cleanup failure can mask the original error and leak batch state

If `discard()` or pipeline close fails while handling an application exception,
the cleanup error can replace the original exception. The producer's local
batch can remain installed, causing later publication to fail through an
already-aborted session.

Batch detachment must happen unconditionally in a `finally` path. Cleanup errors
should not hide the primary application error, and subsequent ordinary
publication must remain usable.

### 4. thread-local state is not async-task or greenlet isolation

`threading.local()` isolates operating-system threads, but two asyncio tasks or
greenlets on the same thread can overlap and share the same producer batch.
An exception in one context can then abort commands belonging to the other.

The API should either use context-aware ownership (for example `contextvars`),
fail fast when a producer is used concurrently by different execution contexts,
or explicitly prohibit concurrent use and test that contract. Silent sharing is
not acceptable.

The first two findings are release blockers. Cleanup should be corrected in the
same revision. The concurrency contract must be resolved before the API is
described as isolated beyond OS threads.

## Ordering and compatibility

Within one Redis queue, commands enter the pipeline in normal publish order.
Kombu uses `LPUSH`, while the consumer removes from the opposite side, so the
existing per-queue FIFO behavior is preserved. Priority queues retain Kombu's
normal key mapping because `_q_for_pri()` remains inside the Redis transport.

Compatibility status:

| Environment | Status |
|---|---|
| Standard Redis | Tested with a real Redis 8.6.1 server |
| Redis direct/topic queues | Tested |
| Redis priorities and ordering | Tested |
| Redis fanout | Tested |
| Non-Redis Kombu transports | Immediate fallback unit-tested; revival bug remains |
| Redis TLS | Same transport path; structural/unit confidence only, not live-tested |
| Redis Sentinel | Same transport family; structural/unit confidence only, not live-tested |
| Redis Cluster | Not supported as an upstream Kombu transport; not tested |
| asyncio/greenlet overlap | Reproduced unsafe sharing; unresolved |

Declarations are intentionally not buffered. Queue/exchange declarations and
other operations whose result is needed immediately stay on the normal path.

## Related upstream demand

The use case is broader than Airflow:

- [Celery issue 9887](https://github.com/celery/celery/issues/9887) independently
  requests native batch publishing for high-throughput task submission. Its
  immediate focus is RabbitMQ publisher confirmations, showing why the public
  surface should be transport-neutral even though broker implementations differ.
- [Kombu issue 147](https://github.com/celery/kombu/issues/147) historically
  requested SQS batching for throughput and API-cost reasons. SQS eventually
  gained transport-owned batching behavior.
- The Airflow scheduler provides a concrete Redis workload: it frequently has
  many already-selected tasks ready to publish at once.

The common requirement is a stable application API with transport-owned
semantics, not one universal batching mechanism or one universal delivery
guarantee.

## Current Kombu PR status

As checked on 2026-08-22:

- PR: [celery/kombu 2572](https://github.com/celery/kombu/pull/2572)
- Title: `Add transport-aware batch publishing with Redis pipelines`
- State: open and marked ready for review (not draft)
- Head: `shivaam:codex/kombu-batch-publishing`
- Implementation commit: `86baa43010907e13175fe1cfdb650588924b51cc`
- Read the Docs: success
- pre-commit.ci: success
- Maintainer review: none yet
- Automated Copilot review: one summary/comment, no maintainer decision

Although GitHub currently marks the PR ready, the reproduced retry, revival,
and cleanup findings should be addressed before requesting maintainer adoption.

## Current Airflow PR status

The prototype's original branch is separately proposed in
[apache/airflow PR 70455](https://github.com/apache/airflow/pull/70455). As
checked on 2026-08-22, that PR is open and marked ready for review, requires a
review, and has CI in progress. A reviewer asked whether the optimization also
applies to RabbitMQ or SQS; the response correctly clarified that the prototype
is Redis-only and leaves other brokers on their existing path.

The older Celery-group draft,
[apache/airflow PR 30049](https://github.com/apache/airflow/pull/30049), is now
closed. Its approach reused a producer but did not reduce one Redis command per
task.

This neutral research branch is not the head of either pull request. No new
Airflow pull request was opened for this branch.

## Will the Kombu PR enable the Airflow work?

Yes, after it is corrected, merged, released, and adopted by Airflow's minimum
Kombu/Celery dependency set. It provides the missing public seam so Airflow no
longer has to replace private Redis methods.

The eventual Airflow call site can remain ordinary Celery code:

```python
with celery_app.producer_or_acquire() as producer:
    with producer.batch(max_size=500):
        for workload, queue, task_id in workloads:
            execute_workload.apply_async(
                args=[workload],
                queue=queue,
                task_id=task_id,
                producer=producer,
            )
```

Celery still creates each message, resolves routing, serializes the payload, and
handles declarations. Kombu's active Redis channel buffers only the final
`LPUSH`/`PUBLISH` commands and flushes them together.

## Recommended next steps

1. Update Kombu PR 2572 to prevent hidden redis-py pipeline replay.
2. Fix channel-revival behavior for unsupported transports and define safe
   pre-flush Redis recovery.
3. Make state detachment unconditional and preserve primary exceptions when
   cleanup fails.
4. Choose and test the asyncio/greenlet concurrency contract.
5. Re-run the focused suite, full unit suite, and real Redis integration tests.
6. Add live Redis TLS and Sentinel validation if suitable services are
   available, or keep those limitations explicit.
7. Once Kombu is merged and released, replace this branch's Airflow-private
   prototype with the public `Producer.batch()` API and repeat the Airflow unit,
   Breeze integration, daemon-load, and latency benchmark runs.

## Branch intent

This branch exists to preserve code and evidence for continued work. It does
not claim or close an issue, and no pull request was opened from it. The Airflow
prototype is useful as a test harness and adoption sketch; the Kombu PR is the
intended production abstraction.
