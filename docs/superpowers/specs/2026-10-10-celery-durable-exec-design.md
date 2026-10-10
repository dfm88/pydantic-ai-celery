# Celery durable execution for Pydantic AI

Status: draft for review. Approach A from the design discussion. No implementation yet.

This library is an external Pydantic AI capability. It checkpoints one agent run inside one Celery task. A worker crash, a retry, or a redelivery starts that task from the top and returns stored results for steps that already finished. The package depends on the public durable-execution builder in `pydantic-ai` 2.55.0 (`BaseDurabilityCapability`, `JournalCallableOperationBackend`, `DurabilityEngineSpec`, `JSON_CODEC`, `JournalOperationNamer`). It does not copy model, toolset, event, or capability-operation machinery.

## Goals

- Attach durability the way `PrefectDurability` and `DBOSDurability` attach: a capability on `Agent`, transparent outside its container.
- Survive a worker crash (`WorkerLostError`) without repeating a model request or tool call that has already been journaled. A configured hard time limit stops the run until `resume`; it does not requeue by itself.
- Give long agent loops an explicit timeout and redelivery policy, including Celery soft and hard time limits, Redis and SQS visibility timeouts, and the RabbitMQ consumer timeout.
- Keep step names stable across process restarts and deploys.

## Out of scope for v1

- Routing model calls and tool calls to different worker pools.
- Suspending a task while a person approves, then continuing the same Python stack. Deferred tools already end the run; a later task resumes from message history.
- Streaming tokens to the process that enqueued the task.
- A SQL journal, Django helpers, and a CLI.
- Parallel tool calls. The engine spec sets `sequential_tools_in_durable_context=True`.

## User API

```python
from celery import Celery
from pydantic_ai import Agent
from pydantic_ai_celery import CeleryDurability, CeleryOperationConfig, configure_celery_app

app = Celery('agents', broker='redis://localhost:6379/0')
configure_celery_app(app, visibility_timeout=86_400)

durability = CeleryDurability('redis://localhost:6379/1')
agent = Agent('openai:gpt-5', name='analyst', capabilities=[durability])


@durability.task(app)
def analyse(prompt: str) -> str:
    return agent.run_sync(prompt).output
```

`CeleryDurability` takes a journal, either a `redis://` or `rediss://` URL or a `Journal` instance. There is no default URL. The journal is a separate store from the Celery result backend. `PENDING` on that backend means "unknown", so it cannot be the checkpoint store.

`@durability.task(app)` registers a bound task that:

- opens the durable container around the user function with `self.request.id` as the run id
- applies the ack, retry, and time-limit policy in [Timeouts and redelivery](#timeouts-and-redelivery)
- returns a stored final result when this task id has already completed

A task written by hand uses the same container:

```python
@app.task(
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=True,
)
def analyse(self, prompt: str) -> str:
    finished = durability.completed_result(self.request.id)
    if finished is not None:
        return finished
    with durability.container(self.request.id, task=self):
        return agent.run_sync(prompt).output
```

The hand-written form must pass `task=self` so a duplicate delivery can call `task.retry`. It must set the three ack flags itself. `@durability.task` is the supported path. `container()` is not reentrant. The decorator checks `completed_result` before entering the container, so a finished task id does not bump the attempt counter and does not call the user function. A hand-written task that skips that check replays journaled steps and runs the user function again.

Sync tasks call `agent.run_sync`. Async tasks are registered when the user function is a coroutine function, and they `await agent.run(...)`. `run_sync` inside a running loop is rejected by Pydantic AI.

Outside `container()`, `in_durable_context` is false and the agent runs normally. No journal writes.

`resume(task, args, kwargs)` republishes that task with the same Celery id so a stopped run continues against the existing journal. It deletes the attempt counter first. If the lease key exists it raises `RunInProgress` and does not publish. The caller retries `resume` after the lease expires when the owner is actually gone.

```python
from pydantic_ai_celery import resume

resume(analyse, args=['Summarize Q3'], kwargs={})
```

`configure_celery_app(app, *, visibility_timeout: int = 86_400, prefetch_multiplier: int = 1)` sets `broker_transport_options['visibility_timeout']`, `result_backend_transport_options['visibility_timeout']` when a result backend is already configured, the app-level `visibility_timeout` Celery reads for the Redis transport, and `worker_prefetch_multiplier`. It is opt-in. Constructing the capability does not mutate app config.

Per-tool Celery config uses metadata key `celery`. The value is a `CeleryOperationConfig` or `False`. `False` runs that function or dynamic tool inline, with no checkpoint. MCP tools cannot opt out; the builder raises `UserError`.

```python
CeleryOperationConfig(timeout=120)  # seconds, this unit only
```

Role defaults live on the capability: `model_config`, `tool_config`, `mcp_config`, `event_config`, `capability_config`. Each is a `CeleryOperationConfig`. The default timeout on every role is `None` (wait until the unit returns or the task time limit fires).

## Runtime

One Celery task is the durable container. One agent run occupies that child for the whole run, including model latency. Concurrency is the number of simultaneous runs.

`CeleryOperationBackend` subclasses `JournalCallableOperationBackend`. `execute()`:

1. Reads the container's encounter counter and increments it synchronously, before any `await`. The counter lives in memory and starts at 0 on every delivery. It is not stored.
2. Builds the persisted name with `JournalOperationNamer.invocation_name`.
3. Loads the journal record at `(task_id, index)`.
4. If the record exists and its operation name matches, returns the stored payload.
5. If the record exists and the name differs, raises `DurabilityDivergence` (non-retryable).
6. Otherwise runs `body()`, then writes one journal record with a single atomic put.
7. Returns the payload. The base decodes it.

`cache_key` is ignored. The builder's projection includes the live `RunContext`, so a content hash changes across deliveries when the context does. Identity is the encounter index plus the operation name, the same idea as Absurd's step order.

`in_durable_context` is true only when the container context var is set and `in_durable_unit()` is false. Nested work inside a step stays direct, which is what the builder's `durable_unit_scope` is for.

The encounter counter is per container, so two sequential `agent.run()` calls in one task share one sequence. That is supported. Two overlapping runs in one task are not.

### What is checkpointed

`JournalOperationNamer` names, including the tool-call label suffix (`...call_tool:{tool}`) for function and dynamic tools. A second occurrence of the same name is the next encounter index, not a `#2` suffix in the name.

Checkpointed units are model request, model stream, compaction, suspended-response cancellation, capability operations, function-tool validation and calls, dynamic-tool discovery, validation, and calls, MCP discovery, instructions, and calls, the event-stream handler, and the workspace call the base binds.

Control-flow results (`approval_required`, `call_deferred`, `model_retry`, `validation_error`, `tool_failed`) are stored as the builder's value wrappers. `tool_call_result_upgrade_lenient` stays `False`. Replay returns the stored wrapper. The tool body does not run again.

A unit that raises does not write a record. The task retry starts the container from the top, serves the earlier records, and runs that unit again.

### Engine spec

```python
DurabilityEngineSpec(
    engine_name='Celery',
    durable_unit_noun='step',
    durable_container_noun='task',
    codec=JSON_CODEC,
    serialization_failure=lambda exc: NonRetryableDurabilityError(str(exc)),
    wrapped_toolset_kinds=frozenset({'function', 'mcp', 'dynamic'}),
    toolset_lifecycles={
        'function': 'enter-always',
        'mcp': 'enter-in-durable-unit',
        'dynamic': 'enter-never',
    },
    tool_call_result_upgrade_lenient=False,
    journal_discovery=True,
    sequential_tools_in_durable_context=True,
    unsupported_runtime_toolset_kinds=frozenset({'function', 'mcp', 'dynamic'}),
    tool_config_key='celery',
)
```

`enter-in-durable-unit` is valid because the step runs in the task process. An MCP toolset is entered on first use inside a step and stays entered until the container closes it. A crash retries the connect.

JSON is the journal encoding. `JSON_CODEC` dumps inside the unit and loads outside it. A value that cannot be encoded becomes `NonRetryableDurabilityError`, which is excluded from autoretry. Pickle is not used.

Function tools are wrapped. On replay the agent loop calls every tool it called the first time. An unwrapped tool would run its side effect again before the next cached model step.

Runtime toolsets passed to `run(toolsets=...)` are rejected inside the container. `ExternalToolset` stays allowed by the builder.

### Journal record

Redis keys, prefix `pydantic-ai-celery`:

| Key | Value |
|---|---|
| `{prefix}:{task_id}:step:{index}` | JSON object `{"operation_name": "...", "payload": ...}` |
| `{prefix}:{task_id}:result` | JSON task return value, when that value is JSON-encodable |
| `{prefix}:{task_id}:attempts` | integer |
| `{prefix}:{task_id}:lease` | owner token, PX ttl |

`payload` is the object `execute()` received from `body()`, which is already `result_codec` output. The Redis value is `json.dumps` of the record. TTL on step, result, and attempt keys is `journal_ttl` seconds, default 7 days, refreshed on each write.

A task return value that is not JSON-encodable still returns to Celery. The completion record is skipped. A duplicate delivery replays steps from the journal. Celery's own serializer is JSON by default, so the return value has to satisfy that anyway.

Sharing the broker's Redis database is allowed. `FLUSHDB` on that database deletes checkpoints. A separate logical database is the recommended URL.

`Journal` is a small synchronous protocol (`get_step`, `put_step`, `get_result`, `put_result`, `bump_attempt`, `acquire_lease`, `refresh_lease`, `release_lease`). `RedisJournal` and `MemoryJournal` implement it. Tests use `MemoryJournal`. Step puts are a single `SET` of the whole record.

The lease owner is checked before `put_step`. A lost lease raises `LeaseLost`, which is retryable. The replacement delivery resumes from the last committed step.

## Timeouts and redelivery

Agent runs are long, and several Celery clocks interact. The library sets task-level flags. Broker and worker settings stay on the app, with a startup check, because changing them from a capability would affect every other task on that app.

### Clocks that exist

| Clock | Default | What it does to a long run |
|---|---|---|
| `soft_time_limit` | none | Celery raises `SoftTimeLimitExceeded` in the child via `SIGUSR1`. The process stays alive. |
| `time_limit` | none | The pool kills the child. In Celery 5.6.3 `Request.on_timeout` records `TimeLimitExceeded` and, when `acks_late` and `acks_on_failure_or_timeout` are both set, acknowledges the message. |
| `acks_late` | off | The message stays unacked until the task returns. Required so a dead child can be redelivered. |
| `acks_on_failure_or_timeout` | on | With `acks_late`, a failed or timed-out task is still acked. `Request.on_failure` requeues `TimeLimitExceeded` only when this flag is off. |
| `reject_on_worker_lost` | off | Requeues when the child dies with `WorkerLostError` (kill, OOM). It does not see the hard time-limit path, because that path is `on_timeout`, and a prior ack makes `on_failure` skip the reject. |
| `autoretry_for` / `dont_autoretry_for` | empty | `dont_autoretry_for` exists since Celery 5.5.0. |
| Redis / SQS `visibility_timeout` | 3600 s | An unacked message is delivered again when this expires. With `acks_late` that includes a task that is still running. If several apps share a broker, the shortest value wins. |
| `worker_prefetch_multiplier` | 4 | Each child reserves that many messages. Reserved long tasks sit unacked, so the visibility timeout applies before they start. |
| `worker_cancel_long_running_tasks_on_connection_loss` | off in 5.6, on in Celery 6 | A broker blip kills late-ack tasks. The message is redelivered. |
| RabbitMQ `consumer_timeout` | 30 minutes (`1_800_000` ms) | The broker closes the channel when a delivery is unacked this long. Since RabbitMQ 3.12 a queue can set `x-consumer-timeout` or the policy key `consumer-timeout`. |

`SoftTimeLimitExceeded` is a `TimeLimitExceeded`, and that is an `Exception`. A bare `autoretry_for=(Exception,)` retries it. After `max_retries`, `on_failure` sees `TimeLimitExceeded`.

### Policy the decorator applies

Defaults, overridable by decorator keyword arguments and by `apply_async(..., soft_time_limit=, time_limit=)`:

| Setting | Value |
|---|---|
| `acks_late` | `True` |
| `acks_on_failure_or_timeout` | `True` |
| `reject_on_worker_lost` | `True` |
| `soft_time_limit` | `None` |
| `time_limit` | `None` |
| `time_limit_grace` | 30 seconds, used only when soft is set and hard is not |
| `autoretry_for` | `(SoftTimeLimitExceeded, ConnectionError, TimeoutError, LeaseLost, UnitTimeout)` |
| `dont_autoretry_for` | `(NonRetryableDurabilityError,)` which covers `DurabilityDivergence` and encode failures |
| `retry_backoff` | `True` |
| `retry_backoff_max` | 600 |
| `retry_jitter` | `True` |
| `max_retries` | 3 |
| `max_deliveries` | 20, journal attempt cap, separate from `max_retries` |

No silent time limit. A limit that is too small kills a healthy long run. Callers who want a bound set it:

```python
@durability.task(app, soft_time_limit=3600)
def analyse(prompt: str) -> str: ...

analyse.apply_async(args=['...'], soft_time_limit=1800, time_limit=1830)
```

When `soft_time_limit` is set and `time_limit` is omitted, the decorator sets `time_limit = soft_time_limit + time_limit_grace`. Pass `time_limit_grace=None` to keep the hard limit unset. When both are set, decoration raises `ValueError` unless `time_limit > soft_time_limit`.

The grace exists so a soft-limit exception can leave the child. The hard limit is the backstop for a handler that catches `SoftTimeLimitExceeded` and keeps working. The library does not catch `SoftTimeLimitExceeded`.

### What each ending does

1. **Step finishes.** One atomic journal write. A later delivery at this index returns it.
2. **Unit `timeout` elapses.** `asyncio.timeout` cancels `body()`. No journal write. `UnitTimeout` is in `autoretry_for`. The next attempt replays earlier steps and runs this unit again. Provider HTTP timeouts stay on the model HTTP client; this library does not set them.
3. **Soft time limit.** The exception propagates out of the task. Completed steps stay in the journal. Autoretry republishes the same task id up to `max_retries`, with backoff. The new attempt replays the journal. When retries are exhausted, `acks_on_failure_or_timeout` acks the message and Celery records the failure. The journal is kept until `journal_ttl`. `resume(task, ...)` continues it.
4. **Hard time limit.** The child is killed. `on_timeout` acks because `acks_on_failure_or_timeout` is true, so `reject_on_worker_lost` does not bring the message back. Completed steps stay in the journal. The run stops. `resume(task, ...)` continues it. Turning `acks_on_failure_or_timeout` off to chase an automatic requeue is rejected by this design: the same flag would also requeue an exhausted soft-limit failure forever (`on_failure` requeues every `TimeLimitExceeded` when the flag is off).
5. **Worker killed, OOM, or other `WorkerLostError`.** `reject_on_worker_lost` requeues even when `acks_on_failure_or_timeout` is true. `Request.on_failure` takes that branch before the ack branch. The new delivery bumps the attempt counter. At `max_deliveries` the wrapper raises `NonRetryableDurabilityError` before calling the user function, the message is acked, and Celery records a failure. That caps the message loop Celery warns about. `resume` deletes the attempt key, then publishes the same task id. The next delivery bumps from zero. `resume` raises `RunInProgress` when the lease is held.
6. **Duplicate delivery while the owner is alive.** Redis or SQS visibility timeout, or a RabbitMQ `consumer_timeout`, can hand the run to a second child while the first is still executing. The second child fails `acquire_lease` and raises `LeaseHeld`. `LeaseHeld` is not in `autoretry_for`. The wrapper turns it into `task.retry(countdown=lease_ttl, max_retries=10_000)`. Celery's `retry()` publishes the replacement, then `on_retry` acks the delivery. The probe budget is independent of the task's `max_retries=3`, because `Task.retry(max_retries=...)` overrides the task default for that call. The owner keeps the lease. If the owner dies, the lease expires and the probe acquires it and resumes. If the owner finishes, it writes the completion record before releasing the lease, and the probe returns that record. `10_000` probes at the default 30 s lease is about 3.5 days. That covers a visibility timeout set shorter than the run without a tight requeue loop. The ack inside `retry()` can acknowledge the owner's original Redis delivery. That is safe because the probe message is already published, the journal still holds committed steps, and the lease still stops the probe from executing until the owner leaves.
7. **Lease refresh fails mid-run.** `put_step` refuses the write and raises `LeaseLost`. Autoretry resumes the run on a delivery that can acquire the lease.

`SIGUSR1` soft limits need a POSIX worker. That matches the platforms this package tests.

### Lease

- Token: random string, key `{prefix}:{task_id}:lease`.
- `acquire_lease` is `SET key token NX PX ttl`.
- Default `lease_ttl` is 30 s. Default `lease_heartbeat` is 10 s.
- A daemon thread refreshes with `SET key token XX PX ttl` on its own borrow from a connection pool. The agent event loop can be blocked in a sync tool, so the heartbeat does not run there.
- Refresh stops before `release_lease`. Release is a compare-and-delete of the token.
- Completion is written before release, while the lease is still held.

### Worker startup check

The first `@durability.task` on an app registers one `worker_init` handler. It logs warnings. It does not refuse to boot. `check_worker_config=False` on the capability skips it.

Warnings:

- Broker URL is Redis or SQS, `time_limit` is set, and `visibility_timeout <= time_limit`.
- Broker URL is Redis or SQS, `time_limit` is unset, and `visibility_timeout` is the default 3600. A run longer than an hour will be redelivered. The lease keeps that from double-executing, and the probe retry covers recovery, at the cost of extra messages.
- `worker_prefetch_multiplier != 1`.
- `worker_cancel_long_running_tasks_on_connection_loss` is true. A broker blip will kill the run. The journal still resumes it on redelivery. The recommendation for agent workers is `False`, including after Celery 6 flips the default.

RabbitMQ's `consumer_timeout` is not visible from the Celery app. The check logs one info line: the broker default is 30 minutes, and a longer run needs a higher `consumer_timeout` or a queue argument `x-consumer-timeout` in milliseconds. `agent_queue(name, consumer_timeout_ms)` returns a Celery `Queue` with that argument set, for the user to put on `task_queues`.

`result_expires` (Celery default 1 day) is the result-backend retention, not the journal TTL. Callers that poll `AsyncResult` past that window lose the Celery result. The journal still has the completion record until `journal_ttl`. The library does not change `result_expires`.

`task_track_started` is left off. It is useful operationally and costs a backend write per run. Users can enable it on the app.

## Failure and idempotency

A step can run twice when the process dies after the side effect and before the journal `SET`. Tools and capability operations need to be idempotent. The library does not offer an at-most-once mode. Celery has no such primitive.

`max_retries` bounds exception retries. `max_deliveries` bounds crash redeliveries. They are separate counters.

Divergence, JSON encode failures, and exhausted deliveries are `NonRetryableDurabilityError`. They are listed in `dont_autoretry_for` and acked as task failures.

Decorator arguments that try to set `acks_late=False` or `acks_on_failure_or_timeout=False` raise `ValueError` at registration. Those combinations fight the recovery rules above.

## Packaging

- Distribution name `pydantic-ai-celery`, import `pydantic_ai_celery`.
- `pydantic-ai>=2.55.0`, `celery>=5.5`, `redis`. Celery 5.6.3 classifiers list Python up through 3.13. This project requires 3.14. The test suite imports Celery on 3.14 and is the compatibility check. The floor stays 3.14 unless that import or the suite fails.
- Public exports: `CeleryDurability`, `CeleryOperationConfig`, `Journal`, `MemoryJournal`, `RedisJournal`, `configure_celery_app`, `agent_queue`, `resume`, `NonRetryableDurabilityError`, `RunInProgress`.
- Remove the scaffold `pydantic-ai-celery` console script.
- No dependency on `pydantic-ai-slim` extra groups for Temporal, DBOS, or Prefect.

### Module split

- `_durability.py` — capability, container, task decorator, resume
- `_backend.py` — `CeleryOperationBackend`
- `_journal.py` — protocol, memory, Redis
- `_context.py` — task-id context var
- `_errors.py`
- `_config.py` — operation config and limit validation
- `_worker_checks.py` — pure function from a conf mapping to warning strings

## Tests

Unit tests use `MemoryJournal` and `TestModel`. No broker, no pytest-celery.

- Outside a container, a tool and a model call write nothing and still return.
- A run that stops after N committed steps, invoked again with the same task id, does not call the model or the tool for those steps and does call the next one.
- A second identical tool call in one run has the next index and its own record.
- `ApprovalRequired` / `ModelRetry` stored as wrapped values are returned on replay and the tool body count stays 1.
- Divergence on a tampered operation name raises `DurabilityDivergence` and does not call the body.
- A non-JSON tool return raises `NonRetryableDurabilityError` and writes no record.
- `UnitTimeout` writes no record. The next container entry runs the body.
- `SoftTimeLimitExceeded` raised from a body writes no record and propagates.
- Lease held: the wrapper requests a probe retry and does not call the user function.
- Lease free after a crash: replay serves committed steps.
- Attempt cap raises non-retryable before the user function. `resume` clears the counter. `resume` while the lease is held raises `RunInProgress` and does not publish.
- Lost lease on write raises `LeaseLost` and leaves the previous record in place.
- Decorator rejects `time_limit <= soft_time_limit` and derives `time_limit` from grace.
- `worker_config_warnings` covers visibility timeout, prefetch, and the connection-loss flag.
- A pinned list of operation names for one fixture agent (default model, one function tool, one MCP id). The pin is updated only when the persisted names are meant to change.

## Decisions

1. **In-process journal on one task.** Closures from `CallableOperationBackend` cannot be sent to another Celery worker. Registered tasks plus `AsyncResult.get()` deadlock the pool and still need this journal, because Celery does not dedupe task ids.
2. **Encounter index, not `cache_key`.** The public cache projection includes `RunContext`.
3. **JSON journal, strict control-flow values.** New engine. `tool_call_result_upgrade_lenient` stays off.
4. **Sequential tool calls.** Parallel starts make the encounter index depend on scheduler order. DBOS has already lost replay safety that way.
5. **No default task time limit.** Limits are opt-in. Soft limit autoretries. Hard limit is terminal until `resume`, because Celery 5.6.3 acks hard timeouts when `acks_on_failure_or_timeout` is on, and turning that flag off requeues exhausted soft timeouts forever.
6. **Lease plus probe retry for duplicate delivery.** Visibility timeout and RabbitMQ `consumer_timeout` both redeliver a live run. The lease stops the second child from executing steps. The probe retry puts a message back after its ack so a dead owner is still resumed.
7. **Separate delivery cap.** `reject_on_worker_lost` has no retry counter. `max_deliveries` supplies one.
8. **Worker and broker knobs are checked, not overwritten**, except by the explicit `configure_celery_app` helper.
