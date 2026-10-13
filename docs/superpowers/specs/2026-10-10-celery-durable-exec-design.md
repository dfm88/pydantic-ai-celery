# Celery durable execution for Pydantic AI

Status: draft for review. Approach A from the design discussion. No implementation yet.

This library is an external Pydantic AI capability. It checkpoints one agent run inside one Celery task. A worker crash, a retry, or a redelivery starts that task from the top and returns stored results for steps that already finished. The package depends on the public durable-execution builder (`BaseDurabilityCapability`, `JournalCallableOperationBackend`, `DurabilityEngineSpec`, `JSON_CODEC`, `JournalOperationNamer`). It does not copy model, toolset, event, or capability-operation machinery.

Python `>=3.11`. `pydantic-ai>=2.36` (the builder landed in 2.36; `JournalOperationNamer` is unchanged from 2.36 through 2.54). `celery>=5.3` (`acks_late`, `acks_on_failure_or_timeout`, `reject_on_worker_lost`, and `Task.retry` are already on `celery.app.task.Task` in 5.3.0). No upper caps. The implementation does not branch on versions: `autoretry_for` is an explicit tuple, so it does not need `dont_autoretry_for` (added in Celery 5.5). MCP uses `enter-always`, which has existed since 2.36. `enter-in-durable-unit` arrived in 2.45 and is not required, because every delivery restarts the container and reconnects.

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
from pydantic_ai_celery import (
    CeleryDurability,
    CeleryOperationConfig,
    configure_celery_app,
)

app = Celery("agents", broker="redis://localhost:6379/0")
configure_celery_app(app)

durability = CeleryDurability("redis://localhost:6379/1")
agent = Agent("openai:gpt-5", name="analyst", capabilities=[durability])


@durability.task(app)
def analyse(prompt: str) -> str:
    return agent.run_sync(prompt).output
```

`CeleryDurability` takes a `RunStore` or a `redis://`, `rediss://`, `postgresql://`, or `postgres://` URL. There is no default URL. The store is separate from the Celery result backend. `PENDING` on that backend means "unknown", so it cannot be the checkpoint store.

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

resume(analyse, args=["Summarize Q3"], kwargs={})
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

`in_durable_context` is true when the container context var is set. On pydantic-ai 2.52 and newer the builder itself suppresses nested work inside a step via its private `in_durable_unit()` helper. This library does not call that helper.

The encounter counter is per container, so two sequential `agent.run()` calls in one task share one sequence. That is supported. Two overlapping runs in one task are not.

### What is checkpointed

`JournalOperationNamer` names, including the tool-call label suffix (`...call_tool:{tool}`) for function and dynamic tools. A second occurrence of the same name is the next encounter index, not a `#2` suffix in the name.

Checkpointed units are model request, model stream, compaction, suspended-response cancellation, capability operations, function-tool validation and calls, dynamic-tool discovery, validation, and calls, MCP discovery, instructions, and calls, the event-stream handler, and the workspace call the base binds.

Control-flow results (`approval_required`, `call_deferred`, `model_retry`, `validation_error`, `tool_failed`) are stored as the builder's value wrappers. `tool_call_result_upgrade_lenient` stays `False`. Replay returns the stored wrapper. The tool body does not run again.

A unit that raises does not write a record. The task retry starts the container from the top, serves the earlier records, and runs that unit again.

### Engine spec

```python
DurabilityEngineSpec(
    engine_name="Celery",
    durable_unit_noun="step",
    durable_container_noun="task",
    codec=JSON_CODEC,
    serialization_failure=lambda exc: NonRetryableDurabilityError(str(exc)),
    wrapped_toolset_kinds=frozenset({"function", "mcp", "dynamic"}),
    toolset_lifecycles={
        "function": "enter-always",
        "mcp": "enter-always",
        "dynamic": "enter-never",
    },
    tool_call_result_upgrade_lenient=False,
    journal_discovery=True,
    sequential_tools_in_durable_context=True,
    unsupported_runtime_toolset_kinds=frozenset({"function", "mcp", "dynamic"}),
    tool_config_key="celery",
)
```

`enter-always` enters function and MCP toolsets around the container. A new delivery is a new process, so the MCP session from the previous process is gone and must be opened again. Connect failures fail that attempt; the task retry enters again.

JSON is the journal encoding. `JSON_CODEC` dumps inside the unit and loads outside it. A value that cannot be encoded becomes `NonRetryableDurabilityError`, which is excluded from autoretry. Pickle is not used.

Function tools are wrapped. On replay the agent loop calls every tool it called the first time. An unwrapped tool would run its side effect again before the next cached model step.

Runtime toolsets passed to `run(toolsets=...)` are rejected inside the container. `ExternalToolset` stays allowed by the builder.

### Run store

`RunStore` is the synchronous protocol behind Redis, Postgres, and the in-memory test double. Checkpoint methods and progress methods live on that one object, so a producer can switch backends without a second configuration.

Checkpoint methods: `get_step`, `put_step`, `get_result`, `put_result`, `bump_attempt`, `reset_attempts`, `acquire_lease`, `refresh_lease`, `release_lease`, `lease_held`, `bump_probe`.

Progress methods: `append_progress`, `read_progress`, `snapshot`.

`put_step` is one atomic write of `{"operation_name", "payload"}`. `payload` is what `body()` returned, already passed through the builder's result codec. The lease owner is checked as part of that write. A lost lease raises `LeaseLost`. The next delivery resumes from the last committed step.

A task return value that is not JSON-encodable still returns to Celery. The completion record is skipped. A duplicate delivery replays steps. Celery's default serializer is JSON, so the return value has to satisfy that anyway.

`MemoryRunStore` is always available. `RedisRunStore` needs the `redis` extra (`redis>=5`). `PostgresRunStore` needs the `postgres` extra (`psycopg[binary]>=3.2`). The constructor accepts a store instance or a URL:

- `redis://` and `rediss://` build a Redis store
- `postgresql://` and `postgres://` build a Postgres store

There is no default URL.

#### Redis layout

Prefix is `settings.key_prefix` (default `pydantic-ai-celery`). `{task_id}` in the table is a Redis hash tag: the key contains `{`, the task id, and `}`. A lease key looks like `pydantic-ai-celery:{task-id}:lease`. Standalone Redis treats those braces as part of the name. Every key a Lua script reads or writes is passed in `KEYS`, including `event:{seq}`.

| Key | Value |
|---|---|
| `{prefix}:{task_id}:step:{index}` | JSON step record |
| `{prefix}:{task_id}:result` | JSON task return value |
| `{prefix}:{task_id}:attempts` | integer |
| `{prefix}:{task_id}:probes` | integer |
| `{prefix}:{task_id}:lease` | owner token |
| `{prefix}:{task_id}:snapshot` | JSON `RunSnapshot` |
| `{prefix}:{task_id}:event:{seq}` | JSON `ProgressEvent` |
| `{prefix}:{task_id}:event_seq` | integer counter |

Step, result, attempt, probe, snapshot, and event keys use `settings.journal_ttl_s`, refreshed on each write. The lease uses `SET key token NX PX lease_ttl_ms` and refresh uses `SET key token XX PX lease_ttl_ms`. Sharing the broker's Redis database is allowed. `FLUSHDB` there deletes checkpoints. A separate logical database is the recommended URL.

#### Postgres layout

Schema is `settings.postgres_schema` (default `pydantic_ai_celery`). `PostgresRunStore.setup()` creates it and the tables. Importing the package does not connect.

`runs` — one row per task id: `status`, `result jsonb`, `attempts`, `probes`, `event_seq`, `lease_token`, `lease_until timestamptz`, `steps_completed`, `last_operation_name`, `error_type`, `error_message`, `updated_at`, `expires_at`.

`steps` — primary key `(task_id, step_index)`, `operation_name`, `payload jsonb`, `expires_at`.

`progress_events` — primary key `(task_id, seq)`, `kind`, `operation_name`, `step_index`, `attempt`, `error_type`, `error_message`, `created_at`, `expires_at`. `seq` is assigned with `UPDATE runs SET event_seq = event_seq + 1 RETURNING event_seq` in the same transaction as the insert.

Lease acquire:

```sql
UPDATE runs
SET lease_token = $token,
    lease_until = statement_timestamp() + make_interval(secs => $ttl)
WHERE task_id = $task_id
  AND (lease_token IS NULL OR lease_until < statement_timestamp() OR lease_token = $token)
```

If no row was updated, insert the run row and retry the update once. Refresh and release compare `lease_token`. Expiry uses `statement_timestamp()` so two app servers with skewed clocks share one clock.

Postgres has no key TTL. Reads ignore rows with `expires_at <= statement_timestamp()`. `purge_expired()` deletes them. The library does not start a reaper thread. The application schedules `purge_expired()` (a Celery beat task is a natural place). `expires_at` is `statement_timestamp() + settings.journal_ttl_s` on every write.

### Settings

Every duration, limit, prefix, and flag lives on `DurabilitySettings`. `CeleryDurability` stores one instance. `@durability.task` and `configure_celery_app` read it. A task decorator keyword overrides that task only. Implementation code does not embed these numbers.

`lease_probe_countdown_s is None` means "use `lease_ttl_s`". That is the only derived default. Construction raises `ValueError` when `lease_heartbeat_s * 2 >= lease_ttl_s` (two missed heartbeats must expire the lease), when `journal_ttl_s <= 0`, when `progress_page_limit < 1`, or when both time limits are set and `time_limit_s <= soft_time_limit_s`.

| Field | Default | Why this value |
|---|---|---|
| `journal_ttl_s` | 604800 (7 days) | Long enough to `resume` after a weekend, short enough that abandoned runs do not grow the store without a bound. |
| `lease_ttl_s` | 30 | A dead owner is replaceable in half a minute. Long enough that one missed heartbeat (a GC pause or a Redis blip) does not drop the lease. |
| `lease_heartbeat_s` | 10 | One third of the lease. The heartbeat thread can miss one beat and the owner still holds the lease. |
| `lease_probe_countdown_s` | `None` → `lease_ttl_s` | The next probe runs after one lease period, when the owner has either refreshed or expired. |
| `lease_probe_max_retries` | 10000 | At a 30 s countdown this is about 3.5 days of duplicate probes. Covers a visibility timeout set shorter than the run, then stops. |
| `soft_time_limit_s` | `None` | Agent runs have no natural duration. A default limit would kill healthy work. |
| `time_limit_s` | `None` | Same reason. The hard limit is opt-in. |
| `time_limit_grace_s` | 30 | When a soft limit is set and the hard limit is omitted, hard = soft + grace. 30 s lets `SoftTimeLimitExceeded` leave `asyncio.run`. It is not long enough to hide a stuck cleanup. `None` leaves the hard limit unset. |
| `max_retries` | 3 | Bounds exception autoretry. Matches the count Celery's own examples use for a short transient failure. A permanent error stops instead of looping. |
| `max_deliveries` | 20 | Crash redelivery cap, separate from `max_retries`. Higher because rolling deploys and OOM can kill a long run more than three times. |
| `retry_backoff` | `True` | After an outage, retries wait longer each time instead of hammering the model provider together. |
| `retry_backoff_max_s` | 600 | Celery's own cap in `celery.app.autoretry` (`retry_backoff_max` default). Ten minutes is the longest pause between exception retries. |
| `retry_jitter` | `True` | Celery's jitter default. Stops many tasks from retrying on the same second. |
| `acks_late` | `True` | The message stays unacked until the task returns, so a dead child can be redelivered. |
| `acks_on_failure_or_timeout` | `True` | Exhausted failures are acked. See the hard-limit section for why this stays on. |
| `reject_on_worker_lost` | `True` | `WorkerLostError` is requeued. `max_deliveries` caps the loop Celery warns about. |
| `key_prefix` | `pydantic-ai-celery` | Keeps Redis keys out of the way of other applications in the same database. |
| `postgres_schema` | `pydantic_ai_celery` | Same idea for tables. |
| `progress_page_limit` | 100 | One poll returns a screenful of steps without shipping an unbounded history. |
| `progress_include_payloads` | `False` | Step payloads hold prompts and tool results. The progress feed stays metadata unless the application opts in. |
| `check_worker_config` | `True` | Operators should see visibility-timeout and prefetch footguns. Set false to silence the worker-start log. |
| `recommended_prefetch` | 1 | Celery's default prefetch is 4. Reserved long tasks sit unacked and the visibility timeout applies before they start. |
| `recommended_visibility_timeout_s` | 86400 | `configure_celery_app` writes this. A day covers a long agent run. Celery's docs warn that a huge visibility timeout delays redelivery after a real power loss, so the default is one day rather than unbounded. |
| `celery_default_visibility_timeout_s` | 3600 | Not written anywhere. The worker check uses it to notice a Redis or SQS broker that is still on Celery's transport default. |

`configure_celery_app(app, settings)` writes `recommended_visibility_timeout_s` into `broker_transport_options`, `result_backend_transport_options` when a result backend is set, and the app-level `visibility_timeout` the Redis transport reads. It writes `recommended_prefetch` into `worker_prefetch_multiplier`. Keyword arguments override those two fields for that call. Constructing the capability does not mutate the app.

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
| `autoretry_for` | empty | Exception classes the worker retries. `dont_autoretry_for` exists only since Celery 5.5, so this library does not use it. |
| Redis / SQS `visibility_timeout` | 3600 s | An unacked message is delivered again when this expires. With `acks_late` that includes a task that is still running. If several apps share a broker, the shortest value wins. |
| `worker_prefetch_multiplier` | 4 | Each child reserves that many messages. Reserved long tasks sit unacked, so the visibility timeout applies before they start. |
| `worker_cancel_long_running_tasks_on_connection_loss` | off in 5.6, on in Celery 6 | A broker blip kills late-ack tasks. The message is redelivered. |
| RabbitMQ `consumer_timeout` | 30 minutes (`1_800_000` ms) | The broker closes the channel when a delivery is unacked this long. Since RabbitMQ 3.12 a queue can set `x-consumer-timeout` or the policy key `consumer-timeout`. |

`SoftTimeLimitExceeded` is a `TimeLimitExceeded`, and that is an `Exception`. A bare `autoretry_for=(Exception,)` retries it. After `max_retries`, `on_failure` sees `TimeLimitExceeded`.

### Policy the decorator applies

The decorator copies ack, retry, and time-limit fields from `DurabilitySettings`. `apply_async(..., soft_time_limit=, time_limit=)` still overrides a single call, which is Celery's own message field (`Request.time_limits`).

`autoretry_for` defaults to `(SoftTimeLimitExceeded, ConnectionError, TimeoutError, LeaseLost, UnitTimeout)`. `NonRetryableDurabilityError` is absent from that tuple, which is what keeps divergence and encode failures from retrying on Celery 5.3. A caller who passes `autoretry_for=(Exception,)` opts into retrying those too.

No default time limit. Callers who want a bound set `soft_time_limit_s` on the settings or on the decorator:

```python
@durability.task(app, soft_time_limit=3600)
def analyse(prompt: str) -> str: ...


analyse.apply_async(args=["..."], soft_time_limit=1800, time_limit=1830)
```

When `soft_time_limit_s` is set and `time_limit_s` is omitted, the decorator sets the Celery hard limit to `soft_time_limit_s + time_limit_grace_s`. `time_limit_grace_s=None` leaves the hard limit unset.

The grace exists so a soft-limit exception can leave the child. The hard limit is the backstop for a handler that catches `SoftTimeLimitExceeded` and keeps working. The library does not catch `SoftTimeLimitExceeded`.

### What each ending does

1. **Step finishes.** One atomic journal write. A later delivery at this index returns it.
2. **Unit `timeout` elapses.** `asyncio.timeout` cancels `body()`. No journal write. `UnitTimeout` is in `autoretry_for`. The next attempt replays earlier steps and runs this unit again. Provider HTTP timeouts stay on the model HTTP client; this library does not set them.
3. **Soft time limit.** The exception propagates out of the task. Completed steps stay in the journal. Autoretry republishes the same task id up to `max_retries`, with backoff. The new attempt replays the journal. When retries are exhausted, `acks_on_failure_or_timeout` acks the message and Celery records the failure. The journal is kept until `settings.journal_ttl_s`. `resume(task, ...)` continues it.
4. **Hard time limit.** The child is killed. `on_timeout` acks because `acks_on_failure_or_timeout` is true, so `reject_on_worker_lost` does not bring the message back. Completed steps stay in the journal. The run stops. `resume(task, ...)` continues it. Turning `acks_on_failure_or_timeout` off to chase an automatic requeue is rejected by this design: the same flag would also requeue an exhausted soft-limit failure forever (`on_failure` requeues every `TimeLimitExceeded` when the flag is off).
5. **Worker killed, OOM, or other `WorkerLostError`.** `reject_on_worker_lost` requeues even when `acks_on_failure_or_timeout` is true. `Request.on_failure` takes that branch before the ack branch. The new delivery bumps the attempt counter. At `max_deliveries` the wrapper raises `NonRetryableDurabilityError` before calling the user function, the message is acked, and Celery records a failure. That caps the message loop Celery warns about. `resume` deletes the attempt key, then publishes the same task id. The next delivery bumps from zero. `resume` raises `RunInProgress` when the lease is held.
6. **Duplicate delivery while the owner is alive.** Redis or SQS visibility timeout, or a RabbitMQ `consumer_timeout`, can hand the run to a second child while the first is still executing. The second child fails `acquire_lease` and raises `LeaseHeld`. `LeaseHeld` is not in `autoretry_for`. The wrapper bumps the probe counter, and once it passes `lease_probe_max_retries` raises `NonRetryableDurabilityError`. Otherwise it republishes with `signature_from_request(..., countdown=lease_probe_countdown, retries=request.retries)` and raises `celery.exceptions.Retry` so `on_retry` acks. It does not call `Task.retry`. In Celery 5.6.3 `Task.retry` assigns `override_max_retries` on the shared task object (`celery/app/task.py`), and `celery/app/autoretry.py` copies that value onto the next ordinary autoretry. Keeping `retries` unchanged means probes do not consume `max_retries`. The owner keeps the lease. If the owner dies, the lease expires and the probe acquires it and resumes. If the owner finishes, it writes the completion record before releasing the lease, and the probe returns that record. The ack can acknowledge the owner's original Redis delivery. The probe message is already published, the journal still holds committed steps, and the lease still stops the probe from executing until the owner leaves.
7. **Lease refresh fails mid-run.** `put_step` refuses the write and raises `LeaseLost`. Autoretry resumes the run on a delivery that can acquire the lease.

`SIGUSR1` soft limits need a POSIX worker. That matches the platforms this package tests.

### Lease

- Token: random string, stored as the Redis lease key or the Postgres `lease_token` column.
- A daemon thread refreshes on its own connection. The agent event loop can be blocked in a sync tool, so the heartbeat does not run there. The thread period is `lease_heartbeat_s` and the TTL it writes is `lease_ttl_s`.
- Refresh stops before `release_lease`. Release is a compare-and-delete of the token.
- Completion is written before release, while the lease is still held.

### Worker startup check

The first `@durability.task` on an app registers one `worker_init` handler. It logs warnings. It does not refuse to boot. `check_worker_config=False` on the capability skips it.

Warnings:

- Broker URL is Redis or SQS, `time_limit` is set, and `visibility_timeout <= time_limit`.
- Broker URL is Redis or SQS, `time_limit_s` is unset, and `visibility_timeout` equals `settings.celery_default_visibility_timeout_s` (Celery's 1 hour transport default). A longer run is redelivered. The lease keeps that from double-executing, and the probe covers recovery, at the cost of extra messages.
- `worker_prefetch_multiplier` differs from `settings.recommended_prefetch`.
- `worker_cancel_long_running_tasks_on_connection_loss` is true. A broker blip will kill the run. The journal still resumes it on redelivery. The recommendation for agent workers is `False`, including after Celery 6 flips the default.

RabbitMQ's `consumer_timeout` is not visible from the Celery app. The check logs one info line: the broker default is 30 minutes, and a longer run needs a higher `consumer_timeout` or a queue argument `x-consumer-timeout` in milliseconds. `agent_queue(name, consumer_timeout_ms)` returns a Celery `Queue` with that argument set, for the user to put on `task_queues`.

`result_expires` (Celery default 1 day) is the result-backend retention, not the journal TTL. Callers that poll `AsyncResult` past that window lose the Celery result. The journal still has the completion record until `settings.journal_ttl_s`. The library does not change `result_expires`.

`task_track_started` is left off. It is useful operationally and costs a backend write per run. Users can enable it on the app.

## Progress

Producers and frontends need the run's status and a cursor through completed steps. The library stores that. It does not open a socket.

Three shapes were considered:

| Shape | What the client learns | Limit |
|---|---|---|
| Celery `AsyncResult.state` | `PENDING`, `STARTED`, `RETRY`, `SUCCESS`, `FAILURE` | `PENDING` means unknown. There is no step cursor. Custom states from `update_state` are clobbered by the next state write and require a result backend. |
| Reading journal step keys | Which indexes exist | The client learns the store layout and, if payloads are included, sees prompts and tool results. Redis and Postgres would each need their own reader. |
| A progress log on `RunStore` | Snapshot plus events after a cursor | One extra write per transition. Payloads stay out unless `progress_include_payloads` is set. |

The progress log is the contract. Polling, server-sent events, and websockets are adapters the application writes against it. A reconnecting client passes the last `seq` it saw and receives anything it missed. Push is optional and lossy; the log is what makes a missed push recoverable.

```python
page = durability.progress(task_id, after=cursor)
# page.snapshot.status, page.events, page.next_seq
```

`RunSnapshot` fields: `task_id`, `status`, `attempt`, `steps_completed`, `last_operation_name`, `error_type`, `error_message`, `updated_at`.

`status` is `unknown` (no row), `queued`, `running`, `interrupted`, `retrying`, `succeeded`, or `failed`. `interrupted` is computed at read time when the stored status is `running` and the lease is missing or expired. Nothing has to flip that column when a worker dies.

`ProgressEvent` fields: `task_id`, `seq`, `kind`, `at`, `operation_name`, `step_index`, `attempt`, `error_type`, `error_message`. `kind` is `queued`, `started`, `step_started`, `step_finished`, `step_failed`, `retrying`, `succeeded`, or `failed`. `seq` starts at 1 and increases by one per task id on both backends.

`progress(task_id, after=None, limit=None)` returns the snapshot and events with `seq > after`, at most `limit` or `progress_page_limit`. `next_seq` is the last seq in the page, or `after` when the page is empty.

`@durability.task` wraps `apply_async` and `delay`. It appends `queued` before publish. If publish raises, it appends `failed` with that error. The container appends `started` after the lease is acquired, `step_started` / `step_finished` around each unit, `step_failed` when a unit raises, `retrying` before a probe or an autoretry leaves the child, and `succeeded` or `failed` with the terminal outcome. A listener exception is logged and does not fail the run.

```python
class ProgressListener(Protocol):
    def on_progress(self, event: ProgressEvent) -> None: ...
```

`CeleryDurability(..., listeners=(fanout,))` calls each listener after the event is stored. A websocket or SSE handler registers a listener that pushes to its connections. On connect it calls `progress(after=last_seq)` and then streams new events. Polling is the same call from a request handler, with no listener.

Celery's result backend remains the place to fetch the task return value. It is not the progress feed. The library does not call `update_state`.

## Failure and idempotency

A step can run twice when the process dies after the side effect and before the journal `SET`. Tools and capability operations need to be idempotent. The library does not offer an at-most-once mode. Celery has no such primitive.

`max_retries` bounds exception retries. `max_deliveries` bounds crash redeliveries. They are separate counters.

Divergence, JSON encode failures, and exhausted deliveries or probes are `NonRetryableDurabilityError`. They are not in `autoretry_for`, so the worker acks them as task failures.

Decorator arguments that try to set `acks_late=False` or `acks_on_failure_or_timeout=False` raise `ValueError` at registration. Those combinations fight the recovery rules above.

## Packaging

- Distribution name `pydantic-ai-celery`, import `pydantic_ai_celery`.
- Requires Python `>=3.11`, `pydantic-ai>=2.36`, `celery>=5.3`. No upper pins.
- Extra `redis`: `redis>=5`. Extra `postgres`: `psycopg[binary]>=3.2`.
- Celery 5.6.3 classifiers stop at Python 3.13 and `Requires-Python` is `>=3.9`, so 3.14 is installable and unverified upstream. The suite on 3.14 is our check. 3.11 and 3.12 stay in the declared range.
- Public exports: `CeleryDurability`, `DurabilitySettings`, `CeleryOperationConfig`, `RunStore`, `MemoryRunStore`, `RedisRunStore`, `PostgresRunStore`, `ProgressEvent`, `ProgressPage`, `RunSnapshot`, `ProgressListener`, `configure_celery_app`, `agent_queue`, `resume`, `NonRetryableDurabilityError`, `RunInProgress`.
- Remove the scaffold `pydantic-ai-celery` console script.
- No dependency on Temporal, DBOS, or Prefect extras.

### Public repository

The repository is the public library, laid out in the spirit of [pydantic-ai-pgtask](https://github.com/Kludex/pydantic-ai-pgtask): a README that states the problem, the install line, one example, and how replay works; an `examples/` script; `py.typed`; MIT license; ruff and pytest; a CI workflow; a tag-triggered PyPI publish workflow using `uv publish` trusted publishing. Version stays the static `version` in `pyproject.toml`. The package stays in `src/pydantic_ai_celery` because that is the layout this repo already builds with `uv_build`.

Also present, because this repo is meant to be cloned by people and by agents:

- `AGENTS.md` at the repo root. It points at this spec, forbids copying Pydantic AI model or toolset internals, and states the version floors and the `DurabilitySettings` rule.
- `CONTRIBUTING.md`. How to install the dev environment, run the unit suite, and opt into Redis and Postgres integration tests.
- `LICENSE`. MIT.

The README example matches `@durability.task` from the user-API section. It shows `durability.progress(task_id)` as the way a producer polls. It does not document a websocket server.

### Module split

- `_durability.py` — capability, container, task decorator, resume
- `_backend.py` — `CeleryOperationBackend`
- `_store.py` — `RunStore`, memory, Redis, Postgres, progress records
- `_context.py` — task-id context var
- `_errors.py`
- `_config.py` — operation config and limit validation
- `_worker_checks.py` — pure function from a conf mapping to warning strings

## Tests

Unit tests use `MemoryRunStore` and `TestModel`. No broker and no database. Postgres and Redis suites run only when `POSTGRES_URL` or `REDIS_URL` is set, and they exercise the same `RunStore` cases (lease expiry, step replay, progress cursor, `purge_expired`).

- Outside a container, a tool and a model call write nothing and still return.
- A run that stops after N committed steps, invoked again with the same task id, does not call the model or the tool for those steps and does call the next one.
- A second identical tool call in one run has the next index and its own record.
- `ApprovalRequired` / `ModelRetry` stored as wrapped values are returned on replay and the tool body count stays 1.
- Divergence on a tampered operation name raises `DurabilityDivergence` and does not call the body.
- A non-JSON tool return raises `NonRetryableDurabilityError` and writes no record.
- `UnitTimeout` writes no record. The next container entry runs the body.
- `SoftTimeLimitExceeded` raised from a body writes no record and propagates.
- Lease held: the wrapper republishes a probe via `signature_from_request` and does not call `Task.retry` or the user function. The probe counter stops the chain at `lease_probe_max_retries`.
- `progress` after a partial run returns `running` or `interrupted`, events in `seq` order, and an empty page when `after` is the last seq.
- A listener that raises does not fail the step.
- A publish that raises stores `queued` and then `failed`.
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
6. **Lease plus a manual probe republish.** Visibility timeout and RabbitMQ `consumer_timeout` both redeliver a live run. The probe uses `signature_from_request` and `Retry`. `Task.retry(max_retries=...)` is avoided because it sets `override_max_retries` on the shared task.
7. **Separate delivery cap.** `reject_on_worker_lost` has no retry counter. `max_deliveries` supplies one. Probes have their own counter.
8. **Worker and broker knobs are checked, not overwritten**, except by the explicit `configure_celery_app` helper.
9. **One `RunStore` for Redis and Postgres.** Progress is methods on that store. HTTP polling, SSE, and websockets stay in the application and share `progress(after=seq)`.
10. **Defaults live only on `DurabilitySettings`.** Each default is the value in the settings table, for the reason written there.
