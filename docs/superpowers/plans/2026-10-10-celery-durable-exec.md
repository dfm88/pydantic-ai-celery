# Celery durable execution implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `pydantic-ai-celery` as a public library that checkpoints a Pydantic AI run inside one Celery task, with Redis or Postgres behind one store and a cursor a producer can poll.

**Architecture:** `CeleryDurability` subclasses `BaseDurabilityCapability` and uses `JournalCallableOperationBackend`. Steps, the lease, and progress events live on `RunStore`. The Celery task is only the container: it replays from the top and returns journaled step results. HTTP, SSE, and websockets stay outside the package.

**Tech Stack:** Python `>=3.11`, `pydantic-ai>=2.36`, `celery>=5.3`, optional `redis>=5` and `psycopg[binary]>=3.2`, uv, pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-10-10-celery-durable-exec-design.md`

## Global Constraints

- Python `>=3.11`. `pydantic-ai>=2.36`. `celery>=5.3`. No upper pins. No version branches.
- Public builder only: `BaseDurabilityCapability`, `JournalCallableOperationBackend`, `DurabilityEngineSpec`, `JSON_CODEC`, `JournalOperationNamer`. Do not copy model, toolset, or event machinery. Do not import `in_durable_unit`.
- MCP lifecycle is `enter-always`. `sequential_tools_in_durable_context=True`. `tool_call_result_upgrade_lenient=False`. `tool_config_key='celery'`.
- Every duration, limit, prefix, and flag is a field on `DurabilitySettings` with the default in the spec table. `lease_probe_countdown_s is None` means `lease_ttl_s`.
- Probe republish uses `signature_from_request` and raises `celery.exceptions.Retry`. It does not call `Task.retry`.
- `autoretry_for` is the explicit tuple in the spec. Do not pass `dont_autoretry_for`.
- `acks_late=True` and `acks_on_failure_or_timeout=True` are required. Registration raises `ValueError` if a caller sets either false.
- Unit tests use `MemoryRunStore` and `TestModel`. No broker. Redis and Postgres tests skip unless `REDIS_URL` or `POSTGRES_URL` is set.
- Package path stays `src/pydantic_ai_celery`. Build backend stays `uv_build`. Version stays the static `version` in `pyproject.toml`. License MIT.
- Remove the `pydantic-ai-celery` console script and `main()`.

## Testing

Not pure TDD. Do not write a failing test before each step, and do not add a test whose subject is packaging, imports, metadata, or whether the README compiles. Task 1 and Task 7 have no test file.

Unit tests cover behavior: settings validation, the memory store, replay, the Celery task policy. Integration tests cover Redis and Postgres, and they skip when `REDIS_URL` or `POSTGRES_URL` is unset. Implement the code and the tests in the same task, then run pytest once.

## Review Focus

- A second delivery that finds the lease held must not call the user function or the step body. A person would otherwise pay for the same model call twice.
- `Task.retry` must not be the probe path. A person who later hits a normal autoretry would inherit `override_max_retries` from the probe.
- `SoftTimeLimitExceeded` during a step writes no record and leaves the process alive so autoretry can replay.
- A tool return that JSON cannot encode writes no record and is not retried.
- A snapshot whose stored status is `running` and whose lease is expired reads as `interrupted`, with no background reaper.

---

### Task 1: Public project metadata and the test runner

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/pydantic_ai_celery/__init__.py`
- Create: `src/pydantic_ai_celery/py.typed`

**Interfaces:**
- Consumes: nothing
- Produces: an importable `pydantic_ai_celery` package, dev group `pytest` and `ruff`, pytest `testpaths = ["tests"]`

- [ ] **Step 1: Turn the scaffold into library metadata**

In `pyproject.toml`: description `Durable execution for Pydantic AI agents on Celery.`, `license = "MIT"`, README, classifiers for Alpha, Developers, and Python 3.11 through 3.14. Delete `[project.scripts]`. Add a `dev` dependency group with `pytest` and `ruff`, and `[tool.uv] default-groups = ["dev"]`. Add pytest `testpaths`. Replace `src/pydantic_ai_celery/__init__.py` with a module docstring and no `main`. Add an empty `py.typed`. No test for this task.

- [ ] **Step 2: Confirm the package installs**

Run: `uv sync --group dev && uv run python -c "import pydantic_ai_celery"`
Expected: exit 0

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml src/pydantic_ai_celery/__init__.py src/pydantic_ai_celery/py.typed uv.lock
git commit -m "Prepare pydantic-ai-celery as a library package."
```

### Task 2: Settings and errors

**Files:**
- Create: `src/pydantic_ai_celery/_config.py`
- Create: `src/pydantic_ai_celery/_errors.py`
- Test: `tests/test_settings.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `DurabilitySettings` keyword-only frozen dataclass. Fields and defaults are the spec table (`journal_ttl_s=604800`, `lease_ttl_s=30`, `lease_heartbeat_s=10`, `lease_probe_countdown_s=None`, `lease_probe_max_retries=10_000`, `soft_time_limit_s=None`, `time_limit_s=None`, `time_limit_grace_s=30`, `max_retries=3`, `max_deliveries=20`, `retry_backoff=True`, `retry_backoff_max_s=600`, `retry_jitter=True`, `acks_late=True`, `acks_on_failure_or_timeout=True`, `reject_on_worker_lost=True`, `key_prefix='pydantic-ai-celery'`, `postgres_schema='pydantic_ai_celery'`, `progress_page_limit=100`, `progress_include_payloads=False`, `check_worker_config=True`, `recommended_prefetch=1`, `recommended_visibility_timeout_s=86400`, `celery_default_visibility_timeout_s=3600`).
  - `DurabilitySettings.probe_countdown_s: float` returns `lease_ttl_s` when `lease_probe_countdown_s` is `None`.
  - `DurabilitySettings.resolved_time_limit_s: int | None` returns `soft_time_limit_s + time_limit_grace_s` when soft is set, hard is omitted, and grace is not `None`.
  - `NonRetryableDurabilityError(Exception)`, `DurabilityDivergence(NonRetryableDurabilityError)`, `RunInProgress(Exception)`, `LeaseHeld(Exception)`, `LeaseLost(Exception)`, `UnitTimeout(Exception)`.

- [ ] **Step 1: Implement the dataclass and the exception classes**

Validate in `__post_init__`. No other module defines these numbers.

- [ ] **Step 2: Add unit tests for the validation rules**

`tests/test_settings.py` asserts: a heartbeat that cannot miss one beat raises `ValueError`; `soft_time_limit_s=3600` with no hard limit resolves to `3630`; `time_limit_s <= soft_time_limit_s` raises `ValueError`; `probe_countdown_s` follows `lease_ttl_s` when unset. Do not assert every default field.

- [ ] **Step 3: Run the unit tests**

Run: `uv run pytest tests/test_settings.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/pydantic_ai_celery/_config.py src/pydantic_ai_celery/_errors.py tests/test_settings.py
git commit -m "Add durability settings and error types."
```

### Task 3: Memory run store

**Files:**
- Create: `src/pydantic_ai_celery/_store.py`
- Test: `tests/test_memory_store.py`

**Interfaces:**
- Consumes: `DurabilitySettings`
- Produces: `RunStore` protocol and `MemoryRunStore(settings: DurabilitySettings | None = None)` with `get_step`, `put_step`, `get_result`, `put_result`, `bump_attempt`, `reset_attempts`, `acquire_lease`, `refresh_lease`, `release_lease`, `lease_held`, `bump_probe`, `append_progress`, `read_progress`, `snapshot`. Records use `operation_name` and `payload`. `append_progress` returns the new `seq` starting at 1. `snapshot(task_id)` returns status `unknown` when absent, and `interrupted` when stored status is `running` and the lease is expired. `put_step` raises `LeaseLost` when the caller is not the lease owner and a lease is currently held.

- [ ] **Step 1: Implement `MemoryRunStore`**

Use `time.monotonic` for the lease deadline so tests can expire it by passing a clock callable `now: Callable[[], float]`. Default clock is `time.monotonic`. Do not start a thread.

- [ ] **Step 2: Add unit tests**

`tests/test_memory_store.py` covers: round-trip a step, second acquire fails while the lease is held, acquire succeeds after expiry, refresh keeps it, release lets a new token acquire, progress seq order, `read_progress(after=last)` is empty, running plus expired lease snapshots as `interrupted`.

- [ ] **Step 3: Run the unit tests**

Run: `uv run pytest tests/test_memory_store.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/pydantic_ai_celery/_store.py tests/test_memory_store.py
git commit -m "Add the in-memory run store."
```

### Task 4: Durable capability and step replay

**Files:**
- Create: `src/pydantic_ai_celery/_backend.py`
- Create: `src/pydantic_ai_celery/_context.py`
- Create: `src/pydantic_ai_celery/_durability.py`
- Modify: `src/pydantic_ai_celery/__init__.py`
- Test: `tests/test_replay.py`

**Interfaces:**
- Consumes: `MemoryRunStore`, `DurabilitySettings`, `LeaseLost`, `UnitTimeout`, `DurabilityDivergence`, `NonRetryableDurabilityError`
- Produces:
  - `current_task_id() -> str | None` and `container_scope(task_id: str)`
  - `CeleryOperationBackend(store, settings)` subclassing `JournalCallableOperationBackend`
  - `CeleryDurability(store: RunStore, *, settings: DurabilitySettings | None = None, listeners: Sequence[ProgressListener] = ())`
  - `CeleryDurability.container(task_id: str)` context manager
  - Engine spec from the spec, with `codec=JSON_CODEC` and `serialization_failure` returning `NonRetryableDurabilityError`
  - Exports listed in the spec's packaging section that exist after this task: `CeleryDurability`, `DurabilitySettings`, `CeleryOperationConfig`, `MemoryRunStore`, `NonRetryableDurabilityError`

`CeleryOperationConfig` is a frozen dataclass with `timeout: float | None = None`.

- [ ] **Step 1: Implement the backend and capability**

`execute` increments an in-memory counter on the context before awaiting. Ignore `cache_key`. On a hit with the same operation name, return the stored payload. On a hit with a different name, raise `DurabilityDivergence`. Wrap `body()` in `asyncio.timeout` when the resolved config timeout is not `None`. Append `step_started` and `step_finished` or `step_failed` through the store. A listener error is logged and swallowed.

- [ ] **Step 2: Add unit tests**

`tests/test_replay.py` uses `TestModel` and `MemoryRunStore`. Assert:

- outside `container()`, a tool call writes no step
- a tool that raises `RuntimeError` on the first container entry journals the model step and not the tool step; the second entry with the same task id does not call the model again and calls the tool body once more
- progress events for that second entry include `step_finished` for the cached model step, and a listener that raises is still followed by a successful tool result
- operation names for an agent named `analyst` with one function toolset id `tools` match `JournalOperationNamer` (`analyst__model.request`, `analyst__function_toolset__tools.call_tool:lookup`)
- two calls to the same tool in one run use indexes 0 and 1 only after the model steps, and each has its own record
- a tool that raises `ApprovalRequired` journals one record and a second entry does not call the body again
- a tampered `operation_name` raises `DurabilityDivergence` and does not call the body
- a tool that returns a non-JSON object raises `NonRetryableDurabilityError` and leaves no record
- `CeleryOperationConfig(timeout=0.01)` around a body that waits raises `UnitTimeout` and writes no record
- `in_durable_context` is false outside the container and true inside it

- [ ] **Step 3: Run the unit tests**

Run: `uv run pytest tests/test_replay.py tests/test_memory_store.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/pydantic_ai_celery/_backend.py src/pydantic_ai_celery/_context.py src/pydantic_ai_celery/_durability.py src/pydantic_ai_celery/__init__.py tests/test_replay.py
git commit -m "Replay journaled agent steps inside a Celery container."
```

### Task 5: Celery task policy, probe, and resume

**Files:**
- Modify: `src/pydantic_ai_celery/_durability.py`
- Create: `src/pydantic_ai_celery/_worker_checks.py`
- Test: `tests/test_task_policy.py`

**Interfaces:**
- Consumes: `CeleryDurability`, `DurabilitySettings`, `MemoryRunStore`
- Produces:
  - `maintain_lease(store, token, settings, stop)` which calls `refresh_lease` until `stop` is set. The container runs it on a daemon thread and sets `stop` on exit.
  - `configure_celery_app(app, settings=None, *, visibility_timeout=None, prefetch_multiplier=None)` writes `recommended_visibility_timeout_s` and `recommended_prefetch` unless overridden. Constructing `CeleryDurability` does not change `app.conf`.
  - `agent_queue(name, consumer_timeout_ms, *, settings=None) -> celery.Queue` with queue argument `x-consumer-timeout`.
  - `CeleryDurability.task(app, **overrides)` decorator
  - `CeleryDurability.completed_result(task_id) -> object | None`
  - `resume(task, *, args=(), kwargs=None) -> AsyncResult` that raises `RunInProgress` when `lease_held` and otherwise `reset_attempts` then `apply_async(..., task_id=existing)`
  - `worker_config_warnings(conf: Mapping[str, object], settings: DurabilitySettings) -> list[str]`
  - `republish_probe(task, store, settings) -> None` which bumps the probe counter, raises `NonRetryableDurabilityError` above `lease_probe_max_retries`, calls `signature_from_request` with `countdown=settings.probe_countdown_s` and `retries=task.request.retries`, `apply_async`, then raises `Retry`

- [ ] **Step 1: Implement the decorator, probe, resume, and warnings**

The decorator checks `completed_result` before entering the container and starts `maintain_lease` on a daemon thread while the container is open. `container()` is not reentrant. Register one `worker_init` handler when `check_worker_config` is true. Do not mutate `app.conf` from the capability constructor.

- [ ] **Step 2: Add unit tests**

`tests/test_task_policy.py` asserts: the decorated task has `acks_late`, `acks_on_failure_or_timeout`, and `reject_on_worker_lost` true, and `acks_late=False` raises `ValueError`. The task's `time_limit` is `resolved_time_limit_s`. A `LeaseHeld` path calls a fake `signature_from_request` and does not call `Task.retry` or the user function. Probe count past `lease_probe_max_retries` raises `NonRetryableDurabilityError`. `max_deliveries` raises before the user function. `resume` clears attempts, and `resume` while the lease is held raises `RunInProgress`. `SoftTimeLimitExceeded` from a step body writes no record and propagates. `worker_config_warnings` warns when Redis `visibility_timeout` is `3600` and `time_limit_s` is unset, and when prefetch is not `recommended_prefetch`. A failed `apply_async` leaves progress kinds `queued` then `failed`. A second delivery with a stored completion returns that value and does not call the user function. `maintain_lease` calls `refresh_lease` and returns once that refresh sets the stop event. `configure_celery_app` sets visibility timeout and prefetch, and constructing the capability leaves `app.conf` unchanged. `agent_queue('agents', 3_600_000).queue_arguments['x-consumer-timeout'] == 3_600_000`.

- [ ] **Step 3: Run the unit tests**

Run: `uv run pytest tests/test_task_policy.py tests/test_replay.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/pydantic_ai_celery/_durability.py src/pydantic_ai_celery/_worker_checks.py tests/test_task_policy.py
git commit -m "Apply Celery ack, probe, and resume policy."
```

### Task 6: Redis and Postgres stores

**Files:**
- Modify: `src/pydantic_ai_celery/_store.py`
- Modify: `src/pydantic_ai_celery/_durability.py` (URL dispatch)
- Test: `tests/test_store_parity.py`

**Interfaces:**
- Consumes: `RunStore`, `DurabilitySettings`, `LeaseLost`
- Produces: `RedisRunStore(url: str, settings: DurabilitySettings | None = None)`, `PostgresRunStore(url: str, settings: DurabilitySettings | None = None)` with `setup()` and `purge_expired()`. `CeleryDurability` accepts `redis://`, `rediss://`, `postgresql://`, and `postgres://`.

- [ ] **Step 1: Implement both stores from the spec layout**

Redis keys use `settings.key_prefix` and `journal_ttl_s`. Postgres uses `settings.postgres_schema`, `statement_timestamp()`, and the lease `UPDATE` in the spec. `setup()` is explicit. Expired Postgres rows are invisible to reads. No reaper thread.

- [ ] **Step 2: Add integration tests**

`tests/test_store_parity.py` runs only when the extra imports and `REDIS_URL` or `POSTGRES_URL` is set. Otherwise the test skips. Do not re-run the memory suite here. Each backend gets one test that acquires a lease, writes a step, reads it back, loses a second acquire while the lease is live, appends a progress event, and reads it by cursor. Postgres also asserts `purge_expired` removes an already-expired step.

- [ ] **Step 3: Run the integration tests**

Run: `uv run pytest tests/test_store_parity.py -v`
Expected: PASS, or SKIP when the URL is unset.

- [ ] **Step 4: Commit**

```bash
git add src/pydantic_ai_celery/_store.py src/pydantic_ai_celery/_durability.py tests/test_store_parity.py
git commit -m "Add Redis and Postgres run stores."
```

### Task 7: README, agent guide, contributing, license, example, CI

**Files:**
- Create: `README.md`
- Create: `AGENTS.md`
- Create: `CONTRIBUTING.md`
- Create: `LICENSE`
- Create: `examples/durable_run.py`
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/publish.yml`
- Modify: `src/pydantic_ai_celery/__init__.py` (export the rest of the public names)

**Interfaces:**
- Consumes: the public API from Tasks 4–6
- Produces: the repository surface in the spec's "Public repository" section

- [ ] **Step 1: Write the public surface**

`README.md` follows the shape of pydantic-ai-pgtask: one-sentence promise, the crash problem, `pip install pydantic-ai-celery`, one example using `@durability.task` and `durability.progress`, how replay works, the deterministic encounter-order note, and that polling is the progress API. `examples/durable_run.py` is that example and is not executed in unit tests.

`AGENTS.md` tells an agent to read the spec, use the public builder only, keep numbers on `DurabilitySettings`, and run `uv run pytest`.

`CONTRIBUTING.md` says `uv sync --group dev --all-extras`, `uv run pytest`, and that `REDIS_URL` and `POSTGRES_URL` opt into integration tests.

`LICENSE` is the MIT text with copyright Diego Margoni.

`ci.yml` runs ruff and pytest on Python 3.11 and 3.14 with `uv`. `publish.yml` runs on tags, builds with `uv build`, and publishes with `uv publish --trusted-publishing always`. It does not use dynamic versioning.

Export every name in the spec's public export list from `pydantic_ai_celery`.

- [ ] **Step 2: Run the unit suite and the linter**

Run: `uv run pytest -v && uv run ruff check src tests`
Expected: unit tests PASS. Integration tests SKIP without `REDIS_URL` and `POSTGRES_URL`.

- [ ] **Step 3: Commit**

```bash
git add README.md AGENTS.md CONTRIBUTING.md LICENSE examples/durable_run.py .github/workflows/ci.yml .github/workflows/publish.yml src/pydantic_ai_celery/__init__.py
git commit -m "Document pydantic-ai-celery as a public library."
```
