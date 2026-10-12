"""Run stores for checkpoints, leases, and progress.

``MemoryRunStore`` is in-process. ``RedisRunStore`` and ``PostgresRunStore``
are the external backends. Constructing either imports its driver and does not
connect. ``setup()`` is explicit, and nothing here starts a reaper thread.
"""

import json
import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable
from urllib.parse import urlsplit

from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._errors import LeaseLost

logger = logging.getLogger(__name__)

RunStatus = Literal[
    "unknown",
    "queued",
    "running",
    "interrupted",
    "retrying",
    "succeeded",
    "failed",
]
ProgressKind = Literal[
    "queued",
    "started",
    "step_started",
    "step_finished",
    "step_failed",
    "retrying",
    "succeeded",
    "failed",
]
_StoredStatus = Literal["queued", "running", "retrying", "succeeded", "failed"]

# ``started`` and step kinds are still a live run. ``interrupted`` is not a
# kind: snapshot computes it when a stored ``running`` row has no live lease.
_STATUS_BY_KIND: dict[ProgressKind, _StoredStatus] = {
    "queued": "queued",
    "started": "running",
    "step_started": "running",
    "step_finished": "running",
    "step_failed": "running",
    "retrying": "retrying",
    "succeeded": "succeeded",
    "failed": "failed",
}


@dataclass(frozen=True, slots=True)
class StepRecord:
    """One journaled step. ``payload`` is the value ``body()`` returned."""

    operation_name: str
    payload: Any


@dataclass(frozen=True, slots=True)
class ResultRecord:
    """A stored task return value. The record, not the payload, means completed."""

    payload: Any


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """Point-in-time view of one task. ``interrupted`` is filled in at read time."""

    task_id: str
    status: RunStatus
    attempt: int
    steps_completed: int
    last_operation_name: str | None
    error_type: str | None
    error_message: str | None
    updated_at: float | None


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    """One progress log entry. ``seq`` starts at 1 for each task."""

    task_id: str
    seq: int
    kind: ProgressKind
    at: float
    operation_name: str | None = None
    step_index: int | None = None
    attempt: int | None = None
    error_type: str | None = None
    error_message: str | None = None


@runtime_checkable
class RunStore(Protocol):
    """Synchronous checkpoint and progress backend."""

    def get_step(self, task_id: str, index: int) -> StepRecord | None: ...

    def put_step(
        self,
        task_id: str,
        index: int,
        operation_name: str,
        payload: Any,
        *,
        token: str,
    ) -> None: ...

    def get_result(self, task_id: str) -> ResultRecord | None: ...

    def put_result(self, task_id: str, payload: Any, *, token: str) -> None: ...

    def bump_attempt(self, task_id: str) -> int: ...

    def reset_attempts(self, task_id: str) -> None: ...

    def acquire_lease(self, task_id: str, token: str) -> bool: ...

    def refresh_lease(self, task_id: str, token: str) -> bool: ...

    def release_lease(self, task_id: str, token: str) -> bool: ...

    def lease_held(self, task_id: str, token: str | None = None) -> bool: ...

    def bump_probe(self, task_id: str) -> int: ...

    def append_progress(
        self,
        task_id: str,
        kind: ProgressKind,
        *,
        operation_name: str | None = None,
        step_index: int | None = None,
        attempt: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> int: ...

    def read_progress(
        self,
        task_id: str,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[ProgressEvent, ...]: ...

    def snapshot(self, task_id: str) -> RunSnapshot: ...


@dataclass(frozen=True, slots=True)
class _Lease:
    token: str
    deadline: float


@dataclass(slots=True)
class _Run:
    steps: dict[int, StepRecord] = field(default_factory=dict)
    events: list[ProgressEvent] = field(default_factory=list)
    result: ResultRecord | None = None
    attempts: int = 0
    probes: int = 0
    status: _StoredStatus | None = None
    last_operation_name: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    updated_at: float | None = None
    lease: _Lease | None = None


def _holder(run: _Run | None, now: float) -> str | None:
    # Due at the deadline, so advancing the clock by exactly lease_ttl_s drops it.
    if run is None or run.lease is None or now >= run.lease.deadline:
        return None
    return run.lease.token


def _unknown(task_id: str) -> RunSnapshot:
    return RunSnapshot(
        task_id=task_id,
        status="unknown",
        attempt=0,
        steps_completed=0,
        last_operation_name=None,
        error_type=None,
        error_message=None,
        updated_at=None,
    )


class MemoryRunStore:
    """Process-local ``RunStore``.

    ``now`` defaults to ``time.monotonic``. Lease deadlines, progress ``at``,
    and snapshot ``updated_at`` read that clock, so tests can expire a lease
    without sleeping. This class does not start a thread.

    A lease is held while ``now() < deadline``. Acquire and refresh set
    ``deadline = now() + settings.lease_ttl_s``. The same token may acquire
    again and slides the deadline; a different token fails while it is held.
    Refresh and release succeed only for the current holder, and refresh does
    not revive an expired lease.

    ``put_step`` and ``put_result`` raise ``LeaseLost`` unless ``token`` is the live holder.
    A task that only holds a lease has no journal row: ``snapshot`` stays
    ``unknown`` until a step, result, counter, or progress event is written.
    Stored status ``running`` is read back as ``interrupted`` when that lease
    is missing or expired. The stored status is not rewritten.
    """

    def __init__(
        self,
        settings: DurabilitySettings | None = None,
        *,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._settings = settings if settings is not None else DurabilitySettings()
        self._now = time.monotonic if now is None else now
        self._lock = threading.Lock()
        self._runs: dict[str, _Run] = {}

    def get_step(self, task_id: str, index: int) -> StepRecord | None:
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return None
            return run.steps.get(index)

    def put_step(
        self,
        task_id: str,
        index: int,
        operation_name: str,
        payload: Any,
        *,
        token: str,
    ) -> None:
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError(f"step index must be a non-negative int, got {index!r}")
        with self._lock:
            now = self._now()
            run = self._runs.get(task_id)
            # Check and write under one lock. A foreign, missing, or expired
            # lease is the same failure: this caller is not the holder.
            if run is None or _holder(run, now) != token:
                raise LeaseLost(
                    f"lease for task {task_id!r} is not held by this caller"
                )
            run.steps[index] = StepRecord(
                operation_name=operation_name, payload=payload
            )
            run.last_operation_name = operation_name
            self._touch(run, now)

    def get_result(self, task_id: str) -> ResultRecord | None:
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return None
            return run.result

    def put_result(self, task_id: str, payload: Any, *, token: str) -> None:
        with self._lock:
            now = self._now()
            run = self._runs.get(task_id)
            # Same holder check as put_step, under the write lock. A probe must
            # not land a completion record between a lost lease and the next owner.
            if run is None or _holder(run, now) != token:
                raise LeaseLost(
                    f"lease for task {task_id!r} is not held by this caller"
                )
            run.result = ResultRecord(payload=payload)
            self._touch(run, now)

    def bump_attempt(self, task_id: str) -> int:
        with self._lock:
            run = self._ensure(task_id)
            run.attempts += 1
            self._touch(run, self._now())
            return run.attempts

    def reset_attempts(self, task_id: str) -> None:
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            run.attempts = 0
            if run.status is not None:
                run.updated_at = self._now()

    def acquire_lease(self, task_id: str, token: str) -> bool:
        with self._lock:
            now = self._now()
            run = self._runs.get(task_id)
            holder = _holder(run, now)
            if holder is not None and holder != token:
                return False
            if run is None:
                run = _Run()
                self._runs[task_id] = run
            run.lease = _Lease(
                token=token,
                deadline=now + self._settings.lease_ttl_s,
            )
            return True

    def refresh_lease(self, task_id: str, token: str) -> bool:
        with self._lock:
            now = self._now()
            run = self._runs.get(task_id)
            if run is None or _holder(run, now) != token:
                return False
            run.lease = _Lease(
                token=token,
                deadline=now + self._settings.lease_ttl_s,
            )
            return True

    def release_lease(self, task_id: str, token: str) -> bool:
        with self._lock:
            now = self._now()
            run = self._runs.get(task_id)
            if run is None or _holder(run, now) != token:
                return False
            run.lease = None
            return True

    def lease_held(self, task_id: str, token: str | None = None) -> bool:
        with self._lock:
            holder = _holder(self._runs.get(task_id), self._now())
            if token is None:
                return holder is not None
            return holder == token

    def bump_probe(self, task_id: str) -> int:
        with self._lock:
            run = self._ensure(task_id)
            run.probes += 1
            self._touch(run, self._now())
            return run.probes

    def append_progress(
        self,
        task_id: str,
        kind: ProgressKind,
        *,
        operation_name: str | None = None,
        step_index: int | None = None,
        attempt: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> int:
        try:
            status = _STATUS_BY_KIND[kind]
        except KeyError:
            raise ValueError(f"unknown progress kind {kind!r}") from None
        stored_error_type = None if kind == "succeeded" else error_type
        stored_error_message = None if kind == "succeeded" else error_message
        with self._lock:
            run = self._ensure(task_id)
            seq = len(run.events) + 1
            now = self._now()
            run.events.append(
                ProgressEvent(
                    task_id=task_id,
                    seq=seq,
                    kind=kind,
                    at=now,
                    operation_name=operation_name,
                    step_index=step_index,
                    attempt=attempt,
                    error_type=stored_error_type,
                    error_message=stored_error_message,
                )
            )
            run.status = status
            if operation_name is not None:
                run.last_operation_name = operation_name
            # Errors stick until the next event that carries some, or success.
            # A step or retry event with no error must not wipe the last one.
            if kind == "succeeded":
                run.error_type = None
                run.error_message = None
            elif error_type is not None or error_message is not None:
                run.error_type = error_type
                run.error_message = error_message
            run.updated_at = now
            return seq

    def read_progress(
        self,
        task_id: str,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[ProgressEvent, ...]:
        if limit is None:
            limit = self._settings.progress_page_limit
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}")
        cursor = 0 if after is None else after
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return ()
            picked: list[ProgressEvent] = []
            for event in run.events:
                if event.seq <= cursor:
                    continue
                picked.append(event)
                if len(picked) == limit:
                    break
            return tuple(picked)

    def snapshot(self, task_id: str) -> RunSnapshot:
        with self._lock:
            run = self._runs.get(task_id)
            # A lease on its own is not a journal row.
            if run is None or run.status is None:
                return _unknown(task_id)
            status: RunStatus = run.status
            # Computed on read. A later acquire of the same row shows running again.
            if status == "running" and _holder(run, self._now()) is None:
                status = "interrupted"
            return RunSnapshot(
                task_id=task_id,
                status=status,
                attempt=run.attempts,
                steps_completed=len(run.steps),
                last_operation_name=run.last_operation_name,
                error_type=run.error_type,
                error_message=run.error_message,
                updated_at=run.updated_at,
            )

    def _ensure(self, task_id: str) -> _Run:
        run = self._runs.get(task_id)
        if run is None:
            run = _Run()
            self._runs[task_id] = run
        return run

    def _touch(self, run: _Run, now: float) -> None:
        if run.status is None:
            run.status = "queued"
        run.updated_at = now


def _check_step_index(index: int) -> None:
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise ValueError(f"step index must be a non-negative int, got {index!r}")


def _page_limit(limit: int | None, settings: DurabilitySettings) -> int:
    if limit is None:
        limit = settings.progress_page_limit
    if limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}")
    return limit


def _stored_status(kind: str) -> _StoredStatus:
    try:
        return _STATUS_BY_KIND[kind]  # type: ignore[index]
    except KeyError:
        raise ValueError(f"unknown progress kind {kind!r}") from None


def _lease_lost(task_id: str) -> LeaseLost:
    return LeaseLost(f"lease for task {task_id!r} is not held by this caller")


def _epoch(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _load_redis() -> Any:
    try:
        import redis
    except ImportError as exc:
        raise ImportError(
            "RedisRunStore requires the redis extra "
            "(pip install 'pydantic-ai-celery[redis]')"
        ) from exc
    return redis


def _load_psycopg() -> tuple[Any, Any, Any, Any]:
    try:
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise ImportError(
            "PostgresRunStore requires the postgres extra "
            "(pip install 'pydantic-ai-celery[postgres]')"
        ) from exc
    return psycopg, sql, dict_row, Jsonb


def _lease_ttl_ms(settings: DurabilitySettings) -> int:
    # round() already returns an int. A sub-millisecond TTL would not stick.
    return max(1, round(settings.lease_ttl_s * 1000))


# Helpers are repeated in each script. Redis does not share Lua functions
# across EVAL calls. ``cjson.null`` is how Lua sees a JSON null.
_REDIS_LUA_PRELUDE = """
local function is_null(value)
  return value == nil or value == cjson.null
end

local function blank_snap(task_id)
  return {
    task_id = task_id,
    status = cjson.null,
    attempt = 0,
    steps_completed = 0,
    last_operation_name = cjson.null,
    error_type = cjson.null,
    error_message = cjson.null,
    updated_at = cjson.null
  }
end

local function load_snap(key, task_id)
  local raw = redis.call('GET', key)
  if not raw then
    return blank_snap(task_id)
  end
  return cjson.decode(raw)
end

local function now_seconds()
  local parts = redis.call('TIME')
  return tonumber(parts[1]) + tonumber(parts[2]) / 1000000
end

local function save_snap(key, snap, ttl)
  redis.call('SET', key, cjson.encode(snap), 'EX', ttl)
end
"""

# Not SET NX. NX would reject the current owner, and a re-acquire has to slide
# the deadline. A different live token still loses.
_REDIS_ACQUIRE = """
local current = redis.call('GET', KEYS[1])
if (not current) or current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
"""

_REDIS_REFRESH = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
"""

_REDIS_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

_REDIS_PUT_STEP = (
    _REDIS_LUA_PRELUDE
    + """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
local ttl = tonumber(ARGV[2])
local is_new = redis.call('EXISTS', KEYS[2]) == 0
redis.call('SET', KEYS[2], ARGV[3], 'EX', ttl)
local snap = load_snap(KEYS[3], ARGV[5])
if is_new then
  local count = snap.steps_completed
  if is_null(count) then
    count = 0
  end
  snap.steps_completed = tonumber(count) + 1
end
snap.last_operation_name = ARGV[4]
if is_null(snap.status) then
  snap.status = 'queued'
end
snap.task_id = ARGV[5]
snap.updated_at = now_seconds()
save_snap(KEYS[3], snap, ttl)
return 1
"""
)

_REDIS_PUT_RESULT = (
    _REDIS_LUA_PRELUDE
    + """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
local ttl = tonumber(ARGV[2])
redis.call('SET', KEYS[2], ARGV[3], 'EX', ttl)
local snap = load_snap(KEYS[3], ARGV[4])
if is_null(snap.status) then
  snap.status = 'queued'
end
snap.task_id = ARGV[4]
snap.updated_at = now_seconds()
save_snap(KEYS[3], snap, ttl)
return 1
"""
)

_REDIS_BUMP = (
    _REDIS_LUA_PRELUDE
    + """
local ttl = tonumber(ARGV[1])
local mode = ARGV[3]
local snap = load_snap(KEYS[2], ARGV[2])
if is_null(snap.status) then
  snap.status = 'queued'
end
-- A later step write refreshes the snapshot TTL and not this counter. If the
-- counter key expired first, continue from the snapshot instead of restarting
-- at 1 and defeating max_deliveries.
if redis.call('EXISTS', KEYS[1]) == 0 then
  local previous = 0
  if mode == 'attempt' and not is_null(snap.attempt) then
    previous = tonumber(snap.attempt) or 0
  elseif mode == 'probe' and not is_null(snap.probes) then
    previous = tonumber(snap.probes) or 0
  end
  if previous > 0 then
    redis.call('SET', KEYS[1], previous, 'EX', ttl)
  end
end
local n = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ttl)
if mode == 'attempt' then
  snap.attempt = n
else
  snap.probes = n
end
snap.task_id = ARGV[2]
snap.updated_at = now_seconds()
save_snap(KEYS[2], snap, ttl)
return n
"""
)

_REDIS_RESET_ATTEMPTS = (
    _REDIS_LUA_PRELUDE
    + """
local present = false
for i = 1, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    present = true
    break
  end
end
if not present then
  return 0
end
local ttl = tonumber(ARGV[1])
redis.call('SET', KEYS[1], '0', 'EX', ttl)
local raw = redis.call('GET', KEYS[2])
if not raw then
  return 1
end
local snap = cjson.decode(raw)
snap.attempt = 0
if not is_null(snap.status) then
  snap.updated_at = now_seconds()
  snap.task_id = ARGV[2]
end
save_snap(KEYS[2], snap, ttl)
return 1
"""
)

# The event key is ``prefix .. seq``. The sequence is chosen inside the script,
# so that key cannot be named in KEYS ahead of time. Standalone Redis allows
# it. Redis Cluster rejects a key that was not declared.
_REDIS_APPEND = (
    _REDIS_LUA_PRELUDE
    + """
local ttl = tonumber(ARGV[1])
local task_id = ARGV[2]
local snap = load_snap(KEYS[2], task_id)
snap.status = ARGV[3]
snap.task_id = task_id
if ARGV[4] == 'set' then
  snap.last_operation_name = ARGV[5]
end
if ARGV[6] == 'clear' then
  snap.error_type = cjson.null
  snap.error_message = cjson.null
elseif ARGV[6] == 'set' then
  snap.error_type = cjson.decode(ARGV[7])
  snap.error_message = cjson.decode(ARGV[8])
end
local now = now_seconds()
snap.updated_at = now
local seq = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ttl)
local event = cjson.decode(ARGV[9])
event.seq = seq
event.at = now
event.task_id = task_id
save_snap(KEYS[2], snap, ttl)
redis.call('SET', ARGV[10] .. tostring(seq), cjson.encode(event), 'EX', ttl)
return seq
"""
)

_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS {runs} (
    task_id text PRIMARY KEY,
    status text,
    result jsonb,
    attempts integer NOT NULL DEFAULT 0,
    probes integer NOT NULL DEFAULT 0,
    event_seq integer NOT NULL DEFAULT 0,
    lease_token text,
    lease_until timestamptz,
    steps_completed integer NOT NULL DEFAULT 0,
    last_operation_name text,
    error_type text,
    error_message text,
    updated_at timestamptz,
    expires_at timestamptz
)
"""

_STEPS_DDL = """
CREATE TABLE IF NOT EXISTS {steps} (
    task_id text NOT NULL,
    step_index integer NOT NULL,
    operation_name text NOT NULL,
    payload jsonb,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (task_id, step_index)
)
"""

_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {events} (
    task_id text NOT NULL,
    seq integer NOT NULL,
    kind text NOT NULL,
    operation_name text,
    step_index integer,
    attempt integer,
    error_type text,
    error_message text,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (task_id, seq)
)
"""


class RedisRunStore:
    """Redis ``RunStore``.

    Keys are ``{key_prefix}:{task_id}:...``. Step, result, attempt, probe,
    snapshot, and event keys expire after ``journal_ttl_s``, refreshed on the
    write that touches that key. The lease key uses ``lease_ttl_s`` and is not
    a journal row: ``snapshot`` stays ``unknown`` until a step, result,
    counter, or progress event is written.

    ``put_step`` and ``put_result`` check the lease and write in one Lua
    script. They raise ``LeaseLost`` unless ``token`` is the live holder.
    ``acquire_lease`` lets the same token slide the TTL. A different live
    token gets ``False``. ``refresh_lease`` does not revive an expired lease.
    Stored status ``running`` is read back as ``interrupted`` when the lease
    key is missing. The stored status is not rewritten.

    ``setup()`` pings. ``purge_expired()`` is a no-op: Redis drops keys at
    their TTL, and this class does not start a thread. Payloads are JSON.
    """

    def __init__(
        self, url: str, settings: DurabilitySettings | None = None
    ) -> None:
        self._redis_mod = _load_redis()
        self._url = url
        self._settings = settings if settings is not None else DurabilitySettings()
        self._client: Any = None
        self._scripts: dict[str, Any] = {}

    def setup(self) -> None:
        """Open a connection and ping. Does not create keys."""
        self._redis().ping()

    def purge_expired(self) -> int:
        """Return 0. Key TTL is the only Redis retention."""
        return 0

    def close(self) -> None:
        client = self._client
        self._client = None
        self._scripts.clear()
        if client is not None:
            client.close()

    def get_step(self, task_id: str, index: int) -> StepRecord | None:
        raw = self._redis().get(self._key(task_id, "step", str(index)))
        if raw is None:
            return None
        data = json.loads(raw)
        return StepRecord(
            operation_name=data["operation_name"], payload=data["payload"]
        )

    def put_step(
        self,
        task_id: str,
        index: int,
        operation_name: str,
        payload: Any,
        *,
        token: str,
    ) -> None:
        _check_step_index(index)
        wrote = self._eval(
            "put_step",
            _REDIS_PUT_STEP,
            [
                self._key(task_id, "lease"),
                self._key(task_id, "step", str(index)),
                self._key(task_id, "snapshot"),
            ],
            [
                token,
                self._settings.journal_ttl_s,
                json.dumps(
                    {"operation_name": operation_name, "payload": payload}
                ),
                operation_name,
                task_id,
            ],
        )
        if int(wrote) != 1:
            raise _lease_lost(task_id)

    def get_result(self, task_id: str) -> ResultRecord | None:
        raw = self._redis().get(self._key(task_id, "result"))
        if raw is None:
            return None
        # The key existing is the record. A stored JSON null is not a miss.
        return ResultRecord(payload=json.loads(raw))

    def put_result(self, task_id: str, payload: Any, *, token: str) -> None:
        wrote = self._eval(
            "put_result",
            _REDIS_PUT_RESULT,
            [
                self._key(task_id, "lease"),
                self._key(task_id, "result"),
                self._key(task_id, "snapshot"),
            ],
            [token, self._settings.journal_ttl_s, json.dumps(payload), task_id],
        )
        if int(wrote) != 1:
            raise _lease_lost(task_id)

    def bump_attempt(self, task_id: str) -> int:
        return self._bump(task_id, "attempt")

    def reset_attempts(self, task_id: str) -> None:
        self._eval(
            "reset_attempts",
            _REDIS_RESET_ATTEMPTS,
            [
                self._key(task_id, "attempts"),
                self._key(task_id, "snapshot"),
                self._key(task_id, "lease"),
                self._key(task_id, "result"),
                self._key(task_id, "probes"),
                self._key(task_id, "event_seq"),
            ],
            [self._settings.journal_ttl_s, task_id],
        )

    def acquire_lease(self, task_id: str, token: str) -> bool:
        wrote = self._eval(
            "acquire",
            _REDIS_ACQUIRE,
            [self._key(task_id, "lease")],
            [token, _lease_ttl_ms(self._settings)],
        )
        return int(wrote) == 1

    def refresh_lease(self, task_id: str, token: str) -> bool:
        wrote = self._eval(
            "refresh",
            _REDIS_REFRESH,
            [self._key(task_id, "lease")],
            [token, _lease_ttl_ms(self._settings)],
        )
        return int(wrote) == 1

    def release_lease(self, task_id: str, token: str) -> bool:
        wrote = self._eval(
            "release",
            _REDIS_RELEASE,
            [self._key(task_id, "lease")],
            [token],
        )
        return int(wrote) == 1

    def lease_held(self, task_id: str, token: str | None = None) -> bool:
        holder = self._redis().get(self._key(task_id, "lease"))
        if token is None:
            return holder is not None
        return holder == token

    def bump_probe(self, task_id: str) -> int:
        return self._bump(task_id, "probe")

    def append_progress(
        self,
        task_id: str,
        kind: ProgressKind,
        *,
        operation_name: str | None = None,
        step_index: int | None = None,
        attempt: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> int:
        status = _stored_status(kind)
        clear_errors = kind == "succeeded"
        has_error = (not clear_errors) and (
            error_type is not None or error_message is not None
        )
        if clear_errors:
            err_mode = "clear"
        elif has_error:
            err_mode = "set"
        else:
            err_mode = "keep"
        event = {
            "task_id": task_id,
            "kind": kind,
            "operation_name": operation_name,
            "step_index": step_index,
            "attempt": attempt,
            "error_type": None if clear_errors else error_type,
            "error_message": None if clear_errors else error_message,
        }
        seq = self._eval(
            "append",
            _REDIS_APPEND,
            [self._key(task_id, "event_seq"), self._key(task_id, "snapshot")],
            [
                self._settings.journal_ttl_s,
                task_id,
                status,
                "set" if operation_name is not None else "keep",
                operation_name or "",
                err_mode,
                json.dumps(None if clear_errors else error_type),
                json.dumps(None if clear_errors else error_message),
                json.dumps(event),
                self._key(task_id, "event") + ":",
            ],
        )
        return int(seq)

    def read_progress(
        self,
        task_id: str,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[ProgressEvent, ...]:
        limit = _page_limit(limit, self._settings)
        cursor = 0 if after is None else after
        client = self._redis()
        last_raw = client.get(self._key(task_id, "event_seq"))
        if last_raw is None:
            return ()
        last = int(last_raw)
        picked: list[ProgressEvent] = []
        seq = cursor + 1
        while seq <= last and len(picked) < limit:
            end = min(last, seq + limit - len(picked) - 1)
            keys = [self._key(task_id, "event", str(n)) for n in range(seq, end + 1)]
            for raw in client.mget(keys):
                if raw is None:
                    continue
                picked.append(_progress_event(json.loads(raw)))
                if len(picked) == limit:
                    break
            seq = end + 1
        return tuple(picked)

    def snapshot(self, task_id: str) -> RunSnapshot:
        raw, holder = self._redis().mget(
            self._key(task_id, "snapshot"), self._key(task_id, "lease")
        )
        return _snapshot_from_json(task_id, raw, lease_held=holder is not None)

    def _bump(self, task_id: str, mode: str) -> int:
        counter = "attempts" if mode == "attempt" else "probes"
        value = self._eval(
            "bump",
            _REDIS_BUMP,
            [self._key(task_id, counter), self._key(task_id, "snapshot")],
            [self._settings.journal_ttl_s, task_id, mode],
        )
        return int(value)

    def _key(self, task_id: str, *parts: str) -> str:
        return ":".join((self._settings.key_prefix, task_id, *parts))

    def _redis(self) -> Any:
        if self._client is None:
            # The pool connects on the first command, not here.
            self._client = self._redis_mod.Redis.from_url(
                self._url, decode_responses=True
            )
        return self._client

    def _eval(
        self, name: str, source: str, keys: list[str], args: list[Any]
    ) -> Any:
        client = self._redis()
        script = self._scripts.get(name)
        if script is None:
            script = client.register_script(source)
            self._scripts[name] = script
        return script(keys=keys, args=args)


def _progress_event(data: dict[str, Any]) -> ProgressEvent:
    return ProgressEvent(
        task_id=data["task_id"],
        seq=int(data["seq"]),
        kind=data["kind"],
        at=float(data["at"]),
        operation_name=data.get("operation_name"),
        step_index=_optional_int(data.get("step_index")),
        attempt=_optional_int(data.get("attempt")),
        error_type=data.get("error_type"),
        error_message=data.get("error_message"),
    )


def _snapshot_from_json(
    task_id: str, raw: str | None, *, lease_held: bool
) -> RunSnapshot:
    if raw is None:
        return _unknown(task_id)
    data = json.loads(raw)
    status = data.get("status")
    if status is None:
        return _unknown(task_id)
    # Computed on read. A later acquire of the same row shows running again.
    if status == "running" and not lease_held:
        status = "interrupted"
    return RunSnapshot(
        task_id=task_id,
        status=status,
        attempt=int(data.get("attempt") or 0),
        steps_completed=int(data.get("steps_completed") or 0),
        last_operation_name=data.get("last_operation_name"),
        error_type=data.get("error_type"),
        error_message=data.get("error_message"),
        updated_at=_epoch(data.get("updated_at")),
    )


class PostgresRunStore:
    """Postgres ``RunStore``.

    ``setup()`` creates ``settings.postgres_schema`` and the ``runs``,
    ``steps``, and ``progress_events`` tables. Importing the package does not
    connect, and neither does constructing this store. There is no reaper
    thread. ``purge_expired()`` deletes rows with ``expires_at`` in the past.
    Reads ignore those rows, using ``statement_timestamp()`` so two app
    servers share the database clock.

    Lease acquire is the spec's compare-and-update, including the same token.
    An expired run row is deleted and replaced on the next acquire, bump, or
    progress append so a finished payload cannot come back after the journal
    TTL. ``put_step`` and ``put_result`` raise ``LeaseLost`` unless ``token``
    holds a live lease. Stored status ``running`` is read back as
    ``interrupted`` when that lease is missing or due.
    """

    def __init__(
        self, url: str, settings: DurabilitySettings | None = None
    ) -> None:
        psycopg, sql, dict_row, jsonb = _load_psycopg()
        self._psycopg = psycopg
        self._sql = sql
        self._dict_row = dict_row
        self._Jsonb = jsonb
        self._url = url
        self._settings = settings if settings is not None else DurabilitySettings()
        schema = self._settings.postgres_schema
        self._schema = sql.Identifier(schema)
        self._runs = sql.Identifier(schema, "runs")
        self._steps = sql.Identifier(schema, "steps")
        self._events = sql.Identifier(schema, "progress_events")
        # Unqualified: CREATE INDEX rejects a schema-qualified name. Postgres
        # puts the index in the table's schema.
        self._runs_expires_idx = sql.Identifier("runs_expires_at_idx")
        self._steps_expires_idx = sql.Identifier("steps_expires_at_idx")
        self._events_expires_idx = sql.Identifier("progress_events_expires_at_idx")
        self._lock = threading.Lock()
        self._conn: Any = None

    def setup(self) -> None:
        """Create the schema and tables. Safe to call more than once."""
        with self._tx() as conn:
            self._exec(conn, "CREATE SCHEMA IF NOT EXISTS {schema}")
            self._exec(conn, _RUNS_DDL)
            self._exec(conn, _STEPS_DDL)
            self._exec(conn, _EVENTS_DDL)
            self._exec(
                conn,
                "CREATE INDEX IF NOT EXISTS {runs_expires_idx} ON {runs} (expires_at)",
            )
            self._exec(
                conn,
                "CREATE INDEX IF NOT EXISTS {steps_expires_idx} "
                "ON {steps} (expires_at)",
            )
            self._exec(
                conn,
                "CREATE INDEX IF NOT EXISTS {events_expires_idx} "
                "ON {events} (expires_at)",
            )

    def purge_expired(self) -> int:
        """Delete expired rows in this schema. Return how many were deleted."""
        removed = 0
        with self._tx() as conn:
            for statement in (
                "DELETE FROM {events} WHERE expires_at <= statement_timestamp()",
                "DELETE FROM {steps} WHERE expires_at <= statement_timestamp()",
                "DELETE FROM {runs} WHERE expires_at <= statement_timestamp()",
            ):
                removed += self._exec(conn, statement).rowcount
        return removed

    def close(self) -> None:
        with self._lock:
            self._discard_connection()

    def get_step(self, task_id: str, index: int) -> StepRecord | None:
        with self._tx() as conn:
            row = self._exec(
                conn,
                """
                SELECT operation_name, payload
                FROM {steps}
                WHERE task_id = %s
                  AND step_index = %s
                  AND expires_at > statement_timestamp()
                """,
                (task_id, index),
            ).fetchone()
        if row is None:
            return None
        return StepRecord(
            operation_name=row["operation_name"], payload=row["payload"]
        )

    def put_step(
        self,
        task_id: str,
        index: int,
        operation_name: str,
        payload: Any,
        *,
        token: str,
    ) -> None:
        _check_step_index(index)
        ttl = self._settings.journal_ttl_s
        with self._tx() as conn:
            self._require_lease(conn, task_id, token)
            self._exec(
                conn,
                """
                INSERT INTO {steps} (
                    task_id, step_index, operation_name, payload, expires_at
                )
                VALUES (
                    %s, %s, %s, %s,
                    statement_timestamp() + make_interval(secs => %s)
                )
                ON CONFLICT (task_id, step_index) DO UPDATE
                SET operation_name = EXCLUDED.operation_name,
                    payload = EXCLUDED.payload,
                    expires_at = EXCLUDED.expires_at
                """,
                (task_id, index, operation_name, self._Jsonb(payload), ttl),
            )
            self._exec(
                conn,
                """
                UPDATE {runs}
                SET last_operation_name = %s,
                    steps_completed = (
                        SELECT COUNT(*)
                        FROM {steps}
                        WHERE task_id = %s
                          AND expires_at > statement_timestamp()
                    ),
                    status = COALESCE(status, 'queued'),
                    updated_at = statement_timestamp(),
                    expires_at = statement_timestamp() + make_interval(secs => %s)
                WHERE task_id = %s
                """,
                (operation_name, task_id, ttl, task_id),
            )

    def get_result(self, task_id: str) -> ResultRecord | None:
        with self._tx() as conn:
            row = self._exec(
                conn,
                """
                SELECT result IS NULL AS missing, result
                FROM {runs}
                WHERE task_id = %s
                  AND (expires_at IS NULL OR expires_at > statement_timestamp())
                """,
                (task_id,),
            ).fetchone()
        if row is None or row["missing"]:
            return None
        return ResultRecord(payload=row["result"])

    def put_result(self, task_id: str, payload: Any, *, token: str) -> None:
        with self._tx() as conn:
            self._require_lease(conn, task_id, token)
            self._exec(
                conn,
                """
                UPDATE {runs}
                SET result = %s,
                    status = COALESCE(status, 'queued'),
                    updated_at = statement_timestamp(),
                    expires_at = statement_timestamp() + make_interval(secs => %s)
                WHERE task_id = %s
                """,
                (self._Jsonb(payload), self._settings.journal_ttl_s, task_id),
            )

    def bump_attempt(self, task_id: str) -> int:
        return self._bump(task_id, "attempts")

    def reset_attempts(self, task_id: str) -> None:
        # An expired row is invisible, same as a task this process never saw.
        with self._tx() as conn:
            self._exec(
                conn,
                """
                UPDATE {runs}
                SET attempts = 0,
                    updated_at = CASE
                        WHEN status IS NOT NULL THEN statement_timestamp()
                        ELSE updated_at
                    END,
                    expires_at = statement_timestamp() + make_interval(secs => %s)
                WHERE task_id = %s
                  AND (expires_at IS NULL OR expires_at > statement_timestamp())
                """,
                (self._settings.journal_ttl_s, task_id),
            )

    def acquire_lease(self, task_id: str, token: str) -> bool:
        with self._tx() as conn:
            if self._acquire_live(conn, task_id, token):
                return True
            # A past expires_at is not a row. Replace it so the primary key
            # can be taken without bringing the old result back.
            self._drop_if_expired(conn, task_id)
            self._insert_run(conn, task_id)
            return self._acquire_live(conn, task_id, token)

    def refresh_lease(self, task_id: str, token: str) -> bool:
        with self._tx() as conn:
            cursor = self._exec(
                conn,
                """
                UPDATE {runs}
                SET lease_until = statement_timestamp()
                        + make_interval(secs => %s),
                    expires_at = statement_timestamp()
                        + make_interval(secs => %s)
                WHERE task_id = %s
                  AND lease_token = %s
                  AND lease_until >= statement_timestamp()
                  AND (expires_at IS NULL OR expires_at > statement_timestamp())
                """,
                (
                    self._settings.lease_ttl_s,
                    self._settings.journal_ttl_s,
                    task_id,
                    token,
                ),
            )
            return cursor.rowcount == 1

    def release_lease(self, task_id: str, token: str) -> bool:
        with self._tx() as conn:
            cursor = self._exec(
                conn,
                """
                UPDATE {runs}
                SET lease_token = NULL,
                    lease_until = NULL
                WHERE task_id = %s
                  AND lease_token = %s
                  AND lease_until >= statement_timestamp()
                  AND (expires_at IS NULL OR expires_at > statement_timestamp())
                """,
                (task_id, token),
            )
            return cursor.rowcount == 1

    def lease_held(self, task_id: str, token: str | None = None) -> bool:
        with self._tx() as conn:
            row = self._exec(
                conn,
                """
                SELECT lease_token
                FROM {runs}
                WHERE task_id = %s
                  AND lease_token IS NOT NULL
                  AND lease_until >= statement_timestamp()
                  AND (expires_at IS NULL OR expires_at > statement_timestamp())
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            return False
        if token is None:
            return True
        return row["lease_token"] == token

    def bump_probe(self, task_id: str) -> int:
        return self._bump(task_id, "probes")

    def append_progress(
        self,
        task_id: str,
        kind: ProgressKind,
        *,
        operation_name: str | None = None,
        step_index: int | None = None,
        attempt: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> int:
        status = _stored_status(kind)
        clear_errors = kind == "succeeded"
        has_error = (not clear_errors) and (
            error_type is not None or error_message is not None
        )
        stored_error_type = None if clear_errors else error_type
        stored_error_message = None if clear_errors else error_message
        ttl = self._settings.journal_ttl_s
        with self._tx() as conn:
            self._revive_or_insert(conn, task_id)
            row = self._exec(
                conn,
                """
                UPDATE {runs}
                SET event_seq = event_seq + 1,
                    status = %s,
                    last_operation_name = CASE
                        WHEN %s THEN %s
                        ELSE last_operation_name
                    END,
                    error_type = CASE
                        WHEN %s THEN NULL
                        WHEN %s THEN %s
                        ELSE error_type
                    END,
                    error_message = CASE
                        WHEN %s THEN NULL
                        WHEN %s THEN %s
                        ELSE error_message
                    END,
                    updated_at = statement_timestamp(),
                    expires_at = statement_timestamp() + make_interval(secs => %s)
                WHERE task_id = %s
                RETURNING event_seq, updated_at
                """,
                (
                    status,
                    operation_name is not None,
                    operation_name,
                    clear_errors,
                    has_error,
                    stored_error_type,
                    clear_errors,
                    has_error,
                    stored_error_message,
                    ttl,
                    task_id,
                ),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"progress append lost task {task_id!r}")
            seq = int(row["event_seq"])
            self._exec(
                conn,
                """
                INSERT INTO {events} (
                    task_id, seq, kind, operation_name, step_index, attempt,
                    error_type, error_message, created_at, expires_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s + make_interval(secs => %s)
                )
                """,
                (
                    task_id,
                    seq,
                    kind,
                    operation_name,
                    step_index,
                    attempt,
                    stored_error_type,
                    stored_error_message,
                    row["updated_at"],
                    row["updated_at"],
                    ttl,
                ),
            )
        return seq

    def read_progress(
        self,
        task_id: str,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[ProgressEvent, ...]:
        limit = _page_limit(limit, self._settings)
        cursor = 0 if after is None else after
        with self._tx() as conn:
            rows = self._exec(
                conn,
                """
                SELECT
                    task_id, seq, kind, operation_name, step_index, attempt,
                    error_type, error_message,
                    EXTRACT(EPOCH FROM created_at) AS at
                FROM {events}
                WHERE task_id = %s
                  AND seq > %s
                  AND expires_at > statement_timestamp()
                ORDER BY seq
                LIMIT %s
                """,
                (task_id, cursor, limit),
            ).fetchall()
        return tuple(
            ProgressEvent(
                task_id=row["task_id"],
                seq=int(row["seq"]),
                kind=row["kind"],
                at=float(row["at"]),
                operation_name=row["operation_name"],
                step_index=_optional_int(row["step_index"]),
                attempt=_optional_int(row["attempt"]),
                error_type=row["error_type"],
                error_message=row["error_message"],
            )
            for row in rows
        )

    def snapshot(self, task_id: str) -> RunSnapshot:
        with self._tx() as conn:
            row = self._exec(
                conn,
                """
                SELECT
                    status,
                    attempts,
                    last_operation_name,
                    error_type,
                    error_message,
                    EXTRACT(EPOCH FROM updated_at) AS updated_at,
                    (
                        lease_token IS NOT NULL
                        AND lease_until >= statement_timestamp()
                    ) AS lease_live,
                    (
                        SELECT COUNT(*)
                        FROM {steps}
                        WHERE task_id = %s
                          AND expires_at > statement_timestamp()
                    ) AS steps_completed
                FROM {runs}
                WHERE task_id = %s
                  AND (expires_at IS NULL OR expires_at > statement_timestamp())
                """,
                (task_id, task_id),
            ).fetchone()
        if row is None or row["status"] is None:
            return _unknown(task_id)
        status: RunStatus = row["status"]
        if status == "running" and not row["lease_live"]:
            status = "interrupted"
        return RunSnapshot(
            task_id=task_id,
            status=status,
            attempt=int(row["attempts"]),
            steps_completed=int(row["steps_completed"]),
            last_operation_name=row["last_operation_name"],
            error_type=row["error_type"],
            error_message=row["error_message"],
            updated_at=_epoch(row["updated_at"]),
        )

    def _bump(self, task_id: str, column: str) -> int:
        if column not in {"attempts", "probes"}:
            raise ValueError(f"unknown counter {column!r}")
        query = self._sql.SQL(
            """
            UPDATE {runs}
            SET {column} = {column} + 1,
                status = COALESCE(status, 'queued'),
                updated_at = statement_timestamp(),
                expires_at = statement_timestamp() + make_interval(secs => %s)
            WHERE task_id = %s
            RETURNING {column}
            """
        ).format(runs=self._runs, column=self._sql.Identifier(column))
        with self._tx() as conn:
            self._revive_or_insert(conn, task_id)
            row = conn.execute(
                query, (self._settings.journal_ttl_s, task_id)
            ).fetchone()
        if row is None:
            raise RuntimeError(f"counter bump lost task {task_id!r}")
        return int(row[column])

    def _acquire_live(self, conn: Any, task_id: str, token: str) -> bool:
        # Same predicate as the spec, plus a visible-row check so a past
        # expires_at is not treated as a live journal.
        cursor = self._exec(
            conn,
            """
            UPDATE {runs}
            SET lease_token = %s,
                lease_until = statement_timestamp() + make_interval(secs => %s),
                expires_at = statement_timestamp() + make_interval(secs => %s)
            WHERE task_id = %s
              AND (expires_at IS NULL OR expires_at > statement_timestamp())
              AND (
                    lease_token IS NULL
                    OR lease_until < statement_timestamp()
                    OR lease_token = %s
                  )
            """,
            (
                token,
                self._settings.lease_ttl_s,
                self._settings.journal_ttl_s,
                task_id,
                token,
            ),
        )
        return cursor.rowcount == 1

    def _require_lease(self, conn: Any, task_id: str, token: str) -> None:
        row = self._exec(
            conn,
            """
            SELECT 1
            FROM {runs}
            WHERE task_id = %s
              AND lease_token = %s
              AND lease_until >= statement_timestamp()
              AND (expires_at IS NULL OR expires_at > statement_timestamp())
            FOR UPDATE
            """,
            (task_id, token),
        ).fetchone()
        if row is None:
            raise _lease_lost(task_id)

    def _revive_or_insert(self, conn: Any, task_id: str) -> None:
        if not self._drop_if_expired(conn, task_id):
            self._insert_run(conn, task_id)

    def _drop_if_expired(self, conn: Any, task_id: str) -> bool:
        row = self._exec(
            conn,
            """
            SELECT (
                expires_at IS NOT NULL
                AND expires_at <= statement_timestamp()
            ) AS expired
            FROM {runs}
            WHERE task_id = %s
            FOR UPDATE
            """,
            (task_id,),
        ).fetchone()
        if row is None:
            return False
        if row["expired"]:
            self._delete_task(conn, task_id)
            return False
        return True

    def _delete_task(self, conn: Any, task_id: str) -> None:
        self._exec(conn, "DELETE FROM {events} WHERE task_id = %s", (task_id,))
        self._exec(conn, "DELETE FROM {steps} WHERE task_id = %s", (task_id,))
        self._exec(conn, "DELETE FROM {runs} WHERE task_id = %s", (task_id,))

    def _insert_run(self, conn: Any, task_id: str) -> None:
        self._exec(
            conn,
            """
            INSERT INTO {runs} (task_id, attempts, probes, event_seq, steps_completed)
            VALUES (%s, 0, 0, 0, 0)
            ON CONFLICT (task_id) DO NOTHING
            """,
            (task_id,),
        )

    def _exec(
        self,
        conn: Any,
        statement: str,
        params: tuple[Any, ...] = (),
    ) -> Any:
        query = self._sql.SQL(statement).format(
            schema=self._schema,
            runs=self._runs,
            steps=self._steps,
            events=self._events,
            runs_expires_idx=self._runs_expires_idx,
            steps_expires_idx=self._steps_expires_idx,
            events_expires_idx=self._events_expires_idx,
        )
        return conn.execute(query, params)

    @contextmanager
    def _tx(self) -> Iterator[Any]:
        # One connection is not safe across the heartbeat thread and the
        # worker. The lock covers connect, the statements, and commit.
        with self._lock:
            conn = self._connection()
            try:
                yield conn
            except Exception:
                self._rollback(conn)
                raise
            else:
                try:
                    conn.commit()
                except Exception:
                    self._rollback(conn)
                    raise

    def _connection(self) -> Any:
        conn = self._conn
        if conn is None or conn.closed:
            conn = self._psycopg.connect(
                self._url, autocommit=False, row_factory=self._dict_row
            )
            self._conn = conn
        return conn

    def _rollback(self, conn: Any) -> None:
        try:
            if conn is not None and not conn.closed:
                conn.rollback()
        except Exception:
            logger.exception("postgres run store rollback failed")
            self._discard_connection()

    def _discard_connection(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        try:
            if not conn.closed:
                conn.close()
        except Exception:
            logger.exception("postgres run store close failed")


_REDIS_SCHEMES = frozenset({"redis", "rediss"})
_POSTGRES_SCHEMES = frozenset({"postgresql", "postgres"})


def run_store_from_url(
    url: str, settings: DurabilitySettings | None = None
) -> RedisRunStore | PostgresRunStore:
    """Build a store for a Redis or Postgres URL. Does not connect."""
    scheme = urlsplit(url).scheme.lower()
    if scheme in _REDIS_SCHEMES:
        return RedisRunStore(url, settings)
    if scheme in _POSTGRES_SCHEMES:
        return PostgresRunStore(url, settings)
    raise ValueError(
        f"unsupported durability store URL {url!r}; expected a redis://, "
        "rediss://, postgresql://, or postgres:// URL"
    )
