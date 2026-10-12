"""In-memory run store for checkpoints, leases, and progress."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._errors import LeaseLost

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

    def put_result(self, task_id: str, payload: Any) -> None: ...

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

    ``put_step`` raises ``LeaseLost`` unless ``token`` is the live holder.
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

    def put_result(self, task_id: str, payload: Any) -> None:
        with self._lock:
            run = self._ensure(task_id)
            run.result = ResultRecord(payload=payload)
            self._touch(run, self._now())

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
