"""Task id, lease token, and encounter index for one durable container."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass
class _Encounter:
    """Mutable so graph tasks that copy the context var still share one counter."""

    index: int = 0


_task_id: ContextVar[str | None] = ContextVar(
    "pydantic_ai_celery.task_id", default=None
)
_lease_token: ContextVar[str | None] = ContextVar(
    "pydantic_ai_celery.lease_token", default=None
)
_encounter: ContextVar[_Encounter | None] = ContextVar(
    "pydantic_ai_celery.encounter", default=None
)


def current_task_id() -> str | None:
    """The task id of the active container, or `None` outside one."""
    return _task_id.get()


def current_lease_token() -> str | None:
    """Lease token of the active container, or `None` outside one."""
    return _lease_token.get()


def take_step_index() -> int:
    """Return the next encounter index and advance the counter.

    The increment mutates the shared counter before `execute` awaits.
    """
    encounter = _encounter.get()
    if encounter is None:
        raise RuntimeError(
            "take_step_index() was called outside a durability container"
        )
    index = encounter.index
    encounter.index += 1
    return index


@contextmanager
def lease_token_scope(token: str) -> Iterator[None]:
    reset = _lease_token.set(token)
    try:
        yield
    finally:
        _lease_token.reset(reset)


@contextmanager
def container_scope(task_id: str) -> Iterator[None]:
    """Bind `task_id` and restart the encounter counter at 0."""
    task_reset = _task_id.set(task_id)
    encounter_reset = _encounter.set(_Encounter())
    try:
        yield
    finally:
        _encounter.reset(encounter_reset)
        _task_id.reset(task_reset)
