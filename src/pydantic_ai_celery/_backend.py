"""Journal backend that replays Celery task steps by encounter index."""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from pydantic_ai.durable_exec import (
    DurableOperationId,
    JournalCallableOperationBackend,
    RoleBasedOperationConfig,
)

from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._context import (
    current_lease_token,
    current_task_id,
    take_step_index,
)
from pydantic_ai_celery._errors import DurabilityDivergence, LeaseLost, UnitTimeout
from pydantic_ai_celery._store import ProgressKind, RunStore

logger = logging.getLogger(__name__)


def emit_progress(
    store: RunStore,
    listeners: Sequence[object],
    task_id: str,
    kind: ProgressKind,
    *,
    operation_name: str | None = None,
    step_index: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    """Append one progress event, then tell listeners. A listener error is logged."""
    seq = store.append_progress(
        task_id,
        kind,
        operation_name=operation_name,
        step_index=step_index,
        error_type=error_type,
        error_message=error_message,
    )
    events = store.read_progress(task_id, after=seq - 1, limit=1)
    if not events or events[0].seq != seq:
        return
    event = events[0]
    for listener in listeners:
        try:
            listener.on_progress(event)  # type: ignore[attr-defined]
        except Exception:
            logger.exception("progress listener %r failed", listener)


class CeleryOperationBackend(JournalCallableOperationBackend[Any]):
    """Sequence journal over a `RunStore`. `cache_key` is not the identity."""

    def __init__(
        self,
        store: RunStore,
        settings: DurabilitySettings | None = None,
        *,
        agent_name: str = "",
        config: RoleBasedOperationConfig[Any] | None = None,
        listeners: Sequence[object] = (),
        default_model_id: str | None = None,
    ) -> None:
        self._store = store
        self._settings = settings if settings is not None else DurabilitySettings()
        self._listeners = tuple(listeners)
        self.agent_name = agent_name
        self.default_model_id = default_model_id or "default"
        if config is None:
            # Imported lazily: the config type lives on the capability module.
            from pydantic_ai_celery._durability import CeleryOperationConfig

            empty = CeleryOperationConfig()
            config = RoleBasedOperationConfig(
                model=empty,
                event=empty,
                capability=empty,
                tool=empty,
            )
        super().__init__(
            agent_name=agent_name, default_model_id=default_model_id, config=config
        )

    async def execute(
        self,
        *,
        operation_id: DurableOperationId,
        name: str,
        body: Callable[[], Awaitable[object]],
        cache_key: tuple[object, ...],
        config: Any,
    ) -> object:
        """Replay the record at this encounter index, or run `body` and store it.

        `cache_key` is ignored. The builder's projection includes the live run
        context, which is not stable across deliveries.
        """
        del operation_id, cache_key
        task_id = current_task_id()
        if task_id is None:
            raise RuntimeError(
                "CeleryOperationBackend.execute() was called outside a durability container"
            )
        # Taken before any await so two sequential units cannot share an index.
        index = take_step_index()
        record = self._store.get_step(task_id, index)
        if record is not None:
            if record.operation_name != name:
                error = DurabilityDivergence(
                    f"task {task_id!r} index {index}: journaled operation "
                    f"{record.operation_name!r} does not match {name!r}"
                )
                self._record_failure(task_id, index, name, error)
                raise error
            self._emit(task_id, "step_started", name, index)
            self._emit(task_id, "step_finished", name, index)
            return record.payload

        token = current_lease_token()
        if token is None:
            raise LeaseLost(f"lease for task {task_id!r} is not held by this caller")

        self._emit(task_id, "step_started", name, index)
        timeout = _timeout_seconds(config)
        try:
            payload = await _await_body(body, timeout, name=name)
        except BaseException as exc:
            self._record_failure(task_id, index, name, exc)
            raise
        try:
            self._store.put_step(task_id, index, name, payload, token=token)
        except BaseException as exc:
            self._record_failure(task_id, index, name, exc)
            raise
        self._emit(task_id, "step_finished", name, index)
        return payload

    def _emit(self, task_id: str, kind: ProgressKind, name: str, index: int) -> None:
        emit_progress(
            self._store,
            self._listeners,
            task_id,
            kind,
            operation_name=name,
            step_index=index,
        )

    def _record_failure(
        self, task_id: str, index: int, name: str, exc: BaseException
    ) -> None:
        try:
            emit_progress(
                self._store,
                self._listeners,
                task_id,
                "step_failed",
                operation_name=name,
                step_index=index,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        except Exception:
            logger.exception(
                "failed to record step_failed for task %s index %s", task_id, index
            )


def _timeout_seconds(config: object) -> float | None:
    timeout = getattr(config, "timeout", None)
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError(
            f"operation timeout must be a number of seconds or None, got {timeout!r}"
        )
    return float(timeout)


async def _await_body(
    body: Callable[[], Awaitable[object]],
    timeout: float | None,
    *,
    name: str,
) -> object:
    if timeout is None:
        return await body()
    deadline = asyncio.timeout(timeout)
    try:
        async with deadline:
            return await body()
    except TimeoutError as exc:
        if deadline.expired():
            raise UnitTimeout(f"{name} timed out after {timeout} seconds") from exc
        raise
