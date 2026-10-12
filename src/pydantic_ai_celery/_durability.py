"""Celery durability capability, task policy, and container."""

import asyncio
import logging
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from inspect import iscoroutinefunction
from typing import Any, Literal, Protocol

from celery import Celery
from celery.app.task import Task as CeleryTask
from celery.exceptions import Retry, SoftTimeLimitExceeded
from celery.signals import worker_init
from kombu import Queue
from kombu.exceptions import EncodeError
from kombu.serialization import dumps, loads
from pydantic_ai import ToolsetTool, UserError
from pydantic_ai.agent import EventStreamHandler
from pydantic_ai.durable_exec import (
    JSON_CODEC,
    BaseDurabilityCapability,
    DurabilityEngineSpec,
    DurableOperationId,
    RoleBasedOperationConfig,
    ToolsetCallToolId,
    ToolsetGetInstructionsId,
    ToolsetGetToolsId,
    ToolsetKind,
    ToolsetValidateToolArgumentsId,
)
from pydantic_ai.models import Model
from pydantic_ai.tools import AgentDepsT

from pydantic_ai_celery._backend import CeleryOperationBackend, emit_progress
from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._context import (
    container_scope,
    current_lease_token,
    current_task_id,
    lease_token_scope,
)
from pydantic_ai_celery._errors import (
    LeaseHeld,
    LeaseLost,
    NonRetryableDurabilityError,
    RunInProgress,
    UnitTimeout,
)
from pydantic_ai_celery._store import (
    ProgressEvent,
    ProgressKind,
    RunStore,
    run_store_from_url,
)
from pydantic_ai_celery._worker_checks import (
    rabbitmq_consumer_timeout_notice,
    worker_config_warnings,
)

logger = logging.getLogger(__name__)

# Explicit tuple so Celery 5.3 does not need dont_autoretry_for (added in 5.5).
_AUTORETRY_FOR = (
    SoftTimeLimitExceeded,
    ConnectionError,
    TimeoutError,
    LeaseLost,
    UnitTimeout,
)
_HEARTBEAT_THREAD_NAME = "pydantic-ai-celery-lease"
_MISSING = object()
# One worker_init receiver per app. The signal itself is process-global.
_WORKER_CHECK_APPS: set[int] = set()


def _serialization_failure(exc: Exception) -> NonRetryableDurabilityError:
    return NonRetryableDurabilityError(str(exc))


@dataclass(frozen=True, slots=True)
class CeleryOperationConfig:
    """Per-unit config. `timeout` is seconds for that unit, or `None` to wait."""

    timeout: float | None = None


class ProgressListener(Protocol):
    """Called after a progress event is stored. Exceptions are logged and ignored."""

    def on_progress(self, event: ProgressEvent) -> None: ...


class CeleryDurability(BaseDurabilityCapability[AgentDepsT]):
    """Checkpoint one agent run inside one Celery task.

    ``store`` is a ``RunStore`` or a ``redis://``, ``rediss://``,
    ``postgresql://``, or ``postgres://`` URL. A URL does not connect, and
    Postgres tables are not created, until that store's ``setup()`` runs.
    Outside `container()`, the capability is transparent and writes nothing.
    `in_durable_context` follows the container context var only.
    """

    engine_spec = DurabilityEngineSpec(
        engine_name="Celery",
        durable_unit_noun="step",
        durable_container_noun="task",
        codec=JSON_CODEC,
        serialization_failure=_serialization_failure,
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

    def __init__(
        self,
        store: RunStore | str,
        *,
        settings: DurabilitySettings | None = None,
        listeners: Sequence[ProgressListener] = (),
        model_config: CeleryOperationConfig | None = None,
        tool_config: CeleryOperationConfig | None = None,
        mcp_config: CeleryOperationConfig | None = None,
        event_config: CeleryOperationConfig | None = None,
        capability_config: CeleryOperationConfig | None = None,
        models: Mapping[str, Model] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(
            models=models, event_stream_handler=event_stream_handler, name=name
        )
        empty = CeleryOperationConfig()
        # One settings object for a URL-built store and this capability. An
        # existing RunStore keeps whatever settings it was built with.
        resolved = settings if settings is not None else DurabilitySettings()
        if isinstance(store, str):
            store = run_store_from_url(store, resolved)
        self._store = store
        self._settings = resolved
        self._listeners = tuple(listeners)
        self._model_config = model_config or empty
        self._tool_config = tool_config or empty
        self._mcp_config = mcp_config or empty
        self._event_config = event_config or empty
        self._capability_config = capability_config or empty
        self._operation_backend: CeleryOperationBackend | None = None

    @property
    def in_durable_context(self) -> bool:
        # True only while the container context var is set. Do not consult
        # `in_durable_unit`: a step body still belongs to this container.
        return current_task_id() is not None

    @contextmanager
    def container(self, task_id: str, *, task: Any = None) -> Iterator[object]:
        """Run `task_id` as one durable container.

        Acquires the run lease, bumps the delivery counter, and refreshes the
        lease from a daemon thread until this block exits. Not reentrant.
        Pass ``task`` so a duplicate delivery republishes a probe instead of
        running the body. The caller stores the task result before leaving
        the block; the lease is still held then, and released afterwards.

        Yields the stored payload when a completion record is already visible
        under the new lease, including JSON null. The attempt counter is not
        bumped in that case. Otherwise yields a private sentinel and the body
        runs.
        """
        if current_task_id() is not None:
            raise RuntimeError("CeleryDurability.container() is not reentrant")
        token = uuid.uuid4().hex
        if not self._store.acquire_lease(task_id, token):
            if task is not None:
                self._duplicate_delivery(task, task_id)
            raise LeaseHeld(f"lease for task {task_id!r} is already held")
        stop = threading.Event()
        heartbeat: threading.Thread | None = None
        try:
            with lease_token_scope(token), container_scope(task_id):
                # A duplicate can pass the pre-check, then acquire only after
                # the owner has stored the result and dropped the lease.
                cached = self._stored_result(task_id)
                if cached is not _MISSING:
                    yield cached
                else:
                    heartbeat = threading.Thread(
                        target=maintain_lease,
                        args=(self._store, task_id, token, self._settings, stop),
                        name=_HEARTBEAT_THREAD_NAME,
                        daemon=True,
                    )
                    heartbeat.start()
                    attempt = self._store.bump_attempt(task_id)
                    if attempt > self._settings.max_deliveries:
                        message = (
                            f"task {task_id!r} delivery {attempt} is above "
                            f"max_deliveries ({self._settings.max_deliveries})"
                        )
                        self._emit(
                            task_id,
                            "failed",
                            error_type=NonRetryableDurabilityError.__name__,
                            error_message=message,
                        )
                        raise NonRetryableDurabilityError(message)
                    emit_progress(self._store, self._listeners, task_id, "started")
                    yield _MISSING
        finally:
            # Join with no timeout so an in-flight refresh cannot outlive release.
            stop.set()
            if heartbeat is not None:
                heartbeat.join()
            self._store.release_lease(task_id, token)

    def completed_result(self, task_id: str) -> object | None:
        """Return the stored task result, or `None` when this id has not finished.

        A finished task whose JSON result is `None` also returns `None`.
        Callers that must tell those apart should read the store record.
        """
        record = self._store.get_result(task_id)
        if record is None:
            return None
        return record.payload

    def task(
        self, app: Celery, **overrides: Any
    ) -> Callable[[Callable[..., Any]], CeleryTask]:
        """Register a bound task that runs inside this container.

        Ack, retry, and time-limit fields come from `DurabilitySettings` unless
        a keyword overrides that task. `acks_late=False` and
        `acks_on_failure_or_timeout=False` are rejected. A stored result is
        returned without calling the user function or bumping the attempt.
        """
        options = self._task_options(overrides)
        self._register_worker_check(app)
        durability = self

        def decorator(fn: Callable[..., Any]) -> CeleryTask:
            if iscoroutinefunction(fn):

                def run(task_self: CeleryTask, *args: Any, **kwargs: Any) -> Any:
                    # Prefork calls run synchronously and does not await a coroutine.
                    return asyncio.run(
                        durability._invoke_async(task_self, fn, args, kwargs)
                    )

            else:

                def run(task_self: CeleryTask, *args: Any, **kwargs: Any) -> Any:
                    return durability._invoke_sync(task_self, fn, args, kwargs)

            _copy_callable_meta(fn, run)
            # shared=False keeps this closure on the app the caller passed.
            # lazy=False registers now, so ack flags exist before finalize.
            celery_task = app.task(shared=False, lazy=False, **options)(run)
            celery_task._durability = durability  # type: ignore[attr-defined]
            durability._install_publish(celery_task)
            return celery_task

        return decorator

    def get_durable_operation_backend(self) -> CeleryOperationBackend:
        backend = self._operation_backend
        if backend is None or backend.agent_name != self.name:
            backend = CeleryOperationBackend(
                self._store,
                self._settings,
                agent_name=self.name,
                default_model_id=self.default_model_id,
                config=self._operation_config(),
                listeners=self._listeners,
            )
            self._operation_backend = backend
        return backend

    def _toolset_base_config(self, kind: ToolsetKind) -> CeleryOperationConfig:
        if kind == "mcp":
            return self._mcp_config
        return self._tool_config

    def _operation_config(self) -> RoleBasedOperationConfig[CeleryOperationConfig]:
        return RoleBasedOperationConfig(
            model=self._model_config,
            event=self._event_config,
            capability=self._capability_config,
            tool=self._tool_config,
            resolve_tool=self._resolve_tool_config,
        )

    def _resolve_tool_config(
        self,
        operation_id: DurableOperationId,
        tool: object | None,
        tool_name: str,
    ) -> CeleryOperationConfig | Literal[False]:
        base = (
            self._mcp_config if _is_mcp_operation(operation_id) else self._tool_config
        )
        if tool is None:
            return base
        raw = _metadata_config(tool, self.engine_spec.tool_config_key or "celery")
        if raw is None:
            return base
        if raw is False:
            return False
        if isinstance(raw, CeleryOperationConfig):
            return raw
        if isinstance(raw, Mapping):
            timeout = raw.get("timeout", base.timeout)
            return CeleryOperationConfig(timeout=timeout)
        key = self.engine_spec.tool_config_key or "celery"
        raise UserError(
            f"Tool {tool_name!r} has invalid {key!r} metadata: expected a CeleryOperationConfig, "
            f"a dict, or False, got {type(raw).__name__}."
        )

    def _invoke_sync(
        self,
        task: CeleryTask,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        task_id = _request_task_id(task)
        cached = self._stored_result(task_id)
        if cached is not _MISSING:
            return cached
        with self.container(task_id, task=task) as leased:
            if leased is not _MISSING:
                return leased
            try:
                value = fn(*args, **kwargs)
                wrote = self._store_result(task_id, value)
            except Exception as exc:
                self._note_task_error(task, task_id, exc)
                raise
            if wrote:
                self._emit(task_id, "succeeded")
            return value

    async def _invoke_async(
        self,
        task: CeleryTask,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        task_id = _request_task_id(task)
        cached = self._stored_result(task_id)
        if cached is not _MISSING:
            return cached
        with self.container(task_id, task=task) as leased:
            if leased is not _MISSING:
                return leased
            try:
                value = await fn(*args, **kwargs)
                wrote = self._store_result(task_id, value)
            except Exception as exc:
                self._note_task_error(task, task_id, exc)
                raise
            if wrote:
                self._emit(task_id, "succeeded")
            return value

    def _stored_result(self, task_id: str) -> object:
        record = self._store.get_result(task_id)
        if record is None:
            return _MISSING
        return record.payload

    def _store_result(self, task_id: str, value: object) -> bool:
        token = current_lease_token()
        if token is None:
            raise LeaseLost(f"lease for task {task_id!r} is not held by this caller")
        try:
            payload = _json_round_trip(value)
        except (EncodeError, TypeError, ValueError):
            # Celery's default serializer is JSON. The value still returns to
            # the worker; a duplicate delivery replays steps. No succeeded
            # event, because this delivery did not record a completion.
            logger.warning(
                "task %s result is not JSON-encodable; completion record skipped",
                task_id,
            )
            return False
        self._store.put_result(task_id, payload, token=token)
        return True

    def _note_task_error(self, task: CeleryTask, task_id: str, exc: Exception) -> None:
        # A probe already recorded `retrying` before raising Retry.
        if isinstance(exc, Retry):
            return
        kind: ProgressKind = "retrying" if _will_autoretry(task, exc) else "failed"
        self._emit(
            task_id,
            kind,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    def _duplicate_delivery(self, task: Any, task_id: str) -> None:
        try:
            republish_probe(task, self._store, self._settings)
        except Retry:
            self._emit(
                task_id,
                "retrying",
                error_type=LeaseHeld.__name__,
                error_message=f"lease for task {task_id!r} is already held",
            )
            raise

    def _emit(
        self,
        task_id: str,
        kind: ProgressKind,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        emit_progress(
            self._store,
            self._listeners,
            task_id,
            kind,
            error_type=error_type,
            error_message=error_message,
        )

    def _task_options(self, overrides: Mapping[str, Any]) -> dict[str, Any]:
        raw = dict(overrides)
        # Non-retryable errors stay out of the explicit autoretry_for tuple.
        # dont_autoretry_for exists only since Celery 5.5 and is not passed.
        if "dont_autoretry_for" in raw:
            raise ValueError(
                "pass autoretry_for explicitly; dont_autoretry_for is not used"
            )
        if raw.get("bind", True) is False:
            raise ValueError("a durable task is bound so it can read request.id")
        acks_late = _flag(raw, "acks_late", self._settings.acks_late)
        acks_on_failure = _flag(
            raw,
            "acks_on_failure_or_timeout",
            self._settings.acks_on_failure_or_timeout,
        )
        if acks_late is False:
            raise ValueError(
                "acks_late cannot be false on a durable task; a dead child "
                "would not be redelivered"
            )
        if acks_on_failure is False:
            raise ValueError(
                "acks_on_failure_or_timeout cannot be false on a durable task; "
                "an exhausted soft time limit would be requeued forever"
            )
        soft, hard = _time_limits(self._settings, raw)
        options: dict[str, Any] = {
            "bind": True,
            "acks_late": acks_late,
            "acks_on_failure_or_timeout": acks_on_failure,
            "reject_on_worker_lost": _flag(
                raw,
                "reject_on_worker_lost",
                self._settings.reject_on_worker_lost,
            ),
            "autoretry_for": raw.get("autoretry_for", _AUTORETRY_FOR),
            "max_retries": raw.get("max_retries", self._settings.max_retries),
            "retry_backoff": raw.get("retry_backoff", self._settings.retry_backoff),
            "retry_backoff_max": raw.get(
                "retry_backoff_max", self._settings.retry_backoff_max_s
            ),
            "retry_jitter": raw.get("retry_jitter", self._settings.retry_jitter),
            "soft_time_limit": soft,
            "time_limit": hard,
        }
        consumed = {
            "bind",
            "acks_late",
            "acks_on_failure_or_timeout",
            "reject_on_worker_lost",
            "autoretry_for",
            "max_retries",
            "retry_backoff",
            "retry_backoff_max",
            "retry_jitter",
            "soft_time_limit",
            "time_limit",
            "lazy",
            "shared",
        }
        for key, value in raw.items():
            if key not in consumed:
                options[key] = value
        return options

    def _register_worker_check(self, app: Celery) -> None:
        if not self._settings.check_worker_config:
            return
        app_id = id(app)
        if app_id in _WORKER_CHECK_APPS:
            return
        _WORKER_CHECK_APPS.add(app_id)
        settings = self._settings

        def _on_worker_init(**_kwargs: object) -> None:
            try:
                conf = _conf_from_app(app)
            except Exception:
                logger.exception("durable worker configuration check failed")
                return
            for message in worker_config_warnings(conf, settings):
                logger.warning("%s", message)
            notice = rabbitmq_consumer_timeout_notice(conf)
            if notice:
                logger.info("%s", notice)

        worker_init.connect(
            _on_worker_init,
            weak=False,
            dispatch_uid=f"pydantic-ai-celery-worker-config-{app_id}",
        )

    def _install_publish(self, task: CeleryTask) -> None:
        durability = self

        def apply_async(
            args: Any = None,
            kwargs: Any = None,
            task_id: str | None = None,
            producer: Any = None,
            link: Any = None,
            link_error: Any = None,
            shadow: Any = None,
            **options: Any,
        ) -> Any:
            options = dict(options)
            if task_id is None:
                task_id = options.pop("task_id", None)
            else:
                options.pop("task_id", None)
            if task_id is None:
                task_id = uuid.uuid4().hex
            task_id = str(task_id)
            # A probe republishes while the owner still holds the lease.
            # Queued would clobber the owner's running status.
            record_queue = not durability._store.lease_held(task_id)
            if record_queue:
                durability._emit(task_id, "queued")
            try:
                return CeleryTask.apply_async(
                    task,
                    args=args,
                    kwargs=kwargs,
                    task_id=task_id,
                    producer=producer,
                    link=link,
                    link_error=link_error,
                    shadow=shadow,
                    **options,
                )
            except Exception as exc:
                if (
                    record_queue
                    and durability._store.snapshot(task_id).status == "queued"
                ):
                    durability._emit(
                        task_id,
                        "failed",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                raise

        task.apply_async = apply_async  # type: ignore[method-assign]


def maintain_lease(
    store: RunStore,
    task_id: str,
    token: str,
    settings: DurabilitySettings,
    stop: threading.Event,
) -> None:
    """Refresh `task_id`'s lease until `stop` is set.

    The first refresh runs immediately. `stop.wait` is the sleep, so a refresh
    that sets `stop` returns without waiting out `lease_heartbeat_s`. One
    failed refresh is logged and the loop continues until the container exits.
    """
    while not stop.is_set():
        try:
            store.refresh_lease(task_id, token)
        except Exception:
            logger.exception("lease refresh failed for task %s", task_id)
        if stop.wait(settings.lease_heartbeat_s):
            return


def configure_celery_app(
    app: Celery,
    settings: DurabilitySettings | None = None,
    *,
    visibility_timeout: int | None = None,
    prefetch_multiplier: int | None = None,
) -> None:
    """Write the recommended Redis visibility timeout and worker prefetch.

    Opt-in. Constructing `CeleryDurability` does not call this. A result
    backend that is already configured gets the same visibility timeout; this
    does not enable a backend. Keyword arguments override the settings fields
    for this call.
    """
    resolved = settings if settings is not None else DurabilitySettings()
    timeout = (
        resolved.recommended_visibility_timeout_s
        if visibility_timeout is None
        else visibility_timeout
    )
    prefetch = (
        resolved.recommended_prefetch
        if prefetch_multiplier is None
        else prefetch_multiplier
    )
    broker_options = dict(app.conf.broker_transport_options or {})
    broker_options["visibility_timeout"] = timeout
    app.conf.broker_transport_options = broker_options
    # The Redis transport reads the broker option. The app-level key is the
    # same number operators look up on `app.conf`.
    app.conf.visibility_timeout = timeout
    backend = app.conf.result_backend
    if isinstance(backend, str) and backend:
        backend_options = dict(app.conf.result_backend_transport_options or {})
        backend_options["visibility_timeout"] = timeout
        app.conf.result_backend_transport_options = backend_options
    app.conf.worker_prefetch_multiplier = prefetch


def agent_queue(
    name: str,
    consumer_timeout_ms: int,
    *,
    settings: DurabilitySettings | None = None,
) -> Queue:
    """Return a queue whose RabbitMQ consumer timeout is `consumer_timeout_ms`.

    `settings` is accepted so callers can pass the capability settings; the
    timeout argument is the value that goes on the queue.
    """
    del settings
    return Queue(
        name,
        queue_arguments={"x-consumer-timeout": consumer_timeout_ms},
    )


def resume(
    task: CeleryTask,
    *,
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
    task_id: str | None = None,
) -> Any:
    """Republish `task` on its existing id after clearing the delivery counter.

    Raises `RunInProgress` when the lease is held and does not publish.
    `task_id` defaults to `task.request.id`.
    """
    durability = getattr(task, "_durability", None)
    if not isinstance(durability, CeleryDurability):
        raise TypeError("resume() expects a task registered with CeleryDurability.task")
    if task_id is None:
        task_id = task.request.id
    if not task_id:
        raise ValueError("resume() requires a task id")
    task_id = str(task_id)
    if durability._store.lease_held(task_id):
        raise RunInProgress(f"task {task_id!r} still holds its lease")
    durability._store.reset_attempts(task_id)
    call_kwargs = {} if kwargs is None else dict(kwargs)
    return task.apply_async(args=tuple(args), kwargs=call_kwargs, task_id=task_id)


def republish_probe(task: Any, store: RunStore, settings: DurabilitySettings) -> None:
    """Count a duplicate delivery and publish the same task id later.

    Does not call `Task.retry`. That assigns `override_max_retries` on the
    shared task, and the next ordinary autoretry copies it.
    """
    task_id = task.request.id
    if not task_id:
        raise RuntimeError("republish_probe requires task.request.id")
    task_id = str(task_id)
    count = store.bump_probe(task_id)
    if count > settings.lease_probe_max_retries:
        raise NonRetryableDurabilityError(
            f"task {task_id!r} probe count {count} is above "
            f"lease_probe_max_retries ({settings.lease_probe_max_retries})"
        )
    signature = task.signature_from_request(
        countdown=settings.probe_countdown_s,
        retries=task.request.retries,
    )
    signature.apply_async()
    raise Retry(
        f"lease for task {task_id!r} is held; probing again "
        f"in {settings.probe_countdown_s}s"
    )


def _is_mcp_operation(operation_id: DurableOperationId) -> bool:
    if isinstance(operation_id, ToolsetGetInstructionsId):
        return True
    if isinstance(
        operation_id,
        (ToolsetCallToolId, ToolsetGetToolsId, ToolsetValidateToolArgumentsId),
    ):
        return operation_id.toolset_kind == "mcp"
    return False


def _metadata_config(tool: object, key: str) -> Any:
    if not isinstance(tool, ToolsetTool):
        return None
    metadata = tool.tool_def.metadata
    if not metadata or key not in metadata:
        return None
    return metadata[key]


def _copy_callable_meta(source: Callable[..., Any], target: Callable[..., Any]) -> None:
    target.__name__ = source.__name__
    target.__qualname__ = source.__qualname__
    target.__module__ = source.__module__
    target.__doc__ = source.__doc__


def _request_task_id(task: CeleryTask) -> str:
    task_id = task.request.id
    if not task_id:
        raise RuntimeError("durable task was called without a Celery request id")
    return str(task_id)


def _json_round_trip(value: object) -> object:
    content_type, content_encoding, data = dumps(value, serializer="json")
    return loads(data, content_type, content_encoding, accept=[content_type])


def _flag(overrides: Mapping[str, Any], name: str, default: bool) -> bool:
    if name not in overrides or overrides[name] is None:
        return default
    return bool(overrides[name])


def _time_limits(
    settings: DurabilitySettings,
    overrides: Mapping[str, Any],
) -> tuple[int | None, int | None]:
    soft_set = "soft_time_limit" in overrides
    hard_set = "time_limit" in overrides
    soft = overrides["soft_time_limit"] if soft_set else settings.soft_time_limit_s
    if hard_set:
        hard = overrides["time_limit"]
    elif soft_set:
        grace = settings.time_limit_grace_s
        hard = None if soft is None or grace is None else int(soft) + grace
    else:
        hard = settings.resolved_time_limit_s
    if soft is not None and hard is not None and hard <= soft:
        raise ValueError(
            f"time_limit must be greater than soft_time_limit ({hard} <= {soft})"
        )
    return soft, hard


def _will_autoretry(task: CeleryTask, exc: BaseException) -> bool:
    if not isinstance(exc, Exception):
        return False
    autoretry_for = tuple(getattr(task, "autoretry_for", ()) or ())
    if not autoretry_for or not isinstance(exc, autoretry_for):
        return False
    dont = tuple(getattr(task, "dont_autoretry_for", ()) or ())
    if dont and isinstance(exc, dont):
        return False
    if hasattr(task, "override_max_retries"):
        max_retries = task.override_max_retries
    else:
        max_retries = task.max_retries
    if max_retries is None:
        return True
    retries = task.request.retries or 0
    return retries < max_retries


def _conf_from_app(app: Celery) -> dict[str, object]:
    conf = app.conf
    mapping: dict[str, object] = {
        "broker_transport_options": dict(conf.broker_transport_options or {}),
        "worker_prefetch_multiplier": conf.worker_prefetch_multiplier,
        "worker_cancel_long_running_tasks_on_connection_loss": (
            conf.worker_cancel_long_running_tasks_on_connection_loss
        ),
    }
    if conf.broker_url:
        mapping["broker_url"] = conf.broker_url
    transport = getattr(conf, "broker_transport", None)
    if transport:
        mapping["broker_transport"] = transport
    visibility = conf.get("visibility_timeout")
    if visibility is not None:
        mapping["visibility_timeout"] = visibility
    if conf.task_time_limit is not None:
        mapping["task_time_limit"] = conf.task_time_limit
    return mapping
