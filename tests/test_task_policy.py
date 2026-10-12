"""Celery ack policy, lease probe, resume, and worker warnings."""

import asyncio
import logging
import threading
from typing import Any

import pytest
from celery import Celery
from celery.app.task import Task as CeleryTask
from celery.exceptions import Retry, SoftTimeLimitExceeded
from celery.signals import worker_init
from pydantic_ai.durable_exec import ModelRequestId

from pydantic_ai_celery import (
    CeleryDurability,
    CeleryOperationConfig,
    DurabilitySettings,
    MemoryRunStore,
    NonRetryableDurabilityError,
)
from pydantic_ai_celery._backend import CeleryOperationBackend
from pydantic_ai_celery._durability import (
    _HEARTBEAT_THREAD_NAME,
    agent_queue,
    configure_celery_app,
    maintain_lease,
    resume,
)
from pydantic_ai_celery._errors import LeaseLost, RunInProgress, UnitTimeout
from pydantic_ai_celery._worker_checks import (
    rabbitmq_consumer_timeout_notice,
    worker_config_warnings,
)


def _app(name: str, broker: str = "memory://") -> Celery:
    return Celery(name, broker=broker, set_as_current=False)


def _call(
    task: Any,
    *args: Any,
    task_id: str = "task-1",
    retries: int = 0,
    **kwargs: Any,
) -> Any:
    task.push_request(id=task_id, retries=retries, args=args, kwargs=kwargs)
    try:
        return task.run(*args, **kwargs)
    finally:
        task.pop_request()


def test_decorated_task_uses_ack_and_time_limit_policy() -> None:
    app = _app("ack-policy")
    settings = DurabilitySettings(soft_time_limit_s=3600)
    durability = CeleryDurability(MemoryRunStore(), settings=settings)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        return prompt

    assert analyse.acks_late is True
    assert analyse.acks_on_failure_or_timeout is True
    assert analyse.reject_on_worker_lost is True
    assert analyse.time_limit == settings.resolved_time_limit_s
    assert analyse.time_limit == 3630
    assert analyse.soft_time_limit == 3600
    assert analyse.autoretry_for == (
        SoftTimeLimitExceeded,
        ConnectionError,
        TimeoutError,
        LeaseLost,
        UnitTimeout,
    )
    assert not getattr(analyse, "dont_autoretry_for", ())
    assert analyse.max_retries == settings.max_retries
    assert analyse.retry_backoff is settings.retry_backoff
    assert analyse.retry_backoff_max == settings.retry_backoff_max_s
    assert analyse.retry_jitter is settings.retry_jitter

    @durability.task(app, soft_time_limit=100)
    def with_grace(prompt: str) -> str:
        return prompt

    assert with_grace.soft_time_limit == 100
    assert with_grace.time_limit == 130

    with pytest.raises(ValueError, match="time_limit"):

        @durability.task(app, soft_time_limit=10, time_limit=10)
        def equal_limits(prompt: str) -> str:
            return prompt


def test_acks_late_false_is_rejected() -> None:
    app = _app("reject-acks")
    durability = CeleryDurability(MemoryRunStore())
    with pytest.raises(ValueError, match="acks_late"):

        @durability.task(app, acks_late=False)
        def analyse(prompt: str) -> str:
            return prompt


def test_dont_autoretry_for_is_rejected() -> None:
    app = _app("no-dont-autoretry")
    durability = CeleryDurability(MemoryRunStore())
    with pytest.raises(ValueError, match="dont_autoretry_for"):

        @durability.task(app, dont_autoretry_for=(RuntimeError,))
        def analyse(prompt: str) -> str:
            return prompt


def test_acks_on_failure_or_timeout_false_is_rejected() -> None:
    app = _app("reject-ack-on-failure")
    durability = CeleryDurability(MemoryRunStore())
    with pytest.raises(ValueError, match="acks_on_failure_or_timeout"):

        @durability.task(app, acks_on_failure_or_timeout=False)
        def analyse(prompt: str) -> str:
            return prompt


def test_lease_held_republishes_without_task_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_retry(self: CeleryTask, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("Task.retry must not be used for probes")

    monkeypatch.setattr(CeleryTask, "retry", fail_retry)
    app = _app("probe")
    settings = DurabilitySettings()
    store = MemoryRunStore()
    durability = CeleryDurability(store, settings=settings)
    calls = {"user": 0, "applied": 0}
    seen: dict[str, Any] = {}

    @durability.task(app)
    def analyse(prompt: str) -> str:
        calls["user"] += 1
        return prompt

    def signature_from_request(*args: Any, **kwargs: Any) -> Any:
        seen["countdown"] = kwargs.get("countdown")
        seen["retries"] = kwargs.get("retries")

        class _Signature:
            def apply_async(self, *_args: Any, **_kwargs: Any) -> None:
                calls["applied"] += 1

        return _Signature()

    analyse.signature_from_request = signature_from_request  # type: ignore[method-assign]
    assert store.acquire_lease("task-1", "owner") is True
    with pytest.raises(Retry):
        _call(analyse, "hello", task_id="task-1", retries=2)
    assert calls == {"user": 0, "applied": 1}
    assert seen["countdown"] == settings.probe_countdown_s
    assert seen["retries"] == 2
    assert store.lease_held("task-1", "owner") is True
    kinds = [event.kind for event in store.read_progress("task-1")]
    assert kinds == ["retrying"]


def test_probe_past_max_raises_non_retryable() -> None:
    app = _app("probe-cap")
    settings = DurabilitySettings(lease_probe_max_retries=1)
    store = MemoryRunStore()
    durability = CeleryDurability(store, settings=settings)
    calls = {"user": 0, "signature": 0}

    @durability.task(app)
    def analyse(prompt: str) -> str:
        calls["user"] += 1
        return prompt

    def signature_from_request(*_args: Any, **_kwargs: Any) -> Any:
        calls["signature"] += 1
        raise AssertionError("probe past the cap must not republish")

    analyse.signature_from_request = signature_from_request  # type: ignore[method-assign]
    assert store.acquire_lease("task-1", "owner") is True
    assert store.bump_probe("task-1") == 1
    with pytest.raises(NonRetryableDurabilityError):
        _call(analyse, "hello", task_id="task-1")
    assert calls == {"user": 0, "signature": 0}
    assert store.lease_held("task-1", "owner") is True


def test_max_deliveries_raises_before_the_user_function() -> None:
    app = _app("deliveries")
    settings = DurabilitySettings(max_deliveries=0)
    store = MemoryRunStore()
    durability = CeleryDurability(store, settings=settings)
    calls = {"user": 0}

    @durability.task(app)
    def analyse(prompt: str) -> str:
        calls["user"] += 1
        return prompt

    with pytest.raises(NonRetryableDurabilityError):
        _call(analyse, "hello", task_id="task-1")
    assert calls["user"] == 0
    assert store.lease_held("task-1") is False
    assert [event.kind for event in store.read_progress("task-1")] == ["failed"]


def test_resume_clears_attempts_and_reuses_the_task_id() -> None:
    app = _app("resume")
    store = MemoryRunStore()
    durability = CeleryDurability(store)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        return prompt

    assert store.bump_attempt("task-1") == 1
    assert store.bump_attempt("task-1") == 2
    seen: dict[str, Any] = {}

    def apply_async(
        args: Any = None,
        kwargs: Any = None,
        task_id: str | None = None,
        **_options: Any,
    ) -> Any:
        seen["args"] = args
        seen["kwargs"] = kwargs
        seen["task_id"] = task_id

        class _Result:
            id = task_id

        return _Result()

    analyse.apply_async = apply_async  # type: ignore[method-assign]
    result = resume(analyse, args=("Summarize Q3",), kwargs={"k": 1}, task_id="task-1")
    assert result.id == "task-1"
    assert seen == {
        "args": ("Summarize Q3",),
        "kwargs": {"k": 1},
        "task_id": "task-1",
    }
    assert store.snapshot("task-1").attempt == 0


def test_resume_while_the_lease_is_held_raises() -> None:
    app = _app("resume-held")
    store = MemoryRunStore()
    durability = CeleryDurability(store)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        return prompt

    assert store.acquire_lease("task-1", "owner") is True
    assert store.bump_attempt("task-1") == 1
    calls = {"publish": 0}

    def apply_async(*_args: Any, **_kwargs: Any) -> None:
        calls["publish"] += 1

    analyse.apply_async = apply_async  # type: ignore[method-assign]
    with pytest.raises(RunInProgress):
        resume(analyse, args=("Summarize Q3",), task_id="task-1")
    assert calls["publish"] == 0
    assert store.snapshot("task-1").attempt == 1
    assert store.lease_held("task-1", "owner") is True


def test_soft_time_limit_from_a_step_body_writes_no_record() -> None:
    store = MemoryRunStore()
    settings = DurabilitySettings()
    durability = CeleryDurability(store, settings=settings)
    backend = CeleryOperationBackend(store, settings, agent_name="analyst")

    async def body() -> dict[str, bool]:
        raise SoftTimeLimitExceeded()

    async def run() -> None:
        with durability.container("task-1"):
            await backend.execute(
                operation_id=ModelRequestId(model_name="test"),
                name="analyst__model.request",
                body=body,
                cache_key=({"ignored": True},),
                config=CeleryOperationConfig(),
            )

    with pytest.raises(SoftTimeLimitExceeded):
        asyncio.run(run())
    assert store.get_step("task-1", 0) is None
    assert store.get_result("task-1") is None
    assert store.lease_held("task-1") is False


def test_worker_config_warnings_for_default_visibility_and_prefetch() -> None:
    settings = DurabilitySettings()
    conf = {
        "broker_url": "redis://localhost:6379/0",
        "broker_transport_options": {"visibility_timeout": 3600},
        "worker_prefetch_multiplier": 4,
    }
    warnings = worker_config_warnings(conf, settings)
    assert any(
        "visibility_timeout" in warning and "3600" in warning for warning in warnings
    )
    assert any("time_limit_s is unset" in warning for warning in warnings)
    assert any("prefetch" in warning for warning in warnings)


def test_visibility_not_above_the_hard_time_limit_warns() -> None:
    settings = DurabilitySettings(time_limit_s=7200)
    conf = {
        "broker_url": "sqs://",
        "broker_transport_options": {"visibility_timeout": 3600},
        "worker_prefetch_multiplier": settings.recommended_prefetch,
    }
    warnings = worker_config_warnings(conf, settings)
    assert any("hard time limit" in warning for warning in warnings)
    assert not any("time_limit_s is unset" in warning for warning in warnings)
    assert not any("prefetch" in warning for warning in warnings)


def test_connection_loss_flag_warns() -> None:
    settings = DurabilitySettings(time_limit_s=10)
    conf = {
        "broker_url": "redis://localhost:6379/0",
        "broker_transport_options": {"visibility_timeout": 86400},
        "worker_prefetch_multiplier": settings.recommended_prefetch,
        "worker_cancel_long_running_tasks_on_connection_loss": True,
    }
    warnings = worker_config_warnings(conf, settings)
    assert len(warnings) == 1
    assert "connection_loss" in warnings[0]


def test_rabbitmq_consumer_timeout_is_a_notice_not_a_warning() -> None:
    conf = {
        "broker_url": "amqp://guest:guest@localhost:5672//",
        "worker_prefetch_multiplier": 1,
    }
    assert rabbitmq_consumer_timeout_notice(conf)
    assert "x-consumer-timeout" in rabbitmq_consumer_timeout_notice(conf)  # type: ignore[operator]
    assert rabbitmq_consumer_timeout_notice({"broker_url": "redis://localhost"}) is None
    warnings = worker_config_warnings(conf, DurabilitySettings())
    assert not any("consumer_timeout" in warning for warning in warnings)


def test_failed_apply_async_records_queued_then_failed() -> None:
    app = _app("publish-fails")
    store = MemoryRunStore()
    durability = CeleryDurability(store)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        raise AssertionError("user function must not run when publish fails")

    def send_task(*_args: Any, **_kwargs: Any) -> None:
        raise ConnectionError("broker down")

    app.send_task = send_task  # type: ignore[method-assign]
    with pytest.raises(ConnectionError, match="broker down"):
        analyse.apply_async(args=("hello",), task_id="task-1")
    events = store.read_progress("task-1")
    assert [event.kind for event in events] == ["queued", "failed"]
    assert events[1].error_type == "ConnectionError"
    assert events[1].error_message == "broker down"


def test_terminal_failure_records_failed_and_releases_the_lease() -> None:
    app = _app("terminal-failure")
    store = MemoryRunStore()
    durability = CeleryDurability(store)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _call(analyse, "hello", task_id="task-1")
    events = store.read_progress("task-1")
    assert [event.kind for event in events] == ["started", "failed"]
    assert events[-1].error_type == "RuntimeError"
    assert events[-1].error_message == "boom"
    assert store.get_result("task-1") is None
    assert store.lease_held("task-1") is False


def test_retryable_failure_records_retrying() -> None:
    app = _app("retryable-failure")
    store = MemoryRunStore()
    durability = CeleryDurability(store)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        raise ConnectionError("down")

    with pytest.raises(ConnectionError, match="down"):
        _call(analyse, "hello", task_id="task-1", retries=0)
    events = store.read_progress("task-1")
    assert [event.kind for event in events] == ["started", "retrying"]
    assert store.get_result("task-1") is None
    assert store.lease_held("task-1") is False


def test_stored_none_is_returned_without_calling_the_user() -> None:
    app = _app("stored-none")
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"user": 0}

    @durability.task(app)
    def analyse(prompt: str) -> None:
        calls["user"] += 1

    assert _call(analyse, "hello", task_id="task-1") is None
    assert calls["user"] == 1
    assert store.get_result("task-1") is not None
    assert _call(analyse, "hello", task_id="task-1") is None
    assert calls["user"] == 1
    assert store.snapshot("task-1").attempt == 1


def test_result_stored_before_acquire_is_returned_without_the_user() -> None:
    app = _app("completion-race")
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"user": 0}
    original_acquire = store.acquire_lease

    def acquire(task_id: str, token: str) -> bool:
        # The pre-check already missed. The owner stores null and releases
        # before this delivery's acquire succeeds.
        if store.get_result(task_id) is None:
            assert original_acquire(task_id, "owner") is True
            store.put_result(task_id, None, token="owner")
            assert store.release_lease(task_id, "owner") is True
        return original_acquire(task_id, token)

    store.acquire_lease = acquire  # type: ignore[method-assign]

    @durability.task(app)
    def analyse(prompt: str) -> str:
        calls["user"] += 1
        return "ran"

    assert _call(analyse, "hello", task_id="task-1") is None
    assert calls["user"] == 0
    assert store.get_result("task-1") is not None
    assert store.get_result("task-1").payload is None  # type: ignore[union-attr]
    assert store.snapshot("task-1").attempt == 0
    assert [event.kind for event in store.read_progress("task-1")] == []
    assert store.lease_held("task-1") is False


def test_second_delivery_returns_the_stored_result() -> None:
    app = _app("completed")
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"user": 0}

    @durability.task(app)
    def analyse(prompt: str) -> str:
        calls["user"] += 1
        return "done"

    assert _call(analyse, "hello", task_id="task-1") == "done"
    assert calls["user"] == 1
    assert store.get_result("task-1") is not None
    assert store.get_result("task-1").payload == "done"  # type: ignore[union-attr]
    assert store.lease_held("task-1") is False

    assert _call(analyse, "hello", task_id="task-1") == "done"
    assert calls["user"] == 1
    assert store.snapshot("task-1").attempt == 1


def test_async_task_runs_under_synchronous_task_run() -> None:
    app = _app("async-task")
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"user": 0}

    @durability.task(app)
    async def analyse(prompt: str) -> str:
        calls["user"] += 1
        return prompt

    # Prefork calls task.run directly. It must not need the test to await.
    assert _call(analyse, "hello", task_id="task-1") == "hello"
    assert calls["user"] == 1
    assert store.get_result("task-1") is not None
    assert store.get_result("task-1").payload == "hello"  # type: ignore[union-attr]
    assert store.lease_held("task-1") is False


def test_maintain_lease_returns_when_refresh_sets_stop() -> None:
    stop = threading.Event()
    seen: dict[str, str] = {}

    class _Store:
        def refresh_lease(self, task_id: str, token: str) -> bool:
            seen["task_id"] = task_id
            seen["token"] = token
            stop.set()
            return True

    # Longer than the test's wait, so a sleep-before-refresh loop fails fast.
    settings = DurabilitySettings(lease_heartbeat_s=60, lease_ttl_s=121)
    finished = threading.Event()

    def run() -> None:
        maintain_lease(_Store(), "task-1", "tok", settings, stop)  # type: ignore[arg-type]
        finished.set()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert finished.wait(timeout=1)
    assert seen == {"task_id": "task-1", "token": "tok"}


def test_heartbeat_joins_before_release() -> None:
    store = MemoryRunStore()
    settings = DurabilitySettings(lease_heartbeat_s=60, lease_ttl_s=121)
    durability = CeleryDurability(store, settings=settings)
    entered = threading.Event()
    allow_refresh = threading.Event()
    refresh_returned = threading.Event()
    original_refresh = store.refresh_lease
    original_release = store.release_lease

    def refresh(task_id: str, token: str) -> bool:
        entered.set()
        assert allow_refresh.wait(timeout=5)
        try:
            return original_refresh(task_id, token)
        finally:
            refresh_returned.set()

    def release(task_id: str, token: str) -> bool:
        assert refresh_returned.is_set()
        alive = [
            item
            for item in threading.enumerate()
            if item.name == _HEARTBEAT_THREAD_NAME and item.is_alive()
        ]
        assert alive == []
        return original_release(task_id, token)

    store.refresh_lease = refresh  # type: ignore[method-assign]
    store.release_lease = release  # type: ignore[method-assign]
    with durability.container("task-1"):
        try:
            assert entered.wait(timeout=5)
        finally:
            allow_refresh.set()
    assert store.lease_held("task-1") is False


def test_container_refreshes_until_it_exits() -> None:
    store = MemoryRunStore()
    settings = DurabilitySettings(lease_heartbeat_s=60, lease_ttl_s=121)
    durability = CeleryDurability(store, settings=settings)
    started = threading.Event()
    release_refresh = threading.Event()
    original = store.refresh_lease

    def refresh(task_id: str, token: str) -> bool:
        started.set()
        assert release_refresh.wait(timeout=5)
        return original(task_id, token)

    store.refresh_lease = refresh  # type: ignore[method-assign]
    with durability.container("task-1"):
        try:
            assert started.wait(timeout=5)
            heartbeat = [
                item
                for item in threading.enumerate()
                if item.name == _HEARTBEAT_THREAD_NAME
            ]
            assert heartbeat
            assert heartbeat[0].daemon is True
        finally:
            release_refresh.set()
    assert store.lease_held("task-1") is False


def test_configure_celery_app_sets_visibility_and_prefetch() -> None:
    app = _app("configure")
    before = (
        app.conf.worker_prefetch_multiplier,
        dict(app.conf.broker_transport_options),
        app.conf.get("visibility_timeout"),
    )
    CeleryDurability(MemoryRunStore())
    after = (
        app.conf.worker_prefetch_multiplier,
        dict(app.conf.broker_transport_options),
        app.conf.get("visibility_timeout"),
    )
    assert after == before

    settings = DurabilitySettings()
    configure_celery_app(app, settings)
    assert app.conf.broker_transport_options["visibility_timeout"] == (
        settings.recommended_visibility_timeout_s
    )
    assert app.conf.visibility_timeout == settings.recommended_visibility_timeout_s
    assert app.conf.worker_prefetch_multiplier == settings.recommended_prefetch
    assert not app.conf.result_backend

    configure_celery_app(app, visibility_timeout=15, prefetch_multiplier=3)
    assert app.conf.broker_transport_options["visibility_timeout"] == 15
    assert app.conf.visibility_timeout == 15
    assert app.conf.worker_prefetch_multiplier == 3

    backend_app = _app("configure-backend")
    backend_app.conf.result_backend = "cache+memory://"
    configure_celery_app(backend_app)
    assert backend_app.conf.result_backend_transport_options["visibility_timeout"] == (
        settings.recommended_visibility_timeout_s
    )


def test_agent_queue_sets_consumer_timeout() -> None:
    queue = agent_queue("agents", 3_600_000)
    assert queue.queue_arguments["x-consumer-timeout"] == 3_600_000


def test_worker_init_is_registered_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = _app("worker-check", broker="redis://localhost:6379/0")
    app.conf.broker_transport_options = {"visibility_timeout": 3600}
    app.conf.worker_prefetch_multiplier = 4
    durability = CeleryDurability(MemoryRunStore())
    before = len(worker_init.receivers)

    @durability.task(app)
    def analyse(prompt: str) -> str:
        return prompt

    @durability.task(app)
    def summarise(prompt: str) -> str:
        return prompt

    assert len(worker_init.receivers) == before + 1
    with caplog.at_level(logging.WARNING, logger="pydantic_ai_celery._durability"):
        worker_init.send(sender=app)
    assert any("visibility_timeout" in record.message for record in caplog.records)

    silent = CeleryDurability(
        MemoryRunStore(),
        settings=DurabilitySettings(check_worker_config=False),
    )
    quiet_app = _app("worker-check-off")
    before_quiet = len(worker_init.receivers)

    @silent.task(quiet_app)
    def quiet(prompt: str) -> str:
        return prompt

    assert len(worker_init.receivers) == before_quiet
