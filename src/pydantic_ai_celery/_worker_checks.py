"""Worker and broker checks. Warnings only; nothing here changes configuration."""

from collections.abc import Mapping

from pydantic_ai_celery._config import DurabilitySettings

# Celery's prefork default. An absent conf value still reserves four messages.
_CELERY_DEFAULT_PREFETCH = 4

_REDIS_SCHEMES = frozenset({"redis", "rediss", "redis+socket", "sentinel"})
_SQS_SCHEMES = frozenset({"sqs"})
_RABBIT_SCHEMES = frozenset({"amqp", "amqps", "pyamqp", "librabbitmq"})

_RABBITMQ_CONSUMER_TIMEOUT_NOTICE = (
    "RabbitMQ consumer_timeout defaults to 30 minutes (1800000 ms); "
    "a longer durable run needs a higher consumer_timeout or the queue "
    "argument x-consumer-timeout in milliseconds"
)


def worker_config_warnings(
    conf: Mapping[str, object],
    settings: DurabilitySettings,
) -> list[str]:
    """Return human-readable warnings for one worker configuration.

    Does not log and does not mutate ``conf``. RabbitMQ's consumer timeout is
    not visible here; ``rabbitmq_consumer_timeout_notice`` carries that note.
    """
    warnings: list[str] = []
    family = _broker_family(conf)
    if family in {"redis", "sqs"}:
        visibility = _visibility_timeout(conf, settings)
        hard_limit = _hard_time_limit(conf, settings)
        label = "Redis" if family == "redis" else "SQS"
        if (
            settings.time_limit_s is None
            and visibility == settings.celery_default_visibility_timeout_s
        ):
            warnings.append(
                f"{label} visibility_timeout is {visibility}, Celery's default, "
                "and time_limit_s is unset; a run longer than that is redelivered "
                "while it is still executing. The lease blocks a second execution "
                "and the probe resumes it, at the cost of extra messages"
            )
        if hard_limit is not None and visibility <= hard_limit:
            warnings.append(
                f"{label} visibility_timeout ({visibility}) is not greater than "
                f"the hard time limit ({hard_limit}); the broker can redeliver "
                "the run while it is still executing"
            )
    prefetch = _prefetch(conf)
    if prefetch != settings.recommended_prefetch:
        warnings.append(
            "worker_prefetch_multiplier is "
            f"{prefetch}, not the recommended {settings.recommended_prefetch}; "
            "reserved long tasks sit unacked and the visibility timeout applies "
            "before they start"
        )
    if _connection_loss_enabled(conf):
        warnings.append(
            "worker_cancel_long_running_tasks_on_connection_loss is enabled; "
            "a broker blip kills late-ack agent runs. The journal still resumes "
            "them on redelivery. Agent workers should leave this off"
        )
    return warnings


def rabbitmq_consumer_timeout_notice(conf: Mapping[str, object]) -> str | None:
    """Info-level note. RabbitMQ's consumer timeout is not on the Celery conf."""
    if _broker_family(conf) != "rabbitmq":
        return None
    return _RABBITMQ_CONSUMER_TIMEOUT_NOTICE


def _broker_family(conf: Mapping[str, object]) -> str | None:
    transport = str(_first(conf, "broker_transport") or "").split("+", 1)[0].lower()
    if transport in _REDIS_SCHEMES:
        return "redis"
    if transport in _SQS_SCHEMES:
        return "sqs"
    if transport in _RABBIT_SCHEMES:
        return "rabbitmq"
    url = str(_first(conf, "broker_url") or "")
    scheme = url.split(":", 1)[0].lower()
    if scheme in _REDIS_SCHEMES:
        return "redis"
    if scheme in _SQS_SCHEMES:
        return "sqs"
    if scheme in _RABBIT_SCHEMES:
        return "rabbitmq"
    return None


def _visibility_timeout(
    conf: Mapping[str, object], settings: DurabilitySettings
) -> int:
    options = _first(conf, "broker_transport_options")
    raw: object | None
    if isinstance(options, Mapping) and "visibility_timeout" in options:
        raw = options["visibility_timeout"]
    else:
        raw = _first(conf, "visibility_timeout")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        # Redis and SQS transports default to one hour when the key is absent.
        return settings.celery_default_visibility_timeout_s
    return int(raw)


def _hard_time_limit(
    conf: Mapping[str, object], settings: DurabilitySettings
) -> int | None:
    if settings.resolved_time_limit_s is not None:
        return settings.resolved_time_limit_s
    raw = _first(conf, "task_time_limit", "time_limit")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    return int(raw)


def _prefetch(conf: Mapping[str, object]) -> int:
    raw = _first(conf, "worker_prefetch_multiplier", "prefetch_multiplier")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return _CELERY_DEFAULT_PREFETCH
    return int(raw)


def _connection_loss_enabled(conf: Mapping[str, object]) -> bool:
    raw = _first(
        conf,
        "worker_cancel_long_running_tasks_on_connection_loss",
        "cancel_long_running_tasks_on_connection_loss",
    )
    return raw is True


def _first(conf: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        if key not in conf:
            continue
        value = conf[key]
        if value is not None:
            return value
    return None
