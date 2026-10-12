"""Durability defaults. Every duration, limit, prefix, and flag lives here."""

from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class DurabilitySettings:
    """Keyword-only settings for one durable Celery capability.

    ``lease_probe_countdown_s=None`` means "use ``lease_ttl_s``". When a soft
    time limit is set and the hard limit is omitted, the hard limit is the
    soft limit plus ``time_limit_grace_s``, unless grace is ``None``.
    """

    journal_ttl_s: int = 604800
    lease_ttl_s: float = 30
    lease_heartbeat_s: float = 10
    lease_probe_countdown_s: float | None = None
    lease_probe_max_retries: int = 10_000
    soft_time_limit_s: int | None = None
    time_limit_s: int | None = None
    time_limit_grace_s: int | None = 30
    max_retries: int = 3
    max_deliveries: int = 20
    retry_backoff: bool = True
    retry_backoff_max_s: int = 600
    retry_jitter: bool = True
    acks_late: bool = True
    acks_on_failure_or_timeout: bool = True
    reject_on_worker_lost: bool = True
    key_prefix: str = "pydantic-ai-celery"
    postgres_schema: str = "pydantic_ai_celery"
    progress_page_limit: int = 100
    progress_include_payloads: bool = False
    check_worker_config: bool = True
    recommended_prefetch: int = 1
    recommended_visibility_timeout_s: int = 86400
    celery_default_visibility_timeout_s: int = 3600

    def __post_init__(self) -> None:
        # Two missed heartbeats must expire the lease, so one miss still holds it.
        if self.lease_heartbeat_s * 2 >= self.lease_ttl_s:
            raise ValueError(
                "lease_heartbeat_s must be less than half of lease_ttl_s "
                f"({self.lease_heartbeat_s} * 2 >= {self.lease_ttl_s}); "
                "one missed heartbeat has to leave the lease held"
            )
        if self.journal_ttl_s <= 0:
            raise ValueError(
                f"journal_ttl_s must be positive, got {self.journal_ttl_s}"
            )
        if self.progress_page_limit < 1:
            raise ValueError(
                f"progress_page_limit must be at least 1, got {self.progress_page_limit}"
            )
        if (
            self.soft_time_limit_s is not None
            and self.time_limit_s is not None
            and self.time_limit_s <= self.soft_time_limit_s
        ):
            raise ValueError(
                "time_limit_s must be greater than soft_time_limit_s "
                f"({self.time_limit_s} <= {self.soft_time_limit_s})"
            )

    @property
    def probe_countdown_s(self) -> float:
        if self.lease_probe_countdown_s is None:
            return float(self.lease_ttl_s)
        return float(self.lease_probe_countdown_s)

    @property
    def resolved_time_limit_s(self) -> int | None:
        if self.time_limit_s is not None:
            return self.time_limit_s
        if self.soft_time_limit_s is None or self.time_limit_grace_s is None:
            return None
        return self.soft_time_limit_s + self.time_limit_grace_s
