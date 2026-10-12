"""Validation and derived fields on DurabilitySettings."""

import pytest

from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._errors import (
    DurabilityDivergence,
    LeaseHeld,
    LeaseLost,
    NonRetryableDurabilityError,
    RunInProgress,
    UnitTimeout,
)


def test_heartbeat_that_cannot_miss_one_beat_is_rejected() -> None:
    # 15 * 2 == 30, so the refresh after one miss lands on lease expiry.
    with pytest.raises(ValueError, match="lease_heartbeat_s"):
        DurabilitySettings(lease_heartbeat_s=15, lease_ttl_s=30)

    # Just under two heartbeats still leaves the lease held after one miss.
    DurabilitySettings(lease_heartbeat_s=14, lease_ttl_s=30)


def test_journal_ttl_must_be_positive() -> None:
    with pytest.raises(ValueError, match="journal_ttl_s"):
        DurabilitySettings(journal_ttl_s=0)
    with pytest.raises(ValueError, match="journal_ttl_s"):
        DurabilitySettings(journal_ttl_s=-1)


def test_progress_page_limit_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="progress_page_limit"):
        DurabilitySettings(progress_page_limit=0)


def test_soft_limit_without_hard_limit_resolves_with_grace() -> None:
    settings = DurabilitySettings(soft_time_limit_s=3600)
    assert settings.resolved_time_limit_s == 3630


def test_hard_limit_must_exceed_soft_limit() -> None:
    with pytest.raises(ValueError, match="time_limit_s"):
        DurabilitySettings(soft_time_limit_s=3600, time_limit_s=3600)
    with pytest.raises(ValueError, match="time_limit_s"):
        DurabilitySettings(soft_time_limit_s=3600, time_limit_s=3599)


def test_explicit_hard_limit_is_kept() -> None:
    settings = DurabilitySettings(soft_time_limit_s=3600, time_limit_s=3700)
    assert settings.resolved_time_limit_s == 3700


def test_omitted_grace_leaves_hard_limit_unset() -> None:
    settings = DurabilitySettings(soft_time_limit_s=3600, time_limit_grace_s=None)
    assert settings.resolved_time_limit_s is None


def test_probe_countdown_follows_lease_ttl_when_unset() -> None:
    default = DurabilitySettings()
    assert default.lease_probe_countdown_s is None
    assert default.probe_countdown_s == default.lease_ttl_s
    assert isinstance(default.probe_countdown_s, float)

    custom = DurabilitySettings(lease_ttl_s=45)
    assert custom.probe_countdown_s == 45


def test_explicit_probe_countdown_is_kept() -> None:
    settings = DurabilitySettings(lease_probe_countdown_s=12)
    assert settings.probe_countdown_s == 12


def test_error_types() -> None:
    assert issubclass(DurabilityDivergence, NonRetryableDurabilityError)
    assert issubclass(NonRetryableDurabilityError, Exception)
    for error in (RunInProgress, LeaseHeld, LeaseLost, UnitTimeout):
        assert issubclass(error, Exception)
        assert not issubclass(error, NonRetryableDurabilityError)
