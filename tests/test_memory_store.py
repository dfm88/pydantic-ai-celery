"""MemoryRunStore leases, journal records, and progress snapshots."""

import pytest

from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._errors import LeaseLost
from pydantic_ai_celery._store import (
    MemoryRunStore,
    ResultRecord,
    RunSnapshot,
    RunStore,
    StepRecord,
)


class Clock:
    """Stand-in for ``time.monotonic``. Starts at 0 and moves only when told."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_step_round_trip() -> None:
    store = MemoryRunStore()
    assert isinstance(store, RunStore)
    assert store.acquire_lease("task-1", "owner") is True
    store.put_step("task-1", 0, "model_request", {"output": "ok"}, token="owner")
    assert store.get_step("task-1", 0) == StepRecord(
        operation_name="model_request",
        payload={"output": "ok"},
    )
    assert store.get_step("task-1", 1) is None
    assert store.get_step("task-2", 0) is None
    snapshot = store.snapshot("task-1")
    assert snapshot.steps_completed == 1
    assert snapshot.last_operation_name == "model_request"

    store.put_step("task-1", 0, "model_request", {"output": "replay"}, token="owner")
    assert store.get_step("task-1", 0) == StepRecord(
        operation_name="model_request",
        payload={"output": "replay"},
    )
    assert store.snapshot("task-1").steps_completed == 1


def test_put_step_requires_the_held_lease() -> None:
    clock = Clock()
    settings = DurabilitySettings()
    store = MemoryRunStore(settings, now=clock)
    with pytest.raises(LeaseLost):
        store.put_step("task-1", 0, "model_request", "x", token="owner")
    assert store.get_step("task-1", 0) is None
    assert store.snapshot("task-1").status == "unknown"

    assert store.acquire_lease("task-1", "owner") is True
    with pytest.raises(LeaseLost):
        store.put_step("task-1", 0, "model_request", "x", token="other")
    assert store.get_step("task-1", 0) is None

    store.put_step("task-1", 0, "model_request", "x", token="owner")
    assert store.get_step("task-1", 0) == StepRecord("model_request", "x")

    clock.advance(settings.lease_ttl_s)
    with pytest.raises(LeaseLost):
        store.put_step("task-1", 1, "model_request", "y", token="owner")
    assert store.get_step("task-1", 1) is None
    assert store.get_step("task-1", 0) == StepRecord("model_request", "x")


def test_put_step_rejects_a_negative_index() -> None:
    store = MemoryRunStore()
    assert store.acquire_lease("task-1", "owner") is True
    with pytest.raises(ValueError, match="step index"):
        store.put_step("task-1", -1, "model_request", None, token="owner")


def test_second_acquire_fails_while_the_lease_is_held() -> None:
    store = MemoryRunStore()
    assert store.acquire_lease("task-1", "owner") is True
    assert store.acquire_lease("task-1", "owner") is True
    assert store.acquire_lease("task-1", "other") is False
    assert store.lease_held("task-1") is True
    assert store.lease_held("task-1", "owner") is True
    assert store.lease_held("task-1", "other") is False


def test_acquire_succeeds_after_expiry() -> None:
    clock = Clock()
    settings = DurabilitySettings()
    store = MemoryRunStore(settings, now=clock)
    assert store.acquire_lease("task-1", "owner") is True

    clock.advance(settings.lease_ttl_s / 2)
    assert store.acquire_lease("task-1", "other") is False

    clock.advance(settings.lease_ttl_s / 2)
    assert store.lease_held("task-1") is False
    assert store.acquire_lease("task-1", "next") is True
    assert store.lease_held("task-1", "next") is True
    assert store.lease_held("task-1", "owner") is False


def test_refresh_keeps_the_lease() -> None:
    clock = Clock()
    settings = DurabilitySettings()
    store = MemoryRunStore(settings, now=clock)
    assert store.acquire_lease("task-1", "owner") is True
    clock.advance(settings.lease_ttl_s / 2)
    assert store.refresh_lease("task-1", "other") is False
    assert store.refresh_lease("task-1", "owner") is True

    # The original deadline has passed. The refresh moved it forward.
    clock.advance(settings.lease_ttl_s / 2)
    assert store.lease_held("task-1", "owner") is True
    assert store.acquire_lease("task-1", "other") is False

    clock.advance(settings.lease_ttl_s / 2)
    assert store.lease_held("task-1") is False
    assert store.refresh_lease("task-1", "owner") is False


def test_release_lets_a_new_token_acquire() -> None:
    store = MemoryRunStore()
    assert store.acquire_lease("task-1", "owner") is True
    assert store.release_lease("task-1", "other") is False
    assert store.lease_held("task-1", "owner") is True
    assert store.release_lease("task-1", "owner") is True
    assert store.lease_held("task-1") is False
    assert store.acquire_lease("task-1", "next") is True
    assert store.lease_held("task-1", "next") is True


def test_result_round_trip_distinguishes_missing_from_none() -> None:
    store = MemoryRunStore()
    assert store.get_result("task-1") is None
    store.put_result("task-1", None)
    assert store.get_result("task-1") == ResultRecord(payload=None)
    store.put_result("task-1", {"ok": True})
    assert store.get_result("task-1") == ResultRecord(payload={"ok": True})


def test_attempt_and_probe_counters() -> None:
    store = MemoryRunStore()
    assert store.acquire_lease("task-1", "owner") is True
    store.put_step("task-1", 0, "model_request", 1, token="owner")
    assert store.bump_attempt("task-1") == 1
    assert store.bump_attempt("task-1") == 2
    assert store.bump_probe("task-1") == 1
    assert store.bump_attempt("task-1") == 3
    assert store.bump_probe("task-2") == 1
    assert store.snapshot("task-1").attempt == 3
    assert store.snapshot("task-2").attempt == 0
    store.reset_attempts("task-1")
    assert store.snapshot("task-1").attempt == 0
    assert store.get_step("task-1", 0) == StepRecord("model_request", 1)
    assert store.bump_attempt("task-1") == 1
    store.reset_attempts("missing")
    assert store.snapshot("missing").status == "unknown"


def test_progress_seq_order() -> None:
    clock = Clock()
    store = MemoryRunStore(now=clock)
    assert store.append_progress("task-1", "queued") == 1
    clock.advance(1)
    assert store.append_progress("task-2", "queued") == 1
    second = store.append_progress(
        "task-1",
        "step_finished",
        operation_name="model_request",
        step_index=0,
        attempt=1,
    )
    assert second == 2
    events = store.read_progress("task-1")
    assert [event.seq for event in events] == [1, 2]
    assert [event.kind for event in events] == ["queued", "step_finished"]
    assert events[0].task_id == "task-1"
    assert events[0].at == 0.0
    assert events[1].at == 1.0
    assert events[1].operation_name == "model_request"
    assert events[1].step_index == 0
    assert events[1].attempt == 1
    assert [event.seq for event in store.read_progress("task-2")] == [1]


def test_read_progress_after_last_is_empty() -> None:
    store = MemoryRunStore()
    store.append_progress("task-1", "queued")
    last = store.append_progress("task-1", "started")
    assert store.read_progress("task-1", after=last) == ()
    assert store.read_progress("task-1", after=last, limit=10) == ()
    assert store.read_progress("absent", after=0) == ()


def test_read_progress_pages_from_the_cursor() -> None:
    settings = DurabilitySettings(progress_page_limit=2)
    store = MemoryRunStore(settings)
    for index in range(5):
        store.append_progress("task-1", "step_finished", step_index=index)
    assert [event.seq for event in store.read_progress("task-1")] == [1, 2]
    assert [
        event.seq for event in store.read_progress("task-1", after=2, limit=10)
    ] == [
        3,
        4,
        5,
    ]
    assert store.read_progress("task-1", after=5, limit=10) == ()
    with pytest.raises(ValueError, match="limit"):
        store.read_progress("task-1", limit=0)


def test_append_progress_rejects_an_unknown_kind() -> None:
    store = MemoryRunStore()
    with pytest.raises(ValueError, match="progress kind"):
        store.append_progress("task-1", "interrupted")  # type: ignore[arg-type]


def test_snapshot_is_unknown_when_absent() -> None:
    snapshot = MemoryRunStore().snapshot("missing")
    assert snapshot == RunSnapshot(
        task_id="missing",
        status="unknown",
        attempt=0,
        steps_completed=0,
        last_operation_name=None,
        error_type=None,
        error_message=None,
        updated_at=None,
    )
    # A lease without a journal write is still absent.
    store = MemoryRunStore()
    assert store.acquire_lease("task-1", "owner") is True
    assert store.snapshot("task-1").status == "unknown"


def test_running_and_expired_lease_snapshots_as_interrupted() -> None:
    clock = Clock()
    settings = DurabilitySettings()
    store = MemoryRunStore(settings, now=clock)
    assert store.acquire_lease("task-1", "owner") is True
    store.append_progress("task-1", "started")
    store.put_step("task-1", 0, "model_request", "done", token="owner")
    running = store.snapshot("task-1")
    assert running.status == "running"
    assert running.steps_completed == 1
    assert running.last_operation_name == "model_request"
    assert running.updated_at == 0.0

    clock.advance(settings.lease_ttl_s)
    interrupted = store.snapshot("task-1")
    assert interrupted.status == "interrupted"
    assert interrupted.task_id == "task-1"
    assert interrupted.steps_completed == 1
    assert interrupted.last_operation_name == "model_request"
    assert interrupted.updated_at == 0.0

    # Interrupted is not stored. The next holder reads running again.
    assert store.acquire_lease("task-1", "next") is True
    assert store.snapshot("task-1").status == "running"


def test_running_without_a_lease_snapshots_as_interrupted() -> None:
    store = MemoryRunStore()
    store.append_progress("task-1", "started")
    assert store.snapshot("task-1").status == "interrupted"


def test_terminal_status_survives_lease_expiry() -> None:
    clock = Clock()
    settings = DurabilitySettings()
    store = MemoryRunStore(settings, now=clock)
    store.append_progress(
        "task-1",
        "failed",
        error_type="RuntimeError",
        error_message="boom",
    )
    failed = store.snapshot("task-1")
    assert failed.status == "failed"
    assert failed.error_type == "RuntimeError"
    assert failed.error_message == "boom"
    store.append_progress("task-1", "retrying")
    retrying = store.snapshot("task-1")
    assert retrying.status == "retrying"
    assert retrying.error_type == "RuntimeError"
    assert retrying.error_message == "boom"

    store.append_progress("task-1", "succeeded", error_type="RuntimeError")
    clock.advance(settings.lease_ttl_s)
    succeeded = store.snapshot("task-1")
    assert succeeded.status == "succeeded"
    assert succeeded.error_type is None
    assert succeeded.error_message is None
    assert store.lease_held("task-1") is False
