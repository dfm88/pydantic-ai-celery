"""Redis and Postgres run stores. Service tests skip without the extra and a URL."""

import os
import time
from uuid import uuid4

import pytest

from pydantic_ai_celery._config import DurabilitySettings
from pydantic_ai_celery._durability import CeleryDurability
from pydantic_ai_celery._store import (
    _PURGE_EXPIRED_SQL,
    MemoryRunStore,
    PostgresRunStore,
    RedisRunStore,
    RunStore,
    StepRecord,
    run_store_from_url,
)

_TASK_ID = "task-1"


def test_purge_locks_expired_runs_before_children() -> None:
    # No database: revive holds the run row, then deletes events and steps.
    # Purge has to lock runs first or the two deadlock.
    statements = [sql.strip() for sql in _PURGE_EXPIRED_SQL]
    assert statements[0].startswith("SELECT")
    assert "FROM {runs}" in statements[0]
    assert "ORDER BY task_id" in statements[0]
    assert statements[0].rstrip().endswith("FOR UPDATE")
    assert statements[1:] == [
        "DELETE FROM {events} WHERE expires_at <= statement_timestamp()",
        "DELETE FROM {steps} WHERE expires_at <= statement_timestamp()",
        "DELETE FROM {runs} WHERE expires_at <= statement_timestamp()",
    ]


def test_unsupported_store_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported durability store URL"):
        run_store_from_url("mysql://localhost/db")
    with pytest.raises(ValueError, match="unsupported durability store URL"):
        CeleryDurability("mysql://localhost/db")


def test_run_store_from_url_selects_the_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, object]] = []

    def fake(kind: str):
        def build(
            url: str, settings: DurabilitySettings | None = None
        ) -> tuple[str, str, object]:
            calls.append((kind, url, settings))
            return (kind, url, settings)

        return build

    monkeypatch.setattr("pydantic_ai_celery._store.RedisRunStore", fake("redis"))
    monkeypatch.setattr("pydantic_ai_celery._store.PostgresRunStore", fake("postgres"))
    settings = DurabilitySettings(key_prefix="from-url")
    assert run_store_from_url("redis://localhost:6379/0", settings)[0] == "redis"
    assert run_store_from_url("rediss://localhost:6379/0", settings)[0] == "redis"
    assert run_store_from_url("postgresql://localhost/db", settings)[0] == "postgres"
    assert run_store_from_url("postgres://localhost/db", settings)[0] == "postgres"
    assert [call[2] for call in calls] == [settings, settings, settings, settings]


def test_durability_keeps_a_run_store_and_dispatches_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    memory = MemoryRunStore()

    def fail(url: str, settings: DurabilitySettings | None = None) -> MemoryRunStore:
        del url, settings
        raise AssertionError("a RunStore must not be parsed as a URL")

    monkeypatch.setattr("pydantic_ai_celery._durability.run_store_from_url", fail)
    kept = DurabilitySettings(key_prefix="kept")
    durability = CeleryDurability(memory, settings=kept)
    assert durability._store is memory
    assert durability._settings is kept

    seen: dict[str, object] = {}

    def fake(url: str, settings: DurabilitySettings | None = None) -> MemoryRunStore:
        seen["url"] = url
        seen["settings"] = settings
        return memory

    monkeypatch.setattr("pydantic_ai_celery._durability.run_store_from_url", fake)
    custom = DurabilitySettings(key_prefix="from-url")
    dispatched = CeleryDurability("postgresql://localhost/db", settings=custom)
    assert seen["url"] == "postgresql://localhost/db"
    assert seen["settings"] is custom
    assert dispatched._store is memory
    assert dispatched._settings is custom

    defaulted = CeleryDurability("redis://localhost:6379/0")
    assert seen["url"] == "redis://localhost:6379/0"
    assert isinstance(seen["settings"], DurabilitySettings)
    assert defaulted._settings is seen["settings"]
    assert defaulted._store is memory


def test_redis_store_parity() -> None:
    pytest.importorskip("redis")
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("REDIS_URL is not set")
    prefix = f"pydantic-ai-celery-parity-{uuid4().hex}"
    settings = DurabilitySettings(key_prefix=prefix)
    durability = CeleryDurability(url, settings=settings)
    store = durability._store
    assert isinstance(store, RedisRunStore)
    assert store._settings.key_prefix == prefix
    store.setup()
    try:
        _assert_lease_step_and_progress(store)
    finally:
        store.close()
        _delete_redis_prefix(url, prefix)


def test_postgres_store_parity() -> None:
    pytest.importorskip("psycopg")
    url = os.environ.get("POSTGRES_URL")
    if not url:
        pytest.skip("POSTGRES_URL is not set")
    schema = f"parity_{uuid4().hex}"
    settings = DurabilitySettings(journal_ttl_s=1, postgres_schema=schema)
    durability = CeleryDurability(url, settings=settings)
    store = durability._store
    assert isinstance(store, PostgresRunStore)
    assert store._settings.postgres_schema == schema
    store.setup()
    try:
        _assert_lease_step_and_progress(store)
        _wait_until_step_expired(store)
        assert store.purge_expired() >= 1
        assert _postgres_step_count(url, schema) == 0
    finally:
        store.close()
        _drop_postgres_schema(url, schema)


def _assert_lease_step_and_progress(store: RunStore) -> None:
    assert store.acquire_lease(_TASK_ID, "owner") is True
    # The holder may acquire again. A bare SET NX would reject this token.
    assert store.acquire_lease(_TASK_ID, "owner") is True
    store.put_step(_TASK_ID, 0, "model_request", {"output": "ok"}, token="owner")
    assert store.get_step(_TASK_ID, 0) == StepRecord(
        operation_name="model_request", payload={"output": "ok"}
    )
    assert store.acquire_lease(_TASK_ID, "other") is False
    seq = store.append_progress(
        _TASK_ID,
        "step_finished",
        operation_name="model_request",
        step_index=0,
        attempt=1,
    )
    events = store.read_progress(_TASK_ID, after=seq - 1)
    assert [event.seq for event in events] == [seq]
    assert events[0].kind == "step_finished"
    assert events[0].task_id == _TASK_ID
    assert events[0].operation_name == "model_request"
    assert events[0].step_index == 0
    assert events[0].attempt == 1
    assert store.read_progress(_TASK_ID, after=seq) == ()


def _wait_until_step_expired(store: RunStore) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if store.get_step(_TASK_ID, 0) is None:
            return
        time.sleep(0.05)
    pytest.fail("step was still visible after journal_ttl_s")


def _delete_redis_prefix(url: str, prefix: str) -> None:
    import redis

    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor=cursor, match=f"{prefix}:*", count=500)
            cursor = int(cursor)
            if keys:
                client.delete(*keys)
            if cursor == 0:
                break
    finally:
        client.close()


def _postgres_step_count(url: str, schema: str) -> int:
    import psycopg
    from psycopg import sql

    with psycopg.connect(url, autocommit=True) as conn:
        row = conn.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(schema, "steps"))
        ).fetchone()
    assert row is not None
    return int(row[0])


def test_redis_keys_hash_tag_the_task_and_append_declares_the_event_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _RedisModule:
        class Redis:
            @staticmethod
            def from_url(*_args: object, **_kwargs: object) -> object:
                raise AssertionError("this test must not open a Redis connection")

    class _Client:
        def __init__(self) -> None:
            self._seqs = [None, "1"]

        def get(self, key: str) -> str | None:
            assert key == "pydantic-ai-celery:{task-1}:event_seq"
            if self._seqs:
                return self._seqs.pop(0)
            return "1"

    monkeypatch.setattr("pydantic_ai_celery._store._load_redis", lambda: _RedisModule)
    store = RedisRunStore("redis://localhost:6379/0")
    assert store._key("task-1", "lease") == "pydantic-ai-celery:{task-1}:lease"
    assert store._key("task-1", "event", "3") == ("pydantic-ai-celery:{task-1}:event:3")
    client = _Client()
    store._redis = lambda: client  # type: ignore[method-assign]
    seen: list[tuple[list[str], str]] = []

    def fake_eval(name: str, source: str, keys: list[str], args: list[object]) -> int:
        del name, args
        seen.append((keys, source))
        return -1 if len(seen) == 1 else 2

    store._eval = fake_eval  # type: ignore[method-assign]
    assert store.append_progress("task-1", "queued") == 2
    assert [call[0][2] for call in seen] == [
        "pydantic-ai-celery:{task-1}:event:1",
        "pydantic-ai-celery:{task-1}:event:2",
    ]
    keys, source = seen[0]
    assert keys == [
        "pydantic-ai-celery:{task-1}:event_seq",
        "pydantic-ai-celery:{task-1}:snapshot",
        "pydantic-ai-celery:{task-1}:event:1",
        "pydantic-ai-celery:{task-1}:lease",
        "pydantic-ai-celery:{task-1}:result",
    ]
    assert "KEYS[3]" in source
    assert "KEYS[4]" in source
    assert "KEYS[5]" in source
    assert ".." not in source


def test_postgres_reconnects_when_the_remembered_pid_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Conn:
        def __init__(self) -> None:
            self.closed = False
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            self.closed = True

    class _Psycopg:
        def __init__(self) -> None:
            self.connected: list[_Conn] = []

        def connect(self, *_args: object, **_kwargs: object) -> _Conn:
            conn = _Conn()
            self.connected.append(conn)
            return conn

    class _Identifier:
        def __init__(self, *parts: object) -> None:
            self.parts = parts

    class _SQL:
        def __init__(self, text: str) -> None:
            self.text = text

        def format(self, **_kwargs: object) -> object:
            return self

    class _Sql:
        Identifier = _Identifier
        SQL = _SQL

    psycopg = _Psycopg()
    monkeypatch.setattr(
        "pydantic_ai_celery._store._load_psycopg",
        lambda: (psycopg, _Sql, None, None),
    )
    store = PostgresRunStore("postgresql://localhost/db")
    inherited = _Conn()
    store._conn = inherited
    store._conn_pid = os.getpid() + 1
    current = store._connection()
    assert current is not inherited
    assert current is psycopg.connected[0]
    assert inherited.close_calls == 0
    assert store._connection() is current
    assert store._conn_pid == os.getpid()
    assert len(psycopg.connected) == 1


def _drop_postgres_schema(url: str, schema: str) -> None:
    import psycopg
    from psycopg import sql

    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
