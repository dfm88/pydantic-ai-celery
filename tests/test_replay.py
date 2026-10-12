"""Journal replay inside a Celery durability container."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import pytest
from pydantic_ai import Agent, ApprovalRequired, DeferredToolRequests, FunctionToolset
from pydantic_ai.durable_exec import (
    JournalOperationNamer,
    ModelRequestId,
    ToolsetCallToolId,
)
from pydantic_ai.models.test import TestModel

from pydantic_ai_celery import (
    CeleryDurability,
    CeleryOperationConfig,
    DurabilitySettings,
    MemoryRunStore,
    NonRetryableDurabilityError,
)
from pydantic_ai_celery._backend import CeleryOperationBackend
from pydantic_ai_celery._context import current_task_id
from pydantic_ai_celery._errors import DurabilityDivergence, LeaseHeld, UnitTimeout
from pydantic_ai_celery._store import ProgressEvent, StepRecord


def _records(store: MemoryRunStore, task_id: str) -> list[StepRecord]:
    found: list[StepRecord] = []
    index = 0
    while True:
        record = store.get_step(task_id, index)
        if record is None:
            return found
        found.append(record)
        index += 1


def _agent(
    durability: CeleryDurability,
    model: TestModel,
    tool: Callable[..., Any],
    *,
    output_type: Any = str,
) -> Agent[None, Any]:
    toolset = FunctionToolset(id="tools")
    toolset.tool_plain(tool)
    return Agent(
        model,
        name="analyst",
        output_type=output_type,
        toolsets=[toolset],
        capabilities=[durability],
    )


def _watch_model(model: TestModel, log: list[str]) -> None:
    original = model.request

    async def request(
        messages: list[Any],
        model_settings: Any,
        model_request_parameters: Any,
    ) -> Any:
        log.append("model")
        return await original(messages, model_settings, model_request_parameters)

    model.request = request  # type: ignore[method-assign]


class _RaisingListener:
    def __init__(self) -> None:
        self.kinds: list[str] = []

    def on_progress(self, event: ProgressEvent) -> None:
        self.kinds.append(event.kind)
        raise RuntimeError("listener boom")


def test_outside_container_a_tool_call_writes_no_step() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"tool": 0}

    def lookup(query: str) -> str:
        calls["tool"] += 1
        assert durability.in_durable_context is False
        assert current_task_id() is None
        return f"found {query}"

    def reject_write(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("journal write outside container")

    store.put_step = reject_write  # type: ignore[method-assign]
    store.put_result = reject_write  # type: ignore[method-assign]
    store.append_progress = reject_write  # type: ignore[method-assign]

    result = _agent(durability, TestModel(custom_output_text="done"), lookup).run_sync(
        "hi"
    )
    assert result.output == "done"
    assert calls["tool"] == 1
    assert durability.in_durable_context is False


def test_failed_tool_replays_the_model_step_and_reruns_the_tool(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The journaled model request is not executed again. The final model request
    # was never stored, so the second entry still runs that one after the tool.
    caplog.set_level(logging.ERROR)
    store = MemoryRunStore()
    listener = _RaisingListener()
    durability = CeleryDurability(store, listeners=(listener,))
    log: list[str] = []
    model = TestModel(custom_output_text="done")
    _watch_model(model, log)
    calls = {"tool": 0}

    def lookup(query: str) -> str:
        calls["tool"] += 1
        log.append("tool")
        if calls["tool"] == 1:
            raise RuntimeError("boom")
        return "found"

    agent = _agent(durability, model, lookup)
    task_id = "task-1"
    with pytest.raises(RuntimeError, match="boom"), durability.container(task_id):
        agent.run_sync("hi")

    assert log == ["model", "tool"]
    first = _records(store, task_id)
    assert [record.operation_name for record in first] == ["analyst__model.request"]
    assert store.lease_held(task_id) is False
    model_step = store.get_step(task_id, 0)
    assert model_step is not None

    cursor = store.read_progress(task_id)[-1].seq
    log.clear()
    with durability.container(task_id):
        result = agent.run_sync("hi")

    assert result.output == "done"
    assert log == ["tool", "model"]
    assert calls["tool"] == 2
    assert store.get_step(task_id, 0) == model_step
    second = store.read_progress(task_id, after=cursor)
    assert any(
        event.kind == "step_finished"
        and event.operation_name == "analyst__model.request"
        for event in second
    )
    assert "step_finished" in listener.kinds
    assert any("progress listener" in record.message for record in caplog.records)
    assert store.lease_held(task_id) is False
    assert any(
        record.operation_name.endswith("call_tool:lookup")
        for record in _records(store, task_id)
    )


def test_operation_names_match_journal_operation_namer() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"tool": 0}

    def lookup(query: str) -> str:
        calls["tool"] += 1
        return "found"

    agent = _agent(durability, TestModel(custom_output_text="done"), lookup)
    task_id = "task-1"
    with durability.container(task_id):
        result = agent.run_sync("hi")
    assert result.output == "done"
    assert calls["tool"] == 1

    namer = JournalOperationNamer("analyst")
    model_name = namer.invocation_name(
        ModelRequestId(model_name="test"), label=None
    ).operation_name
    tool_name = namer.invocation_name(
        ToolsetCallToolId("function", toolset_id="tools"),
        label="lookup",
    ).operation_name
    assert model_name == "analyst__model.request"
    assert tool_name == "analyst__function_toolset__tools.call_tool:lookup"
    names = [record.operation_name for record in _records(store, task_id)]
    assert model_name in names
    assert tool_name in names
    assert store.lease_held(task_id) is False


def test_two_calls_to_the_same_tool_have_their_own_records_after_model_steps() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    seen: list[str] = []

    def lookup(query: str) -> str:
        value = f"result-{len(seen)}"
        seen.append(value)
        return value

    model = TestModel(call_tools=["lookup", "lookup"], custom_output_text="done")
    agent = _agent(durability, model, lookup)
    task_id = "task-1"
    with durability.container(task_id):
        result = agent.run_sync("hi")
    assert result.output == "done"
    assert seen == ["result-0", "result-1"]

    records = _records(store, task_id)
    # Index 0 is the first model step. The two lookup calls are the next records.
    assert [record.operation_name for record in records] == [
        "analyst__model.request",
        "analyst__function_toolset__tools.call_tool:lookup",
        "analyst__function_toolset__tools.call_tool:lookup",
        "analyst__model.request",
    ]
    assert records[1] != records[2]
    assert "result-0" in str(records[1].payload)
    assert "result-1" in str(records[2].payload)


def test_approval_required_is_journaled_and_not_rerun() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"tool": 0}

    def lookup(query: str) -> str:
        calls["tool"] += 1
        raise ApprovalRequired()

    agent = _agent(
        durability,
        TestModel(),
        lookup,
        output_type=[str, DeferredToolRequests],
    )
    task_id = "task-1"
    with durability.container(task_id):
        first = agent.run_sync("hi")
    assert isinstance(first.output, DeferredToolRequests)
    assert first.output.approvals
    tool_records = [
        record
        for record in _records(store, task_id)
        if record.operation_name.endswith("call_tool:lookup")
    ]
    assert len(tool_records) == 1
    assert "approval_required" in str(tool_records[0].payload)

    with durability.container(task_id):
        second = agent.run_sync("hi")
    assert isinstance(second.output, DeferredToolRequests)
    assert calls["tool"] == 1
    assert (
        len(
            [
                record
                for record in _records(store, task_id)
                if record.operation_name.endswith("call_tool:lookup")
            ]
        )
        == 1
    )


def test_tampered_operation_name_raises_divergence() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"model": 0, "tool": 0}
    model = TestModel(custom_output_text="done")
    log: list[str] = []
    _watch_model(model, log)

    def lookup(query: str) -> str:
        calls["tool"] += 1
        return "found"

    agent = _agent(durability, model, lookup)
    task_id = "task-1"
    assert store.acquire_lease(task_id, "setup") is True
    store.put_step(task_id, 0, "tampered", {"no": True}, token="setup")
    assert store.release_lease(task_id, "setup") is True

    with pytest.raises(DurabilityDivergence), durability.container(task_id):
        agent.run_sync("hi")

    assert log == []
    assert calls["tool"] == 0
    assert store.get_step(task_id, 0) == StepRecord("tampered", {"no": True})
    assert store.lease_held(task_id) is False


class _NotJson:
    pass


def test_non_json_tool_return_is_non_retryable_and_writes_no_record() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)

    def lookup(query: str) -> Any:
        return _NotJson()

    agent = _agent(durability, TestModel(custom_output_text="done"), lookup)
    task_id = "task-1"
    with (
        pytest.raises(NonRetryableDurabilityError) as caught,
        durability.container(task_id),
    ):
        agent.run_sync("hi")
    assert type(caught.value) is NonRetryableDurabilityError
    assert all(
        "call_tool" not in record.operation_name for record in _records(store, task_id)
    )
    assert store.lease_held(task_id) is False


def test_operation_timeout_writes_no_record() -> None:
    store = MemoryRunStore()
    settings = DurabilitySettings()
    durability = CeleryDurability(store, settings=settings)
    backend = CeleryOperationBackend(store, settings, agent_name="analyst")
    calls = {"body": 0}

    async def body() -> dict[str, bool]:
        calls["body"] += 1
        await asyncio.sleep(1)
        return {"done": True}

    async def run() -> None:
        with durability.container("task-1"):
            await backend.execute(
                operation_id=ModelRequestId(model_name="test"),
                name="analyst__model.request",
                body=body,
                cache_key=({"ignored": True},),
                config=CeleryOperationConfig(timeout=0.01),
            )

    with pytest.raises(UnitTimeout):
        asyncio.run(run())
    assert calls["body"] == 1
    assert store.get_step("task-1", 0) is None
    assert store.snapshot("task-1").steps_completed == 0
    assert store.lease_held("task-1") is False


def test_tool_config_timeout_cancels_the_tool_and_writes_no_tool_record() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(
        store, tool_config=CeleryOperationConfig(timeout=0.01)
    )
    calls = {"tool": 0}

    async def lookup(query: str) -> str:
        calls["tool"] += 1
        await asyncio.sleep(1)
        return "late"

    agent = _agent(durability, TestModel(custom_output_text="done"), lookup)
    task_id = "task-1"
    with pytest.raises(UnitTimeout), durability.container(task_id):
        agent.run_sync("hi")
    assert calls["tool"] == 1
    assert all(
        "call_tool" not in record.operation_name for record in _records(store, task_id)
    )
    assert store.get_step(task_id, 0) is not None
    assert store.get_step(task_id, 0).operation_name == "analyst__model.request"  # type: ignore[union-attr]
    assert store.lease_held(task_id) is False


def test_in_durable_context_follows_the_container() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    seen: dict[str, Any] = {}

    def lookup(query: str) -> str:
        seen["inside"] = durability.in_durable_context
        seen["task_id"] = current_task_id()
        seen["lease"] = store.lease_held("task-1")
        return "found"

    agent = _agent(durability, TestModel(custom_output_text="done"), lookup)
    assert durability.in_durable_context is False
    assert current_task_id() is None
    with durability.container("task-1"):
        assert durability.in_durable_context is True
        assert current_task_id() == "task-1"
        result = agent.run_sync("hi")
    assert result.output == "done"
    assert seen == {"inside": True, "task_id": "task-1", "lease": True}
    assert durability.in_durable_context is False
    assert current_task_id() is None
    assert store.lease_held("task-1") is False


def test_tool_metadata_timeout_reaches_execute() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"tool": 0}

    async def lookup(query: str) -> str:
        calls["tool"] += 1
        await asyncio.sleep(1)
        return "late"

    toolset = FunctionToolset(id="tools")
    toolset.tool_plain(lookup, metadata={"celery": CeleryOperationConfig(timeout=0.01)})
    agent = Agent(
        TestModel(custom_output_text="done"),
        name="analyst",
        toolsets=[toolset],
        capabilities=[durability],
    )
    with pytest.raises(UnitTimeout), durability.container("task-1"):
        agent.run_sync("hi")
    assert calls["tool"] == 1
    assert all(
        "call_tool" not in record.operation_name for record in _records(store, "task-1")
    )


def test_false_tool_metadata_skips_the_checkpoint() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    calls = {"tool": 0}

    def lookup(query: str) -> str:
        calls["tool"] += 1
        return "inline"

    toolset = FunctionToolset(id="tools")
    toolset.tool_plain(lookup, metadata={"celery": False})
    agent = Agent(
        TestModel(custom_output_text="done"),
        name="analyst",
        toolsets=[toolset],
        capabilities=[durability],
    )
    task_id = "task-1"
    with durability.container(task_id):
        assert agent.run_sync("hi").output == "done"
    with durability.container(task_id):
        assert agent.run_sync("hi").output == "done"
    assert calls["tool"] == 2
    names = [record.operation_name for record in _records(store, task_id)]
    assert names == ["analyst__model.request", "analyst__model.request"]


def test_container_refuses_a_lease_held_by_someone_else() -> None:
    store = MemoryRunStore()
    durability = CeleryDurability(store)
    assert store.acquire_lease("task-1", "other") is True
    with pytest.raises(LeaseHeld), durability.container("task-1"):
        pass
    assert store.lease_held("task-1", "other") is True
    assert durability.in_durable_context is False


def test_container_is_not_reentrant() -> None:
    durability = CeleryDurability(MemoryRunStore())
    with (
        durability.container("task-1"),
        pytest.raises(RuntimeError, match="not reentrant"),
        durability.container("task-1"),
    ):
        pass
    assert durability.in_durable_context is False
