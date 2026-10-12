"""Celery durability capability and container."""

import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal, Protocol

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
    current_task_id,
    lease_token_scope,
)
from pydantic_ai_celery._errors import LeaseHeld, NonRetryableDurabilityError
from pydantic_ai_celery._store import ProgressEvent, RunStore


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
        store: RunStore,
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
        self._store = store
        self._settings = settings if settings is not None else DurabilitySettings()
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
    def container(self, task_id: str) -> Iterator[None]:
        """Run `task_id` as one durable container.

        Acquires the run lease before the body and releases it afterwards,
        including when the body raises. Not reentrant. Does not start a heartbeat.
        """
        if current_task_id() is not None:
            raise RuntimeError("CeleryDurability.container() is not reentrant")
        token = uuid.uuid4().hex
        if not self._store.acquire_lease(task_id, token):
            raise LeaseHeld(f"lease for task {task_id!r} is already held")
        try:
            with lease_token_scope(token), container_scope(task_id):
                emit_progress(self._store, self._listeners, task_id, "started")
                yield
        finally:
            self._store.release_lease(task_id, token)

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
