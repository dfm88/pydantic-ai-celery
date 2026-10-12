# Agents

Read [docs/superpowers/specs/2026-10-10-celery-durable-exec-design.md](docs/superpowers/specs/2026-10-10-celery-durable-exec-design.md) before changing this library.

Use the public builder in `pydantic_ai.durable_exec` only (`BaseDurabilityCapability`, `JournalCallableOperationBackend`, `DurabilityEngineSpec`, `JSON_CODEC`, `JournalOperationNamer`). Do not copy model or toolset code.

Keep every duration and limit on `DurabilitySettings`. Do not embed those numbers in implementation code.

Version floors: Python `>=3.11`, `pydantic-ai>=2.36`, `celery>=5.3`. No upper pins.

Run `uv run pytest`.

Conventional commits. Do not sign commits.
