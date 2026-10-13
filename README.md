# pydantic-ai-celery

Durable execution for Pydantic AI agents on Celery.

A worker crash mid-run repeats model and tool calls. The run starts again at the top of the task and pays for every request that already finished.

## Install

```bash
pip install pydantic-ai-celery
```

The journal is not the Celery broker. Install the extra for the store you use:

```bash
pip install "pydantic-ai-celery[redis]"
pip install "pydantic-ai-celery[postgres]"
```

## Example

`examples/durable_run.py` is this script. It is not part of the test suite.

```python
import time

from celery import Celery
from pydantic_ai import Agent

from pydantic_ai_celery import (
    CeleryDurability,
    DurabilitySettings,
    configure_celery_app,
)

# Celery rewrites __main__ to this, matching the worker's import name.
app = Celery("examples.durable_run", broker="redis://localhost:6389/0")
configure_celery_app(app)

settings = DurabilitySettings()
# Journal URL. Separate from the broker. A URL does not connect until a store
# command, and the string itself has no setup().
durability = CeleryDurability("redis://localhost:6389/1", settings=settings)
agent = Agent("openai:gpt-5", name="analyst", capabilities=[durability])


@durability.task(app)
def analyse(prompt: str) -> str:
    return agent.run_sync(prompt).output


def poll(task_id: str) -> None:
    """Producer. Drain each page, then pass ``next_seq`` back as ``after``."""
    page = durability.progress(task_id)
    while True:
        for event in page.events:
            print(event.seq, event.kind, page.snapshot.status)
        # A full page can hide later events, so keep reading until a short one.
        if len(page.events) >= settings.progress_page_limit:
            page = durability.progress(task_id, after=page.next_seq)
            continue
        status = page.snapshot.status
        if status == "interrupted":
            print(f"run {task_id} stopped and can be continued with resume")
            return
        if status in {"succeeded", "failed"}:
            return
        time.sleep(0.5)
        page = durability.progress(task_id, after=page.next_seq)


if __name__ == "__main__":
    # Worker, from the repository root:
    #   celery -A examples.durable_run:app worker --loglevel=info
    pending = analyse.delay("Summarize Q3")
    poll(pending.id)
```

Polling that cursor is the progress API. `durability.progress(task_id)` returns the snapshot, the events after `after`, and `next_seq` to pass back as `after`. The library does not open a websocket. A frontend that wants push registers a `ProgressListener` and still catches up with this call.

## Local services

This repository has its own Redis and Postgres. They are not the databases of other projects. Host ports are 6389 and 5444.

```bash
docker compose up -d
```

Broker `redis://localhost:6389/0`. Journal `redis://localhost:6389/1`. Postgres `postgresql://celery:celery@localhost:5444/celery`.

The OpenAI example needs `OPENAI_API_KEY`. Start the worker in the foreground with a solo pool so one process runs the task:

```bash
uv run celery -A examples.durable_run:app worker --pool=solo --loglevel=info
```

In another terminal, from the repository root: `uv run python examples/durable_run.py`.

`examples/trace_local.py` needs neither a key nor Docker. TestModel and an in-memory journal run the same container in this process:

```bash
uv run python examples/trace_local.py
```

## Flow

A producer publishes one Celery task. The worker acquires a lease, then the agent loop runs. Each model request and each tool call is one journal step, numbered from zero in the order the loop reaches them. The task result is stored before the lease is released. `progress(task_id)` reads that journal. It does not read Celery's result backend.

Weather question, first delivery. The model asks for a lookup, the tool runs, the model answers.

```mermaid
sequenceDiagram
    participant Producer
    participant Broker
    participant Worker
    participant Journal
    participant Model

    Producer->>Journal: queued
    Producer->>Broker: analyse.delay
    Broker->>Worker: task id
    Worker->>Journal: lease, started
    Worker->>Model: model request
    Worker->>Journal: step 0 response
    Worker->>Model: lookup tool
    Worker->>Journal: step 1 tool result
    Worker->>Model: model request
    Worker->>Journal: step 2 final answer
    Worker->>Journal: result, succeeded
    Worker->>Broker: ack
    Producer->>Journal: progress
```

The worker dies after step 0 is stored and before the tool is stored. Celery redelivers the same task id. The new process starts the agent at the top. Step 0 comes back from the journal and the model is not called again. The tool runs and is stored as step 1.

```mermaid
sequenceDiagram
    participant Broker
    participant First as Worker, then dies
    participant Second as Redelivered worker
    participant Journal

    First->>Journal: step 0 stored
    Note over First: crash before the tool write
    Broker->>Second: same task id
    Second->>Journal: lease, encounter starts at 0
    Journal-->>Second: step 0, no model call
    Second->>Journal: step 1, tool runs
```

A crash after the tool's side effect and before its journal write runs that tool again. Keep tool side effects idempotent.

## Replay

One Celery task is the container. Each model request and tool call is a journal step. A redelivery returns every stored step and runs the steps that were not stored. A step whose side effect happened and whose journal write did not is run again.

## Determinism

Tools run sequentially because step identity is encounter order. The index is how many steps this delivery has already seen. A parallel tool call would make that index depend on finish order, and a replay would not line up with the journal.

## Postgres

```python
store = PostgresRunStore(url)
store.setup()
durability = CeleryDurability(store)
```

A URL string does not expose `setup()`. `CeleryDurability("postgresql://...")` builds a store internally, and that string has no `setup()` method. Construct `PostgresRunStore` and call `setup()` so the tables exist.

## Design

[docs/superpowers/specs/2026-10-10-celery-durable-exec-design.md](docs/superpowers/specs/2026-10-10-celery-durable-exec-design.md)
