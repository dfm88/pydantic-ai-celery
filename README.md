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
app = Celery("examples.durable_run", broker="redis://localhost:6379/0")
configure_celery_app(app)

settings = DurabilitySettings()
# Journal URL. Separate from the broker. A URL does not connect until a store
# command, and the string itself has no setup().
durability = CeleryDurability("redis://localhost:6379/1", settings=settings)
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

## Replay

One Celery task is the container. Each model request and tool call is a journal step. A redelivery of that task returns the stored steps and runs only the step that was not journaled.

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
