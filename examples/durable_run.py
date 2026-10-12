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
