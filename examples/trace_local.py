"""Run one durable agent in this process. No API key and no broker.

TestModel answers locally. MemoryRunStore keeps the journal in memory.
Celery eager mode calls the task here, so a debugger in this terminal
sees the container and every step.

    uv run python examples/trace_local.py
"""

from celery import Celery
from pydantic_ai import Agent, FunctionToolset
from pydantic_ai.models.test import TestModel

from pydantic_ai_celery import CeleryDurability, MemoryRunStore

app = Celery("examples.trace_local")
app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    broker_url="memory://",
    result_backend="cache+memory://",
)

store = MemoryRunStore()
durability = CeleryDurability(store)
model = TestModel()
toolset = FunctionToolset(id="tools")


@toolset.tool_plain
def lookup(city: str) -> str:
    return f"sunny in {city}"


agent = Agent(
    model,
    name="analyst",
    toolsets=[toolset],
    capabilities=[durability],
)


@durability.task(app)
def analyse(prompt: str) -> str:
    return agent.run_sync(prompt).output


if __name__ == "__main__":
    result = analyse.delay("Weather in Milan")
    print(result.get())
    page = durability.progress(result.id)
    print(page.snapshot.status, [event.kind for event in page.events])
