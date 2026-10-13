# Contributing

```bash
uv sync --group dev --all-extras
uv run pytest
uv run ruff check src tests examples
```

The unit suite does not need a broker or a database.

Integration tests use this repository's Compose services, not another project's Redis or Postgres. Host ports are 6389 and 5444.

```bash
docker compose up -d
REDIS_URL=redis://localhost:6389/1 \
POSTGRES_URL=postgresql://celery:celery@localhost:5444/celery \
  uv run pytest tests/test_store_parity.py -q
```

Without those variables the Redis and Postgres tests skip.

`examples/trace_local.py` runs one agent with TestModel and an in-memory journal. `examples/durable_run.py` needs the Compose Redis and `OPENAI_API_KEY`. The worker has to be `--pool=solo` if you want a debugger in that process.
