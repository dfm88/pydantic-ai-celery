# Contributing

```bash
uv sync --group dev --all-extras
uv run pre-commit install
uv run pytest
uv run ruff check src tests examples
uv run ruff format --check src tests examples
```

`pre-commit install` runs Ruff on each commit: `ruff check --fix`, then `ruff format`. The hook version is Ruff 0.17.0, the same release as the `ruff` dev dependency.

The unit suite does not need a broker or a database.

Integration tests use this repository's Compose services, not another project's Redis or Postgres. Host ports are 6389 and 5444.

```bash
docker compose up -d
cp .env.example .env
uv run pytest tests/test_store_parity.py -q
```

`.env` is gitignored. The example file has the Compose URLs. Without `REDIS_URL` and `POSTGRES_URL` the Redis and Postgres tests skip. A variable already set in the shell is not replaced by `.env`.

`examples/trace_local.py` runs one agent with TestModel and an in-memory journal. `examples/durable_run.py` needs the Compose Redis and `OPENAI_API_KEY`. The worker has to be `--pool=solo` if you want a debugger in that process.
