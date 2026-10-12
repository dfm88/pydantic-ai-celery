# Contributing

```bash
uv sync --group dev --all-extras
uv run pytest
```

`REDIS_URL` and `POSTGRES_URL` opt into the integration tests. Without those variables the Redis and Postgres tests skip. No broker or database is required for the unit suite.

```bash
uv run ruff check src tests
```
