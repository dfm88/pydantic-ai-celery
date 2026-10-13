"""Load a local .env before tests read REDIS_URL and POSTGRES_URL."""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
