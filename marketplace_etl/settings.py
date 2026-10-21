from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

PIPELINE = "marketplace_orders"

CLICKHOUSE_DATABASE = "marketplace"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = Path(os.environ.get("ENV_FILE", PROJECT_ROOT / ".env"))


@dataclass(frozen=True)
class MariaDbSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class Settings:
    mariadb: MariaDbSettings
    clickhouse: ClickHouseSettings
    source_table: str = "marketplace_orders"
    full_load_page_size: int = 10_000
    delete_scan_page_size: int = 50_000
    insert_batch_size: int = 10_000
    tie_out_baseline_rows: int = 29
    freshness_limit_seconds: int = 7_200


def parse_mariadb_url(url: str) -> MariaDbSettings:
    parsed = urlparse(url)
    if not parsed.hostname or not parsed.path.lstrip("/"):
        raise ValueError(f"MARIADB_URL is not a usable database URL: {url!r}")
    return MariaDbSettings(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
    )


def _ensure_env_loaded() -> None:
    if DEFAULT_ENV_FILE.exists():
        from dotenv import load_dotenv

        load_dotenv(DEFAULT_ENV_FILE, override=False)


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is not set. Copy .env.example to .env and fill in local values."
        )
    return value


def load_settings() -> Settings:
    _ensure_env_loaded()
    clickhouse = ClickHouseSettings(
        host=_require("CLICKHOUSE_HOST"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        user=_require("CLICKHOUSE_USER"),
        password=_require("CLICKHOUSE_PASSWORD"),
        database=CLICKHOUSE_DATABASE,
    )
    return Settings(mariadb=parse_mariadb_url(_require("MARIADB_URL")), clickhouse=clickhouse)
