from __future__ import annotations

import contextlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .errors import StorageUnavailable
from .migration import LATEST_VERSION, configure_connection, migrate


@dataclass(frozen=True)
class QueueConfig:
    max_payload_bytes: int = 1_048_576
    max_result_bytes: int = 1_048_576
    max_tags: int = 32
    max_tag_length: int = 128
    max_queue_length: int = 128
    min_priority: int = -1_000_000
    max_priority: int = 1_000_000
    max_attempts: int = 1_000
    max_lease_ms: int = 86_400_000
    max_retry_delay_ms: int = 604_800_000
    default_lease_ms: int = 30_000
    page_limit_max: int = 500


class SQLiteStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.path, timeout=5.0, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            configure_connection(conn)
            conn.execute("PRAGMA synchronous=FULL")
            return conn
        except sqlite3.Error as exc:
            raise StorageUnavailable("database is unavailable") from exc

    def initialize(self) -> None:
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        with contextlib.closing(self.connect()) as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                migrate(conn, LATEST_VERSION)
            except sqlite3.Error as exc:
                raise StorageUnavailable("database initialization failed") from exc

    @contextlib.contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

