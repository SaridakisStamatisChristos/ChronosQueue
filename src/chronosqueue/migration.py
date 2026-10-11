from __future__ import annotations

import sqlite3
from importlib import resources

LATEST_VERSION = 2


def configure_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")


def _sql(version: int, direction: str) -> str:
    name = {
        1: "0001_initial",
        2: "0002_evidence",
    }[version]
    return resources.files("chronosqueue.migrations").joinpath(f"{name}.{direction}.sql").read_text(encoding="utf-8")


def current_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def migrate(conn: sqlite3.Connection, target: int = LATEST_VERSION) -> list[tuple[int, int]]:
    if target < 0 or target > LATEST_VERSION:
        raise ValueError(f"target schema version must be between 0 and {LATEST_VERSION}")
    configure_connection(conn)
    before = current_version(conn)
    steps: list[tuple[int, int]] = []
    while before < target:
        nxt = before + 1
        conn.executescript("BEGIN IMMEDIATE;\n" + _sql(nxt, "up") + f"\nPRAGMA user_version={nxt};\nCOMMIT;")
        steps.append((before, nxt))
        before = nxt
    while before > target:
        conn.executescript("BEGIN IMMEDIATE;\n" + _sql(before, "down") + f"\nPRAGMA user_version={before - 1};\nCOMMIT;")
        steps.append((before, before - 1))
        before -= 1
    return steps
