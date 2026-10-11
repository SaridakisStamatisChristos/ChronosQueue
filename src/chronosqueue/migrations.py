from __future__ import annotations

import sqlite3

CURRENT_VERSION = 2

MIGRATIONS = {
    1: r'''
PRAGMA foreign_keys=ON;
CREATE TABLE jobs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    queue TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    scheduled_at_us INTEGER NOT NULL,
    available_at_us INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('scheduled','ready','leased','succeeded','failed','dead-lettered','cancelled')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
    retry_kind TEXT NOT NULL CHECK(retry_kind IN ('fixed','exponential')),
    retry_delay_us INTEGER NOT NULL CHECK(retry_delay_us >= 0),
    priority INTEGER NOT NULL DEFAULT 0,
    tags_json TEXT NOT NULL DEFAULT '[]',
    dedupe_key TEXT,
    lease_owner TEXT,
    lease_token TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0 CHECK(lease_generation >= 0),
    lease_expires_at_us INTEGER,
    result_json TEXT,
    error_json TEXT,
    terminal_at_us INTEGER,
    updated_at_us INTEGER NOT NULL,
    CHECK((state='leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_us IS NOT NULL) OR
          (state!='leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at_us IS NULL)),
    CHECK(attempt_count <= max_attempts),
    CHECK(state!='leased' OR attempt_count >= 1),
    CHECK(state!='failed' OR attempt_count < max_attempts),
    CHECK(state!='dead-lettered' OR attempt_count >= max_attempts),
    CHECK((state IN ('succeeded','dead-lettered','cancelled')) = (terminal_at_us IS NOT NULL))
);
CREATE UNIQUE INDEX jobs_dedupe_unique ON jobs(queue, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX jobs_claim_idx ON jobs(state, queue, priority DESC, available_at_us, seq);
CREATE INDEX jobs_lease_idx ON jobs(state, lease_expires_at_us);
CREATE INDEX jobs_terminal_idx ON jobs(state, terminal_at_us);
CREATE TABLE lease_receipts (
    lease_token TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('succeeded','failed','dead-lettered')),
    committed_at_us INTEGER NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
''',
    2: r'''
CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT,
    event TEXT NOT NULL,
    at_us INTEGER NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE SET NULL
);
CREATE INDEX audit_events_job_idx ON audit_events(job_id, id);
''',
}

DOWNS = {
    2: "DROP TABLE IF EXISTS audit_events;",
    1: "DROP TABLE IF EXISTS lease_receipts; DROP TABLE IF EXISTS jobs;",
}


def configure(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=5000")


def version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def migrate(conn: sqlite3.Connection, target: int = CURRENT_VERSION, *, allow_destructive: bool = False) -> None:
    if target < 0 or target > CURRENT_VERSION:
        raise ValueError(f"target migration must be between 0 and {CURRENT_VERSION}")
    current = version(conn)
    if target < current and not allow_destructive:
        raise ValueError("downgrade requires allow_destructive=True")
    while current < target:
        nxt = current + 1
        with conn:
            conn.executescript(MIGRATIONS[nxt])
            conn.execute(f"PRAGMA user_version={nxt}")
        current = nxt
    while current > target:
        with conn:
            conn.executescript(DOWNS[current])
            conn.execute(f"PRAGMA user_version={current - 1}")
        current -= 1
