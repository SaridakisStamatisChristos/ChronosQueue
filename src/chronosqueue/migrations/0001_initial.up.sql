CREATE TABLE jobs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    queue_name TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    scheduled_at_us INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('scheduled','ready','leased','succeeded','failed','dead-lettered','cancelled')),
    max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0 AND attempt_count <= max_attempts),
    lease_owner TEXT,
    lease_token TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    lease_expires_at_us INTEGER,
    retry_kind TEXT NOT NULL CHECK (retry_kind IN ('fixed','exponential')),
    retry_delay_ms INTEGER NOT NULL CHECK (retry_delay_ms >= 0),
    retry_max_delay_ms INTEGER NOT NULL CHECK (retry_max_delay_ms >= retry_delay_ms),
    priority INTEGER NOT NULL DEFAULT 0,
    tags_json TEXT NOT NULL DEFAULT '[]',
    dedupe_key TEXT,
    request_fingerprint TEXT NOT NULL,
    result_json TEXT,
    error_json TEXT,
    terminal_at_us INTEGER,
    updated_at_us INTEGER NOT NULL,
    CHECK ((state = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_us IS NOT NULL)
        OR (state <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at_us IS NULL))
);

CREATE UNIQUE INDEX uq_jobs_queue_dedupe ON jobs(queue_name, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX ix_jobs_claim ON jobs(queue_name, state, priority DESC, scheduled_at_us ASC, seq ASC);
CREATE INDEX ix_jobs_state_seq ON jobs(state, seq DESC);
CREATE INDEX ix_jobs_terminal ON jobs(terminal_at_us) WHERE terminal_at_us IS NOT NULL;

CREATE TABLE job_tags (
    job_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY(job_id, tag),
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
CREATE INDEX ix_job_tags_tag ON job_tags(tag, job_id);

CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES ('health_sentinel', 'ok');
