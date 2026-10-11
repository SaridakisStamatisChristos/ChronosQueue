CREATE TABLE job_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    at_us INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX ix_job_events_job ON job_events(job_id, event_id);

CREATE TABLE completion_receipts (
    lease_token TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('complete','fail')),
    response_json TEXT NOT NULL,
    created_at_us INTEGER NOT NULL
);

CREATE TABLE metrics_counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0 CHECK (value >= 0)
);
INSERT INTO metrics_counters(name, value) VALUES
 ('jobs_submitted',0),('jobs_leased',0),('jobs_succeeded',0),('jobs_failed',0),
 ('jobs_retried',0),('jobs_dead_lettered',0),('jobs_cancelled',0),
 ('lease_expirations',0),('stale_completion_attempts',0),('claim_latency_us_total',0),('claim_requests',0);
