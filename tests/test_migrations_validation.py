from __future__ import annotations

import math
import sqlite3
import tempfile
import unittest
from pathlib import Path

from chronosqueue.errors import ChronosError, PayloadTooLarge
from chronosqueue.migration import migrate, current_version
from chronosqueue.service import QueueConfig, QueueService


class MigrationValidationTests(unittest.TestCase):
    def test_full_reversible_migration_sequence_and_legacy_row(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"q.db"; c=sqlite3.connect(p,isolation_level=None); c.row_factory=sqlite3.Row
            migrate(c,1); self.assertEqual(current_version(c),1)
            c.execute("""INSERT INTO jobs(id,queue_name,created_at_us,scheduled_at_us,payload_json,state,max_attempts,retry_kind,retry_delay_ms,retry_max_delay_ms,priority,tags_json,request_fingerprint,updated_at_us)
                       VALUES('00000000-0000-0000-0000-000000000001','legacy',1,1,'{}','ready',3,'fixed',1000,1000,0,'[]','fp',1)""")
            migrate(c,2); self.assertEqual(current_version(c),2)
            self.assertEqual(c.execute("SELECT queue_name FROM jobs").fetchone()[0],"legacy")
            migrate(c,1); self.assertEqual(current_version(c),1)
            migrate(c,0); self.assertEqual(current_version(c),0)
            migrate(c,2); self.assertEqual(current_version(c),2); c.close()

    def test_invalid_numeric_and_nonfinite_json_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            s=QueueService(str(Path(d)/"q.db"))
            with self.assertRaises(ChronosError): s.submit(payload={"x": math.nan})
            with self.assertRaises(ChronosError): s.submit(payload={},priority=10_000_000)
            with self.assertRaises(ChronosError): s.submit(payload={},max_attempts=0)
            with self.assertRaises(ChronosError): s.claim(worker_id="w",lease_ms=0)
            j=s.submit(payload={}); l=s.claim(worker_id="w")
            with self.assertRaises(ChronosError):
                s.complete(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=True,result={})

    def test_payload_bound(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            s=QueueService(str(Path(d)/"q.db"),config=QueueConfig(max_payload_bytes=32))
            with self.assertRaises(PayloadTooLarge): s.submit(payload={"x":"z"*100})

    def test_sqlite_check_prevents_corrupt_executable_state(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"q.db"; s=QueueService(str(p)); c=sqlite3.connect(p)
            with self.assertRaises(sqlite3.IntegrityError):
                c.execute("UPDATE jobs SET state='evil' WHERE 1=0") if False else c.execute("INSERT INTO jobs(id,queue_name,created_at_us,scheduled_at_us,payload_json,state,max_attempts,retry_kind,retry_delay_ms,retry_max_delay_ms,priority,tags_json,request_fingerprint,updated_at_us) VALUES('x','q',0,0,'{}','evil',1,'fixed',0,0,0,'[]','x',0)")
            c.close()
