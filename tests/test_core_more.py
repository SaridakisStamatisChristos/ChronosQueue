from __future__ import annotations

import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from chronosqueue.clock import FakeClock
from chronosqueue.errors import InvalidTransition, StaleLease, ValidationError
from chronosqueue.migrations import CURRENT_VERSION, version
from chronosqueue.store import Store


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "q.db")
        self.clock = FakeClock(1_700_000_000_000_000)
        self.store = Store(self.db, clock=self.clock)
        self.store.migrate()

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, **kw):
        base = dict(queue="q", payload={"x": 1}, max_attempts=3, retry_kind="fixed", retry_delay_us=10)
        base.update(kw)
        return self.store.submit(**base)[0]

    def test_exponential_backoff_boundaries(self):
        self.submit(max_attempts=4, retry_kind="exponential", retry_delay_us=10)
        c1 = self.store.claim(worker_id="a", queues=["q"], lease_us=100)
        self.store.fail(job_id=c1.job["id"], worker_id="a", lease_token=c1.lease_token)
        self.clock.advance_us(10); c2 = self.store.claim(worker_id="b", queues=["q"], lease_us=100)
        self.store.fail(job_id=c2.job["id"], worker_id="b", lease_token=c2.lease_token)
        self.clock.advance_us(19); self.assertIsNone(self.store.claim(worker_id="c", queues=["q"], lease_us=100))
        self.clock.advance_us(1); self.assertIsNotNone(self.store.claim(worker_id="c", queues=["q"], lease_us=100))

    def test_zero_and_overlong_lease_rejected(self):
        self.submit()
        with self.assertRaises(ValidationError): self.store.claim(worker_id="w", queues=["q"], lease_us=0)
        with self.assertRaises(ValidationError): self.store.claim(worker_id="w", queues=["q"], lease_us=24*60*60*1_000_000 + 1)

    def test_oversized_payload_and_invalid_policy_rejected(self):
        tiny = Store(self.db, clock=self.clock, payload_limit=8)
        with self.assertRaises(ValidationError): tiny.submit(queue="q", payload={"long":"payload"})
        with self.assertRaises(ValidationError): self.store.submit(queue="q", payload={}, retry_kind="bogus")
        with self.assertRaises(ValidationError): self.store.submit(queue="q", payload={}, priority=2**31)

    def test_corrupt_persisted_invariant_fails_readiness(self):
        import sqlite3
        j = self.submit()
        conn = sqlite3.connect(self.db)
        conn.execute("PRAGMA ignore_check_constraints=ON")
        conn.execute("UPDATE jobs SET state='leased' WHERE id=?", (j["id"],))
        conn.commit(); conn.close()
        self.assertFalse(self.store.ready())

    def test_unavailable_storage_fails_readiness(self):
        bad = Store(self.tmp.name)  # directory path cannot be opened as SQLite database
        self.assertFalse(bad.ready())


    def test_restart_with_active_lease_recovers_after_expiry(self):
        self.submit(max_attempts=2)
        c = self.store.claim(worker_id="w", queues=["q"], lease_us=10)
        restarted = Store(self.db, clock=self.clock)
        self.assertEqual("leased", restarted.get(c.job["id"])["state"])
        self.clock.advance_us(10)
        c2 = restarted.claim(worker_id="w2", queues=["q"], lease_us=10)
        self.assertIsNotNone(c2)
        self.assertEqual(2, c2.job["attempt_count"])

    def test_batch_requeue_all_or_nothing_and_purge(self):
        a=self.submit(); b=self.submit()
        self.store.cancel(a["id"]); self.store.cancel(b["id"])
        items=self.store.requeue_many([a["id"],b["id"]])
        self.assertEqual({"ready"},{x["state"] for x in items})
        ca=self.store.claim(worker_id="a",queues=["q"],lease_us=100)
        cb=self.store.claim(worker_id="b",queues=["q"],lease_us=100)
        self.store.complete(job_id=ca.job["id"],worker_id="a",lease_token=ca.lease_token)
        self.store.complete(job_id=cb.job["id"],worker_id="b",lease_token=cb.lease_token)
        self.clock.advance_us(1)
        self.assertEqual(2,self.store.purge_terminal(older_than_us=self.clock.now_us()))



    def test_non_array_tags_and_queues_rejected(self):
        with self.assertRaises(ValidationError): self.store.submit(queue="q",payload={},tags="oops")
        self.submit()
        with self.assertRaises(ValidationError): self.store.claim(worker_id="w",queues="q",lease_us=100)

    def test_schema_rejects_partial_lease_metadata(self):
        import sqlite3
        j=self.submit()
        conn=sqlite3.connect(self.db)
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("UPDATE jobs SET lease_owner='orphan' WHERE id=?",(j["id"],)); conn.commit()
        conn.rollback(); conn.close()

    def test_non_finite_payload_rejected(self):
        with self.assertRaises(ValidationError): self.store.submit(queue="q",payload={"x":float("nan")})


    def test_true_expiry_completion_vs_reassignment_race(self):
        self.submit(max_attempts=3)
        old=self.store.claim(worker_id="old",queues=["q"],lease_us=10)
        self.clock.advance_us(10)
        barrier=threading.Barrier(2); out={}
        def old_complete():
            barrier.wait()
            try: self.store.complete(job_id=old.job["id"],worker_id="old",lease_token=old.lease_token); out["old"]="completed"
            except Exception as exc: out["old"]=type(exc).__name__
        def new_claim():
            barrier.wait(); out["new"]=self.store.claim(worker_id="new",queues=["q"],lease_us=100)
        with ThreadPoolExecutor(max_workers=2) as ex: list(ex.map(lambda fn: fn(),[old_complete,new_claim]))
        self.assertEqual("StaleLease",out["old"]); self.assertIsNotNone(out["new"])
        self.assertEqual("new",self.store.get(old.job["id"])["lease_owner"])

    def test_true_expiry_heartbeat_vs_reassignment_race(self):
        self.submit(max_attempts=3)
        old=self.store.claim(worker_id="old",queues=["q"],lease_us=10)
        self.clock.advance_us(10)
        barrier=threading.Barrier(2); out={}
        def old_heartbeat():
            barrier.wait()
            try: self.store.heartbeat(job_id=old.job["id"],worker_id="old",lease_token=old.lease_token,lease_us=100); out["old"]="renewed"
            except Exception as exc: out["old"]=type(exc).__name__
        def new_claim():
            barrier.wait(); out["new"]=self.store.claim(worker_id="new",queues=["q"],lease_us=100)
        with ThreadPoolExecutor(max_workers=2) as ex: list(ex.map(lambda fn: fn(),[old_heartbeat,new_claim]))
        self.assertEqual("StaleLease",out["old"]); self.assertIsNotNone(out["new"])

    def test_pagination_during_concurrent_inserts_and_transitions(self):
        original=[self.submit()["id"] for _ in range(80)]
        first=self.store.list_jobs(limit=7)
        seen=[x["id"] for x in first["items"]]; cursor=first["next_cursor"]
        stop=threading.Event()
        def churn():
            for _ in range(30): self.submit()
            while not stop.is_set():
                c=self.store.claim(worker_id="churn",queues=["q"],lease_us=1_000_000)
                if c is None: break
                self.store.complete(job_id=c.job["id"],worker_id="churn",lease_token=c.lease_token)
        t=threading.Thread(target=churn); t.start()
        try:
            while cursor:
                page=self.store.list_jobs(limit=7,cursor=cursor)
                seen.extend(x["id"] for x in page["items"]); cursor=page["next_cursor"]
        finally:
            stop.set(); t.join(timeout=5)
        self.assertEqual(set(original),set(seen)); self.assertEqual(len(original),len(seen))

    def test_failure_receipt_is_idempotent(self):
        self.submit(max_attempts=3,retry_delay_us=100)
        c=self.store.claim(worker_id="w",queues=["q"],lease_us=1000)
        first,changed1=self.store.fail(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token,error={"x":1})
        second,changed2=self.store.fail(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token,error={"x":1})
        self.assertTrue(changed1); self.assertFalse(changed2); self.assertEqual(first["id"],second["id"])

    def test_succeeded_job_cannot_be_requeued(self):
        self.submit(); c=self.store.claim(worker_id="w",queues=["q"],lease_us=100)
        self.store.complete(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token)
        with self.assertRaises(InvalidTransition): self.store.requeue(c.job["id"])

    def test_legacy_v1_row_survives_upgrade(self):
        j=self.submit()
        self.store.migrate(1,allow_destructive=True)
        self.store.migrate(CURRENT_VERSION)
        self.assertEqual(j["id"],self.store.get(j["id"])["id"])
        with self.assertRaises(ValueError): self.store.migrate(1)

if __name__ == "__main__": unittest.main()
