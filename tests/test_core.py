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

    def test_no_premature_execution_exact_boundary(self):
        due = self.clock.now_us() + 100
        self.submit(scheduled_at_us=due)
        self.assertIsNone(self.store.claim(worker_id="w", queues=["q"], lease_us=1000))
        self.clock.advance_us(99)
        self.assertIsNone(self.store.claim(worker_id="w", queues=["q"], lease_us=1000))
        self.clock.advance_us(1)
        self.assertIsNotNone(self.store.claim(worker_id="w", queues=["q"], lease_us=1000))

    def test_20_workers_race_one_job_exactly_one_lease(self):
        self.submit()
        barrier = threading.Barrier(20)
        def run(i):
            barrier.wait()
            return self.store.claim(worker_id=f"w{i}", queues=["q"], lease_us=1000)
        with ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(run, range(20)))
        claims = [r for r in results if r is not None]
        self.assertEqual(1, len(claims))
        self.assertEqual("leased", self.store.get(claims[0].job["id"])["state"])

    def test_many_workers_many_jobs_no_duplicate_active_lease(self):
        for _ in range(60): self.submit()
        seen = set(); lock = threading.Lock()
        def worker(i):
            local = []
            while True:
                c = self.store.claim(worker_id=f"w{i}", queues=["q"], lease_us=1_000_000)
                if c is None: break
                local.append(c.job["id"])
            with lock:
                for jid in local:
                    self.assertNotIn(jid, seen); seen.add(jid)
        with ThreadPoolExecutor(max_workers=24) as ex: list(ex.map(worker, range(24)))
        self.assertEqual(60, len(seen))

    def test_expiry_reassignment_stale_completion_rejected(self):
        self.submit(max_attempts=3)
        c1 = self.store.claim(worker_id="a", queues=["q"], lease_us=10)
        self.clock.advance_us(10)
        c2 = self.store.claim(worker_id="b", queues=["q"], lease_us=10)
        self.assertIsNotNone(c2)
        with self.assertRaises(StaleLease):
            self.store.complete(job_id=c1.job["id"], worker_id="a", lease_token=c1.lease_token)
        self.store.complete(job_id=c2.job["id"], worker_id="b", lease_token=c2.lease_token)

    def test_heartbeat_generation_staleness(self):
        self.submit(); c1 = self.store.claim(worker_id="a", queues=["q"], lease_us=10)
        self.clock.advance_us(10); c2 = self.store.claim(worker_id="b", queues=["q"], lease_us=10)
        with self.assertRaises(StaleLease):
            self.store.heartbeat(job_id=c1.job["id"], worker_id="a", lease_token=c1.lease_token, lease_us=50)
        self.store.heartbeat(job_id=c2.job["id"], worker_id="b", lease_token=c2.lease_token, lease_us=50)

    def test_completion_idempotency(self):
        self.submit(); c = self.store.claim(worker_id="a", queues=["q"], lease_us=100)
        first, changed1 = self.store.complete(job_id=c.job["id"], worker_id="a", lease_token=c.lease_token, result={"ok":1})
        second, changed2 = self.store.complete(job_id=c.job["id"], worker_id="a", lease_token=c.lease_token, result={"ok":1})
        self.assertTrue(changed1); self.assertFalse(changed2); self.assertEqual(first["state"], second["state"])

    def test_retry_fixed_and_max_attempts(self):
        self.submit(max_attempts=2, retry_delay_us=50)
        c1 = self.store.claim(worker_id="a", queues=["q"], lease_us=100)
        j, _ = self.store.fail(job_id=c1.job["id"], worker_id="a", lease_token=c1.lease_token, error={"e":1})
        self.assertEqual("failed", j["state"])
        self.clock.advance_us(49); self.assertIsNone(self.store.claim(worker_id="b", queues=["q"], lease_us=100))
        self.clock.advance_us(1); c2 = self.store.claim(worker_id="b", queues=["q"], lease_us=100)
        j, _ = self.store.fail(job_id=c2.job["id"], worker_id="b", lease_token=c2.lease_token)
        self.assertEqual("dead-lettered", j["state"])
        self.assertIsNone(self.store.claim(worker_id="c", queues=["q"], lease_us=100))

    def test_cancellation_claim_and_completion_safety(self):
        j = self.submit(); cancelled, _ = self.store.cancel(j["id"])
        self.assertEqual("cancelled", cancelled["state"]); self.assertIsNone(self.store.claim(worker_id="w", queues=["q"], lease_us=100))
        j2 = self.submit(); c = self.store.claim(worker_id="w", queues=["q"], lease_us=100); self.store.cancel(j2["id"])
        with self.assertRaises(StaleLease): self.store.complete(job_id=j2["id"], worker_id="w", lease_token=c.lease_token)

    def test_completion_then_cancel_rejected(self):
        self.submit(); c = self.store.claim(worker_id="w", queues=["q"], lease_us=100)
        self.store.complete(job_id=c.job["id"], worker_id="w", lease_token=c.lease_token)
        with self.assertRaises(InvalidTransition): self.store.cancel(c.job["id"])

    def test_idempotent_submission_under_concurrency(self):
        barrier = threading.Barrier(16)
        def run(_):
            barrier.wait(); return self.store.submit(queue="q", payload={"x":1}, dedupe_key="same")[0]["id"]
        with ThreadPoolExecutor(max_workers=16) as ex: ids = list(ex.map(run, range(16)))
        self.assertEqual(1, len(set(ids)))

    def test_ordering_priority_due_sequence(self):
        a = self.submit(priority=1); b = self.submit(priority=3); c = self.submit(priority=3)
        first = self.store.claim(worker_id="w", queues=["q"], lease_us=100)
        second = self.store.claim(worker_id="x", queues=["q"], lease_us=100)
        self.assertEqual(b["id"], first.job["id"]); self.assertEqual(c["id"], second.job["id"])

    def test_restart_preserves_acknowledged_and_retry_state(self):
        j = self.submit(max_attempts=2, retry_delay_us=50)
        other = Store(self.db, clock=self.clock)
        self.assertEqual(j["id"], other.get(j["id"])["id"])
        c = other.claim(worker_id="w", queues=["q"], lease_us=100)
        other.fail(job_id=j["id"], worker_id="w", lease_token=c.lease_token)
        third = Store(self.db, clock=self.clock)
        self.assertEqual("failed", third.get(j["id"])["state"])

    def test_keyset_pagination_freezes_insert_ceiling(self):
        for _ in range(5): self.submit()
        p1 = self.store.list_jobs(limit=2)
        self.submit()
        p2 = self.store.list_jobs(limit=2, cursor=p1["next_cursor"])
        p3 = self.store.list_jobs(limit=2, cursor=p2["next_cursor"])
        ids = [x["id"] for x in p1["items"] + p2["items"] + p3["items"]]
        self.assertEqual(5, len(ids)); self.assertEqual(5, len(set(ids)))

    def test_malformed_cursor(self):
        with self.assertRaises(ValidationError): self.store.list_jobs(cursor="!!!")

    def test_migration_roundtrip(self):
        self.store.migrate(1, allow_destructive=True)
        self.store.migrate(CURRENT_VERSION)
        self.assertTrue(self.store.ready())
        self.store.migrate(0, allow_destructive=True)
        self.store.migrate(CURRENT_VERSION)
        self.assertTrue(self.store.ready())


if __name__ == "__main__": unittest.main()
