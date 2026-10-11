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

    def test_concurrent_cancel_vs_claim_final_cancelled(self):
        j = self.submit()
        barrier = threading.Barrier(2)
        out = {}
        def do_claim():
            barrier.wait()
            out["claim"] = self.store.claim(worker_id="w", queues=["q"], lease_us=1000)
        def do_cancel():
            barrier.wait()
            try:
                out["cancel"] = self.store.cancel(j["id"])
            except Exception as exc:
                out["cancel_exc"] = exc
        with ThreadPoolExecutor(max_workers=2) as ex:
            list(ex.map(lambda fn: fn(), [do_claim, do_cancel]))
        self.assertEqual("cancelled", self.store.get(j["id"])["state"])
        if out.get("claim") is not None:
            with self.assertRaises(StaleLease):
                self.store.complete(job_id=j["id"], worker_id="w", lease_token=out["claim"].lease_token)

    def test_concurrent_cancel_vs_complete_has_single_winner(self):
        self.submit(); c = self.store.claim(worker_id="w", queues=["q"], lease_us=1000)
        barrier = threading.Barrier(2); outcomes = []
        def complete():
            barrier.wait()
            try: outcomes.append(("complete", self.store.complete(job_id=c.job["id"], worker_id="w", lease_token=c.lease_token)[0]["state"]))
            except Exception as exc: outcomes.append(("complete_exc", type(exc).__name__))
        def cancel():
            barrier.wait()
            try: outcomes.append(("cancel", self.store.cancel(c.job["id"])[0]["state"]))
            except Exception as exc: outcomes.append(("cancel_exc", type(exc).__name__))
        with ThreadPoolExecutor(max_workers=2) as ex: list(ex.map(lambda fn: fn(), [complete, cancel]))
        final = self.store.get(c.job["id"])["state"]
        self.assertIn(final, {"succeeded", "cancelled"})
        self.assertNotEqual({"complete", "cancel"}, {x[0] for x in outcomes})

    def test_concurrent_retry_eligibility_exactly_one_reclaim(self):
        self.submit(max_attempts=3, retry_delay_us=10)
        c = self.store.claim(worker_id="seed", queues=["q"], lease_us=100)
        self.store.fail(job_id=c.job["id"], worker_id="seed", lease_token=c.lease_token)
        self.clock.advance_us(10)
        barrier = threading.Barrier(20)
        def run(i):
            barrier.wait(); return self.store.claim(worker_id=f"r{i}", queues=["q"], lease_us=100)
        with ThreadPoolExecutor(max_workers=20) as ex: claims = list(ex.map(run, range(20)))
        self.assertEqual(1, sum(x is not None for x in claims))

if __name__ == "__main__": unittest.main()
