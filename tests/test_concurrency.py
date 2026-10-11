from __future__ import annotations

import concurrent.futures
import threading
import unittest

from chronosqueue.errors import Conflict, StaleLease
from tests.helpers import service_at


class ConcurrencyTests(unittest.TestCase):
    def test_32_workers_race_for_one_job_exactly_one_lease(self) -> None:
        with service_at() as (s, _, _):
            j=s.submit(payload={})
            barrier=threading.Barrier(32)
            def run(i: int):
                barrier.wait(); return s.claim(worker_id=f"w{i}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:
                results=list(ex.map(run, range(32)))
            won=[r for r in results if r is not None]
            self.assertEqual(len(won),1)
            self.assertEqual(won[0]["id"],j["id"])
            self.assertEqual(len(s.leases()),1)

    def test_many_workers_many_jobs_no_duplicate_claim(self) -> None:
        with service_at() as (s, _, _):
            ids={s.submit(payload={"n":i})["id"] for i in range(120)}
            claimed=[]; lock=threading.Lock()
            def worker(i: int):
                local=[]
                while True:
                    j=s.claim(worker_id=f"w{i}")
                    if j is None: break
                    local.append(j["id"])
                with lock: claimed.extend(local)
            with concurrent.futures.ThreadPoolExecutor(max_workers=24) as ex:
                list(ex.map(worker, range(24)))
            self.assertEqual(len(claimed),120)
            self.assertEqual(set(claimed),ids)

    def test_expired_lease_reassigned_and_old_completion_rejected(self) -> None:
        with service_at(1_000_000) as (s, clock, _):
            j=s.submit(payload={},max_attempts=3)
            old=s.claim(worker_id="old",lease_ms=100)
            clock.advance_us(100_000)
            new=s.claim(worker_id="new",lease_ms=100)
            self.assertEqual(new["id"],j["id"])
            self.assertEqual(new["attempt_count"],2)
            with self.assertRaises(StaleLease):
                s.complete(job_id=j["id"],worker_id="old",lease_token=old["lease"]["token"],lease_generation=old["lease"]["generation"],result={})

    def test_heartbeat_at_expiry_is_stale(self) -> None:
        with service_at(0) as (s, clock, _):
            j=s.submit(payload={}); l=s.claim(worker_id="w",lease_ms=10)
            clock.set_us(10_000)
            with self.assertRaises(StaleLease):
                s.heartbeat(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],lease_ms=10)
            self.assertIsNotNone(s.claim(worker_id="w2"))

    def test_concurrent_idempotent_submission(self) -> None:
        with service_at() as (s, _, _):
            barrier=threading.Barrier(20)
            def submit(_i: int):
                barrier.wait(); return s.submit(payload={"same":1},queue="q",dedupe_key="same-key")
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                rows=list(ex.map(submit, range(20)))
            self.assertEqual(len({r["id"] for r in rows}),1)
            self.assertEqual(sum(not r["deduplicated"] for r in rows),1)

    def test_same_idempotency_key_different_semantics_conflicts(self) -> None:
        with service_at() as (s, _, _):
            s.submit(payload={"a":1},dedupe_key="k")
            with self.assertRaises(Conflict): s.submit(payload={"a":2},dedupe_key="k")

    def test_cancel_races_completion_one_terminal_winner(self) -> None:
        with service_at() as (s, _, _):
            j=s.submit(payload={}); l=s.claim(worker_id="w")
            barrier=threading.Barrier(2); outcomes=[]; lock=threading.Lock()
            def cancel():
                barrier.wait()
                try: out=s.cancel(j["id"])["state"]
                except Exception as e: out=type(e).__name__
                with lock: outcomes.append(out)
            def complete():
                barrier.wait()
                try: out=s.complete(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],result={})["state"]
                except Exception as e: out=type(e).__name__
                with lock: outcomes.append(out)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                ex.submit(cancel); ex.submit(complete)
            final=s.get_job(j["id"])["state"]
            self.assertIn(final,{"cancelled","succeeded"})
            self.assertEqual(sum(x in {"cancelled","succeeded"} for x in outcomes),1)

    def test_concurrent_expiry_scanners_record_once(self) -> None:
        with service_at(0) as (s, clock, _):
            jobs=[]
            for i in range(30):
                j=s.submit(payload={}); jobs.append(j["id"]); s.claim(worker_id=f"w{i}",lease_ms=10)
            clock.set_us(10_000)
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
                list(ex.map(lambda _: s.maintenance(), range(12)))
            events=sum(sum(1 for e in s.audit_events(j) if e["event_type"]=="lease_expired") for j in jobs)
            self.assertEqual(events,30)
