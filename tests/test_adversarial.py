from __future__ import annotations

import concurrent.futures
import unittest

from chronosqueue.errors import Conflict, NotFound, StaleLease
from tests.helpers import service_at


class AdversarialTests(unittest.TestCase):
    def test_cancellation_after_claim_invalidates_token(self) -> None:
        with service_at() as (s,_,_):
            j=s.submit(payload={}); l=s.claim(worker_id="w"); s.cancel(j["id"])
            with self.assertRaises(StaleLease): s.complete(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],result={})

    def test_heartbeat_races_reassignment_only_current_generation_survives(self) -> None:
        with service_at(0) as (s,clock,_):
            j=s.submit(payload={},max_attempts=3); old=s.claim(worker_id="old",lease_ms=10); clock.set_us(10_000)
            barrier=__import__('threading').Barrier(2)
            def hb():
                barrier.wait()
                try: s.heartbeat(job_id=j["id"],worker_id="old",lease_token=old["lease"]["token"],lease_generation=old["lease"]["generation"],lease_ms=100); return "renewed"
                except StaleLease: return "stale"
            def cl(): barrier.wait(); return s.claim(worker_id="new",lease_ms=100)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                a=ex.submit(hb); b=ex.submit(cl); hr=a.result(); new=b.result()
            self.assertEqual(hr,"stale"); self.assertIsNotNone(new); self.assertEqual(new["lease"]["generation"],old["lease"]["generation"]+1)

    def test_heartbeat_missing_job_returns_not_found_not_name_error(self) -> None:
        with service_at() as (s,_,_):
            import uuid
            with self.assertRaises(NotFound):
                s.heartbeat(job_id=str(uuid.uuid4()), worker_id="w", lease_token=str(uuid.uuid4()), lease_generation=1)

    def test_duplicate_completion_with_changed_payload_is_receipt_idempotent(self) -> None:
        with service_at() as (s,_,_):
            j=s.submit(payload={}); l=s.claim(worker_id="w"); a=dict(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"])
            one=s.complete(**a,result={"first":1}); two=s.complete(**a,result={"second":2})
            self.assertEqual(one["result"],{"first":1}); self.assertEqual(two["result"],{"first":1})

    def test_dead_letter_never_resurrects_on_maintenance(self) -> None:
        with service_at(0) as (s,clock,_):
            j=s.submit(payload={},max_attempts=1); l=s.claim(worker_id="w")
            s.fail(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],error={})
            for t in (1,10_000_000,10**15): clock.set_us(t); s.maintenance(); self.assertEqual(s.get_job(j["id"])["state"],"dead-lettered")

    def test_zero_duration_lease_rejected_and_long_bounded(self) -> None:
        with service_at() as (s,_,_):
            s.submit(payload={})
            with self.assertRaises(Exception): s.claim(worker_id="w",lease_ms=0)
            with self.assertRaises(Exception): s.claim(worker_id="w",lease_ms=s.config.max_lease_ms+1)

    def test_purge_is_terminal_only(self) -> None:
        with service_at(100) as (s,_,_):
            ready=s.submit(payload={},priority=0)
            done=s.submit(payload={},priority=1); l=s.claim(worker_id="w"); s.complete(job_id=done["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],result={})
            out=s.purge(terminal_before_us=101); self.assertEqual(out["purged"],1); self.assertEqual(s.get_job(ready["id"])["state"],"ready")


class RequeueBudgetTests(unittest.TestCase):
    def test_exhausted_attempt_budget_cannot_be_kept_on_requeue(self) -> None:
        with service_at() as (s,_,_):
            j=s.submit(payload={},max_attempts=1); l=s.claim(worker_id="w")
            s.fail(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],error={})
            with self.assertRaises(Conflict): s.requeue(j["id"],reset_attempts=False)
            self.assertEqual(s.requeue(j["id"],reset_attempts=True)["attempt_count"],0)

    def test_worker_identifier_is_path_safe(self) -> None:
        with service_at() as (s,_,_):
            s.submit(payload={})
            with self.assertRaises(Exception): s.claim(worker_id="bad/worker")
