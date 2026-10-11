from __future__ import annotations

import unittest

from chronosqueue.errors import Conflict, StaleLease
from tests.helpers import service_at


class CoreTests(unittest.TestCase):
    def test_durable_acceptance_survives_restart(self) -> None:
        with service_at() as (s, clock, path):
            created = s.submit(payload={"x": 1}, queue="q")
            restarted = type(s)(str(path), clock=clock)
            self.assertEqual(restarted.get_job(created["id"])["payload"], {"x": 1})

    def test_scheduled_job_not_claimable_early_and_exact_boundary_is_ready(self) -> None:
        with service_at(1_000_000) as (s, clock, _):
            j = s.submit(payload={}, scheduled_at_us=2_000_000)
            self.assertIsNone(s.claim(worker_id="w"))
            clock.set_us(1_999_999)
            self.assertIsNone(s.claim(worker_id="w"))
            clock.set_us(2_000_000)
            self.assertEqual(s.claim(worker_id="w")["id"], j["id"])

    def test_completion_is_idempotent_and_stale_generation_fails(self) -> None:
        with service_at() as (s, _, _):
            j = s.submit(payload={})
            lease = s.claim(worker_id="w")
            args = dict(job_id=j["id"], worker_id="w", lease_token=lease["lease"]["token"], lease_generation=lease["lease"]["generation"])
            one = s.complete(**args, result={"ok": True})
            two = s.complete(**args, result={"ok": True})
            self.assertEqual(one["state"], "succeeded")
            self.assertEqual(two["state"], "succeeded")
            with self.assertRaises(StaleLease):
                s.fail(**args, error={"late": True})

    def test_retry_fixed_and_max_attempts(self) -> None:
        with service_at(10_000_000) as (s, clock, _):
            j = s.submit(payload={}, max_attempts=2, retry={"kind": "fixed", "delay_ms": 50, "max_delay_ms": 50})
            l1 = s.claim(worker_id="a")
            r1 = s.fail(job_id=j["id"], worker_id="a", lease_token=l1["lease"]["token"], lease_generation=l1["lease"]["generation"], error={"n":1})
            self.assertEqual(r1["state"], "scheduled")
            self.assertEqual(r1["scheduled_at_us"], 10_050_000)
            clock.set_us(10_049_999); self.assertIsNone(s.claim(worker_id="b"))
            clock.set_us(10_050_000); l2 = s.claim(worker_id="b")
            r2 = s.fail(job_id=j["id"], worker_id="b", lease_token=l2["lease"]["token"], lease_generation=l2["lease"]["generation"], error={"n":2})
            self.assertEqual(r2["state"], "dead-lettered")
            self.assertIsNone(s.claim(worker_id="c"))

    def test_exponential_backoff(self) -> None:
        with service_at(0) as (s, clock, _):
            j = s.submit(payload={}, max_attempts=4, retry={"kind":"exponential","delay_ms":10,"max_delay_ms":25})
            expected = [10_000, 20_000, 25_000]
            now = 0
            for i, delta in enumerate(expected, 1):
                lease=s.claim(worker_id=f"w{i}")
                out=s.fail(job_id=j["id"], worker_id=f"w{i}", lease_token=lease["lease"]["token"], lease_generation=lease["lease"]["generation"], error={})
                self.assertEqual(out["scheduled_at_us"], now + delta)
                now += delta; clock.set_us(now)

    def test_terminal_state_requires_explicit_requeue(self) -> None:
        with service_at() as (s, _, _):
            j=s.submit(payload={}); l=s.claim(worker_id="w")
            done=s.complete(job_id=j["id"],worker_id="w",lease_token=l["lease"]["token"],lease_generation=l["lease"]["generation"],result={})
            self.assertEqual(done["state"],"succeeded")
            with self.assertRaises(Conflict): s.cancel(j["id"])
            with self.assertRaises(Conflict): s.requeue(j["id"])

    def test_cancelled_can_be_explicitly_requeued(self) -> None:
        with service_at() as (s, _, _):
            j=s.submit(payload={}); self.assertEqual(s.cancel(j["id"])["state"],"cancelled")
            self.assertEqual(s.requeue(j["id"])["state"],"ready")

    def test_ordering_priority_then_schedule_then_sequence(self) -> None:
        with service_at(1000) as (s, _, _):
            a=s.submit(payload={"n":"a"},priority=1)
            b=s.submit(payload={"n":"b"},priority=2)
            c=s.submit(payload={"n":"c"},priority=2)
            self.assertEqual(s.claim(worker_id="w1")["id"], b["id"])
            self.assertEqual(s.claim(worker_id="w2")["id"], c["id"])
            self.assertEqual(s.claim(worker_id="w3")["id"], a["id"])

    def test_immediate_idempotency_replay_after_time_advance(self) -> None:
        with service_at(1_000_000) as (s, clock, _):
            first=s.submit(payload={"x":1},dedupe_key="network-retry")
            clock.advance_us(9_000_000)
            second=s.submit(payload={"x":1},dedupe_key="network-retry")
            self.assertEqual(first["id"],second["id"]); self.assertTrue(second["deduplicated"])

    def test_client_job_id_can_safely_replay_same_submission(self) -> None:
        with service_at() as (s, _, _):
            jid="00000000-0000-0000-0000-000000000111"
            first=s.submit(payload={"x":1},job_id=jid)
            second=s.submit(payload={"x":1},job_id=jid)
            self.assertEqual(first["id"],second["id"]); self.assertTrue(second["deduplicated"])
