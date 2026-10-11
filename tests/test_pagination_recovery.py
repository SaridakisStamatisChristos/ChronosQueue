from __future__ import annotations

import unittest

from chronosqueue.errors import ChronosError
from tests.helpers import service_at


class PaginationRecoveryTests(unittest.TestCase):
    def test_keyset_pagination_excludes_concurrent_inserts_and_never_repeats(self) -> None:
        with service_at() as (s, _, _):
            initial=[s.submit(payload={"n":i})["id"] for i in range(25)]
            first=s.list_jobs(limit=7)
            seen=[x["id"] for x in first["items"]]
            for i in range(10): s.submit(payload={"new":i})
            cursor=first["next_cursor"]
            while cursor:
                page=s.list_jobs(limit=7,cursor=cursor)
                seen.extend(x["id"] for x in page["items"]); cursor=page["next_cursor"]
            self.assertEqual(len(seen),len(set(seen)))
            self.assertEqual(set(seen),set(initial))

    def test_cursor_filter_mismatch_and_malformed_rejected(self) -> None:
        with service_at() as (s, _, _):
            for _ in range(3): s.submit(payload={},queue="a")
            page=s.list_jobs(queue="a",limit=1)
            with self.assertRaises(ChronosError): s.list_jobs(queue="b",limit=1,cursor=page["next_cursor"])
            with self.assertRaises(ChronosError): s.list_jobs(cursor="not-base64***")

    def test_restart_with_active_lease_recovers_from_persisted_state(self) -> None:
        with service_at(0) as (s, clock, path):
            j=s.submit(payload={},max_attempts=3); old=s.claim(worker_id="old",lease_ms=10)
            clock.set_us(10_000)
            restarted=type(s)(str(path),clock=clock)
            new=restarted.claim(worker_id="new")
            self.assertEqual(new["id"],j["id"]); self.assertEqual(new["attempt_count"],2)
            self.assertNotEqual(new["lease"]["token"],old["lease"]["token"])

    def test_final_attempt_lease_expiry_dead_letters(self) -> None:
        with service_at(0) as (s, clock, _):
            j=s.submit(payload={},max_attempts=1); s.claim(worker_id="w",lease_ms=10)
            clock.set_us(10_000); s.maintenance()
            self.assertEqual(s.get_job(j["id"])["state"],"dead-lettered")
