from __future__ import annotations
import os, tempfile, unittest
from chronosqueue.clock import FakeClock
from chronosqueue.store import Store

class Boom(RuntimeError): pass

def explode_at(target):
    def hook(point):
        if point == target: raise Boom(point)
    return hook

class CrashAtomicityCase(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.db=os.path.join(self.tmp.name,"q.db"); self.clock=FakeClock(1_700_000_000_000_000)
        self.base=Store(self.db,clock=self.clock); self.base.migrate()
    def tearDown(self): self.tmp.cleanup()
    def hooked(self,point): return Store(self.db,clock=self.clock,fault_hook=explode_at(point))
    def submit(self): return self.base.submit(queue="q",payload={},max_attempts=3,retry_delay_us=10)[0]

    def test_submission_interruption_rolls_back(self):
        with self.assertRaises(Boom): self.hooked("submit.before_commit").submit(queue="q",payload={})
        self.assertEqual([],self.base.list_jobs()["items"])

    def test_claim_interruption_rolls_back_attempt_and_lease(self):
        j=self.submit()
        with self.assertRaises(Boom): self.hooked("claim.before_commit").claim(worker_id="w",queues=["q"],lease_us=100)
        got=self.base.get(j["id"]); self.assertEqual("ready",got["state"]); self.assertEqual(0,got["attempt_count"])

    def test_heartbeat_interruption_rolls_back_expiry(self):
        self.submit(); c=self.base.claim(worker_id="w",queues=["q"],lease_us=100); before=self.base.get(c.job["id"])["lease_expires_at"]
        with self.assertRaises(Boom): self.hooked("heartbeat.before_commit").heartbeat(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token,lease_us=1000)
        self.assertEqual(before,self.base.get(c.job["id"])["lease_expires_at"])

    def test_completion_interruption_rolls_back(self):
        self.submit(); c=self.base.claim(worker_id="w",queues=["q"],lease_us=100)
        with self.assertRaises(Boom): self.hooked("complete.before_commit").complete(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token)
        self.assertEqual("leased",self.base.get(c.job["id"])["state"])
        self.base.complete(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token)

    def test_retry_scheduling_interruption_rolls_back(self):
        self.submit(); c=self.base.claim(worker_id="w",queues=["q"],lease_us=100)
        with self.assertRaises(Boom): self.hooked("fail.before_commit").fail(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token,error={"x":1})
        self.assertEqual("leased",self.base.get(c.job["id"])["state"])

    def test_cancellation_interruption_rolls_back(self):
        j=self.submit()
        with self.assertRaises(Boom): self.hooked("cancel.before_commit").cancel(j["id"])
        self.assertEqual("ready",self.base.get(j["id"])["state"])

    def test_requeue_interruption_rolls_back(self):
        j=self.submit(); self.base.cancel(j["id"])
        with self.assertRaises(Boom): self.hooked("requeue.before_commit").requeue(j["id"])
        self.assertEqual("cancelled",self.base.get(j["id"])["state"])

    def test_purge_interruption_rolls_back_delete(self):
        self.submit(); c=self.base.claim(worker_id="w",queues=["q"],lease_us=100); self.base.complete(job_id=c.job["id"],worker_id="w",lease_token=c.lease_token)
        self.clock.advance_us(1)
        with self.assertRaises(Boom): self.hooked("purge.before_commit").purge_terminal(older_than_us=self.clock.now_us())
        self.assertEqual("succeeded",self.base.get(c.job["id"])["state"])

if __name__=='__main__': unittest.main()
