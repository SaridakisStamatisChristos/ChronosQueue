from __future__ import annotations
import json, os, tempfile, threading, unittest, urllib.error, urllib.request
from http.server import ThreadingHTTPServer
from chronosqueue.api import Handler
from chronosqueue.clock import FakeClock
from chronosqueue.store import Store

class ApiCase(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.db=os.path.join(self.tmp.name,"q.db")
        self.clock=FakeClock(1_700_000_000_000_000); self.store=Store(self.db,clock=self.clock); self.store.migrate()
        self.server=ThreadingHTTPServer(("127.0.0.1",0),Handler); self.server.store=self.store
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.base=f"http://127.0.0.1:{self.server.server_port}"
    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=2); self.tmp.cleanup()
    def req(self, method, path, body=None):
        data=None if body is None else json.dumps(body).encode()
        r=urllib.request.Request(self.base+path,data=data,method=method,headers={"Content-Type":"application/json"})
        try:
            with urllib.request.urlopen(r,timeout=3) as x:
                raw=x.read(); return x.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as x:
            raw=x.read(); return x.code, json.loads(raw) if raw else None
    def test_submit_claim_complete_smoke(self):
        st,b=self.req("POST","/v1/jobs",{"queue":"q","payload":{"x":1}}); self.assertEqual(201,st); jid=b["job"]["id"]
        st,c=self.req("POST","/v1/worker/claim",{"worker_id":"w","queues":["q"],"lease_ms":1000}); self.assertEqual(200,st)
        st,d=self.req("POST","/v1/worker/w/complete",{"job_id":jid,"lease_token":c["lease_token"],"result":{"ok":True}}); self.assertEqual(200,st); self.assertEqual("succeeded",d["job"]["state"])
    def test_delay_uses_server_clock(self):
        st,b=self.req("POST","/v1/jobs",{"queue":"q","payload":{},"delay_ms":1}); jid=b["job"]["id"]
        st,_=self.req("POST","/v1/worker/claim",{"worker_id":"w","queues":["q"],"lease_ms":100}); self.assertEqual(204,st)
        self.clock.advance_us(1000)
        st,c=self.req("POST","/v1/worker/claim",{"worker_id":"w","queues":["q"],"lease_ms":100}); self.assertEqual(200,st); self.assertEqual(jid,c["job"]["id"])
    def test_malformed_json_and_route(self):
        r=urllib.request.Request(self.base+"/v1/jobs",data=b"{",method="POST",headers={"Content-Type":"application/json"})
        try: urllib.request.urlopen(r,timeout=3); self.fail("expected HTTPError")
        except urllib.error.HTTPError as x: self.assertEqual(422,x.code)
        st,b=self.req("GET","/not-a-route"); self.assertEqual(404,st); self.assertEqual("not_found",b["error"])
    def test_ready_and_metrics(self):
        st,b=self.req("GET","/health/live"); self.assertEqual(200,st)
        st,b=self.req("GET","/health/ready"); self.assertEqual(200,st)
        with urllib.request.urlopen(self.base+"/metrics",timeout=3) as x: self.assertEqual(200,x.status); self.assertIn(b"chronosqueue_",x.read())


    def test_admin_requeue_purge_and_time_filter(self):
        st,a=self.req("POST","/v1/jobs",{"queue":"q","payload":{}}); jid=a["job"]["id"]
        self.req("POST",f"/v1/jobs/{jid}/cancel",{})
        st,r=self.req("POST","/v1/admin/requeue",{"job_ids":[jid]}); self.assertEqual(200,st); self.assertEqual("ready",r["items"][0]["state"])
        st,page=self.req("GET","/v1/jobs?created_after=2020-01-01T00:00:00Z&created_before=2030-01-01T00:00:00Z"); self.assertEqual(200,st); self.assertEqual(1,len(page["items"]))
        self.req("POST",f"/v1/jobs/{jid}/cancel",{})
        self.clock.advance_us(1)
        st,p=self.req("POST","/v1/admin/purge",{"older_than":"2023-11-14T22:13:21Z"}); self.assertEqual(200,st); self.assertEqual(1,p["purged"])


    def test_malformed_numeric_input_is_safe_422(self):
        st,b=self.req("POST","/v1/jobs",{"queue":"q","payload":{},"priority":"not-an-int"})
        self.assertEqual(422,st); self.assertEqual("validation_error",b["error"])
        self.assertNotIn("not-an-int",b["message"])

    def test_metrics_escape_queue_label(self):
        self.req("POST","/v1/jobs",{"queue":"q\\\"x","payload":{}})
        with urllib.request.urlopen(self.base+"/metrics",timeout=3) as x:
            text=x.read().decode()
        self.assertIn('queue="q\\\\\\\"x"',text)


    def test_non_array_queues_tags_and_retry_rejected(self):
        st,b=self.req("POST","/v1/jobs",{"queue":"q","payload":{},"tags":"bad"}); self.assertEqual(422,st)
        st,b=self.req("POST","/v1/jobs",{"queue":"q","payload":{},"retry":"bad"}); self.assertEqual(422,st)
        st,b=self.req("POST","/v1/worker/claim",{"worker_id":"w","queues":"q","lease_ms":100}); self.assertEqual(422,st)
    def test_empty_claim_is_true_204(self):
        r=urllib.request.Request(self.base+"/v1/worker/claim",data=json.dumps({"worker_id":"w","queues":["q"],"lease_ms":100}).encode(),method="POST",headers={"Content-Type":"application/json"})
        with urllib.request.urlopen(r,timeout=3) as x:
            self.assertEqual(204,x.status); self.assertEqual(b"",x.read())


    def test_duplicate_http_completion_is_idempotent(self):
        _,b=self.req("POST","/v1/jobs",{"queue":"q","payload":{}}); jid=b["job"]["id"]
        _,c=self.req("POST","/v1/worker/claim",{"worker_id":"w","queues":["q"],"lease_ms":1000})
        body={"job_id":jid,"lease_token":c["lease_token"],"result":{"ok":True}}
        st,a=self.req("POST","/v1/worker/w/complete",body); self.assertTrue(a["changed"])
        st,b=self.req("POST","/v1/worker/w/complete",body); self.assertEqual(200,st); self.assertFalse(b["changed"])
    def test_schema_incompatibility_fails_ready_not_live(self):
        import sqlite3
        conn=sqlite3.connect(self.db); conn.execute("PRAGMA user_version=99"); conn.commit(); conn.close()
        st,_=self.req("GET","/health/live"); self.assertEqual(200,st)
        st,_=self.req("GET","/health/ready"); self.assertEqual(503,st)
    def test_malformed_job_id_is_safe_not_found(self):
        st,b=self.req("GET","/v1/jobs/not-a-uuid"); self.assertEqual(404,st); self.assertEqual("not_found",b["error"])

if __name__ == '__main__': unittest.main()
