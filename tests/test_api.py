from __future__ import annotations

import http.client
import json
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from chronosqueue.api import ChronosHTTPServer, IMPLEMENTED_ROUTES, MAX_HTTP_BODY
from tests.helpers import service_at


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx=service_at(); self.service,self.clock,self.path=self.ctx.__enter__()
        self.server=ChronosHTTPServer(("127.0.0.1",0),self.service)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.base=f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=2); self.ctx.__exit__(None,None,None)

    def call(self, method: str, path: str, body=None):
        data=None if body is None else json.dumps(body).encode()
        req=Request(self.base+path,data=data,method=method,headers={"Content-Type":"application/json"})
        try:
            with urlopen(req,timeout=5) as r: return r.status,json.loads(r.read()) if "json" in r.headers.get("Content-Type","") else r.read().decode()
        except HTTPError as e: return e.code,json.loads(e.read())

    def test_real_submit_claim_complete_flow(self) -> None:
        code,j=self.call("POST","/v1/jobs",{"payload":{"task":"x"},"dedupe_key":"e2e"}); self.assertEqual(code,201)
        code,c=self.call("POST","/v1/worker/claim",{"worker_id":"worker-1"}); self.assertEqual(code,200)
        lease=c["job"]["lease"]
        code,done=self.call("POST","/v1/worker/worker-1/complete",{"job_id":j["id"],"lease_token":lease["token"],"lease_generation":lease["generation"],"result":{"ok":True}})
        self.assertEqual(code,200); self.assertEqual(done["state"],"succeeded")
        code,done2=self.call("POST","/v1/worker/worker-1/complete",{"job_id":j["id"],"lease_token":lease["token"],"lease_generation":lease["generation"],"result":{"ok":True}})
        self.assertEqual(code,200); self.assertEqual(done2["state"],"succeeded")

    def test_health_liveness_and_readiness_differ(self) -> None:
        self.assertEqual(self.call("GET","/health/live")[0],200); self.assertEqual(self.call("GET","/health/ready")[0],200)
        self.service.store.path=str(self.path.parent/"missing"/"db.sqlite")
        self.assertEqual(self.call("GET","/health/live")[0],200); self.assertEqual(self.call("GET","/health/ready")[0],503)

    def test_malformed_json_and_unknown_route_safe_errors(self) -> None:
        conn=http.client.HTTPConnection("127.0.0.1",self.server.server_port,timeout=5)
        conn.request("POST","/v1/jobs",body=b"{bad",headers={"Content-Type":"application/json","Content-Length":"4"})
        r=conn.getresponse(); body=json.loads(r.read()); self.assertEqual(r.status,400); self.assertEqual(body["error"]["code"],"invalid_request"); conn.close()
        self.assertEqual(self.call("GET","/does-not-exist")[0],404)

    def test_oversized_request_rejected_before_body_read(self) -> None:
        conn=http.client.HTTPConnection("127.0.0.1",self.server.server_port,timeout=5)
        conn.putrequest("POST","/v1/jobs"); conn.putheader("Content-Type","application/json"); conn.putheader("Content-Length",str(MAX_HTTP_BODY+1)); conn.endheaders()
        r=conn.getresponse(); self.assertEqual(r.status,413); r.read(); conn.close()

    def test_metrics_and_openapi_served(self) -> None:
        code,text=self.call("GET","/metrics"); self.assertEqual(code,200); self.assertIn("chronosqueue_events_total",text)
        with urlopen(self.base+"/openapi.yaml",timeout=5) as r: self.assertIn(b"openapi: 3.1.0",r.read())

    def test_openapi_route_set_equals_declared_runtime_route_set(self) -> None:
        text=Path(__file__).parents[1].joinpath("src/chronosqueue/openapi.yaml").read_text()
        routes=set(); current=None
        for line in text.splitlines():
            if line.startswith("  /") and line.endswith(":"):
                current=line.strip()[:-1]
            elif current and line.startswith("    ") and line.strip() in {"get:","post:","put:","delete:","patch:"}:
                routes.add((line.strip()[:-1].upper(),current))
        self.assertEqual(routes,IMPLEMENTED_ROUTES)
