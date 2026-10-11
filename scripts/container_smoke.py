#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen

BASE=os.environ.get("CHRONOSQUEUE_URL","http://127.0.0.1:8080")

def call(method,path,body=None):
    data=None if body is None else json.dumps(body).encode()
    req=Request(BASE+path,data=data,method=method,headers={"Content-Type":"application/json"})
    with urlopen(req,timeout=5) as r: return r.status,json.loads(r.read())

assert call("GET","/health/ready")[0]==200
status,job=call("POST","/v1/jobs",{"payload":{"smoke":True},"dedupe_key":"container-smoke","max_attempts":2})
assert status in (200,201)
_,claim=call("POST","/v1/worker/claim",{"worker_id":"ci-smoke","queues":["default"],"lease_ms":30000})
leased=claim["job"]; assert leased and leased["id"]==job["id"]
lease=leased["lease"]
_,done=call("POST","/v1/worker/ci-smoke/complete",{"job_id":job["id"],"lease_token":lease["token"],"lease_generation":lease["generation"],"result":{"ok":True}})
assert done["state"]=="succeeded"
print(json.dumps({"container_smoke":"passed","job_id":job["id"]}))
