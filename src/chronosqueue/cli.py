from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.error
import urllib.parse
import urllib.request

from .api import serve
from .clock import parse_timestamp_us
from .migrations import CURRENT_VERSION
from .logging_utils import configure_logging
from .store import Store


def _request(base: str, method: str, path: str, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base.rstrip("/") + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        msg = json.loads(raw) if raw else {"error": str(exc)}
        raise SystemExit(f"HTTP {exc.code}: {msg}")


def _print(obj, as_json: bool) -> None:
    if as_json:
        print(json.dumps(obj, indent=2, ensure_ascii=False))
    elif isinstance(obj, dict) and "job" in obj:
        j = obj["job"]
        print(f"{j['id']} {j['state']} queue={j['queue']} attempts={j['attempt_count']}/{j['max_attempts']}")
        if "lease_token" in obj:
            print(f"lease_token={obj['lease_token']}")
    else:
        print(json.dumps(obj, indent=2, ensure_ascii=False))


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="chronosqueue")
    p.add_argument("--url", default="http://127.0.0.1:8080")
    p.add_argument("--json", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve"); s.add_argument("--db", default="./data/chronosqueue.db"); s.add_argument("--host", default="127.0.0.1"); s.add_argument("--port", type=int, default=8080)
    m = sub.add_parser("migrate"); m.add_argument("--db", default="./data/chronosqueue.db"); m.add_argument("--target", type=int, default=CURRENT_VERSION); m.add_argument("--allow-destructive-downgrade", action="store_true")
    s = sub.add_parser("submit"); s.add_argument("--queue", default="default"); s.add_argument("--payload", required=True); s.add_argument("--scheduled-at"); s.add_argument("--delay-ms", type=int); s.add_argument("--priority", type=int, default=0); s.add_argument("--tag", action="append", default=[]); s.add_argument("--idempotency-key"); s.add_argument("--max-attempts", type=int, default=3); s.add_argument("--retry-kind", choices=["fixed","exponential"], default="fixed"); s.add_argument("--retry-delay-ms", type=int, default=1000)
    for name in ("get", "cancel", "requeue"):
        x = sub.add_parser(name); x.add_argument("job_id")
    l = sub.add_parser("list"); l.add_argument("--queue"); l.add_argument("--state"); l.add_argument("--tag"); l.add_argument("--priority", type=int); l.add_argument("--created-after"); l.add_argument("--created-before"); l.add_argument("--lease-owner"); l.add_argument("--cursor"); l.add_argument("--limit", type=int, default=100)
    c = sub.add_parser("claim"); c.add_argument("--worker", required=True); c.add_argument("--queue", action="append", required=True); c.add_argument("--lease-ms", type=int, default=30000)
    for name in ("complete", "fail", "heartbeat"):
        x = sub.add_parser(name); x.add_argument("--worker", required=True); x.add_argument("--job", required=True); x.add_argument("--lease-token", required=True)
        if name == "complete": x.add_argument("--data", default="null")
        if name == "fail": x.add_argument("--error", default="null")
        if name == "heartbeat": x.add_argument("--lease-ms", type=int, default=30000)
    sub.add_parser("queues"); sub.add_parser("leases"); sub.add_parser("health")
    x = sub.add_parser("purge"); x.add_argument("--older-than", required=True); x.add_argument("--limit", type=int, default=1000)
    x = sub.add_parser("requeue-many"); x.add_argument("job_ids", nargs="+"); x.add_argument("--scheduled-at")
    return p


def main(argv=None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if "--json" in raw:
        raw = [x for x in raw if x != "--json"]
        raw.insert(0, "--json")
    args = parser().parse_args(raw)
    configure_logging()
    if args.cmd == "serve":
        store = Store(args.db); store.migrate(); serve(store, args.host, args.port); return 0
    if args.cmd == "migrate":
        Store(args.db).migrate(args.target, allow_destructive=args.allow_destructive_downgrade); return 0
    if args.cmd == "submit":
        body = {"queue": args.queue, "payload": json.loads(args.payload), "priority": args.priority, "tags": args.tag, "idempotency_key": args.idempotency_key, "max_attempts": args.max_attempts, "retry": {"kind": args.retry_kind, "delay_ms": args.retry_delay_ms}}
        if args.scheduled_at and args.delay_ms is not None:
            raise SystemExit("use only one of --scheduled-at or --delay-ms")
        if args.scheduled_at: body["scheduled_at"] = args.scheduled_at
        if args.delay_ms is not None: body["delay_ms"] = args.delay_ms
        _, out = _request(args.url, "POST", "/v1/jobs", body); _print(out, args.json); return 0
    if args.cmd == "get": _, out = _request(args.url, "GET", f"/v1/jobs/{args.job_id}"); _print(out, args.json); return 0
    if args.cmd in {"cancel", "requeue"}: _, out = _request(args.url, "POST", f"/v1/jobs/{args.job_id}/{args.cmd}", {}); _print(out, args.json); return 0
    if args.cmd == "list":
        q = {k: v for k, v in {"queue":args.queue,"state":args.state,"tag":args.tag,"priority":args.priority,"created_after":args.created_after,"created_before":args.created_before,"lease_owner":args.lease_owner,"cursor":args.cursor,"limit":args.limit}.items() if v is not None}
        _, out = _request(args.url, "GET", "/v1/jobs?" + urllib.parse.urlencode(q)); _print(out, args.json); return 0
    if args.cmd == "claim": _, out = _request(args.url, "POST", "/v1/worker/claim", {"worker_id":args.worker,"queues":args.queue,"lease_ms":args.lease_ms}); _print(out, args.json); return 0
    if args.cmd in {"complete","fail","heartbeat"}:
        body = {"job_id":args.job,"lease_token":args.lease_token}
        if args.cmd == "complete": body["result"] = json.loads(args.data)
        if args.cmd == "fail": body["error"] = json.loads(args.error)
        if args.cmd == "heartbeat": body["lease_ms"] = args.lease_ms
        _, out = _request(args.url, "POST", f"/v1/worker/{args.worker}/{args.cmd}", body); _print(out, args.json); return 0
    if args.cmd in {"queues","leases"}: _, out = _request(args.url, "GET", f"/v1/{args.cmd}"); _print(out, args.json); return 0
    if args.cmd == "health": _, out = _request(args.url, "GET", "/health/ready"); _print(out, args.json); return 0
    if args.cmd == "purge": _, out = _request(args.url, "POST", "/v1/admin/purge", {"older_than":args.older_than,"limit":args.limit}); _print(out, args.json); return 0
    if args.cmd == "requeue-many":
        body={"job_ids":args.job_ids}
        if args.scheduled_at: body["scheduled_at"]=args.scheduled_at
        _, out = _request(args.url, "POST", "/v1/admin/requeue", body); _print(out, args.json); return 0
    return 2

if __name__ == "__main__":
    raise SystemExit(main())
