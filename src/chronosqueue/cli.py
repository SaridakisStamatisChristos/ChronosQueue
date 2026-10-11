from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .api import serve
from .clock import rfc3339_to_us
from .logging_utils import configure_logging
from .migration import LATEST_VERSION, migrate
from .service import QueueService

DEFAULT_URL = "http://127.0.0.1:8080"


def _json_arg(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid JSON: {exc}") from exc


def _request(base: str, method: str, path: str, body: Any | None = None) -> Any:
    data = None if body is None else json.dumps(body, allow_nan=False).encode()
    req = Request(base.rstrip("/") + path, data=data, method=method,
                  headers={"Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urlopen(req, timeout=10) as response:
            raw = response.read()
            ctype = response.headers.get("Content-Type", "")
            return json.loads(raw) if "json" in ctype else raw.decode()
    except HTTPError as exc:
        raw = exc.read()
        try:
            detail = json.loads(raw)
        except Exception:
            detail = {"error": {"message": raw.decode(errors="replace")}}
        raise SystemExit(f"HTTP {exc.code}: {json.dumps(detail, ensure_ascii=False)}") from exc
    except URLError as exc:
        raise SystemExit(f"server unavailable: {exc.reason}") from exc


def _print(value: Any, json_mode: bool) -> None:
    if json_mode or not isinstance(value, dict):
        print(json.dumps(value, indent=2, ensure_ascii=False) if not isinstance(value, str) else value)
        return
    if "id" in value:
        print(f"{value['id']}  {value.get('state','')}  queue={value.get('queue','')}  attempts={value.get('attempt_count','')}/{value.get('max_attempts','')}")
    else:
        print(json.dumps(value, indent=2, ensure_ascii=False))


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="chronosqueue", description="ChronosQueue server and client")
    p.add_argument("--url", default=os.environ.get("CHRONOSQUEUE_URL", DEFAULT_URL), help="server URL for client commands")
    p.add_argument("--json", action="store_true", help="machine-readable JSON output")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("serve")
    s.add_argument("--db", default=os.environ.get("CHRONOSQUEUE_DB", "./data/chronosqueue.db"))
    s.add_argument("--host", default=os.environ.get("CHRONOSQUEUE_HOST", "127.0.0.1"))
    s.add_argument("--port", type=int, default=int(os.environ.get("CHRONOSQUEUE_PORT", "8080")))
    s.add_argument("--log-level", default=os.environ.get("CHRONOSQUEUE_LOG_LEVEL", "INFO"))

    s = sub.add_parser("submit")
    s.add_argument("--payload", type=_json_arg, required=True)
    s.add_argument("--queue", default="default"); s.add_argument("--delay-ms", type=int)
    s.add_argument("--scheduled-at"); s.add_argument("--priority", type=int, default=0)
    s.add_argument("--tag", action="append", default=[]); s.add_argument("--dedupe-key")
    s.add_argument("--max-attempts", type=int, default=3); s.add_argument("--retry-kind", choices=["fixed","exponential"], default="fixed")
    s.add_argument("--retry-delay-ms", type=int, default=1000); s.add_argument("--retry-max-delay-ms", type=int, default=86400000)
    s.add_argument("--id")

    s = sub.add_parser("get"); s.add_argument("job_id")
    s = sub.add_parser("list"); s.add_argument("--queue"); s.add_argument("--state"); s.add_argument("--tag", action="append", default=[]); s.add_argument("--limit", type=int, default=50); s.add_argument("--cursor")
    s = sub.add_parser("cancel"); s.add_argument("job_id")
    s = sub.add_parser("requeue"); s.add_argument("job_id"); s.add_argument("--scheduled-at"); s.add_argument("--keep-attempts", action="store_true")
    s = sub.add_parser("claim"); s.add_argument("--worker", required=True); s.add_argument("--queue", action="append", default=[]); s.add_argument("--lease-ms", type=int)
    for name in ("heartbeat", "complete", "fail"):
        s = sub.add_parser(name); s.add_argument("--worker", required=True); s.add_argument("--job", required=True); s.add_argument("--token", required=True); s.add_argument("--generation", type=int, required=True)
        if name == "heartbeat": s.add_argument("--lease-ms", type=int)
        if name == "complete": s.add_argument("--result", type=_json_arg, default=None)
        if name == "fail": s.add_argument("--error", type=_json_arg, default=None)
    sub.add_parser("queues"); sub.add_parser("leases"); sub.add_parser("health")
    s = sub.add_parser("purge"); s.add_argument("--terminal-before", required=True); s.add_argument("--limit", type=int, default=1000)
    s = sub.add_parser("migrate"); s.add_argument("--db", default=os.environ.get("CHRONOSQUEUE_DB", "./data/chronosqueue.db")); s.add_argument("--target", type=int, default=LATEST_VERSION); s.add_argument("--allow-destructive", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "serve":
        configure_logging(args.log_level)
        service = QueueService(args.db)
        serve(service, args.host, args.port)
        return 0
    if args.command == "migrate":
        if args.target < LATEST_VERSION and not args.allow_destructive:
            raise SystemExit("downgrades require --allow-destructive")
        conn = sqlite3.connect(args.db, isolation_level=None)
        try:
            steps = migrate(conn, args.target)
        finally:
            conn.close()
        _print({"target": args.target, "steps": steps}, args.json); return 0

    if args.command == "submit":
        body = {"payload": args.payload, "queue": args.queue, "delay_ms": args.delay_ms, "priority": args.priority,
                "tags": args.tag, "dedupe_key": args.dedupe_key, "max_attempts": args.max_attempts,
                "retry": {"kind": args.retry_kind, "delay_ms": args.retry_delay_ms, "max_delay_ms": args.retry_max_delay_ms}, "id": args.id}
        body = {k:v for k,v in body.items() if v is not None}
        if args.scheduled_at: body["scheduled_at"] = args.scheduled_at
        out = _request(args.url, "POST", "/v1/jobs", body)
    elif args.command == "get": out = _request(args.url, "GET", f"/v1/jobs/{args.job_id}")
    elif args.command == "list":
        q: list[tuple[str,str]] = []
        for k in ("queue","state","limit","cursor"):
            v=getattr(args,k)
            if v is not None: q.append((k,str(v)))
        q.extend(("tag", t) for t in args.tag)
        out=_request(args.url,"GET","/v1/jobs?"+urlencode(q))
    elif args.command == "cancel": out=_request(args.url,"POST",f"/v1/jobs/{args.job_id}/cancel",{})
    elif args.command == "requeue":
        body={"reset_attempts":not args.keep_attempts}
        if args.scheduled_at: body["scheduled_at"]=args.scheduled_at
        out=_request(args.url,"POST",f"/v1/jobs/{args.job_id}/requeue",body)
    elif args.command == "claim": out=_request(args.url,"POST","/v1/worker/claim",{"worker_id":args.worker,"queues":args.queue or ["default"],"lease_ms":args.lease_ms})
    elif args.command in {"heartbeat","complete","fail"}:
        body={"job_id":args.job,"lease_token":args.token,"lease_generation":args.generation}
        if args.command=="heartbeat": body["lease_ms"]=args.lease_ms
        elif args.command=="complete": body["result"]=args.result
        else: body["error"]=args.error
        out=_request(args.url,"POST",f"/v1/worker/{args.worker}/{args.command}",body)
    elif args.command == "queues": out=_request(args.url,"GET","/v1/queues")
    elif args.command == "leases": out=_request(args.url,"GET","/v1/leases")
    elif args.command == "health": out={"live":_request(args.url,"GET","/health/live"),"ready":_request(args.url,"GET","/health/ready")}
    elif args.command == "purge": out=_request(args.url,"POST","/v1/admin/purge",{"terminal_before_us":rfc3339_to_us(args.terminal_before),"limit":args.limit})
    else: raise AssertionError(args.command)
    _print(out,args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
