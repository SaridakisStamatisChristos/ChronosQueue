from __future__ import annotations
import json, pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from chronosqueue.api import ROUTE_SPEC
spec = json.loads((ROOT / "openapi.json").read_text())
openapi_routes = set()
for path, methods in spec["paths"].items():
    for method in methods:
        if method.lower() in {"get","post","put","delete","patch"}:
            openapi_routes.add((method.upper(), path))
missing = sorted(ROUTE_SPEC - openapi_routes)
extra = sorted(openapi_routes - ROUTE_SPEC)
if missing or extra:
    raise SystemExit(f"route divergence missing={missing} extra={extra}")
print(f"route contract OK: {len(ROUTE_SPEC)} routes")
