#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "evidence" / "MANIFEST.sha256"
EXCLUDE_PREFIXES = (".git/", "dist/", "build/", ".venv/", "__pycache__/")
EXCLUDE_FILES = {"evidence/MANIFEST.sha256"}


def tracked_paths() -> list[str]:
    out=[]
    for p in ROOT.rglob("*"):
        if not p.is_file():
            continue
        rel=p.relative_to(ROOT).as_posix()
        if rel in EXCLUDE_FILES or any(rel.startswith(x) for x in EXCLUDE_PREFIXES) or "/__pycache__/" in rel:
            continue
        out.append(rel)
    return sorted(out)


def digest(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""): h.update(chunk)
    return h.hexdigest()

expected={}
for line in MANIFEST.read_text(encoding="utf-8").splitlines():
    if not line.strip(): continue
    sha, rel=line.split("  ",1); expected[rel]=sha
actual=tracked_paths()
if sorted(expected)!=actual:
    print("manifest file set mismatch",file=sys.stderr)
    print("missing from manifest:",sorted(set(actual)-set(expected)),file=sys.stderr)
    print("stale in manifest:",sorted(set(expected)-set(actual)),file=sys.stderr)
    raise SystemExit(1)
for rel in actual:
    got=digest(ROOT/rel)
    if got!=expected[rel]:
        print(f"hash mismatch: {rel}",file=sys.stderr); raise SystemExit(1)
print(f"manifest verified: {len(actual)} files")
