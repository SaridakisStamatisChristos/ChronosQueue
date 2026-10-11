#!/usr/bin/env python3
from __future__ import annotations

import hashlib
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"evidence"/"MANIFEST.sha256"
EXCLUDE_PREFIXES=(".git/","dist/","build/",".venv/","__pycache__/")
EXCLUDE_FILES={"evidence/MANIFEST.sha256"}
rows=[]
for p in ROOT.rglob("*"):
    if not p.is_file(): continue
    rel=p.relative_to(ROOT).as_posix()
    if rel in EXCLUDE_FILES or any(rel.startswith(x) for x in EXCLUDE_PREFIXES) or "/__pycache__/" in rel: continue
    h=hashlib.sha256(p.read_bytes()).hexdigest(); rows.append((rel,h))
OUT.write_text("".join(f"{h}  {rel}\n" for rel,h in sorted(rows)),encoding="utf-8")
print(f"wrote {len(rows)} entries to {OUT}")
