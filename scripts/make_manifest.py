from __future__ import annotations
import hashlib, pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
paths=sorted(p.relative_to(ROOT).as_posix() for p in (ROOT/"evidence").glob("*.md"))
lines=[f"{hashlib.sha256((ROOT/p).read_bytes()).hexdigest()}  {p}" for p in paths]
(ROOT/"evidence/MANIFEST.sha256").write_text("\n".join(lines)+"\n")
print(f"manifest wrote {len(paths)} evidence files")
