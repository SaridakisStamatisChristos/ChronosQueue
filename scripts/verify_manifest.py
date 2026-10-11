from __future__ import annotations
import hashlib, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
manifest = ROOT / "evidence" / "MANIFEST.sha256"
for line in manifest.read_text().splitlines():
    if not line.strip(): continue
    digest, rel = line.split("  ", 1)
    got = hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()
    if got != digest: raise SystemExit(f"manifest mismatch: {rel}")
print("manifest OK")
