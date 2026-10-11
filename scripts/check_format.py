from __future__ import annotations
import pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
BAD=[]
for base in (ROOT/'src',ROOT/'tests',ROOT/'scripts'):
    for p in base.rglob('*.py'):
        text=p.read_text()
        if '\t' in text: BAD.append(f"{p.relative_to(ROOT)}: tab character")
        if not text.endswith('\n'): BAD.append(f"{p.relative_to(ROOT)}: missing final newline")
        for i,line in enumerate(text.splitlines(),1):
            if line.rstrip()!=line: BAD.append(f"{p.relative_to(ROOT)}:{i}: trailing whitespace")
if BAD: raise SystemExit('\n'.join(BAD))
print('format hygiene OK')
