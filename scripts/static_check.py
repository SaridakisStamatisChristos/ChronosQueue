from __future__ import annotations
import ast, pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
violations=[]
for base in (ROOT/'src',ROOT/'tests',ROOT/'scripts'):
    for p in base.rglob('*.py'):
        tree=ast.parse(p.read_text(),filename=str(p))
        for node in ast.walk(tree):
            if isinstance(node,ast.ImportFrom) and any(a.name=='*' for a in node.names): violations.append(f"{p}: wildcard import")
            if isinstance(node,ast.ExceptHandler) and node.type is None: violations.append(f"{p}:{node.lineno}: bare except")
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id in {'eval','exec'}: violations.append(f"{p}:{node.lineno}: {node.func.id} forbidden")
if violations: raise SystemExit('\n'.join(violations))
print('static AST checks OK')
