#!/usr/bin/env python3
"""Dependency-free repository formatting/static sanity gate."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {".git", ".venv", "dist", "build", "__pycache__"}
errors: list[str] = []

for path in sorted(ROOT.rglob("*")):
    if not path.is_file() or any(part in EXCLUDED for part in path.parts):
        continue
    if path.suffix not in {".py", ".md", ".yaml", ".yml", ".toml", ".sql"} and path.name not in {"Dockerfile", "Makefile", ".gitignore", ".dockerignore"}:
        continue
    try:
        raw = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        errors.append(f"{path}: not UTF-8")
        continue
    if raw and not raw.endswith("\n"):
        errors.append(f"{path}: missing final newline")
    for lineno, line in enumerate(raw.splitlines(), 1):
        if line.rstrip(" \t") != line:
            errors.append(f"{path}:{lineno}: trailing whitespace")
        if "\t" in line and path.name != "Makefile":
            errors.append(f"{path}:{lineno}: tab character")
    if path.suffix == ".py":
        try:
            tree = ast.parse(raw, filename=str(path))
        except SyntaxError as exc:
            errors.append(f"{path}:{exc.lineno}: syntax error: {exc.msg}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.ImportFrom, ast.Import)):
                for alias in node.names:
                    if alias.name == "*":
                        errors.append(f"{path}:{node.lineno}: wildcard import")
            if isinstance(node, ast.ExceptHandler) and node.type is None:
                errors.append(f"{path}:{node.lineno}: bare except")

if errors:
    print("quality gate failed:", file=sys.stderr)
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)
print("quality gate passed")
