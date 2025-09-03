from __future__ import annotations
import re
from pathlib import Path
from typing import Iterable
from ..graph import Edge
from ..utils import strip_comments

# Accept literal paths AND $VAR/ or ${VAR}/ prefixes; allow optional quotes around the path.
CALL_RE = re.compile(
    r"""
    (?:
      # bash/sh/ksh x.sh
      (?:bash|sh|ksh)\s+
      (?P<path1>["']?(?:\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/)?[\w./-]+\.(?:sh|bash|ksh)["']?)
    )
    |
    (?:
      # . file  OR  source file
      (?:\.|source)\s+
      (?P<path2>["']?(?:\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/)?[\w./-]+\.(?:sh|bash|ksh)["']?)
    )
    |
    (?:
      # direct invocation ./x.sh (no interpreter)
      (?P<path3>["']?(?:\./)?[\w./-]+\.(?:sh|bash|ksh)["']?)
    )
    """,
    re.VERBOSE,
)

VAR_HINT = re.compile(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?")

def _destinations(cmd: str) -> Iterable[str]:
    outs: list[str] = []
    for m in CALL_RE.finditer(cmd):
        p = m.group("path1") or m.group("path2") or m.group("path3")
        if p:
            # ignore variable assignments like FOO=./x.sh
            if "=" in p.split("/")[0]:
                continue
            # strip surrounding quotes if present
            if (p.startswith("'") and p.endswith("'")) or (p.startswith('"') and p.endswith('"')):
                p = p[1:-1]
            outs.append(p)
    return outs

def parse_shell(root: Path, src_path: str, text: str):
    edges: list[Edge] = []
    for raw in text.splitlines():
        line = strip_comments(raw)
        if not line.strip():
            continue
        # dynamic if it references $VAR / ${VAR} or uses command substitution/eval
        dynamic = bool(VAR_HINT.search(line)) or "`" in line or "$(" in line or "eval" in line
        kind = "source" if line.lstrip().startswith((". ", "source ")) else "call"
        for d in _destinations(line):
            edges.append(
                Edge(
                    src=src_path,
                    dst=d,
                    kind=kind,
                    command=line,
                    dynamic=dynamic,
                    resolved=not dynamic,
                    confidence=0.9 if not dynamic else 0.5,
                )
            )
    return edges
