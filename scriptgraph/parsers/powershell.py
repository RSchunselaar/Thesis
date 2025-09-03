from __future__ import annotations
import re
from pathlib import Path
from ..graph import Edge
from ..utils import strip_comments

# & "./x.ps1" or & '.\x.ps1' or ./x.ps1 or . .\x.ps1 (dot-sourcing)
CALL_RE = re.compile(r"(?:&\s+)?['\"]?([\w./\\-]+\.ps1)['\"]?")
DOTSRC_RE = re.compile(r"^\s*\.\s+['\"]?([\w./\\-]+\.ps1)['\"]?")
DYN_HINT = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*|\$\(|Invoke-Expression")


def parse_powershell(root: Path, src_path: str, text: str):
    edges = []
    for raw in text.splitlines():
        line = strip_comments(raw)
        if not line.strip():
            continue
        dynamic = bool(DYN_HINT.search(line))
        kind = "call"
        for m in CALL_RE.findall(line):
            if DOTSRC_RE.search(line):
                kind = "source"
            edges.append(
                Edge(
                    src=src_path,
                    dst=m,
                    kind=kind,
                    command=line,
                    dynamic=dynamic,
                    resolved=not dynamic,
                    confidence=0.9 if not dynamic else 0.5,
                )
            )
    return edges
