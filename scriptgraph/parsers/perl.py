from __future__ import annotations
import re
from pathlib import Path
from ..graph import Edge
from ..utils import strip_comments

CALL_RE = re.compile(
    r"(?:system|exec)\s*\(\s*['\"]([^'\"]+\.(?:sh|pl|bat|cmd|ps1))['\"]"
)
DYN_HINT = re.compile(r"\$[A-Za-z_]|`|\$\(")


def parse_perl(root: Path, src_path: str, text: str):
    edges = []
    for raw in text.splitlines():
        line = strip_comments(raw)
        if not line.strip():
            continue
        for m in CALL_RE.findall(line):
            edges.append(
                Edge(
                    src=src_path,
                    dst=m,
                    kind="call",
                    command=line,
                    dynamic=bool(DYN_HINT.search(line)),
                    resolved=not bool(DYN_HINT.search(line)),
                    confidence=0.7,
                )
            )
    return edges
