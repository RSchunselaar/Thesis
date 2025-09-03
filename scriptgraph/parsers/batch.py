from __future__ import annotations
import re
from pathlib import Path
from ..graph import Edge
from ..utils import strip_comments


CALL_RE = re.compile(r"(?:call\s+)?([\w./\\-]+\.(?:bat|cmd))", re.IGNORECASE)
VAR_HINT = re.compile(r"%[A-Za-z_][A-Za-z0-9_]*%")


def parse_batch(root: Path, src_path: str, text: str):
    edges = []
    for raw in text.splitlines():
        line = strip_comments(raw)
        if not line.strip():
            continue
        dynamic = bool(VAR_HINT.search(line)) or "%%" in line or "!" in line
        for m in CALL_RE.findall(line):
            edges.append(
                Edge(
                    src=src_path,
                    dst=m,
                    kind="call",
                    command=line,
                    dynamic=dynamic,
                    resolved=not dynamic,
                    confidence=0.9 if not dynamic else 0.5,
                )
            )
    return edges
