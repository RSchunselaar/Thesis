from __future__ import annotations
import re
from pathlib import Path
from ..graph import Edge
from ..utils import strip_comments


CALL_RE = re.compile(r"(?:call\s+)?([\w./\\-]+\.(?:bat|cmd))", re.IGNORECASE)
# e.g., powershell -File .\ps\stage.ps1  or pwsh .\x.ps1
PS_CALL_RE = re.compile(r"""(?ix)
    \b(?:powershell(?:\.exe)?|pwsh(?:\.exe)?)\b
    (?:\s+-NoProfile|\s+-ExecutionPolicy\s+\w+|\s+-NonInteractive|\s+-NoLogo|\s+-Sta|\s+-Mta)*\s*
    (?:-File\s+)?["']?([\w./\\-]+\.ps1)["']?
""")
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
        for m in PS_CALL_RE.findall(line):
            edges.append(
                Edge(
                    src=src_path,
                    dst=m,
                    kind="call",
                    command=line,
                    dynamic=dynamic,
                    resolved=not dynamic,
                    confidence=0.8 if not dynamic else 0.5,
                )
            )
    return edges
