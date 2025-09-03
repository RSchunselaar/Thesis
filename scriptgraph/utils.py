from __future__ import annotations
import re
import socket
from pathlib import Path

# Strip whole-line comments starting with '#' or '//' (kept simple for MVP).
COMMENT_RE = re.compile(r"^\s*(#|//).*")


class BlockAllSockets(socket.socket):
    """Kill-switch: any attempt to open a socket raises."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError("Network egress is disabled for this run.")


def disable_network():
    # Monkey-patch the socket class. Reversible only by restarting the process.
    socket.socket = BlockAllSockets  # type: ignore[attr-defined]


def norm_path(root: Path, p: str) -> str:
    try:
        cand = (root / p).resolve()
    except Exception:
        cand = (root / p)
    try:
        out = str(cand.relative_to(root.resolve()))
    except Exception:
        out = str(cand)
    return out.replace("\\", "/")  # <— normalize on Windows


def is_executable_script(path: Path, include_ext: set[str]) -> bool:
    return path.suffix.lower() in include_ext


def strip_comments(line: str) -> str:
    return COMMENT_RE.sub("", line)

def canon(p: str) -> str:
    if not isinstance(p, str):
        return p
    p = p.replace("\\", "/")
    p = re.sub(r"^\./", "", p)      # drop leading ./ once
    p = p.replace("/./", "/")       # collapse /./
    while "//" in p:
        p = p.replace("//", "/")    # collapse //
    return p
