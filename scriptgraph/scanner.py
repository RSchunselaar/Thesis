from __future__ import annotations
from pathlib import Path
from typing import Callable, Dict
from .graph import Graph
from .utils import is_executable_script, norm_path
from .parsers import (
    parse_shell,
    parse_batch,
    parse_powershell,
    parse_python_cli,
    parse_perl,
)


PARSERS: Dict[str, Callable] = {}
for ext in [".sh", ".bash", ".ksh"]:
    PARSERS[ext] = parse_shell
for ext in [".bat", ".cmd"]:
    PARSERS[ext] = parse_batch
PARSERS[".ps1"] = parse_powershell
PARSERS[".py"] = parse_python_cli
PARSERS[".pl"] = parse_perl


class Scanner:
    def __init__(self, include_ext: list[str]):
        self.include_ext = set(e.lower() for e in include_ext)

    def scan(self, root_dir: str) -> Graph:
        root = Path(root_dir).resolve()

        g = Graph()

        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if not is_executable_script(p, self.include_ext):
                continue
            rel = str(p.relative_to(root))
            g.add_node(rel)
            parser = PARSERS.get(p.suffix.lower())
            if not parser:
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for e in parser(root, rel, text):
                if "$" in e.dst:  # dynamic path; keep as-is but normalize slashes
                    e.dst = e.dst.replace("\\", "/")
                    if e.dst.startswith("./"): e.dst = e.dst[2:]
                else:
                    e.dst = norm_path(root, e.dst)
                g.add_edge(e)

        return g
