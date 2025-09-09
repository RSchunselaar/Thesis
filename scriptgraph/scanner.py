from __future__ import annotations
from pathlib import Path
from typing import Callable, Dict
import json
from .graph import Graph
from .utils import is_executable_script, norm_path
from .exporter import write_artifacts
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

    def _is_windows_bundle(self, root: Path) -> bool:
        try:
            meta = json.loads((root / "meta.json").read_text(encoding="utf-8"))
            return str(meta.get("platform", "")).lower() == "windows"
        except Exception:
            return False

    def _canon_rel_str(self, s: str, windows: bool) -> str:
        s = (s or "").replace("\\", "/")
        if s.startswith("./"): s = s[2:]
        while "//" in s: s = s.replace("//", "/")
        return s.lower() if windows else s    

    def scan(self, root_dir: str) -> Graph:
        root = Path(root_dir).resolve()
        windows = self._is_windows_bundle(root)

        g = Graph()

        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if not is_executable_script(p, self.include_ext):
                continue
            rel = p.relative_to(root).as_posix()
            rel = self._canon_rel_str(rel, windows)
            g.add_node(rel)
            parser = PARSERS.get(p.suffix.lower())
            if not parser:
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for e in parser(root, rel, text):
                e.src = self._canon_rel_str(getattr(e, "src", rel), windows)
                if "$" in e.dst:
                    e.dst = self._canon_rel_str(e.dst, windows)
                else:
                    e.dst = self._canon_rel_str(norm_path(root, e.dst), windows)
                g.add_edge(e)

        return g

    def scan_to_artifacts(self, root_dir: str, out_dir: str) -> Graph:
        """Static scan that emits normalized artifacts identical in shape to agent runs."""
        root = Path(root_dir).resolve()
        g = self.scan(root_dir)
        coverage = {"touched": len(g.nodes), "total": len(g.nodes)}
        unresolved: list[dict] = []
        write_artifacts(root=root, out_dir=Path(out_dir), graph=g, coverage=coverage, unresolved=unresolved, logger=None)
        return g
