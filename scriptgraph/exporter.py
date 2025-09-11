from __future__ import annotations
import json
from pathlib import Path
from .graph import Graph

def _canon_rel(p: str, root: Path, windows: bool) -> str:
    s = (p or "").strip().replace("\\", "/")
    if s.startswith("./"): s = s[2:]
    try:
        pp = Path(s)
        if pp.is_absolute():
            s = pp.relative_to(root).as_posix()
    except Exception:
        pass
    return s.lower() if windows else s

def write_artifacts(
    *,
    root: Path,
    out_dir: Path,
    graph: Graph,
    coverage: dict,
    unresolved: list[dict],
    logger=None,
    nodes_policy: str = "participating",
    create_run_report: bool = True,
) -> None:
    """
    Serialize graph + diagnostics to predicted_graph.yaml, graph.dot, and optionally run_report.json.
    nodes_policy:
      - "participating": keep only nodes that appear in at least one edge,
        plus any unresolved sources (default).
      - "all": keep graph.nodes as-is.
    create_run_report:
      - True: create run_report.json (default for scanner/cli usage)
      - False: skip run_report.json creation (used when data is included in run_stats.json)
    """
    # detect platform from bundle meta.json
    windows = False
    try:
        meta = json.loads((root / "meta.json").read_text(encoding="utf-8"))
        windows = str(meta.get("platform","")).lower() == "windows"
    except Exception:
        pass

    # --- Apply node policy before serialization ---
    if nodes_policy == "participating":
        used = set()
        for e in graph.edges:
            used.add(e.src); used.add(e.dst)
        # Keep unresolved sources visible for triage
        for u in (unresolved or []):
            src = u.get("src")
            if src:
                used.add(src)
        # Drop orphans deterministically
        graph.nodes = {n: meta for n, meta in graph.nodes.items() if n in used}

    # YAML export
    nodes = sorted({ _canon_rel(n, root, windows) for n in graph.nodes.keys() })
    lines = ["nodes:\n"] + [f"  - {json.dumps(n)}\n" for n in nodes]
    lines.append("edges:\n")
    for e in graph.edges:
        src = _canon_rel(e.src, root, windows)
        dst = _canon_rel(e.dst, root, windows)
        lines += [f"  - src: {json.dumps(src)}\n", f"    dst: {json.dumps(dst)}\n", f"    kind: {json.dumps(e.kind)}\n"]
        if getattr(e, "command", None):   lines.append(f"    command: {json.dumps(e.command)}\n")
        if getattr(e, "dynamic", None) is not None:   lines.append(f"    dynamic: {str(bool(e.dynamic)).lower()}\n")
        if getattr(e, "resolved", None) is not None:  lines.append(f"    resolved: {str(bool(e.resolved)).lower()}\n")
        if getattr(e, "confidence", None) is not None: lines.append(f"    confidence: {float(e.confidence):.3f}\n")
        if getattr(e, "reason", None):    lines.append(f"    reason: {json.dumps(e.reason)}\n")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "predicted_graph.yaml").write_text("".join(lines), encoding="utf-8")
    (out_dir / "graph.dot").write_text(graph.to_dot(), encoding="utf-8")
    
    if create_run_report:
        (out_dir / "run_report.json").write_text(json.dumps({
            "coverage": coverage, "unresolved": unresolved[:50],
        }, indent=2), encoding="utf-8")
        if logger:
            logger.log("INFO", f"Artifacts: {out_dir/'predicted_graph.yaml'} ; {out_dir/'run_report.json'}")
    else:
        if logger:
            logger.log("INFO", f"Artifacts: {out_dir/'predicted_graph.yaml'}")
