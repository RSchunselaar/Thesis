from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Set, Tuple, List, Optional
import yaml, json
import re

# ---- Canonicalization (matches thesis metric spec) -------------------------
def _canon_path(p: str, case_sensitive: bool = True) -> str:
    if not isinstance(p, str): return ""
    s = p.replace("\\", "/")
    while s.startswith("./"): s = s[2:]
    # collapse // and resolve . and ..
    parts = []
    for seg in s.split("/"):
        if seg in ("", "."): continue
        if seg == "..":
            if parts: parts.pop()
            continue
        parts.append(seg)
    s = "/".join(parts)
    return s if case_sensitive else s.lower()

def _maybe_prefix(p: str, prefix: Optional[str]) -> str:
    if not prefix:
        return p
    # Don't double-prefix absolute/anchored paths
    if p.startswith("/") or re.match(r"^[A-Za-z]:", p):
        return p
    if p.startswith(prefix.rstrip("/") + "/"):
        return p
    return f"{prefix.rstrip('/')}/{p.lstrip('./')}"

def _canon_graph(obj: Dict[str, Any], case_sensitive: bool, *, prefix: Optional[str] = None) -> Dict[str, Any]:
    nodes = set(_canon_path(_maybe_prefix(n, prefix), case_sensitive) for n in (obj.get("nodes") or []))
    edges = set()
    for e in (obj.get("edges") or []):
        src = _canon_path(_maybe_prefix(e.get("src",""), prefix), case_sensitive)
        dst = _canon_path(_maybe_prefix(e.get("dst",""), prefix), case_sensitive)
        kind = (e.get("kind") or "call").strip()
        edges.add((src, dst, kind))
    return {"nodes": nodes, "edges": edges}

def _prf(pred: Set, truth: Set):
    tp = len(pred & truth)
    p = 0.0 if not pred else tp/len(pred)
    r = 0.0 if not truth else tp/len(truth)
    f1 = 0.0 if (p+r)==0 else 2*p*r/(p+r)
    return p,r,f1

@dataclass
class Scores:
    precision_nodes: float; recall_nodes: float; f1_nodes: float
    precision_edges: float; recall_edges: float; f1_edges: float
    gcr: int

def score_pair(pred: Dict[str,Any], truth: Dict[str,Any], *, case_sensitive=True, pred_prefix: Optional[str] = None) -> Scores:
    P = _canon_graph(pred, case_sensitive, prefix=pred_prefix)
    T = _canon_graph(truth, case_sensitive, prefix=None)
    pn, rn, fn = _prf(P["nodes"], T["nodes"])
    pe, re, fe = _prf(P["edges"], T["edges"])
    gcr = 1 if (P["nodes"]==T["nodes"] and P["edges"]==T["edges"]) else 0
    return Scores(pn,rn,fn, pe,re,fe, gcr)

def _load_yaml(path: str) -> Dict[str,Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def cli():
    import argparse
    ap = argparse.ArgumentParser(description="Score predicted_graph.yaml against ground truth.")
    ap.add_argument("--pred", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--case-insensitive", action="store_true",
                    help="Use for Windows-only bundles.")
    ap.add_argument("--pred-prefix", help="Prefix to prepend to predicted paths before scoring.")
    args = ap.parse_args()
    s = score_pair(_load_yaml(args.pred), _load_yaml(args.truth),
                    case_sensitive=not args.case_insensitive,
                    pred_prefix=args.pred_prefix)
    print(json.dumps(s.__dict__, indent=2))

if __name__ == "__main__":
    cli()
