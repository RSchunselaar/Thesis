from __future__ import annotations
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import json
import sqlite3
import yaml


@dataclass
class GraphStats:
    nodes: int
    edges: int
    edges_static: int
    edges_dynamic_resolved: int
    edges_dynamic_unresolved: int
    kinds: dict
    top_callers: list # list[[src, outdeg]]
    top_callees: list # list[[dst, indeg]]
    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def summarize_graph(graph_yaml: str) -> GraphStats:
    data = yaml.safe_load(Path(graph_yaml).read_text(encoding="utf-8")) or {}
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])
    edges_static = sum(1 for e in edges if not e.get("dynamic", False))
    edges_dynamic_resolved = sum(1 for e in edges if e.get("dynamic", False) and e.get("resolved", False))
    edges_dynamic_unresolved = sum(1 for e in edges if e.get("dynamic", False) and not e.get("resolved", False))
    kinds = Counter(e.get("kind", "call") for e in edges)
    out_deg = Counter(e.get("src") for e in edges)
    in_deg = Counter(e.get("dst") for e in edges)
    top_callers = [[k, v] for k, v in out_deg.most_common(5)]
    top_callees = [[k, v] for k, v in in_deg.most_common(5)]
    return GraphStats(
        nodes=len(nodes),
        edges=len(edges),
        edges_static=edges_static,
        edges_dynamic_resolved=edges_dynamic_resolved,
        edges_dynamic_unresolved=edges_dynamic_unresolved,
        kinds=dict(kinds),
        top_callers=top_callers,
        top_callees=top_callees,
    )


@dataclass
class RunAgg:
    by_cmd: dict # {cmd: {count, mean_sec, p50_sec, p95_sec}}
    def to_json(self) -> str:
        return json.dumps(self.by_cmd, indent=2)

def summarize_runs(sqlite_path: str) -> RunAgg:
    p = Path(sqlite_path)
    if not p.exists():
        return RunAgg(by_cmd={})
    conn = sqlite3.connect(str(p))
    cur = conn.execute("SELECT cmd, started_at, finished_at FROM runs")
    buckets: dict[str, list[float]] = defaultdict(list)
    for cmd, start_s, end_s in cur:
        if not (start_s and end_s):
            continue
        try:
            start = datetime.fromisoformat(start_s)
            end = datetime.fromisoformat(end_s)
            dur = max(0.0, (end - start).total_seconds())
            buckets[cmd].append(dur)
        except Exception:
            continue
    conn.close()
    def pct(xs: list[float], p: float) -> float:
        if not xs: return 0.0
        xs = sorted(xs)
        i = int(round((p / 100.0) * (len(xs) - 1)))
        return xs[i]
    out: dict[str, dict] = {}
    for cmd, xs in buckets.items():
        if not xs: continue
        mean = sum(xs) / len(xs)
        out[cmd] = {"count": len(xs), "mean_sec": round(mean, 3),
                    "p50_sec": round(pct(xs, 50), 3),
                    "p95_sec": round(pct(xs, 95), 3)}
    return RunAgg(by_cmd=out)

def print_graph_stats(gs: GraphStats):
    print(f"Nodes: {gs.nodes}")
    print(f"Edges: {gs.edges}")
    print(f"  Static: {gs.edges_static}")
    print(f"  Dynamic resolved: {gs.edges_dynamic_resolved}")
    print(f"  Dynamic unresolved: {gs.edges_dynamic_unresolved}")
    if gs.kinds:
        print("Kinds:")
        for k, v in gs.kinds.items():
            print(f"  - {k}: {v}")
    if gs.top_callers:
        print("Top callers (out-degree):")
        for n, d in gs.top_callers:
            print(f"  - {n}: {d}")
    if gs.top_callees:
        print("Top callees (in-degree):")
        for n, d in gs.top_callees:
            print(f"  - {n}: {d}")

def print_run_stats(ra: RunAgg):
    if not ra.by_cmd:
        print("No completed runs found.")
        return
    print("Run durations by command:")
    for cmd, m in ra.by_cmd.items():
        print(f"  {cmd}: count={m['count']} mean={m['mean_sec']}s p50={m['p50_sec']}s p95={m['p95_sec']}s")