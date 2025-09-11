import json, sys, statistics as st
from collections import defaultdict

"""
Usage:
  py -3 tools/bench_aggregate.py artifacts/bench_results.jsonl
"""

def mean(xs): return 0.0 if not xs else st.mean(xs)

def main(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rows.append(json.loads(line))
    by_role = defaultdict(list)
    by_tier_role = defaultdict(list)   # (tier, role) -> list
    def tier_of(b):
        s = str(b).lower()
        return "easy" if s.startswith("easy-") else ("hard" if s.startswith("hard-") else "unknown")
    for r in rows:
        role = r.get("role") or "unknown"
        bundle = r.get("bundle") or ""
        sc = r.get("score") or {}
        lat = r.get("latency") or {}
        by_role[role].append((
            sc.get("f1_nodes", 0.0),
            sc.get("f1_edges", 0.0),
            sc.get("gcr", 0),
            lat.get("total", None),
        ))
        by_tier_role[(tier_of(bundle), role)].append((
            sc.get("f1_nodes", 0.0),
            sc.get("f1_edges", 0.0),
            sc.get("gcr", 0),
            lat.get("total", None),
        ))
    print("role\tN\tNode-F1\tEdge-F1\tGCR\tTotal-ms")
    for role, items in by_role.items():
        n = len(items)
        nF1 = mean([a for a,_,_,_ in items])
        eF1 = mean([b for _,b,_,_ in items])
        gcr = mean([c for *_,c,_ in items])
        tms = mean([d for *__,d in items if d is not None])
        print(f"{role}\t{n}\t{nF1:.3f}\t{eF1:.3f}\t{gcr:.3f}\t{(tms if tms else 0):.1f}")

    print("\n# by-tier")
    print("tier\trole\tN\tNode-F1\tEdge-F1\tGCR\tTotal-ms")
    for (tier, role), items in sorted(by_tier_role.items()):
        n = len(items)
        nF1 = mean([a for a,_,_,_ in items])
        eF1 = mean([b for _,b,_,_ in items])
        gcr = mean([c for *_,c,_ in items])
        tms = mean([d for *__,d in items if d is not None])
        print(f"{tier}\t{role}\t{n}\t{nF1:.3f}\t{eF1:.3f}\t{gcr:.3f}\t{(tms if tms else 0):.1f}")

if __name__ == "__main__":
    p = sys.argv[1] if len(sys.argv) > 1 else "artifacts/bench_results.jsonl"
    main(p)
