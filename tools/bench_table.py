import sys, json, statistics

def _mean(xs): 
    xs=[x for x in xs if x is not None]
    return 0.0 if not xs else sum(xs)/len(xs)

def main(path: str):
    rows=[]
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rows.append(json.loads(line))
    by_role={}
    for r in rows:
        role=r.get("role","")
        s=r.get("score",{})
        by_role.setdefault(role, []).append(s)
    print("| System | N | Node‑F1 | Edge‑F1 | GCR |")
    print("|:--|--:|--:|--:|--:|")
    for role, arr in sorted(by_role.items()):
        n=len(arr)
        nf=_mean([x.get("f1_nodes") for x in arr])
        ef=_mean([x.get("f1_edges") for x in arr])
        gcr=_mean([x.get("gcr") for x in arr])
        print(f"| {role} | {n} | {nf:.3f} | {ef:.3f} | {gcr:.3f} |")

if __name__=="__main__":
    p = sys.argv[1] if len(sys.argv)>1 else "artifacts/bench_results.jsonl"
    main(p)
