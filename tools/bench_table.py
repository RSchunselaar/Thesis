import sys, json, statistics

def _mean(xs):
    xs = [x for x in xs if x is not None]
    return 0.0 if not xs else sum(xs)/len(xs)

def main(path: str):
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            if not s.startswith("{"):
                i = s.find("{")
                if i == -1:
                    continue
                s = s[i:]
            try:
                rows.append(json.loads(s))
            except Exception:
                continue

    by_role = {}
    for r in rows:
        role = r.get("role","")
        sc   = r.get("score",{}) or {}
        lat  = r.get("latency",{}) or {}
        by_role.setdefault(role, {"scores": [], "lat": []})
        by_role[role]["scores"].append(sc)
        if lat:
            # normalize expected keys; missing -> None
            by_role[role]["lat"].append({
                "total":   lat.get("total"),
                "Planner": lat.get("Planner"),
                "Reader":  lat.get("Reader"),
                "Mapper":  lat.get("Mapper"),
                "Writer":  lat.get("Writer"),
            })

    # Quality
    print("| System | N | Node F1 | Edge F1 | GCR |")
    print("|:--|--:|--:|--:|--:|")
    for role, grp in sorted(by_role.items()):
        arr = grp["scores"]
        n   = len(arr)
        nf  = _mean([x.get("f1_nodes") for x in arr])
        ef  = _mean([x.get("f1_edges") for x in arr])
        gcr = _mean([x.get("gcr")      for x in arr])
        print(f"| {role} | {n} | {nf:.3f} | {ef:.3f} | {gcr:.3f} |")

    # Latency (if present)
    any_lat = any(grp["lat"] for grp in by_role.values())
    if any_lat:
        print("\n| System | N | mean total (ms) | mean Planner | mean Reader | mean Mapper | mean Writer |")
        print("|:--|--:|--:|--:|--:|--:|--:|")
        for role, grp in sorted(by_role.items()):
            lat = grp["lat"]
            if not lat:
                print(f"| {role} | 0 |  |  |  |  |  |")
                continue
            n   = len(lat)
            m   = lambda k: _mean([e.get(k) for e in lat])
            print(f"| {role} | {n} | {m('total'):.1f} | {m('Planner'):.1f} | {m('Reader'):.1f} | {m('Mapper'):.1f} | {m('Writer'):.1f} |")

if __name__=="__main__":
    p = sys.argv[1] if len(sys.argv)>1 else "artifacts/bench_results.jsonl"
    main(p)
