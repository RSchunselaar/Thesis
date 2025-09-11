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
    by_bundle_role = {}   # bundle -> role -> score dict
    tiers = {}            # bundle -> tier
    def _tier(b):
        b = str(b).lower()
        return "easy" if b.startswith("easy-") else ("hard" if b.startswith("hard-") else "unknown")

    for r in rows:
        role = r.get("role","")
        bundle = r.get("bundle","")
        tiers[bundle] = _tier(bundle)
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
        # for win-rate computations
        by_bundle_role.setdefault(bundle, {})[role] = sc

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

    # --- By-tier summaries (Edge F1 & GCR) ---
    print("\n## By-tier summary (Edge F1 and GCR)")
    print("| Tier | System | N | mean Edge F1 | mean GCR |")
    print("|:--|:--|--:|--:|--:|")
    roles = sorted(by_role.keys())
    for tier in ("easy", "hard"):
        for role in roles:
            vals = []
            gcrs = []
            n = 0
            for b, sysmap in by_bundle_role.items():
                if tiers.get(b) != tier: 
                    continue
                sc = sysmap.get(role)
                if sc is None: 
                    continue
                vals.append(sc.get("f1_edges"))
                gcrs.append(sc.get("gcr"))
                n += 1
            if n > 0:
                print(f"| {tier} | {role} | {n} | {_mean(vals):.3f} | {_mean(gcrs):.3f} |")

    # --- Win-rates vs static on Edge F1 & GCR (ties excluded) ---
    print("\n## Win-rates vs static (ties excluded)")
    print("| Tier | B vs A | N | win_rate(B>A) on Edge F1 | win_rate(B>A) on GCR |")
    print("|:--|:--|--:|--:|--:|")
    for tier in ("easy", "hard"):
        for b in ("2R", "4R"):
            n_e = n_g = 0
            w_e = w_g = 0
            for bundle, sysmap in by_bundle_role.items():
                if tiers.get(bundle) != tier:
                    continue
                a_sc = sysmap.get("static")
                b_sc = sysmap.get(b)
                if not a_sc or not b_sc:
                    continue
                # Edge F1
                ea, eb = a_sc.get("f1_edges"), b_sc.get("f1_edges")
                if ea is not None and eb is not None and eb != ea:
                    n_e += 1
                    if eb > ea: w_e += 1
                # GCR
                ga, gb = a_sc.get("gcr"), b_sc.get("gcr")
                if ga is not None and gb is not None and gb != ga:
                    n_g += 1
                    if gb > ga: w_g += 1
            wr_e = (w_e / n_e) if n_e else 0.0
            wr_g = (w_g / n_g) if n_g else 0.0
            print(f"| {tier} | {b} vs static | {n_e} | {wr_e:.3f} | {wr_g:.3f} |")


if __name__=="__main__":
    p = sys.argv[1] if len(sys.argv)>1 else "artifacts/bench_results.jsonl"
    main(p)
