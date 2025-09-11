import sqlite3, json, sys, os

"""
Usage:
  py -3 tools/export_prompts.py out/runlog.sqlite out/prompts.jsonl [out/llm_stats.json]
Writes:
  1) JSONL of prompts (role,prompt)
  2) Optional aggregated stats per role (counts, mean/median prompt length; if llm_calls exists: tokens, latency)
"""

db = sys.argv[1] if len(sys.argv) > 1 else "./out/runlog.sqlite"
out_prompts = sys.argv[2] if len(sys.argv) > 2 else "./out/prompts.jsonl"
out_stats = sys.argv[3] if len(sys.argv) > 3 else None
os.makedirs(os.path.dirname(out_prompts), exist_ok=True)

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

# 1) dump prompts
cur = conn.execute("SELECT role, prompt FROM llm_prompts ORDER BY id ASC")
with open(out_prompts, "w", encoding="utf-8") as f:
    for row in cur:
        role = row["role"]; prompt = row["prompt"]
        f.write(json.dumps({"role": role, "prompt": prompt}, ensure_ascii=False) + "\n")
print(f"Wrote {out_prompts}")

# 2) optional aggregate stats
if out_stats:
    stats = {}
    # Prompt lengths
    cur = conn.execute("SELECT role, LENGTH(prompt) AS plen FROM llm_prompts")
    by_role = {}
    for r, plen in cur.fetchall():
        by_role.setdefault(r, []).append(plen or 0)
    for role, arr in by_role.items():
        arr = [int(x) for x in arr]
        arr_sorted = sorted(arr)
        if arr_sorted:
            mid = len(arr_sorted) // 2
            median = (arr_sorted[mid] if len(arr_sorted)%2==1 else (arr_sorted[mid-1]+arr_sorted[mid])/2)
        else:
            median = 0.0
        stats.setdefault(role, {})["count_prompts"] = len(arr_sorted)
        stats[role]["prompt_len_mean"] = float(sum(arr_sorted)/len(arr_sorted)) if arr_sorted else 0.0
        stats[role]["prompt_len_median"] = float(median)

    # If llm_calls table exists, pull tokens + latency
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='llm_calls'")
        if cur.fetchone():
            cur2 = conn.execute("""
                SELECT role, 
                       COALESCE(input_tokens,0) AS in_tok, 
                       COALESCE(completion_tokens,0) AS out_tok,
                       COALESCE(total_tokens,0) AS tot_tok,
                       COALESCE(latency_ms,0.0) AS lat_ms
                FROM llm_calls
            """)
            agg = {}
            for row in cur2.fetchall():
                r = row["role"]
                agg.setdefault(r, {"in":[], "out":[], "tot":[], "lat":[]})
                agg[r]["in"].append(float(row["in_tok"]))
                agg[r]["out"].append(float(row["out_tok"]))
                agg[r]["tot"].append(float(row["tot_tok"]))
                agg[r]["lat"].append(float(row["lat_ms"]))
            for r, d in agg.items():
                def _m(xs): return float(sum(xs)/len(xs)) if xs else 0.0
                xs = stats.setdefault(r, {})
                xs["calls"] = len(d["tot"])
                xs["mean_prompt_tokens"] = _m(d["in"])
                xs["mean_completion_tokens"] = _m(d["out"])
                xs["mean_total_tokens"] = _m(d["tot"])
                xs["mean_latency_ms"] = _m(d["lat"])
    except Exception:
        pass

    os.makedirs(os.path.dirname(out_stats), exist_ok=True)
    with open(out_stats, "w", encoding="utf-8") as f:
        f.write(json.dumps({"by_role": stats}, indent=2))
    print(f"Wrote {out_stats}")