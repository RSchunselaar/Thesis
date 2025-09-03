import sqlite3, json, sys, os
db = sys.argv[1] if len(sys.argv) > 1 else "./out/runlog.sqlite"
out = sys.argv[2] if len(sys.argv) > 2 else "./out/prompts.jsonl"
os.makedirs(os.path.dirname(out), exist_ok=True)
conn = sqlite3.connect(db)
cur = conn.execute("SELECT role, prompt FROM llm_prompts ORDER BY id ASC")
with open(out, "w", encoding="utf-8") as f:
    for role, prompt in cur:
        f.write(json.dumps({"role": role, "prompt": prompt}, ensure_ascii=False) + "\n")
print(f"Wrote {out}")