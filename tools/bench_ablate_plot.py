#!/usr/bin/env python3
import argparse, json, math
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def load_jsonl(p: Path) -> list[dict]:
    rows = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or not s.startswith("{"): 
            continue
        try:
            rows.append(json.loads(s))
        except Exception:
            pass
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    rows = load_jsonl(Path(args.jsonl))
    if not rows:
        raise SystemExit("no rows")

    # Flatten score.* keys into top-level columns
    flat = []
    for r in rows:
        sc = r.get("score") or {}
        rec = {
            "bundle": r.get("bundle"),
            "role": r.get("role"),
            "max_files": r.get("max_files"),
            "f1_edges": sc.get("f1_edges"),
            "f1_nodes": sc.get("f1_nodes"),
            "precision_edges": sc.get("precision_edges"),
            "recall_edges": sc.get("recall_edges"),
            "gcr": sc.get("gcr"),
            "total_ms": r.get("total_ms"),
        }
        flat.append(rec)
    df = pd.DataFrame.from_records(flat)
    df = df.dropna(subset=["role","max_files"])

    # Group means
    grp = df.groupby(["role","max_files"]).agg(
        mean_f1_edges=("f1_edges","mean"),
        mean_total_ms=("total_ms","mean"),
        n=("bundle","count"),
    ).reset_index()

    # Pivot for CSV
    piv_f1 = grp.pivot_table(index="max_files", columns="role", values="mean_f1_edges")
    piv_lat= grp.pivot_table(index="max_files", columns="role", values="mean_total_ms")
    piv = pd.concat({"f1_edges":piv_f1, "total_ms":piv_lat}, axis=1)
    piv.to_csv(out_dir/"ablate_by_budget.csv")

    # Markdown summary table (F1 only)
    md_lines = ["# Ablation: Mean F1_edges by max_files\n",
                "| max_files | " + " | ".join(piv_f1.columns) + " |",
                "|:--|" + "|".join([":--:" for _ in piv_f1.columns]) + "|"]
    for mf, row in piv_f1.iterrows():
        md_lines.append("| {} | {} |".format(mf, " | ".join(f"{row[c]:.3f}" if not math.isnan(row[c]) else "NA" for c in piv_f1.columns)))
    (out_dir/"ablate_summary.md").write_text("\n".join(md_lines), encoding="utf-8")

    # Plot F1 vs budget
    plt.figure()
    for role, sub in grp.groupby("role"):
        sub = sub.sort_values("max_files")
        plt.plot(sub["max_files"], sub["mean_f1_edges"], marker="o", label=role)
    plt.xlabel("max_files (Reader budget)")
    plt.ylabel("Mean F1_edges")
    plt.title("F1 vs Reader budget")
    plt.legend()
    plt.grid(True, linestyle=":")
    plt.tight_layout()
    plt.savefig(out_dir/"ablate_f1_vs_budget.png", dpi=150)

    # Plot latency vs budget (only 2R/4R have latencies; static will often be NA)
    plt.figure()
    for role, sub in grp.groupby("role"):
        sub = sub.sort_values("max_files")
        plt.plot(sub["max_files"], sub["mean_total_ms"], marker="o", label=role)
    plt.xlabel("max_files (Reader budget)")
    plt.ylabel("Mean total latency (ms)")
    plt.title("Latency vs Reader budget")
    plt.legend()
    plt.grid(True, linestyle=":")
    plt.tight_layout()
    plt.savefig(out_dir/"ablate_latency_vs_budget.png", dpi=150)

if __name__ == "__main__":
    main()
