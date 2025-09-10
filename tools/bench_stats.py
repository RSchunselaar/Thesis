#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import math
import random
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats


# ----------------------------- IO ---------------------------------

def _load_jsonl(path: str) -> List[dict]:
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            # Some pipelines write log lines like "Wrote artifacts/bench_results.jsonl"
            # Try strict parse first; otherwise skip lines that are not JSON.
            if not s.startswith("{"):
                # Try to salvage JSON starting from the first '{'
                i = s.find("{")
                if i == -1:
                    continue
                s = s[i:]
            try:
                obj = json.loads(s)
                rows.append(obj)
            except Exception:
                # Not a JSON object; skip
                continue
    return rows


# ----------------------------- Stats helpers ---------------------------------

def _paired_t(y: np.ndarray, x: np.ndarray) -> Tuple[float, float]:
    # returns t-statistic and two-sided p-value
    if len(y) < 2:
        return (float("nan"), float("nan"))
    t, p = stats.ttest_rel(y, x, nan_policy="omit")
    return (float(t), float(p))


def _wilcoxon_signed(y: np.ndarray, x: np.ndarray) -> Tuple[float, float]:
    # SciPy's wilcoxon fails when all diffs are zero; handle gracefully.
    d = y - x
    d = d[~np.isnan(d)]
    if len(d) == 0 or np.allclose(d, 0.0):
        return (0.0, 1.0)
    # 'pratt' includes zeros in ranking but not in W; robust default
    try:
        w, p = stats.wilcoxon(d, zero_method="pratt", alternative="two-sided", correction=False, mode="auto")
        return (float(w), float(p))
    except Exception:
        return (float("nan"), float("nan"))


def _cohen_dz(d: np.ndarray) -> float:
    # Cohen's d for paired samples: mean(diff) / sd(diff)
    d = d[~np.isnan(d)]
    if len(d) < 2:
        return float("nan")
    sd = np.std(d, ddof=1)
    return float(np.mean(d) / sd) if sd > 0 else float("nan")


def _rank_biserial(d: np.ndarray) -> Tuple[float, float]:
    """
    Paired rank-biserial correlation derived from sign of differences:
      r_rb = (n_pos - n_neg) / (n_pos + n_neg)
    Also return win-rate = n_pos / (n_pos + n_neg).
    Zeros (ties) are ignored in denominator.
    """
    d = d[~np.isnan(d)]
    if d.size == 0:
        return float("nan"), float("nan")
    n_pos = int(np.sum(d > 0))
    n_neg = int(np.sum(d < 0))
    denom = n_pos + n_neg
    if denom == 0:
        return 0.0, 0.5  # all ties
    r_rb = (n_pos - n_neg) / denom
    win_rate = n_pos / denom
    return float(r_rb), float(win_rate)


def _bootstrap_ci_mean(d: np.ndarray, n_boot: int = 10000, seed: int = 0, alpha: float = 0.05) -> Tuple[float, float]:
    d = d[~np.isnan(d)]
    if len(d) == 0:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(d), size=(n_boot, len(d)))
    boots = np.mean(d[idx], axis=1)
    lo = float(np.percentile(boots, 100 * (alpha / 2)))
    hi = float(np.percentile(boots, 100 * (1 - alpha / 2)))
    return (lo, hi)


def _holm_bonferroni(pvals: List[Tuple[str, float]]) -> Dict[str, float]:
    """
    Holm-Bonferroni step-down adjustment.
    pvals: list of (key, p)
    returns dict key -> p_adj
    """
    # Filter NaNs
    valid = [(k, p) for k, p in pvals if (p is not None and not math.isnan(p))]
    m = len(valid)
    if m == 0:
        return {k: float("nan") for k, _ in pvals}
    # sort ascending by p
    sorted_idx = sorted(range(m), key=lambda i: valid[i][1])
    adj = {}
    min_so_far = 1.0
    for rank, i in enumerate(sorted_idx, start=1):
        k, p = valid[i]
        padj = (m - rank + 1) * p
        padj = min(1.0, padj)
        # ensure monotonicity
        min_so_far = max(min_so_far, padj) if rank == 1 else max(min_so_far, padj)
        adj[k] = padj
    # put back NaNs for invalid
    out = {k: adj.get(k, float("nan")) for k, _ in pvals}
    return out


# ----------------------------- Core analysis ---------------------------------

def analyze(df: pd.DataFrame, metric_key: str, alpha: float, n_boot: int, seed: int, systems: List[str] | None) -> str:
    """
    df columns: bundle, role, score.<metric_key>
    """
    # Pivot: bundles x roles
    pivot = df.pivot_table(index="bundle", columns="role", values=metric_key, aggfunc="first")
    roles = list(pivot.columns) if not systems else [r for r in systems if r in pivot.columns]
    if len(roles) < 2:
        return "Not enough systems to compare.\n"

    # Per-system summary
    lines = []
    lines.append(f"# Bench Statistics ({metric_key})")
    lines.append("")
    lines.append("## Per-system summary (over bundles)")
    lines.append("| System | N | Mean | Median | Std | Min | Max |")
    lines.append("|:--|--:|--:|--:|--:|--:|--:|")
    for r in roles:
        x = pivot[r].dropna().to_numpy()
        if x.size == 0:
            lines.append(f"| {r} | 0 |  |  |  |  |  |")
            continue
        lines.append(f"| {r} | {x.size} | {np.mean(x):.3f} | {np.median(x):.3f} | {np.std(x, ddof=1):.3f} | {np.min(x):.3f} | {np.max(x):.3f} |")
    lines.append("")

    # --- (optional) latency summary if present ---
    if "lat_total_ms" not in df.columns:
        # flatten latency dict if present in rows
        if "latency" in df.columns:
            # already flattened by caller; skip
            pass
        else:
            # nothing to do
            pass
    else:
        lines.append("## Per-system runtime (total ms)")
        lines.append("| System | N | Mean | Median | Std | Min | Max |")
        lines.append("|:--|--:|--:|--:|--:|--:|--:|")
        for r in roles:
            x = pivot.join(df[df["role"]==r].set_index("bundle")["lat_total_ms"], how="left")["lat_total_ms"].dropna().to_numpy()
            if x.size == 0:
                lines.append(f"| {r} | 0 |  |  |  |  |  |")
                continue
            lines.append(f"| {r} | {x.size} | {np.mean(x):.1f} | {np.median(x):.1f} | {np.std(x, ddof=1):.1f} | {np.min(x):.1f} | {np.max(x):.1f} |")
        lines.append("")

    # Pairwise tests
    lines.append("## Pairwise comparisons (paired, two-sided)")
    header = "| A vs B | N | mean(B-A) | t | p_t | W | p_w | p_t_adj | p_w_adj | Cohen_dz | r_rb | win_rate(B>A) | 95% CI mean(B-A) |"
    lines.append(header)
    lines.append("|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")

    # Collect p-values for Holm per test family
    pvals_t_keys: List[Tuple[str, float]] = []
    pvals_w_keys: List[Tuple[str, float]] = []

    pair_rows = []
    for a, b in combinations(roles, 2):
        # matched bundles
        sub = pivot[[a, b]].dropna()
        n = len(sub)
        if n < 2:
            row = (a, b, n, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, ("nan", "nan"))
            pair_rows.append(row)
            continue
        x = sub[a].to_numpy()
        y = sub[b].to_numpy()
        d = y - x

        mean_diff = float(np.mean(d))
        t, p_t = _paired_t(y, x)
        w, p_w = _wilcoxon_signed(y, x)
        dz = _cohen_dz(d)
        r_rb, win_rate = _rank_biserial(d)
        lo, hi = _bootstrap_ci_mean(d, n_boot=n_boot, seed=seed, alpha=alpha)

        key_t = f"{a}__{b}__t"
        key_w = f"{a}__{b}__w"
        pvals_t_keys.append((key_t, p_t))
        pvals_w_keys.append((key_w, p_w))

        pair_rows.append((a, b, n, mean_diff, t, p_t, w, p_w, key_t, key_w, dz, r_rb, win_rate, (lo, hi)))

    # Adjust with Holm separately for t and Wilcoxon families
    p_t_adj_map = _holm_bonferroni(pvals_t_keys)
    p_w_adj_map = _holm_bonferroni(pvals_w_keys)

    for row in pair_rows:
        a, b, n, mean_diff, t, p_t, w, p_w, key_t, key_w, dz, r_rb, win_rate, ci = row
        lo, hi = ci if isinstance(ci, tuple) else (float("nan"), float("nan"))
        padj_t = p_t_adj_map.get(key_t, float("nan"))
        padj_w = p_w_adj_map.get(key_w, float("nan"))
        lines.append(
            f"| {a} vs {b} | {n} | {mean_diff:.3f} | "
            f"{_fmt(t)} | {_fmt(p_t)} | {_fmt(w)} | {_fmt(p_w)} | {_fmt(padj_t)} | {_fmt(padj_w)} | "
            f"{_fmt(dz)} | {_fmt(r_rb)} | {_fmt(win_rate)} | [{_fmt(lo)},{_fmt(hi)}] |"
        )

    lines.append("")
    lines.append("Notes:")
    lines.append("- mean(B-A) > 0 means system B outperforms A on the chosen metric.")
    lines.append("- Cohen_dz is effect size for paired samples (mean diff / sd diff).")
    lines.append("- r_rb is the paired rank-biserial correlation; win_rate is fraction of bundles where B > A among non-ties.")
    lines.append("- p-values adjusted with Holm-Bonferroni per test family (t-tests, Wilcoxon) across all pairs.")
    return "\n".join(lines)


def _fmt(x) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "NA"
    try:
        return f"{float(x):.3g}"
    except Exception:
        return str(x)


# ----------------------------- CLI ---------------------------------

def main():
    ap = argparse.ArgumentParser(description="Statistical comparisons of systems over bundles (paired tests).")
    ap.add_argument("jsonl", help="Path to artifacts/bench_results.jsonl")
    ap.add_argument("--out", help="Write Markdown report to this path (default: stdout)")
    ap.add_argument("--csv", help="Optional: write per-bundle wide CSV for the chosen metric")
    ap.add_argument("--metric", default="f1_edges",
                    help="Metric key from score: f1_edges (default), f1_nodes, precision_edges, recall_edges, precision_nodes, recall_nodes, gcr")
    ap.add_argument("--alpha", type=float, default=0.05, help="Alpha for bootstrap CI (default 0.05 => 95%% CI)")
    ap.add_argument("--boot", type=int, default=10000, help="Bootstrap resamples for CI (default 10000)")
    ap.add_argument("--seed", type=int, default=0, help="Random seed for bootstrap")
    ap.add_argument("--systems", nargs="*", help="Optional ordered list of systems to include (e.g., static 2R 4R)")
    args = ap.parse_args()

    rows = _load_jsonl(args.jsonl)
    if not rows:
        sys.exit("No JSON rows found in input.")

    # Flatten to DataFrame
    recs = []
    for r in rows:
        bundle = r.get("bundle")
        role = r.get("role")
        sc = r.get("score", {}) or {}
        if not bundle or not role:
            continue
        # Normalize keys to a consistent set
        rec = {
            "bundle": str(bundle),
            "role": str(role),
        }
        for k, v in sc.items():
            # keep only numeric
            if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
                rec[k] = float(v)
        recs.append(rec)

        lat = r.get("latency") or {}
        if isinstance(lat, dict):
            rec["lat_total_ms"]   = float(lat.get("total")) if lat.get("total") is not None else np.nan
            rec["lat_reader_ms"]  = float(lat.get("Reader")) if lat.get("Reader") is not None else np.nan
            rec["lat_mapper_ms"]  = float(lat.get("Mapper")) if lat.get("Mapper") is not None else np.nan
            rec["lat_writer_ms"]  = float(lat.get("Writer")) if lat.get("Writer") is not None else np.nan
            rec["lat_planner_ms"] = float(lat.get("Planner")) if lat.get("Planner") is not None else np.nan

    if not recs:
        sys.exit("No usable records found (missing bundle/role/score).")

    df = pd.DataFrame.from_records(recs)
    metric_key = args.metric
    if metric_key not in df.columns:
        # allow synonyms
        alt = f"score.{metric_key}"
        if alt in df.columns:
            metric_key = alt
        else:
            avail = ", ".join(sorted([c for c in df.columns if c not in ("bundle", "role")]))
            sys.exit(f"Metric '{args.metric}' not found. Available: {avail}")

    # Optional CSV of the wide table (for your appendix)
    pivot = df.pivot_table(index="bundle", columns="role", values=metric_key, aggfunc="first")
    pivot = pivot.sort_index()
    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        pivot.to_csv(args.csv, index=True)

    report = analyze(df[["bundle", "role", metric_key]].copy(), metric_key, args.alpha, args.boot, args.seed, args.systems)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Wrote {args.out}")
    else:
        print(report)


if __name__ == "__main__":
    main()
