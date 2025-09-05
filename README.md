# ScriptGraph (MVP)

Discover **who-calls-whom** across legacy script folders (sh/bash, batch, PowerShell, Perl, CLI-style Python). Hybrid static+agent approach, with strict **no-egress** by default.

## Quickstart

### Reproducibility (prompts & model)

Each run writes `out/llm_specs.json` with the exact LLM config and the static prompt templates
(agents + agent_mapper). This, together with `tools/export_prompts.py` (which dumps the **actual**
prompts used during the run from SQLite), lets you include the required AI‑prompt appendix in the thesis.

```bash
python -m venv .venv && source .venv/bin/activate # Windows: .venv\\Scripts\\activate
pip install -e .
# No‑egress is the default (see config.example.yaml). Enable per experiment if needed.
scriptgraph scan data/bundles/examples/bundle1 --out out --config config.example.yaml
scriptgraph map out/graph.yaml --out out --config config.example.yaml
# (when you have ground truth) score predictions
scriptgraph score --pred out/predicted_graph.yaml --truth data/bundles/examples/bundle1/truth.yaml

### Seeds (planner hints)
If a bundle contains a `seeds.txt` (one relative path per line), the Planner/2R will prioritize those files (e.g., `run.sh`, `windows/main.bat`).

### Bench summary table
py tools\\bench_table.py artifacts\\bench_results.jsonl > artifacts\\bench_table.md

### Generate Statistical method statistics
py tools\bench_stats.py artifacts\bench_results.jsonl `
  --metric f1_edges `
  --out artifacts\bench_stats_f1_edges.md `
  --csv artifacts\per_bundle_f1_edges.csv `
  --boot 20000 --alpha 0.05 --seed 42
py tools\bench_stats.py artifacts\bench_results.jsonl `
  --metric f1_nodes `
  --out artifacts\bench_stats_f1_nodes.md `
  --csv artifacts\per_bundle_f1_nodes.csv `
  --boot 20000 --alpha 0.05 --seed 42
py tools\bench_stats.py artifacts\bench_results.jsonl `
  --metric gcr `
  --out artifacts\bench_stats_gcr.md `
  --csv artifacts\per_bundle_gcr.csv `
  --boot 20000 --alpha 0.05 --seed 42
```
