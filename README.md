# ScriptGraph (MVP)

Discover **who-calls-whom** across legacy script folders (sh/bash, batch, PowerShell, Perl, CLI-style Python). Hybrid static+agent approach, with strict **no-egress** by default.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate # Windows: .venv\\Scripts\\activate
pip install -e .
# No‑egress is the default (see config.example.yaml). Enable per experiment if needed.
scriptgraph scan data/bundles/examples/bundle1 --out out --config config.example.yaml
scriptgraph map out/graph.yaml --out out --config config.example.yaml
# (when you have ground truth) score predictions
scriptgraph score --pred out/predicted_graph.yaml --truth data/bundles/examples/bundle1/truth.yaml --pred-prefix data/bundles/examples/bundle1

### Seeds (planner hints)
If a bundle contains a `seeds.txt` (one relative path per line), the Planner/2R will prioritize those files (e.g., `run.sh`, `windows/main.bat`).

### Bench summary table
py tools\\bench_table.py artifacts\\bench_results.jsonl > artifacts\\bench_table.md
```
