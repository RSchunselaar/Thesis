# ScriptGraph (MVP)

Discover **who-calls-whom** across legacy script folders (sh/bash, batch, PowerShell, Perl, CLI-style Python). Hybrid static+agent approach, with strict **no-egress** by default.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate # Windows: .venv\\Scripts\\activate
pip install -e .
scriptgraph scan examples/bundle1 --out out --config config.example.yaml
scriptgraph map out/graph.yaml --out out
```
