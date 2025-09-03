from __future__ import annotations
import argparse
import yaml
import json
from pathlib import Path
from .config import Config
from .utils import disable_network
from .logging_db import RunLogger
from .scanner import Scanner
from .graph import Graph
from .agent_mapper import AgentMapper
from .graph import Graph, Edge
from .stats_cmd import summarize_graph, summarize_runs, print_graph_stats, print_run_stats  
from .env import load_env_file
from .metrics import score_pair

load_env_file()  # picks up OPENAI_API_KEY / AZURE_OPENAI_API_KEY from .env

try:
    from .agents import AgentRunner, llm_from_config
    HAS_AGENTS = True
except Exception:
    HAS_AGENTS = False

try:
    from .privacy import Redactor
except Exception:
    Redactor = None

try:
    from .llm_adapter import LLMClient, LLMConfig
    HAS_LLM = True
except Exception:
    HAS_LLM = False

def _write_outputs(out_dir: Path, g: Graph):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "graph.yaml").write_text(g.to_yaml(), encoding="utf-8")
    (out_dir / "graph.dot").write_text(g.to_dot(), encoding="utf-8")
    (out_dir / "predicted_graph.yaml").write_text(g.to_yaml(), encoding="utf-8")

def cmd_score(args):
    pred = yaml.safe_load(Path(args.pred).read_text(encoding="utf-8")) or {}
    truth = yaml.safe_load(Path(args.truth).read_text(encoding="utf-8")) or {}
    s = score_pair(pred, truth, case_sensitive=not args.case_insensitive)
    print(json.dumps(s.__dict__, indent=2))

def _llm_from_config(cfg):
    if not HAS_LLM:
        return None
    prov = getattr(cfg.llm, "provider", "disabled")
    if prov == "openai":
        return LLMClient(LLMConfig(
            provider="openai",
            model=getattr(cfg.llm, "model", "") or "gpt-5-mini",
            openai_base=(getattr(cfg.llm, "openai", {}) or {}).get("base_url", "https://api.openai.com"),
            temperature=getattr(cfg.llm, "temperature", None),
            max_tokens=getattr(cfg.llm, "max_tokens", None),
        ))
    if prov == "azure":
        az = getattr(cfg.llm, "azure", {}) or {}
        return LLMClient(LLMConfig(
            provider="azure",
            model=None,
            azure_endpoint=az.get("endpoint"),
            azure_deployment=az.get("deployment"),
            azure_api_version=az.get("api_version", "2025-08-01-preview"),
            temperature=getattr(cfg.llm, "temperature", None),
            max_tokens=getattr(cfg.llm, "max_tokens", None),
        ))
    return None

def cmd_scan(args):
    cfg = Config.load(args.config)
    if not cfg.runtime.egress:
        disable_network()
    logger = RunLogger(cfg.runtime.sqlite_path, echo=getattr(args, "verbose", False))
    logger.start(cmd="scan", config_hash=cfg.hash())
    try:
        scanner = Scanner(cfg.parsing.include_ext)
        g = scanner.scan(args.folder)
        _write_outputs(Path(args.out), g)
        logger.log(
            "INFO", f"Scanned {args.folder}; nodes={len(g.nodes)} edges={len(g.edges)}"
        )
    finally:
        logger.finish()


def cmd_map(args):
    cfg = Config.load(args.config)
    if not cfg.runtime.egress:
        disable_network()
    logger = RunLogger(cfg.runtime.sqlite_path, echo=getattr(args, "verbose", False))
    logger.start(cmd="map", config_hash=cfg.hash())
    try:
        yml = Path(args.graph_yaml).read_text(encoding="utf-8")
        data = yaml.safe_load(yml)
        g = Graph()
        for n in data.get("nodes", []):
            g.add_node(n)
        for e in data.get("edges", []):
            g.add_edge(Edge(
                src=e["src"],
                dst=e["dst"],
                kind=e.get("kind", "call"),
                command=e.get("command", ""),
                dynamic=e.get("dynamic", False),
                resolved=e.get("resolved", True),
                confidence=e.get("confidence", 0.9),
            ))
        agent = AgentMapper(client=_llm_from_config(cfg))
        # Use the YAML's directory as root when possible
        root_dir = Path(args.root or Path(args.graph_yaml).parent).resolve()
        g2 = agent.map_bundle(str(root_dir), g)
        _write_outputs(Path(args.out), g2)
        logger.log("INFO", f"Mapped dynamic edges; edges={len(g2.edges)}")
    finally:
        logger.finish()


def cmd_all(args):
    cfg = Config.load(args.config)
    if not cfg.runtime.egress:
        disable_network()
    logger = RunLogger(cfg.runtime.sqlite_path, echo=getattr(args, "verbose", False))
    logger.start(cmd="all", config_hash=cfg.hash())
    try:
        scanner = Scanner(cfg.parsing.include_ext)
        g = scanner.scan(args.folder)
        agent = AgentMapper(client=_llm_from_config(cfg))
        g2 = agent.map_bundle(args.folder, g)
        _write_outputs(Path(args.out), g2)
        logger.log(
            "INFO", f"Completed scan+map; nodes={len(g2.nodes)} edges={len(g2.edges)}"
        )
    finally:
        logger.finish()

def cmd_agents(args):
    if not HAS_AGENTS:
        raise SystemExit("Multi-agent module not found. Ensure scriptgraph/agents.py etc. are present.")
    cfg = Config.load(args.config)
    if not cfg.runtime.egress:
        disable_network()
    logger = RunLogger(cfg.runtime.sqlite_path, echo=getattr(args, "verbose", False))
    logger.start(cmd=f"agents-{args.roles}", config_hash=cfg.hash())
    try:
        scanner = Scanner(cfg.parsing.include_ext)
        g = scanner.scan(args.folder)
        client = llm_from_config(cfg)
        red = Redactor(cfg.privacy.redact_paths, cfg.privacy.redact_ips, cfg.privacy.redact_emails) if Redactor else None
        runner = AgentRunner(args.roles, client, logger,
                             log_prompts=bool(getattr(cfg.privacy, "log_prompts", False)),
                             redactor=red,
                             use_llm_reader_hints=False)  # keep OFF unless explicitly desired
        g2 = runner.run(args.folder, g, args.out)
        logger.log("INFO", f"agents {args.roles} finished; nodes={len(g2.nodes)} edges={len(g2.edges)}")
    finally:
        logger.finish()

def cmd_stats_graph(args):
    gs = summarize_graph(args.graph_yaml)
    if args.json:
        print(gs.to_json())
    else:
        print_graph_stats(gs)

def cmd_stats_runs(args):
    cfg = Config.load(args.config)
    ra = summarize_runs(cfg.runtime.sqlite_path)
    if args.json:
        print(ra.to_json())
    else:
        print_run_stats(ra)

def cmd_watch(args):
    import sqlite3, time, os
    cfg = Config.load(args.config)
    db = cfg.runtime.sqlite_path
    if not os.path.exists(db):
        print("No DB yet:", db); return
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    last_id = cur.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0]
    print("Watching", db, "from id", last_id)
    try:
        while True:
            rows = list(cur.execute(
                "SELECT id, ts, level, msg FROM events WHERE id > ? ORDER BY id ASC", (last_id,)
            ))
            for rid, ts, level, msg in rows:
                print(f"[{ts}] {level:5s} {msg}")
                last_id = rid
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

def main():
    p = argparse.ArgumentParser(prog="scriptgraph")
    p.add_argument("--verbose", action="store_true", help="Echo progress logs to the console")
    sub = p.add_subparsers(dest="cmd", required=True)

    # Scan command
    ps = sub.add_parser("scan", help="Static scan a folder")
    ps.add_argument("folder", help="Root folder to scan")
    ps.add_argument("--out", default="./out", help="Output directory")
    ps.add_argument("--config", default="config.example.yaml", help="Config file")
    ps.set_defaults(func=cmd_scan)

    # Map command
    pm = sub.add_parser("map", help="Map dynamic edges in a graph")
    pm.add_argument("graph_yaml", help="Input graph YAML file")
    pm.add_argument("--root", help="Root directory for mapping")
    pm.add_argument("--out", default="./out", help="Output directory")
    pm.add_argument("--config", default="config.example.yaml", help="Config file")
    pm.set_defaults(func=cmd_map)

    # All command
    pa = sub.add_parser("all", help="Run scan+map pipeline")
    pa.add_argument("folder", help="Root folder to scan")
    pa.add_argument("--out", default="./out", help="Output directory")
    pa.add_argument("--config", default="config.example.yaml", help="Config file")
    pa.set_defaults(func=cmd_all)

    # stats parent
    pstats = sub.add_parser("stats", help="Show statistics for a graph or for logged runs")
    ssub = pstats.add_subparsers(dest="target", required=True)

    # stats graph
    pg = ssub.add_parser("graph", help="Summarize a graph.yaml")
    pg.add_argument("graph_yaml", help="Path to graph.yaml")
    pg.add_argument("--json", action="store_true", help="Output JSON")
    pg.set_defaults(func=cmd_stats_graph)

    # stats runs
    pr = ssub.add_parser("runs", help="Summarize run durations from SQLite")
    pr.add_argument("--config", default="config.example.yaml", help="Config file (for sqlite path)")
    pr.add_argument("--json", action="store_true", help="Output JSON")
    pr.set_defaults(func=cmd_stats_runs)

    pw = sub.add_parser("watch", help="Tail run progress from SQLite")
    pw.add_argument("--config", default="config.example.yaml", help="Config file")
    pw.set_defaults(func=cmd_watch)

    # Score command
    pscr = sub.add_parser("score", help="Score predicted_graph.yaml vs. ground truth")
    pscr.add_argument("--pred", required=True, help="Path to predicted_graph.yaml")
    pscr.add_argument("--truth", required=True, help="Path to truth graph.yaml")
    pscr.add_argument("--case-insensitive", action="store_true", help="Windows-only bundles")
    pscr.set_defaults(func=cmd_score)

    # multi-agent subcommand (only shows if module present)
    if HAS_AGENTS:
        pag = sub.add_parser("agents", help="Run multi-role agent pipeline (2R or 4R)")
        pag.add_argument("folder", help="Root folder to scan")
        pag.add_argument("--roles", default="4R", choices=["2R", "4R"], help="Agent configuration")
        pag.add_argument("--out", default="./out", help="Output directory")
        pag.add_argument("--config", default="config.example.yaml", help="Config file")
        pag.set_defaults(func=cmd_agents)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
