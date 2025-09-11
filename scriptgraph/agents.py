from __future__ import annotations
import json
import re
import time
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Dict, List, Tuple, Optional
from .logging_db import RunLogger
from .graph import Graph, Edge
from .exporter import write_artifacts
from .scanner import Scanner
from .llm_adapter import LLMClient, LLMConfig
from typing import Any


# Optional redactor (safe if missing)
try:
    from .privacy import Redactor
except Exception:  # pragma: no cover
    class Redactor:  # type: ignore
        def __init__(self, *a, **k): pass
        def redact(self, s: str) -> str: return s

@dataclass
class ReadManifest:
    files: list[dict]                 # [{"path": "run.sh", "priority": 100, "peek": [(0, 4096)]}, ...]
    env_hints: dict[str, str]         # seed env (global)
    policy: dict[str, Any]            # normalization policy
    budget: dict[str, int]            # {"max_tool_calls": 50, "max_latency_ms": 60000, "max_loops": 1, "max_files": 0}
    worklist: list[str] = field(default_factory=list)            # NEW: prioritized sources to read first

@dataclass
class ObservationBatch:
    files: list[dict]                 # [{"path":"run.sh","lang":"sh","size":1234,"hash":""}]
    env_vars: list[dict]              # [{"scope":"run.sh","name":"UTILS","value":"./utils","prec":10}]
    call_sites: list[dict]            # [{"src":"run.sh","raw":"${UTILS}/cleanup.sh","kind":"source","line":12,"col":3,"span":[120,155],"dynamic":1,"conf":0.7}]

@dataclass
class GraphSnapshot:
    graph: Graph
    unresolved: list[dict]            # [{"src":"x","raw":"y","reason":"..."}]
    coverage: dict[str, Any]          # {"touched": n, "total": m}

# -------------------- Prompts --------------------

PLANNER_PROMPT = (
    "ROLE: Planner (orchestrator & budgeter)\n"
    "OBJECTIVE: Choose the best order of SOURCE FILES to process so we reach a complete dependency graph with "
    "minimal tool calls/latency.\n"
    "INPUT: A JSON object {\"unresolved\": [{\"src\":\"<path>\", \"command\":\"<raw cmd>\"}, ...]}\n"
    "CONSTRAINTS:\n"
    " - Prefer sources whose commands have concrete paths and few variables.\n"
    " - De-prioritize sources whose commands are very dynamic (many ${VAR}, $VAR, %VAR%).\n"
    " - Be conservative—if uncertain, include fewer items rather than more.\n"
    "OUTPUT (STRICT JSON): {\"worklist\":[\"<src1>\", \"<src2>\", ...], \"reasoning\":\"<why>\"}\n"
    "NOTES: Only return 'worklist' and 'reasoning'. No extra keys, no prose outside JSON.\n"
)

READER_PROMPT = (
    "ROLE: Reader (evidence collector)\n"
    "OBJECTIVE: From the given script SNIPPET, infer path-relevant variables/aliases for dependency resolution.\n"
    "FOCUS: Only variables that influence file paths (e.g., UTILS=./utils, SCRIPTS=../bin). Ignore unrelated values.\n"
    "FORMAT RESTRICTIONS: Values must match [A-Za-z0-9_./-].\n"
    "OUTPUT (STRICT JSON): {\"hints\": {\"VAR\":\"value\", ...}, \"reasoning\":\"<why>\"}\n"
    "BE CONSERVATIVE: If unsure, leave 'hints' empty. Never invent paths or variables.\n"
)

MAPPER_PROMPT = (
    "ROLE: Mapper (resolver & graph builder)\n"
    "OBJECTIVE: Resolve the target script path(s) for a given command line, relative to the project root.\n"
    "YOU RECEIVE (as user JSON): {\"root\":\"<root>\", \"src\":\"<src file>\", \"command\":\"<cmd line>\", "
    "\"hints\": {VAR: value, ...}, \"allowed_paths\":[\"...\" (optional)], "
    "\"observations\": {\"src_snippet\":\"...\", \"dir_listings\": {\"utils\":[\"utils/cleanup.sh\", ...]}} (optional)}\n"
    "RESOLUTION RULES:\n"
    " - Apply variable expansion (${VAR}, $VAR, %VAR%) using provided 'hints'.\n"
    " - Normalize slashes to '/'; strip leading './' when possible; return paths relative to 'root'.\n"
    " - Consider only plausible script files (.sh,.bash,.ksh,.bat,.cmd,.ps1,.pl,.py).\n"
    " - IF 'allowed_paths' is provided, choose only from that list; otherwise be conservative.\n"
    " - IF 'observations' are present, use them to refine your choice, but still obey 'allowed_paths'.\n"
    "OUTPUT (STRICT JSON): {\"targets\":[\"relative/path\", ...], \"reasoning\":\"<brief why>\"}\n"
    "FAIL SAFE: If uncertain, return an empty 'targets' list (do not guess).\n"
)

WRITER_PROMPT = (
    "ROLE: Writer (validator & exporter — human summary)\n"
    "OBJECTIVE: Given a small JSON summary (nodes/edges/unresolved counts), write 5–8 crisp bullets for a run report.\n"
    "STYLE: No intro/outro; just bullets. Mention unresolved/dynamic edges if any and next best actions.\n"
    "OUTPUT: Plain text bullets (one per line, starting with '- ').\n"
)

# -------------------- Utilities --------------------

SAFE_VAL = re.compile(r"^[A-Za-z0-9_./-]+$")

def _json_load(s: str) -> dict:
    try:
        return json.loads(s)
    except Exception:
        return {}

def _clean_val(v: str) -> Optional[str]:
    v = (v or "").strip().strip('"').strip("'")
    return v if SAFE_VAL.match(v) else None

def _norm_path(p: str) -> str:
    p = (p or "").strip().strip('"').strip("'").replace("\\", "/")
    if p.startswith("./"): p = p[2:]
    while "//" in p: p = p.replace("//", "/")
    return p

def _write_run_stats(out_dir: Path, roles: str, lat_ms: dict[str, float], g: Graph, unresolved: list[dict], coverage: dict = None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "system": roles,
        "latency_ms": {k: int(v) for k, v in lat_ms.items()},  # ints for easy CSV/plots
        "nodes": len(g.nodes),
        "edges": len(g.edges),
        "unresolved": len(unresolved),
    }
    # Add coverage data if provided
    if coverage:
        payload["coverage"] = coverage
        payload["unresolved_details"] = unresolved[:50]  # Limit to first 50 for performance
    
    (out_dir / "run_stats.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

# -------------------- LLM wrappers --------------------

def _llm_plan(client: Optional[LLMClient], edges: List[Tuple[str, str]]) -> Tuple[List[str], str]:
    if not client or not edges:
        return [], ""
    # Trim to a small prompt
    items = [{"src": s, "command": c} for s, c in edges[:40]]
    user = json.dumps({"unresolved": items}, ensure_ascii=False)
    content, meta = client.chat(PLANNER_PROMPT, user, return_meta=True)
    data = _json_load(content)
    wl = data.get("worklist") or []
    why = data.get("reasoning", "")
    # Keep only sources that actually exist in the candidate set
    cand = {s for s, _ in edges}
    ordered = [s for s in wl if isinstance(s, str) and s in cand]
    return ordered, why

def _llm_read_hints(client: Optional[LLMClient], src_path: str, snippet: str, *, redactor: Optional[Redactor] = None) -> Tuple[Dict[str, str], str, dict, str]:
    if not client or not snippet.strip():
        return {}, "", {}, ""
    red = redactor or Redactor()
    user = json.dumps({"source": src_path, "snippet": red.redact(snippet[:4000])}, ensure_ascii=False)
    content, meta = client.chat(READER_PROMPT, user, return_meta=True)
    data = _json_load(content)
    hints = {}
    d = data.get("hints") or {}
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(k, str) and isinstance(v, str):
                vv = _clean_val(v)
                if k and vv:
                    hints[k] = vv
    why = data.get("reasoning", "")
    return hints, why, (meta or {}), user

def _llm_map_targets(client: Optional[LLMClient], root: str, src: str, cmd: str, hints: Dict[str, str]) -> Tuple[List[str], str]:
    if not client:
        return [], ""
    user = json.dumps({"root": root, "src": src, "command": cmd, "hints": hints}, ensure_ascii=False)
    data = _json_load(client.chat(MAPPER_PROMPT, user))
    out = []
    for t in data.get("targets", []) or []:
        if isinstance(t, str):
            out.append(_norm_path(t))
    why = data.get("reasoning", "")
    return out, why

# -------------------- Roles --------------------

@dataclass
class RoleIO:
    root: Path
    graph: Graph

class Planner:
    def __init__(self, client: Optional[LLMClient] = None, logger: Optional[RunLogger] = None):
        self.client = client; self.logger = logger

    def _lang(self, p: str) -> str:
        px = p.lower()
        if px.endswith((".sh",".bash",".ksh")): return "sh"
        if px.endswith((".bat",".cmd")): return "cmd"
        if px.endswith(".ps1"): return "ps1"
        if px.endswith(".py"): return "py"
        if px.endswith(".pl"): return "pl"
        return "other"

    def _light_crawl(self, root: Path) -> list[str]:
        paths = []
        for ext in (".sh",".bash",".ksh",".bat",".cmd",".ps1",".pl",".py"):
            for p in root.rglob(f"*{ext}"):
                try:
                    rel = p.relative_to(root).as_posix()
                    paths.append(rel)
                except Exception:
                    pass
        return sorted(set(paths))

    def _load_seeds(self, root: Path) -> set[str]:
        seeds: set[str] = set()
        for name in ("seeds.txt", ".seeds"):
            p = root / name
            if p.exists():
                try:
                    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                        s = (line or "").strip()
                        if not s or s.startswith("#"): continue
                        s = s.replace("\\","/").lstrip("./")
                        seeds.add(s)
                except Exception:
                    pass
        return seeds

    def _build_worklist(self, io: RoleIO, files: list[str]) -> list[str]:
        # Start with seeds and obvious entry points
        seeds = self._load_seeds(io.root)
        seeds = {s for s in seeds if s in files}
        entry = {f for f in files if f.rsplit("/", 1)[-1].lower() in {"run.sh", "run.cmd", "run.bat", "start.cmd", "main.bat"}}
        wl = list(seeds | entry)

        # Add sources that *contain* dynamic unresolved edges in the static graph
        dyn_sources = []
        try:
            for e in getattr(io, "graph", Graph()).edges:
                if e.dynamic and not e.resolved and e.src in files:
                    dyn_sources.append(e.src)
        except Exception:
            pass
        # keep stable but unique
        seen = set(wl)
        for s in dyn_sources:
            if s not in seen:
                wl.append(s); seen.add(s)

        # Optional: ask LLM to re-rank based on unresolved commands (if available)
        if self.client:
            pairs = [(e.src, e.command or "") for e in getattr(io, "graph", Graph()).edges if e.dynamic and not e.resolved]
            ordered, _ = _llm_plan(self.client, pairs)
            if ordered:
                # keep only those that exist and precede others
                ordered = [s for s in ordered if s in files]
                # stable merge: ordered first, then the rest of wl
                left = [s for s in ordered if s not in set(wl)]
                wl = ordered + [s for s in wl if s not in set(ordered)]

        # cap size (a planner shouldn't create a huge list)
        return wl[:200]        

    def run(self, io: RoleIO, budget: dict | None = None) -> ReadManifest:
        root = io.root
        files = self._light_crawl(root)
        seeds = self._load_seeds(root)
        # entrypoints first
        prio = {}
        for f in files:
            base = f.rsplit("/",1)[-1].lower()
            p = 10
            if f in seeds: p = 500
            elif base in {"run.sh","main.bat","start.cmd"}: p = 100
            prio[f] = p
        # normalization policy (simple, extend later)
        policy = {
            "canon_slashes": True,
            "strip_dot_slash": True,
            "var_precedence": ["export","set",".env"],  # placeholder
            "workdir": ".",
            "llm_reader_hints": False,
        }
        # seed env hints (global)
        env_hints = {}
        # budget defaults (can pass from config later)
        budget = budget or {"max_tool_calls": 100, "max_latency_ms": 60000, "max_loops": 1, "max_files": 60}

        # build manifest
        worklist = self._build_worklist(io, files)
        manifest = ReadManifest(
            files=[{"path": f, "priority": prio[f], "peek": [(0, 4096)]} for f in files],
            env_hints={},
            policy=policy,
            budget=budget,
            worklist=worklist,   
        )
        if self.logger:
            self.logger.log("INFO", f"Planner: indexed {len(manifest.files)} files; budget={manifest.budget}")

        # write to SQLite memory
        if self.logger and self.logger.run_id is not None:
            c = self.logger.conn
            c.executescript("""
            CREATE TABLE IF NOT EXISTS plan_files(run_id INT, path TEXT, priority INT);
            CREATE TABLE IF NOT EXISTS plan_params(run_id INT, key TEXT, value TEXT);
            CREATE TABLE IF NOT EXISTS env_hints(run_id INT, name TEXT, value TEXT);
            """)
            c.executemany("INSERT INTO plan_files VALUES (?,?,?)",
                          [(self.logger.run_id, it["path"], it["priority"]) for it in manifest.files])
            for k,v in (policy|{"budget":json.dumps(budget)}).items():
                c.execute("INSERT INTO plan_params VALUES (?,?,?)", (self.logger.run_id, k, json.dumps(v) if not isinstance(v,str) else v))
            if env_hints:
                c.executemany("INSERT INTO env_hints VALUES (?,?,?)",
                              [(self.logger.run_id, k, v) for k, v in env_hints.items()])
            c.execute("CREATE TABLE IF NOT EXISTS plan_worklist(run_id INT, path TEXT)")
            c.executemany("INSERT INTO plan_worklist VALUES (?,?)", [(self.logger.run_id, p) for p in worklist])
            c.commit()
        return manifest


class Reader:
    def __init__(self, client: Optional[LLMClient] = None, logger: Optional[RunLogger] = None,
                 *, log_prompts: bool = False, use_llm_hints: bool = False, redactor: Optional[Redactor] = None):
        self.client = client; self.logger = logger
        self.log_prompts = bool(log_prompts)
        self.use_llm_hints = bool(use_llm_hints)
        self._redactor = redactor or Redactor()
        self._rx_env_sh  = re.compile(
            r'^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["\']?([A-Za-z0-9_./${}-]+)["\']?\s*$',
            re.M
        )
        self._rx_env_cmd_any = re.compile(
            r'(?i)\bset(?!\s*local)\s+([A-Za-z_][A-Za-z0-9_]*)=(.+?)\s*(?:&|$)'
        )
        self._rx_cmd_for_in = re.compile(
            r'(?i)\bfor\s+%%(?P<var>[A-Za-z])\s+in\s*\((?P<val>[^)]+)\)'
        )

        self._rx_call_sh = re.compile(
            r'(?P<kind>(?:\.|source|bash|sh|ksh|python|python3|perl)\s+)(?P<target>[^\s;]+)'
        )
        self._rx_call_sh_var = re.compile(r'^\s*(?P<target>["\']?\$[A-Za-z_][A-Za-z0-9_]*["\']?)(?:\s|$)')
        self._rx_call_cmd = re.compile(r'(?i)\b(?P<kind>call|start)\s+(?P<target>[^\s&]+)')
        self._rx_call_ps1 = re.compile(r'(?:(?:^|\s)\.\s+(?P<dot>[^\s]+)|(?:^|\s)&\s*(?P<amp>[^\s]+))')
        self._rx_dyn_sh  = re.compile(r'\$\{?[A-Za-z_][A-Za-z0-9_]*\}?|`|\$\(|\beval\b')
        self._rx_dyn_cmd = re.compile(r'%[A-Za-z_][A-Za-z0-9_]*%|![A-Za-z_][A-Za-z0-9_]*!')
        self._rx_dyn_ps1 = re.compile(r'(?<!\w)\$[A-Za-z_]\w*|\$\(|Join-Path|Resolve-Path|Invoke-Expression')
        self._rx_ps_assign_str  = re.compile(r'^\s*\$([A-Za-z_]\w*)\s*=\s*[\'"]([A-Za-z0-9_./\\-]+)[\'"]\s*$', re.M)
        self._rx_ps_assign_join = re.compile(r'(?i)^\s*\$([A-Za-z_]\w*)\s*=\s*Join-Path\s+([^\s;]+)\s+([^\s;]+)')
        self._rx_call_sh_interp_var = re.compile(r'^\s*\$[A-Za-z_][A-Za-z0-9_]*\s+(?P<target>["\']?\$[A-Za-z_][A-Za-z0-9_]*["\']?)')


    def _lang(self, p: str) -> str:
        p=p.lower()
        if p.endswith((".sh",".bash",".ksh")): return "sh"
        if p.endswith((".bat",".cmd")): return "cmd"
        if p.endswith(".ps1"): return "ps1"
        if p.endswith(".py"): return "py"
        if p.endswith(".pl"): return "pl"
        return "other"

    def _plausible_target(self, tok: str) -> bool:
        t = (tok or "").strip().strip('"').strip("'")
        if not t:
            return False
        if ("/" in t or "\\" in t or t.lower().endswith((".sh",".bash",".ksh",".bat",".cmd",".ps1",".pl",".py"))):
            return True
        return bool(re.match(
            r"^(\$[A-Za-z_][A-Za-z0-9_]*|\$\{[A-Za-z_][A-Za-z0-9_]*\}|%[A-Za-z_][A-Za-z0-9_]*%|![A-Za-z_][A-Za-z0-9_]*!)$",
            t
        ))

    def _is_dynamic_sh(self, s: str) -> bool:
        return bool(self._rx_dyn_sh.search(s))

    def _is_dynamic_cmd(self, s: str) -> bool:
        return bool(self._rx_dyn_cmd.search(s))

    def _is_dynamic_ps1(self, full: str, tgt: str) -> bool:
        return bool(tgt.strip().startswith("$") or self._rx_dyn_ps1.search(full))

    def _expand_cmd_value(self, s: str, env: dict[str,str], loop_vars: dict[str,str]) -> str:
        """
        Expand a CMD expression using a small, order-aware model:
          - %%F      => loop_vars['F'] (if present)
          - !VAR!    => env['VAR']    (delayed expansion)
          - %VAR%    => env['VAR']    (normal expansion)
        Runs a few passes to allow chained expansions.
        """
        if s is None: return ""
        out = s.strip().strip('"').strip("'")

        def sub_loop(m: re.Match) -> str:
            k = m.group(1).upper()
            return loop_vars.get(k, m.group(0))

        for _ in range(4):
            prev = out
            # first: FOR loop tokens
            out = re.sub(r'%%([A-Za-z])', sub_loop, out)
            # then: variable expansions (case-insensitive names)
            for k, v in env.items():
                kk = k.upper()
                vv = (v or "").strip().strip('"').strip("'")
                out = out.replace(f"%{kk}%", vv).replace(f"!{kk}!", vv)
            if out == prev:
                break
        return out.replace("\\", "/")


    def run(self, io: RoleIO, manifest: ReadManifest) -> ObservationBatch:
        files_meta, env_vars, call_sites = [], [], []
        total = len(manifest.files)

        # --- NEW: ordering & budget enforcement ---
        wl = set(getattr(manifest, "worklist", []) or [])
        max_files = int((manifest.budget or {}).get("max_files", 0) or 0)

        # Reorder: (1) in worklist, (2) by priority desc, (3) lexicographically for stability
        ordered = sorted(
            manifest.files,
            key=lambda it: (
                0 if it["path"] in wl else 1,
                -int(it.get("priority", 0)),
                it["path"]
            )
        )
        # Enforce budget if set
        if max_files > 0 and len(ordered) > max_files:
            ordered = ordered[:max_files]

        for i, it in enumerate(ordered):
            rel = it["path"]
            path = io.root / rel
            lang = self._lang(rel)
            try:
                peek = it.get("peek", [(0,4096)])[0]
                data = path.read_bytes()[peek[0]:peek[1]]
                text = data.decode("utf-8", errors="ignore")
            except Exception:
                text = ""

            files_meta.append({"path": rel, "lang": lang, "size": path.stat().st_size if path.exists() else 0, "hash": ""})
            if self.logger and (i % 10 == 0 or i == total):
                self.logger.log("INFO", f"Reader: {i}/{total} files peeked")

            if lang == "sh":
                # 1) env vars (coarse but effective)
                for m in self._rx_env_sh.finditer(text):
                     # avoid capturing values with command substitution/backticks
                     val = m.group(2)
                     if "(" in val or "`" in val:
                         continue
                     env_vars.append({"scope": rel, "name": m.group(1), "value": val, "prec": 10})


                # 2) optional LLM reader hints
                if self.use_llm_hints and self.client and text.strip():
                    try:
                        hints, why, meta, user = _llm_read_hints(self.client, rel, text, redactor=self._redactor)
                        if hints:
                            for k, v in hints.items():
                                env_vars.append({"scope": rel, "name": k, "value": v, "prec": 5})
                        if self.logger:
                            self.logger.log_llm(role="reader", model=str(meta.get("model","")), endpoint=str(meta.get("endpoint","")),
                                                prompt_chars=len(user), input_tokens=meta.get("prompt_tokens"),
                                                output_tokens=meta.get("completion_tokens"), total_tokens=meta.get("total_tokens"),
                                                latency_ms=float(meta.get("latency_ms") or 0.0), status="ok",
                                                src=rel, command_snippet="reader-hints", targets_count=len(hints), reasoning=(why or "")[:500])
                            if self.log_prompts:
                                self.logger.log_prompt(role="reader", prompt=user)
                    except Exception as ex:
                        if self.logger:
                            self.logger.log_llm(role="reader", status=f"error:{ex}", targets_count=0)

                # 3) call sites (interpreter, dot-source, direct, and var-only)
                for m in self._rx_call_sh.finditer(text):
                    full = m.group(0).strip()
                    tgt  = m.group("target")
                    if not self._plausible_target(tgt):
                        continue
                    kraw = m.group("kind").strip()
                    kind = "source" if kraw.startswith((".","source")) else "call"
                    call_sites.append({
                        "src": rel, "raw": tgt, "cmd": full, "kind": kind,
                        "line": 0, "col": 0, "span": [0,0],
                        "dynamic": 1 if self._is_dynamic_sh(full) else 0, "conf": 0.7
                    })
                # interpreter var + target var  (e.g., $INTERP "$TARGET")
                for raw in text.splitlines():
                    mi = self._rx_call_sh_interp_var.search(raw)
                    if mi:
                        tgt = mi.group("target")
                        if not self._plausible_target(tgt):
                            continue
                        call_sites.append({
                            "src": rel, "raw": tgt, "cmd": raw.strip(), "kind": "call",
                            "line": 0, "col": 0, "span": [0,0], "dynamic": 1, "conf": 0.7
                        })

                for raw in text.splitlines():
                    m = self._rx_call_sh_var.search(raw)
                    if not m:
                        continue
                    tgt = m.group("target")
                    if not self._plausible_target(tgt):
                        continue
                    call_sites.append({
                        "src": rel, "raw": tgt, "cmd": raw.strip(), "kind": "call",
                        "line": 0, "col": 0, "span": [0,0],
                        "dynamic": 1, "conf": 0.7
                    })

            elif lang == "cmd":
                # CMD is order-sensitive. Scan once, top-to-bottom, tracking env and FOR loop bindings.
                local_env: dict[str, str] = {}   # names stored UPPERCASE
                loop_vars: dict[str, str] = {}   # e.g., {'F': 'step.cmd'}

                for raw in text.splitlines():
                    line = raw.rstrip()

                    # FOR loop binding (%%F)
                    mf = self._rx_cmd_for_in.search(line)
                    if mf:
                        var = mf.group("var").upper()
                        val = mf.group("val").strip().strip('"').strip("'")
                        loop_vars[var] = val

                    # 'set NAME=value' anywhere on the line (but not setlocal)
                    for ma in self._rx_env_cmd_any.finditer(line):
                        name = (ma.group(1) or "").upper()
                        vraw = (ma.group(2) or "")
                        val  = self._expand_cmd_value(vraw, local_env, loop_vars)
                        if name:
                            local_env[name] = val
                            env_vars.append({"scope": rel, "name": name, "value": val, "prec": 10})

                    # calls: 'call foo.cmd' or 'start foo.cmd' (dynamic if %VAR% or !VAR! appears)
                    for mc in self._rx_call_cmd.finditer(line):
                        full = mc.group(0).strip()
                        tgt  = mc.group("target")
                        if not self._plausible_target(tgt):
                            continue
                        dyn = 1 if self._is_dynamic_cmd(full) else 0
                        call_sites.append({
                            "src": rel, "raw": tgt, "cmd": full, "kind": "call",
                            "line": 0, "col": 0, "span": [0,0],
                            "dynamic": dyn, "conf": 0.7
                        })

                # (optional) LLM reader hints for CMD are rarely needed; omit by default

            elif lang == "ps1":
                # 0) simple PS variable assignments
                ps_locals: dict[str, str] = {}
                for line in text.splitlines():
                    ms = self._rx_ps_assign_str.match(line)
                    if ms:
                        name, val = ms.group(1), ms.group(2)
                        val = _norm_path(val)
                        ps_locals[name] = val
                        env_vars.append({"scope": rel, "name": name, "value": val, "prec": 10})
                        continue
                    mj = self._rx_ps_assign_join.match(line)
                    if mj:
                        dest, a, b = mj.group(1), mj.group(2).strip(), mj.group(3).strip()
                        def _tok(t: str) -> Optional[str]:
                            if t.startswith(("'", '"')) and t.endswith(("'", '"')):
                                return _norm_path(t.strip("'\""))
                            if t.startswith("$"):
                                nm = t[1:]
                                if nm.upper() == "PSSCRIPTROOT": return "."
                                return ps_locals.get(nm)
                            return _norm_path(t)
                        a1, b1 = _tok(a), _tok(b)
                        if a1 and b1:
                            val = _norm_path(f"{a1.rstrip('/')}/{b1.lstrip('/')}")
                            ps_locals[dest] = val
                            env_vars.append({"scope": rel, "name": dest, "value": val, "prec": 9})

                # PowerShell call sites and dynamic markers
                for m in self._rx_call_ps1.finditer(text):
                    full = m.group(0).strip()
                    tgt  = m.group("dot") or m.group("amp")
                    if not self._plausible_target(tgt):
                        continue
                    is_dyn = 1 if self._is_dynamic_ps1(full, tgt) else 0
                    call_sites.append({
                        "src": rel, "raw": tgt, "cmd": full,
                        "kind": "source" if m.group("dot") else "call",
                        "line": 0, "col": 0, "span": [0,0],
                        "dynamic": is_dyn, "conf": 0.7
                    })

                # We intentionally skip PS env extraction here (rarely path-like; covered by static scan)

            else:
                # other languages: nothing for now
                pass

        # ---------- write to SQLite ----------
        if self.logger and self.logger.run_id is not None:
            c = self.logger.conn
            c.executescript("""
            CREATE TABLE IF NOT EXISTS files(run_id INT, path TEXT, lang TEXT, size INT, hash TEXT);
            CREATE TABLE IF NOT EXISTS env_vars(run_id INT, scope_path TEXT, name TEXT, value TEXT, precedence INT);
            CREATE TABLE IF NOT EXISTS call_sites(run_id INT, src TEXT, raw_target TEXT, kind TEXT, line INT, col INT, span_start INT, span_end INT, dynamic_flag INT, confidence REAL);
            """)
            c.executemany("INSERT INTO files VALUES (?,?,?,?,?)",
                          [(self.logger.run_id, f["path"], f["lang"], f["size"], f["hash"]) for f in files_meta])
            c.executemany("INSERT INTO env_vars VALUES (?,?,?,?,?)",
                          [(self.logger.run_id, v["scope"], v["name"], v["value"], v["prec"]) for v in env_vars])
            c.executemany("INSERT INTO call_sites VALUES (?,?,?,?,?,?,?,?,?,?)",
                          [(self.logger.run_id, cs["src"], cs["raw"], cs["kind"], cs["line"], cs["col"], cs["span"][0], cs["span"][1], cs["dynamic"], cs["conf"]) for cs in call_sites])
            c.commit()

        return ObservationBatch(files=files_meta, env_vars=env_vars, call_sites=call_sites)

class Mapper:
    def __init__(self, client: Optional[LLMClient] = None, logger: Optional[RunLogger] = None,
                 use_heuristic_fallback: bool = False, *, log_prompts: bool = False, redactor: Optional[Redactor] = None):
        self.client = client; self.logger = logger; self.use_heuristic_fallback = use_heuristic_fallback
        self.log_prompts = bool(log_prompts)
        self._redactor = redactor or Redactor()

    def _env_for_src(self, obs: ObservationBatch, src: str) -> dict[str,str]:
        env: dict[str, str] = {}
        for v in obs.env_vars:
            if v["scope"] == src:
                env[v["name"]] = v["value"]
        return env

    def _subst(self, s: str, env: dict[str,str]) -> str:
        out = (s or "").strip().strip('"').strip("'")
        for _ in range(5):
            prev = out
            for k, v in env.items():
                vv = (v or "").strip().strip('"').strip("'")
                # Windows CMD
                out = re.sub(rf"%{re.escape(k)}%", vv, out, flags=re.I)
                out = re.sub(rf"!{re.escape(k)}!", vv, out, flags=re.I)
                # POSIX / PS
                out = out.replace(f"${{{k}}}", vv).replace(f"${k}", vv)
            if out == prev:
                break
        return _norm_path(out)

    def _extract_dirs(self, src: str, raw: str, env: dict[str,str], allowed_set: set[str]) -> list[str]:
        """Infer promising directories to list: caller's folder, any path-like envs, and literals in the raw string."""
        dirs: set[str] = set()
        # caller folder
        parent = _norm_path("/".join(src.split("/")[:-1])) or "."
        if parent and parent != ".": dirs.add(parent)
        # path-like env values
        for v in env.values():
            v = (v or "").strip().strip('"').strip("'").replace("\\", "/")
            if ("/" in v or v.startswith(".")) or any(p == v or p.startswith(v + "/") for p in allowed_set):
                dirs.add(v.rstrip("/"))
        # literals of the form "name/" in the raw target
        import re as _re
        for m in _re.findall(r"([A-Za-z0-9_.-]+/)", raw or ""):
            dirs.add(m.rstrip("/"))
        # ${VAR}/ where VAR is in env
        for m in _re.findall(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?/", raw or ""):
            val = env.get(m)
            if val:
                dirs.add(val.rstrip("/"))
        # keep only dirs that actually contain allowed files
        pruned = set()
        for d in dirs:
            if any(p == d or p.startswith(d + "/") for p in allowed_set):
                pruned.add(d)
        return sorted(pruned)

    def _list_candidates(self, allowed_set: set[str], base_dirs: list[str]) -> dict[str, list[str]]:
        """Return directory -> up to 50 candidate script files under that directory from allowed_set."""
        ALLOWED_EXTS = (".sh",".bash",".ksh",".bat",".cmd",".ps1",".pl",".py")
        out: dict[str, list[str]] = {}
        for d in base_dirs:
            prefix = d.rstrip("/") + "/"
            files = [p for p in allowed_set if p.startswith(prefix)]
            files = [p for p in files if p.lower().endswith(ALLOWED_EXTS)]
            if files:
                out[d] = sorted(files)[:50]
        return out

    def _make_observations(self, io: RoleIO, src: str, raw: str, env: dict[str,str], allowed_set: set[str]) -> dict:
        """Build a small observation payload for a second mapper pass."""
        snippet = ""
        try:
            snippet = (io.root / src).read_text(encoding="utf-8", errors="ignore")[:1000]
        except Exception:
            pass
        dirs = self._extract_dirs(src, raw, env, allowed_set)
        return {"src_snippet": snippet, "dir_listings": self._list_candidates(allowed_set, dirs)}

    def _ps_eval_joins(self, io: RoleIO, src: str, env: dict[str,str]) -> dict[str,str]:
        """Evaluate simple Join-Path assignments in a PS1 file using current env."""
        try:
            text = (io.root / src).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return env
        rx = re.compile(r'(?i)^\s*\$([A-Za-z_]\w*)\s*=\s*Join-Path\s+([^\s;]+)\s+([^\s;]+)', re.M)
        def tok(t: str) -> Optional[str]:
            t = t.strip()
            if t.startswith(("'", '"')) and t.endswith(("'", '"')):
                return _norm_path(t.strip("'\""))
            if t.startswith("$"):
                nm = t[1:]
                if nm.upper() == "PSSCRIPTROOT": return "."
                return env.get(nm)
            return _norm_path(t)
        for m in rx.finditer(text):
            dest, a, b = m.group(1), m.group(2), m.group(3)
            a1, b1 = tok(a), tok(b)
            if a1 and b1:
                env[dest] = _norm_path(f"{a1.rstrip('/')}/{b1.lstrip('/')}")
        return env

    def run(self, io: RoleIO, obs: ObservationBatch, policy: dict) -> GraphSnapshot:
        g = Graph()
        unresolved: list[dict] = []

        # --- Build allowed file index and platform case policy early ---
        ALLOWED_EXTS = (".sh",".bash",".ksh",".bat",".cmd",".ps1",".pl",".py")
        allowed_set: set[str] = set()
        for ext in ALLOWED_EXTS:
            for p in io.root.rglob(f"*{ext}"):
                try:
                    rel = p.relative_to(io.root).as_posix()
                    allowed_set.add(_norm_path(rel))
                except Exception:
                    pass
        windowsish = False
        try:
            meta = json.loads((io.root / "meta.json").read_text(encoding="utf-8"))
            windowsish = str(meta.get("platform","")).lower() == "windows"
        except Exception:
            pass
        def _canon_case(p: str) -> str:
            return p.lower() if windowsish else p
        allowed_lower = {p.lower() for p in allowed_set}
        allowed_list = sorted(allowed_set)
        has_static_baseline = any(not e.dynamic for e in getattr(io, "graph", Graph()).edges)

        # Collect statically-known source imports (if baseline is present)
        static_sources: dict[str, list[str]] = {}
        for e in getattr(io, "graph", Graph()).edges:
            if (not e.dynamic) and e.kind == "source":
                static_sources.setdefault(e.src, []).append(e.dst)

        # Quick index of observed dot-source call sites per src
        source_calls_by_src: dict[str, list[dict]] = {}
        for cs in obs.call_sites:
            if cs["kind"] == "source":
                source_calls_by_src.setdefault(cs["src"], []).append(cs)

        def _resolve_sourced_target(src: str, raw: str) -> Optional[str]:
            """
            Resolve a dot-sourced target using local substitutions and caller-relative paths.
            Works even without a static baseline.
            """
            # Try local substitution using vars defined in the same script
            local_env = {v["name"]: v["value"] for v in obs.env_vars if v["scope"] == src}
            cand1 = self._subst(raw, local_env)
            for tok in [cand1, _norm_path(raw)]:
                if not tok:
                    continue
                if (tok in allowed_set) or (windowsish and tok.lower() in allowed_lower):
                    return tok
                rel = _norm_path(os.path.join(os.path.dirname(src), tok))
                if (rel in allowed_set) or (windowsish and rel.lower() in allowed_lower):
                    return rel
            return None

        def env_for(src: str) -> dict[str,str]:
            """
            Build an env map for 'src' where:
              1) within the same scope, higher 'prec' wins,
              2) local scope wins over imported scopes,
              3) imported scopes fill only missing names (no override).
            """
            def _sorted_pairs_for(scope: str) -> list[dict]:
                pairs = [v for v in obs.env_vars if v.get("scope") == scope]
                # higher precedence first; stable sort
                pairs.sort(key=lambda v: int(v.get("prec", 0)), reverse=True)
                return pairs

            env: dict[str, str] = {}

            # 1) Local variables for 'src', apply precedence
            for v in _sorted_pairs_for(src):
                name, val = v.get("name"), v.get("value")
                if name and val and name not in env:
                    env[name] = val

            # 2) One-hop import from statically-sourced files, fill only missing
            for t in static_sources.get(src, []):
                for v in _sorted_pairs_for(t):
                    name, val = v.get("name"), v.get("value")
                    if name and val and name not in env:
                        env[name] = val

            # 3) One-hop import from locally observed dot-sources (non-dynamic)
            for cs in source_calls_by_src.get(src, []):
                if cs.get("dynamic", 0):
                    continue
                tgt = _resolve_sourced_target(src, cs["raw"])
                if not tgt:
                    continue
                for v in _sorted_pairs_for(tgt):
                    name, val = v.get("name"), v.get("value")
                    if name and val and name not in env:
                        env[name] = val

            # 4) PowerShell Join-Path derivations at call site
            if src.lower().endswith(".ps1"):
                env = self._ps_eval_joins(io, src, env)
            return env

        ALLOWED_PREFIXES = (
            ". ", "source ", "& ", "call ", "start ",
            "bash ", "sh ", "ksh ", "python ", "python3 ", "perl "
        )
        # Accept direct script invocations like: ./x.sh, x.sh, utils/x.sh, "utils/x.sh"
        DIRECT_CALL_RE = re.compile(
            r"""^["']?                  # optional opening quote
                (?:\./|../|/)?          # optional ./, ../ or /
                [\w./-]+
                \.(?:sh|bash|ksh|bat|cmd|ps1|pl|py)  # known script ext
                (?:\s|["']?$)           # end or whitespace or closing quote
            """, re.VERBOSE | re.IGNORECASE
        )
        # carry over static edges, canonicalized, with sanity filters
        for e in getattr(io, "graph", Graph()).edges:
            if not e.dynamic:
                cmd = (getattr(e, "command", "") or "").strip().lower()
                dst_c = _canon_case(e.dst)
                # Keep only if destination exists in our index (case-aware)
                if not ((dst_c in allowed_set) or (windowsish and dst_c in {p.lower() for p in allowed_set})):
                    continue
                # If we have a command string, require it to look like a real invocation
                if cmd and not (any(cmd.startswith(pfx) for pfx in ALLOWED_PREFIXES) or DIRECT_CALL_RE.match(cmd)):
                    continue
                g.add_edge(Edge(
                    src=_canon_case(e.src), dst=dst_c,
                    kind=e.kind, command=e.command,
                    dynamic=e.dynamic, resolved=e.resolved,
                    confidence=e.confidence, reason=e.reason
                ))
        

        seen: set[tuple[str, str, str]] = set()
        resolved = 0
        nonresolved = 0
        total = len(obs.call_sites)

        for idx, cs in enumerate(obs.call_sites, 1):
            src = cs["src"]; raw = cs["raw"]; kind = cs["kind"]
            cmd = cs.get("cmd") or f"{kind} {raw}"
            env = env_for(src)

            # --- Non-dynamic call-sites: if no static baseline, resolve and keep them ---
            if not cs.get("dynamic", 0):
                if not has_static_baseline:
                    direct_candidates: list[str] = []
                    # Prefer local substitution first (covers things like ". ${UTILS}/lib.sh")
                    t_sub = self._subst(raw, env)
                    if t_sub and t_sub != _norm_path(raw):
                        direct_candidates.append(t_sub)
                    direct_candidates.append(_norm_path(raw))
                    added = False
                    for t in direct_candidates:
                        # as-is
                        if (t in allowed_set) or (windowsish and t.lower() in allowed_lower):
                            g.add_edge(Edge(src=_canon_case(src), dst=_canon_case(t), kind=kind,
                                            command=cmd, dynamic=False, resolved=True, confidence=0.9,
                                            reason="static-direct in 2R/4R"))
                            added = True
                            break
                        # relative to caller
                        rel = _norm_path(os.path.join(os.path.dirname(src), t))
                        if (rel in allowed_set) or (windowsish and rel.lower() in allowed_lower):
                            g.add_edge(Edge(src=_canon_case(src), dst=_canon_case(rel), kind=kind,
                                            command=cmd, dynamic=False, resolved=True, confidence=0.9,
                                            reason="static-direct in 2R/4R"))
                            added = True
                            break
                    if not added:
                        g.add_node(src)
                        unresolved.append({"src": _canon_case(src), "raw_target": raw, "reason": "non-dynamic-unresolved"})
                else:
                    # baseline present: these edges were already carried over, avoid duplicates
                    g.add_node(src)
                continue

            # LLM first
            targets: list[str] = []
            why = ""
            if self.client:
                user = json.dumps({"root": str(io.root), "src": src, "command": cmd, "hints": env, "allowed_paths": allowed_list}, ensure_ascii=False)
                try:
                    content, meta = self.client.chat(MAPPER_PROMPT, user, return_meta=True)
                    data = _json_load(content or "{}")
                    cand = data.get("targets") or []
                    if isinstance(cand, list):
                        targets = [_norm_path(t) for t in cand if isinstance(t, str)]
                        targets = [t for t in targets if (t in allowed_set) or (windowsish and t.lower() in allowed_lower)]
                    why = (data.get("reasoning") or "").strip()
                    if self.logger:
                        self.logger.log_llm(role="mapper", model=str(meta.get("model","")), endpoint=str(meta.get("endpoint","")),
                                            prompt_chars=len(user), input_tokens=meta.get("prompt_tokens"),
                                            output_tokens=meta.get("completion_tokens"), total_tokens=meta.get("total_tokens"),
                                            latency_ms=float(meta.get("latency_ms") or 0.0), status="ok",
                                            src=src, command_snippet=cmd[:200],
                                            targets_count=len(targets), reasoning=why[:500])
                        if self.log_prompts:
                            # store pre-redacted prompt; redact if policy enabled in Redactor
                            self.logger.log_prompt(role="mapper", prompt=self._redactor.redact(user))
                except Exception as ex:
                    if self.logger:
                        self.logger.log_llm(role="mapper", model="", endpoint="", prompt_chars=0, latency_ms=0.0,
                                            status=f"error:{ex}", src=src, command_snippet=cmd[:200], targets_count=0)

            # Optional second pass: small reason–act loop with tool observations
            if not targets and self.client:
                obs_payload = self._make_observations(io, src, raw, env, allowed_set)
                if obs_payload.get("src_snippet") or obs_payload.get("dir_listings"):
                    user2 = json.dumps({
                        "root": str(io.root), "src": src, "command": cmd,
                        "hints": env, "allowed_paths": allowed_list,
                        "observations": obs_payload
                    }, ensure_ascii=False)
                    try:
                        content2, meta2 = self.client.chat(MAPPER_PROMPT, user2, return_meta=True)
                        data2 = _json_load(content2 or "{}")
                        cand2 = data2.get("targets") or []
                        if isinstance(cand2, list):
                            targets = [_norm_path(t) for t in cand2 if isinstance(t, str)]
                            targets = [t for t in targets if (t in allowed_set) or (windowsish and t.lower() in allowed_lower)]
                        why = (data2.get("reasoning") or "").strip() or why
                        if self.logger:
                            self.logger.log_llm(role="mapper", model=str(meta2.get("model","")), endpoint=str(meta2.get("endpoint","")),
                                                prompt_chars=len(user2), input_tokens=meta2.get("prompt_tokens"),
                                                output_tokens=meta2.get("completion_tokens"), total_tokens=meta2.get("total_tokens"),
                                                latency_ms=float(meta2.get("latency_ms") or 0.0), status="ok",
                                                src=src, command_snippet=(cmd + " [loop2]")[:200], targets_count=len(targets), reasoning=why[:500])
                            if self.log_prompts:
                                self.logger.log_prompt(role="mapper", prompt=self._redactor.redact(user2))
                    except Exception as ex:
                        if self.logger:
                            self.logger.log_llm(role="mapper", model="", endpoint="", prompt_chars=0, latency_ms=0.0,
                                                status=f"error:{ex}", src=src, command_snippet=(cmd + " [loop2]")[:200], targets_count=0)


                # Safe heuristic fallback: always attempt local substitution if LLM produced nothing
            if not targets:
                t = self._subst(raw, env)
                if t != _norm_path(raw) and t in allowed_set:
                    targets = [t]
                    why = "local var substitution"

            provenance = {"static_carryover": 0, "llm": 0, "heuristic": 0}
            src_c = _canon_case(src)

            if targets:
                resolved += 1
                if why.startswith("local var substitution"):
                    provenance["heuristic"] += 1
                elif why:
                    provenance["llm"] += 1
                else:
                    provenance["static_carryover"] += 1

                for t in targets:
                    t_c = _canon_case(t)
                    key = (src_c, t_c, kind)
                    if key in seen:
                        continue
                    seen.add(key)
                    g.add_edge(Edge(src=src_c, dst=t_c, kind=kind, command=cmd,
                        dynamic=bool(cs.get("dynamic", 0)),
                        resolved=True, confidence=0.7, reason=why or None))
            else:
                nonresolved += 1
                g.add_node(src)
                unresolved.append({"src": src_c, "raw_target": raw, "reason": "no-targets-from-LLM"})
            if self.logger and (idx % 10 == 0 or idx == total):
                self.logger.log("INFO", f"Mapper: processed {idx}/{total}; "
                                f"resolved={resolved} unresolved={nonresolved}")
        # write coverage + indices
        coverage = {"touched": len({cs["src"] for cs in obs.call_sites}), "total": len(obs.files)}
        if self.logger and self.logger.run_id is not None:
            c = self.logger.conn
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes_idx(run_id INT, path TEXT);
                CREATE TABLE IF NOT EXISTS edges_idx(run_id INT, src TEXT, dst TEXT, kind TEXT, resolved INT, dynamic INT, confidence REAL);
                CREATE TABLE IF NOT EXISTS unresolved(run_id INT, src TEXT, raw_target TEXT, reason TEXT);
                CREATE TABLE IF NOT EXISTS coverage_stats(run_id INT, stats_json TEXT);
                """
            )
            c.executemany("INSERT INTO nodes_idx VALUES (?,?)", [(self.logger.run_id, n) for n in sorted(g.nodes.keys())])
            c.executemany("INSERT INTO edges_idx VALUES (?,?,?,?,?,?,?)",
                          [(self.logger.run_id, e.src, e.dst, e.kind, int(e.resolved), int(e.dynamic), float(e.confidence)) for e in g.edges])
            c.executemany("INSERT INTO unresolved VALUES (?,?,?,?)",
                          [(self.logger.run_id, u["src"], u["raw_target"], u["reason"]) for u in unresolved])
            c.execute("INSERT INTO coverage_stats VALUES (?,?)", (self.logger.run_id, json.dumps(coverage)))
            c.commit()

        return GraphSnapshot(graph=g, unresolved=unresolved, coverage=coverage)

class Writer:
    def __init__(self, client: Optional[LLMClient] = None, logger: Optional[RunLogger] = None):
        self.client = client; self.logger = logger

    def _validate(self, g: Graph) -> list[str]:
        errs = []
        nodes = set(g.nodes.keys())
        for e in g.edges:
            if e.src not in nodes: errs.append(f"edge src not in nodes: {e.src}")
            if e.dst not in nodes: errs.append(f"edge dst not in nodes: {e.dst}")
        return errs

    def run(self, io: RoleIO, out_dir: Path, snap: GraphSnapshot):
        out_dir.mkdir(parents=True, exist_ok=True)
        # collapse duplicate edges before export (matches scoring's set semantics)
        self._dedupe_edges(snap.graph)
        errs = self._validate(snap.graph)
        if errs and self.logger:
            for m in errs: self.logger.log("WARN", m)

        # Deterministic export & normalization shared by all variants
        write_artifacts(root=io.root, out_dir=out_dir, graph=snap.graph,
                        coverage=snap.coverage, unresolved=snap.unresolved, logger=self.logger,
                        create_run_report=False)

        # Optional human bullets via LLM
        if self.client:
            summary = {"nodes": len(snap.graph.nodes), "edges": len(snap.graph.edges),
                       "dynamic_unresolved": sum(1 for e in snap.graph.edges if e.dynamic and not e.resolved)}
            try:
                content, meta = self.client.chat(WRITER_PROMPT, json.dumps(summary), return_meta=True)
                (out_dir / "report.md").write_text(content.strip(), encoding="utf-8")
                self.logger and self.logger.log_llm(role="writer", model=str(meta.get("model","")), endpoint=str(meta.get("endpoint","")),
                                                    prompt_chars=len(str(summary)), input_tokens=meta.get("prompt_tokens"),
                                                    output_tokens=meta.get("completion_tokens"), total_tokens=meta.get("total_tokens"),
                                                    latency_ms=float(meta.get("latency_ms") or 0.0), status="ok", targets_count=0)
            except Exception as ex:
                self.logger and self.logger.log_llm(role="writer", model="", endpoint="", prompt_chars=0, latency_ms=0.0, status=f"error:{ex}", targets_count=0)

    def _dedupe_edges(self, g: Graph) -> None:
        seen = set()
        uniq = []
        for e in g.edges:
            key = (e.src, e.dst, e.kind, e.command, e.dynamic, e.resolved)
            if key in seen:
                continue
            seen.add(key)
            uniq.append(e)
        g.edges = uniq
# -------------------- Runner + Factory --------------------

class AgentRunner:
    def __init__(self, roles: str, client: LLMClient, logger: RunLogger,
                 *, log_prompts: bool = False, redactor: Optional[Redactor] = None, use_llm_reader_hints: bool = False):
        assert roles in {"2R","4R"}
        self.roles = roles; self.client = client; self.logger = logger
        self.log_prompts = bool(log_prompts)
        self._redactor = redactor or Redactor()
        self.use_llm_reader_hints = bool(use_llm_reader_hints)

    def run(self, root_dir: str, base_graph: Graph, out_dir: str):
        io = RoleIO(root=Path(root_dir).resolve(), graph=base_graph)
        planner = Planner(self.client, self.logger)
        reader  = Reader(self.client, self.logger, log_prompts=self.log_prompts, use_llm_hints=self.use_llm_reader_hints, redactor=self._redactor)
        allow_fallback = (self.client.cfg.provider == "disabled")
        mapper  = Mapper(self.client, self.logger, use_heuristic_fallback=allow_fallback, log_prompts=self.log_prompts, redactor=self._redactor)
        writer  = Writer(self.client, self.logger)

        # Budget (could come from cfg)
        def _env_int(name: str, default: int) -> int:
            try:
                v = os.environ.get(name)
                return int(v) if (v is not None and str(v).strip() != "") else default
            except Exception:
                return default

        budget = {
            "max_tool_calls": _env_int("SG_MAX_TOOL_CALLS", 100),
            "max_latency_ms": _env_int("SG_MAX_LAT_MS", 60000),
            "max_loops":      _env_int("SG_MAX_LOOPS", 1),
            "max_files":      _env_int("SG_MAX_FILES", 60)
        }

        run_t0 = time.monotonic()
        lat_ms: dict[str, float] = {}

        if self.roles == "4R":
            t0 = time.monotonic()
            manifest = planner.run(io, budget=budget)
            reader.use_llm_hints = bool(manifest.policy.get("llm_reader_hints", reader.use_llm_hints))
            lat_ms["Planner"] = (time.monotonic() - t0) * 1000.0
            try:
                manifest.policy["llm_reader_hints"] = bool(self.use_llm_reader_hints)
            except Exception:
                pass

            t0 = time.monotonic()
            obs = reader.run(io, manifest)
            lat_ms["Reader"] = (time.monotonic() - t0) * 1000.0

            t0 = time.monotonic()
            snap = mapper.run(io, obs, manifest.policy)
            lat_ms["Mapper"] = (time.monotonic() - t0) * 1000.0
        else:  # "2R": Reader -> Mapper only
            pl = Planner(self.client, self.logger)
            files = pl._light_crawl(io.root)
            seeds = pl._load_seeds(io.root)
            flist = []
            for pth in files:
                pr = 500 if pth in seeds else 10
                flist.append({"path": pth, "priority": pr, "peek":[(0,4096)]})
            manifest = ReadManifest(files=flist, env_hints={}, policy={"workdir":"."}, budget=budget, worklist=[])

            t0 = time.monotonic()
            obs  = reader.run(io, manifest)
            lat_ms["Reader"] = (time.monotonic() - t0) * 1000.0

            t0 = time.monotonic()
            snap = mapper.run(io, obs, manifest.policy)
            lat_ms["Mapper"] = (time.monotonic() - t0) * 1000.0

        # Optional extra loop if many unresolved (bounded)
        if snap.unresolved and budget["max_loops"] > 0:
            budget["max_loops"] -= 1
            # Promote unresolved sources for a deeper peek
            promote = {u["src"] for u in snap.unresolved}
            for it in manifest.files:
                if it["path"] in promote:
                    it["peek"] = [(0, 8192)]  # read twice as much
                    it["priority"] = 200
            t0 = time.monotonic()
            obs2 = reader.run(io, manifest)
            lat_ms["Reader_loop2"] = lat_ms.get("Reader_loop2", 0.0) + (time.monotonic() - t0) * 1000.0

            t0 = time.monotonic()
            snap = mapper.run(io, obs2, manifest.policy)
            lat_ms["Mapper_loop2"] = lat_ms.get("Mapper_loop2", 0.0) + (time.monotonic() - t0) * 1000.0

        outp = Path(out_dir)
        # Export graph
        if self.roles == "4R":
            t0 = time.monotonic()
            writer.run(io, outp, snap)
            lat_ms["Writer"] = (time.monotonic() - t0) * 1000.0
        else:
            write_artifacts(root=io.root, out_dir=outp, graph=snap.graph,
                            coverage=snap.coverage, unresolved=snap.unresolved, logger=self.logger,
                            create_run_report=False)

        lat_ms["total"] = (time.monotonic() - run_t0) * 1000.0

        # Persist lightweight run stats for bench.ps1
        _write_run_stats(outp, self.roles, lat_ms, snap.graph, snap.unresolved, snap.coverage)

        return snap.graph

def llm_from_config(cfg) -> LLMClient:
    if cfg.llm.provider == "openai":
        return LLMClient(LLMConfig(provider="openai", model=cfg.llm.model or "gpt-5-mini"))
    return LLMClient(LLMConfig(provider="disabled"))
