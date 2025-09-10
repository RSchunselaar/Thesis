from __future__ import annotations
import json
import re
import time
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
    budget: dict[str, int]            # {"max_tool_calls": 50, "max_latency_ms": 60000}

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
        budget = budget or {"max_tool_calls": 100, "max_latency_ms": 60000, "max_loops": 1}

        # build manifest
        manifest = ReadManifest(
            files=[{"path": f, "priority": prio[f], "peek": [(0, 4096)]} for f in files],
            env_hints=env_hints,
            policy=policy,
            budget=budget,
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
            r'^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["\']?([\w./-]+)["\']?\s*$',
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

        for i, it in enumerate(sorted(manifest.files, key=lambda x: -x["priority"])):
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
                    env_vars.append({"scope": rel, "name": m.group(1), "value": m.group(2), "prec": 10})

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
                out = out.replace(f"%{k}%", vv)
                out = out.replace(f"!{k}!", vv)
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
     

    def run(self, io: RoleIO, obs: ObservationBatch, policy: dict) -> GraphSnapshot:
        g = Graph()
        unresolved: list[dict] = []

        # seed nodes with discovered files
        for f in obs.files: g.add_node(f["path"])
        # carry over STATIC edges from the initial scan (never regress on static)
        for e in getattr(io, "graph", Graph()).edges:
            if not e.dynamic:
                g.add_edge(e)

        # Allowed list for LLM prompt + Python-side guard
        allowed_set = {_norm_path(f["path"]) for f in obs.files}
        allowed_list = sorted(allowed_set)  # JSON-serializable
        
        seen: set[tuple[str, str, str]] = set()
        resolved = 0
        nonresolved = 0
        total = len(obs.call_sites)

        for idx, cs in enumerate(obs.call_sites, 1):
            src = cs["src"]; raw = cs["raw"]; kind = cs["kind"]
            cmd = cs.get("cmd") or f"{kind} {raw}"
            env = self._env_for_src(obs, src)

            # --- IMPORTANT: skip static call-sites ---
            # Static (non-dynamic) edges were already captured by the Scanner and
            # carried over above. Avoid re-adding them here to prevent duplicates.
            if not cs.get("dynamic", 0):
                g.add_node(src)  # keep node coverage consistent
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
                        targets = [t for t in targets if t in allowed_set]
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
                            targets = [t for t in targets if t in allowed_set]
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

            if targets:
                resolved += 1
                for t in targets:
                    key = (src, t, kind)
                    if key in seen:
                        continue
                    seen.add(key)
                    g.add_edge(Edge(src=src, dst=t, kind=kind, command=cmd,
                                    dynamic=bool(cs.get("dynamic", 0)), resolved=True, confidence=0.7, reason=why or None))
            else:
                nonresolved += 1
                g.add_node(src)
                unresolved.append({"src": src, "raw_target": raw, "reason": "no-targets-from-LLM"})
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
                        coverage=snap.coverage, unresolved=snap.unresolved, logger=self.logger)

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
        # if egress is off or provider is disabled, allow heuristic fallback
        allow_fallback = (self.client.cfg.provider == "disabled")
        mapper  = Mapper(self.client, self.logger, use_heuristic_fallback=allow_fallback, log_prompts=self.log_prompts, redactor=self._redactor)  # LLM-first
        writer  = Writer(self.client, self.logger)

        # Budget (could come from cfg)
        budget = {"max_tool_calls": 100, "max_latency_ms": 60000, "max_loops": 1}

        import time
        if self.roles == "4R":
            t0 = time.monotonic()
            manifest = planner.run(io, budget=budget)
            try:
                manifest.policy["llm_reader_hints"] = bool(self.use_llm_reader_hints)
            except Exception:
                pass 
            self.logger.log_role_latency("Planner", time.monotonic()-t0)
            t0 = time.monotonic(); obs      = reader.run(io, manifest);       self.logger.log_role_latency("Reader",  time.monotonic()-t0)
            t0 = time.monotonic(); snap     = mapper.run(io, obs, manifest.policy); self.logger.log_role_latency("Mapper",  time.monotonic()-t0)
        else:  # "2R": Reader -> Mapper only
            pl = Planner(self.client, self.logger)
            files = pl._light_crawl(io.root)
            seeds = pl._load_seeds(io.root)
            # Minimal manifest for Reader (honor seeds)
            flist = []
            for pth in files:
                pr = 500 if pth in seeds else 10
                flist.append({"path": pth, "priority": pr, "peek":[(0,4096)]})
            manifest = ReadManifest(files=flist, env_hints={}, policy={"workdir":"."}, budget=budget)
            t0 = time.monotonic(); obs  = reader.run(io, manifest);            self.logger.log_role_latency("Reader",  time.monotonic()-t0)
            t0 = time.monotonic(); snap = mapper.run(io, obs, manifest.policy); self.logger.log_role_latency("Mapper",  time.monotonic()-t0)

        # Optional extra loop if many unresolved (bounded)
        if snap.unresolved and budget["max_loops"] > 0:
            budget["max_loops"] -= 1
            # Promote unresolved sources for a deeper peek
            promote = {u["src"] for u in snap.unresolved}
            for it in manifest.files:
                if it["path"] in promote:
                    it["peek"] = [(0, 8192)]  # read twice as much
                    it["priority"] = 200
            obs2  = reader.run(io, manifest)
            snap  = mapper.run(io, obs2, manifest.policy)

        outp = Path(out_dir)
        if self.roles == "4R":
            writer.run(io, outp, snap)               # Writer uses shared Exporter + optional LLM bullets
        else:
            # 2R: reuse Exporter directly (no LLM bullets)
            write_artifacts(root=io.root, out_dir=outp, graph=snap.graph,
                            coverage=snap.coverage, unresolved=snap.unresolved, logger=self.logger)
        return snap.graph

def llm_from_config(cfg) -> LLMClient:
    if cfg.llm.provider == "openai":
        return LLMClient(LLMConfig(provider="openai", model=cfg.llm.model or "gpt-5-mini"))
    return LLMClient(LLMConfig(provider="disabled"))
