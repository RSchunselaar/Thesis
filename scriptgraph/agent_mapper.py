from __future__ import annotations
import json, re
from pathlib import Path
from .graph import Graph, Edge

# Accept plain, quoted, or "export VAR=..." assignments (first 50 lines per .sh file)
VAR_ASSIGN = re.compile(
    r'^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["\']?([\w./-]+)["\']?\s*$'
)

# Optional LLM client (will be None if you don't have llm_adapter.py)
try:
    from .llm_adapter import LLMClient  # type: ignore
except Exception:  # pragma: no cover
    LLMClient = None  # type: ignore


SYSTEM_PROMPT = (
    "You resolve dynamic script invocations in legacy script collections. "
    "Return STRICT JSON only. Schema: "
    '{"targets":[{"path":"relative/path","confidence":0.0}],"reasoning":"..."} '
    "(confidence in [0,1], omit if unsure). "
    "Only include plausible script paths (.sh,.bash,.ksh,.bat,.cmd,.ps1,.pl,.py)."
)

class AgentMapper:
    def __init__(self, client: "LLMClient|None" = None):
        self.client = client

    def map_bundle(self, root_dir: str, g: Graph) -> Graph:
        root = Path(root_dir).resolve()
        var_env = self._gather_vars(root)
        new_edges: list[Edge] = []

        for e in g.edges:
            # Keep non-dynamic or already-resolved edges as-is
            if e.resolved or not e.dynamic:
                new_edges.append(e)
                continue

            # 1) Heuristic substitution
            candidate = self._subst(e.dst, var_env)
            if candidate != e.dst:
                new_edges.append(self._resolved_edge(
                    e, candidate, self._conf_subst(root, candidate), reason="local var substitution"
                ))
                continue

            # 2) LLM resolution (if available and network allowed)
            if self.client is not None:
                targets, why = self._llm_resolve(str(root), e.src, e.command, var_env)
                if targets:
                    for p, c in targets:
                        new_edges.append(self._resolved_edge(e, p, self._conf_llm(root, p, c), why or None))
                    continue

            # 3) Still unresolved
            new_edges.append(e)

        g.edges = new_edges
        for ed in g.edges:
            g.add_node(ed.dst)
        return g

    # --- confidence policy ---
    def _exists(self, root: Path, p: str) -> bool:
        return (root / p).exists()

    def _conf_subst(self, root: Path, p: str) -> float:
        # var substitution is usually strong; add a small bonus if the file exists
        return 0.8 + (0.1 if self._exists(root, p) else 0.0)

    def _conf_llm(self, root: Path, p: str, llm_c: float | None) -> float:
        # base for LLM inference
        base = 0.6 if llm_c is None else max(0.5, min(0.9, 0.5 + 0.4 * float(llm_c)))
        # existence bonus
        if self._exists(Path(root), p):
            base = min(0.95, base + 0.1)
        return base     

    # --- helpers ---

    def _resolved_edge(self, base: Edge, dst: str, conf: float, reason: str | None) -> Edge:
        dst = dst.replace("\\", "/")
        if dst.startswith("./"): dst = dst[2:]
        try:
            return Edge(src=base.src, dst=dst, kind=base.kind, command=base.command,
                        dynamic=True, resolved=True, confidence=round(conf, 3), reason=reason)
        except TypeError:
            return Edge(src=base.src, dst=dst, kind=base.kind, command=base.command,
                        dynamic=True, resolved=True, confidence=round(conf, 3))

    def _gather_vars(self, root: Path) -> dict[str, str]:
        env: dict[str, str] = {}
        for p in root.rglob("*.sh"):
            try:
                for line in p.read_text(encoding="utf-8", errors="ignore").splitlines()[:200]:
                    m = VAR_ASSIGN.match(line)
                    if m: env[m.group(1)] = m.group(2)
            except Exception:
                continue
        return env


    def _subst(self, s: str, env: dict[str, str]) -> str:
        out = s
        for k, v in env.items():
            out = out.replace(f"${{{k}}}", v).replace(f"${k}", v)
        return out.replace("\\", "/")

    def _llm_resolve(self, root: str, src: str, cmd: str, hints: dict[str, str]) -> tuple[list[tuple[str,float|None]], str]:
        if self.client is None: return [], ""
        hint_str = "\n".join(f"{k}={v}" for k, v in sorted(hints.items()))
        user = (
            f"Root: {root}\nSource script: {src}\nCommand: {cmd}\n"
            f"Known variables/prefixes:\n{hint_str}\n"
            "Output STRICT JSON only as per schema."
        )
        try:
            raw = self.client.chat(SYSTEM_PROMPT, user)
            data = json.loads(raw)
            items = data.get("targets", [])
            out: list[tuple[str,float|None]] = []
            for it in items:
                # accept either {"path": "...", "confidence": 0.x} or plain strings for back-compat
                if isinstance(it, str):
                    p, c = it, None
                elif isinstance(it, dict):
                    p, c = it.get("path") or it.get("p") or it.get("target"), it.get("confidence")
                else:
                    continue
                if not p: continue
                p = p.strip().strip('"').strip("'").replace("\\", "/")
                if p.startswith("./"): p = p[2:]
                out.append((p, None if c is None else float(c)))
            return out, data.get("reasoning", "")
        except Exception:
            return [], ""
