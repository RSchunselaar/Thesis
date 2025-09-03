from __future__ import annotations
from dataclasses import dataclass, field, asdict
from .utils import canon

@dataclass
class Edge:
    src: str
    dst: str
    kind: str
    command: str
    dynamic: bool = False
    resolved: bool = True
    confidence: float = 0.9
    reason: str | None = None  # you already added this

@dataclass
class Graph:
    nodes: dict[str, dict] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)

    def add_node(self, path: str):
        self.nodes.setdefault(canon(path), {})

    def add_edge(self, e: Edge):
        e.src = canon(e.src)
        e.dst = canon(e.dst)
        self.edges.append(e)
        self.add_node(e.src)
        self.add_node(e.dst)

    def to_yaml(self) -> str:
        import yaml
        data = {
            "nodes": sorted(list(self.nodes.keys())),
            "edges": [asdict(e) for e in self.edges],
        }
        return yaml.safe_dump(data, sort_keys=False)

    def to_dot(self) -> str:
        def color(e: Edge) -> str:
            if not e.resolved:
                return "orange"
            return "black" if not e.dynamic else "blue"
        lines = ["digraph ScriptGraph {", "  rankdir=LR;"]
        for n in sorted(self.nodes.keys()):
            lines.append(f'  "{n}";')
        for e in self.edges:
            label = e.kind
            lines.append(f'  "{e.src}" -> "{e.dst}" [label="{label}", color="{color(e)}"];')
        lines.append("}")
        return "\n".join(lines)
