from __future__ import annotations
import ast
from pathlib import Path
from ..graph import Edge


SHELL_EXTS = {".sh", ".bash", ".ksh", ".cmd", ".bat", ".ps1"}


class CallVisitor(ast.NodeVisitor):
    def __init__(self):
        self.cmds: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call):
        # subprocess.run(["bash", "./x.sh"]) or os.system("./y.sh")
        try:
            func = getattr(node.func, "attr", getattr(node.func, "id", ""))
        except Exception:
            func = ""
        if func in {"run", "Popen", "call", "system"}:
            # string literal arg
            if node.args:
                arg0 = node.args[0]
            if (
                isinstance(arg0, ast.List)
                and arg0.elts
                and isinstance(arg0.elts[0], ast.Constant)
            ):
                val = " ".join(
                    [e.value for e in arg0.elts if isinstance(e, ast.Constant)]
                )
                self.cmds.append((node.lineno, val))
            elif isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
                self.cmds.append((node.lineno, arg0.value))
        self.generic_visit(node)


def parse_python_cli(root: Path, src_path: str, text: str):
    edges = []
    try:
        tree = ast.parse(text)
    except Exception:
        return edges
    v = CallVisitor()
    v.visit(tree)
    for _, cmd in v.cmds:
        # naive: if command references a known script extension, add edge
        for ext in SHELL_EXTS:
            if ext in cmd:
                # extract last token ending with ext
                parts = [tok for tok in cmd.split() if tok.endswith(ext)]
                for p in parts:
                    edges.append(
                        Edge(
                            src=src_path,
                            dst=p,
                            kind="call",
                            command=cmd,
                            dynamic=False,
                            resolved=True,
                            confidence=0.8,
                        )
                    )
    return edges
