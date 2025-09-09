#!/usr/bin/env python3
import argparse, random, textwrap, json, hashlib
from pathlib import Path

KINDS = ("easy","hard")
DIR_VOCAB = ["utils","lib","jobs","steps","tasks","mods","bin","pipes","scripts"]
VERBS = ["prep","load","filter","merge","archive","rotate","sync","ship","stage","ingest"]
ECHOES = ["ok","done","ready","processed","success","step-complete","hello","ping","work"]

def canon(p: Path, root: Path, windows: bool):
    # make path relative to bundle root, normalize to forward slashes
    rel = p.relative_to(root)
    s = str(rel).replace("\\","/")
    return s.lower() if windows else s

def write(p: Path, content: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")

def salted(rnd: random.Random, base: str) -> str:
    # deterministic short salt for visible variation
    n = rnd.randrange(1_000_000)
    return f"{base} // sg-salt:{n}"

def dge_bash(target: str, verb: str) -> str:
    if verb == "bash": return f'bash "{target}"'
    if verb == "sh":   return f'sh "{target}"'
    return f'"{target}"'

# helpers for PS/CMD dge strings
def dge_ps_source(var_or_path: str) -> str:
    # dot-sourcing in PowerShell
    return f". {var_or_path}"

def dge_cmd_call(var_or_path: str) -> str:
    return f'call "{var_or_path}"'

def mk_shell_linear(root: Path, windows: bool, rnd: random.Random):
    dirn = rnd.choice(DIR_VOCAB)
    v = rnd.choice(VERBS)
    run = root/"run.sh"
    util = root/dirn/f"{v}.sh"
    body_util = f"#!/usr/bin/env bash\n# {salted(rnd,'util')}\necho \"{rnd.choice(ECHOES)}\"\n"
    body_run  = f"#!/usr/bin/env bash\n# {salted(rnd,'run')}\n./{dirn}/{v}.sh\n"
    write(util, body_util); write(run, body_run)
    src = canon(run,root,windows); dst = canon(util,root,windows)
    feats = {"direct-call"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge":f'./{dirn}/{v}.sh'}], [run], feats
 

def mk_shell_dispatch(root: Path, windows: bool, rnd: random.Random):
    dirn = rnd.choice(DIR_VOCAB)
    verbs = rnd.sample(VERBS, k=2)
    run = root/"run.sh"
    a = root/dirn/f"{verbs[0]}.sh"
    b = root/dirn/f"{verbs[1]}.sh"
    write(a, f"#!/usr/bin/env bash\n# {salted(rnd,'a')}\necho \"{rnd.choice(ECHOES)}\"\n")
    write(b, f"#!/usr/bin/env bash\n# {salted(rnd,'b')}\necho \"{rnd.choice(ECHOES)}\"\n")
    write(run, f"#!/usr/bin/env bash\n# {salted(rnd,'run')}\n./{dirn}/{verbs[0]}.sh\n./{dirn}/{verbs[1]}.sh\n")
    nodes = {canon(p,root,windows) for p in [run,a,b]}
    edges = [
        {"src": canon(run,root,windows), "dst": canon(a,root,windows), "kind":"call","dge":f'./{dirn}/{verbs[0]}.sh'},
        {"src": canon(run,root,windows), "dst": canon(b,root,windows), "kind":"call","dge":f'./{dirn}/{verbs[1]}.sh'},
    ]
    feats = {"fan-out","direct-call"}
    return nodes, edges, [run], feats

def mk_shell_varind(root: Path, windows: bool, rnd: random.Random):
    dirn = rnd.choice(DIR_VOCAB)
    v = rnd.choice(VERBS)
    run = root/"run.sh"
    tgt = root/dirn/f"{v}.sh"
    write(tgt, f"#!/usr/bin/env bash\n# {salted(rnd,'tgt')}\necho \"{rnd.choice(ECHOES)}\"\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
BASE="./{dirn}"
NAME="{v}.sh"
TARGET="$BASE/$NAME"
{rnd.choice(['bash','$TARGET','$TARGET'])} "$TARGET"
"""
    # normalize the verb used in dge
    verb = "bash"
    write(run, body)
    src = canon(run,root,windows); dst = canon(tgt,root,windows)
    feats = {"var-indirection","interpreter-hop-bash"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge": dge_bash("$TARGET", verb)}], [run], feats
 
def mk_ps_dotsource(root: Path, windows: bool, rnd: random.Random):
    run = root/"Run.ps1"
    mod = root/"Utils.ps1"
    write(mod, f"function Invoke-{rnd.choice(VERBS).capitalize()} {{ Write-Host \"{rnd.choice(ECHOES)}\" }}\n# {salted(rnd,'ps-mod')}\n")
    write(run, f". ./Utils.ps1\nInvoke-{rnd.choice(VERBS).capitalize()}\n# {salted(rnd,'ps-run')}\n")
    src = canon(run,root,windows); dst = canon(mod,root,windows)
    feats = {"dot-sourcing"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"source","dge": dge_ps_source("./Utils.ps1")}], [run], feats
 
def mk_cmd_varind(root: Path, windows: bool, rnd: random.Random):
    run = root/"Run.cmd"
    dirn = rnd.choice(["bin","steps","tasks"])
    step = rnd.choice(VERBS)
    sub = root/dirn/f"{step}.cmd"
    write(sub, "@echo off\r\necho "+rnd.choice(ECHOES)+"\r\n")
    body = "@echo off\r\nsetlocal EnableDelayedExpansion\r\nset BASE="+dirn+"\r\nset NAME="+step+".cmd\r\nset TARGET=!BASE!\\!NAME!\r\ncall \"!TARGET!\"\r\n"
    write(run, body)
    src = canon(run,root,windows); dst = canon(sub,root,windows)
    feats = {"delayed-expansion","var-indirection"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge": dge_cmd_call("!TARGET!")}], [run], feats

# --- NEW HARD PATTERNS --
def mk_shell_varcall(root: Path, windows: bool, rnd: random.Random):
    """
    Harder variant: pure variable invocation of the target ($TARGET), no explicit interpreter.
    """
    dirn = rnd.choice(DIR_VOCAB)
    v = rnd.choice(VERBS)
    run = root/"run.sh"
    tgt = root/dirn/f"{v}.sh"
    write(tgt, f"#!/usr/bin/env bash\n# {salted(rnd,'tgt')}\necho \"{rnd.choice(ECHOES)}\"\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
BASE="./{dirn}"
NAME="{v}.sh"
TARGET="$BASE/$NAME"
"$TARGET" "$TARGET"
"""
    write(run, body)
    src = canon(run,root,windows); dst = canon(tgt,root,windows)
    feats = {"var-indirection"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge": '"$TARGET"'}], [run], feats

def mk_shell_interp_python_var(root: Path, windows: bool, rnd: random.Random):
    """
    Interpreter hop via a variable ($INTERP) + variable target ($TARGET):
      INTERP="python"; TARGET="./tools/worker.py"; $INTERP "$TARGET"
    """
    run = root/"run.sh"
    pyf = root/"tools"/"worker.py"
    write(pyf, f"#!/usr/bin/env python3\n# {salted(rnd,'py')}\nprint('{rnd.choice(ECHOES)}')\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
TARGET="./tools/worker.py"
INTERP="python"
$INTERP "$TARGET"
"""
    write(run, body)
    src = canon(run,root,windows); dst = canon(pyf,root,windows)
    feats = {"interpreter-hop-python","cross-language","var-indirection"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge": '$INTERP "$TARGET"'}], [run], feats

def mk_shell_source_varprefix(root: Path, windows: bool, rnd: random.Random):
    """
    Dot-source with a variable prefix: . ${UTILS}/lib.sh
    """
    dirn = rnd.choice(DIR_VOCAB)
    v = "lib"
    run = root/"run.sh"
    lib = root/dirn/f"{v}.sh"
    write(lib, f"#!/usr/bin/env bash\n# {salted(rnd,'lib')}\nhello(){{ echo \"{rnd.choice(ECHOES)}\"; }}\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
UTILS="./{dirn}"
. ${{UTILS}}/{v}.sh
hello
"""
    write(run, body)
    src = canon(run,root,windows); dst = canon(lib,root,windows)
    feats = {"dot-sourcing","var-indirection"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"source","dge":". ${UTILS}/lib.sh"}], [run], feats
 
def mk_shell_dot_source(root: Path, windows: bool, rnd: random.Random):
    # Bash dot/source to pull functions into caller scope
    dirn = rnd.choice(DIR_VOCAB)
    run = root/"run.sh"
    lib = root/dirn/"lib.sh"
    write(lib, f"#!/usr/bin/env bash\n# {salted(rnd,'lib')}\nhello(){{ echo \"{rnd.choice(ECHOES)}\"; }}\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
. ./{dirn}/lib.sh
hello
"""
    write(run, body)
    src = canon(run,root,windows); dst = canon(lib,root,windows)
    feats = {"dot-sourcing"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"source","dge":". ./"+dirn+"/lib.sh"}], [run], feats

def mk_shell_interp_python(root: Path, windows: bool, rnd: random.Random):
    # cross-language hop via python interpreter
    run = root/"run.sh"
    pyf = root/"tools"/"worker.py"
    write(pyf, f"#!/usr/bin/env python3\n# {salted(rnd,'py')}\nprint('{rnd.choice(ECHOES)}')\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
TARGET="./tools/worker.py"
python "$TARGET"
"""
    write(run, body)
    src = canon(run,root,windows); dst = canon(pyf,root,windows)
    feats = {"interpreter-hop-python","cross-language"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge":'python "$TARGET"'}], [run], feats

def mk_shell_interp_perl(root: Path, windows: bool, rnd: random.Random):
    run = root/"run.sh"
    plf = root/"scripts"/"w.pl"
    write(plf, f"#!/usr/bin/env perl\n# {salted(rnd,'pl')}\nprint \"{rnd.choice(ECHOES)}\\n\";\n")
    body = f"""#!/usr/bin/env bash
# {salted(rnd,'run')}
perl "./scripts/w.pl"
"""
    write(run, body)
    src = canon(run,root,windows); dst = canon(plf,root,windows)
    feats = {"interpreter-hop-perl","cross-language"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge":'perl "./scripts/w.pl"'}], [run], feats

def mk_cmd_for_loop(root: Path, windows: bool, rnd: random.Random):
    # FOR loop with delayed expansion and CALL through var
    run = root/"Run.cmd"
    dirn = rnd.choice(["bin","steps","tasks"])
    sub = root/dirn/"step.cmd"
    write(sub, "@echo off\r\necho "+rnd.choice(ECHOES)+"\r\n")
    body = (
        "@echo off\r\nsetlocal EnableDelayedExpansion\r\n"
        f"set D={dirn}\r\nfor %%F in (step.cmd) do set T=!D!\\%%F\r\n"
        "call \"!T!\"\r\n"
    )
    write(run, body)
    src = canon(run,root,windows); dst = canon(sub,root,windows)
    feats = {"for-loop","delayed-expansion","var-indirection"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"call","dge": dge_cmd_call("!T!")}], [run], feats

def mk_ps_var_dotsource(root: Path, windows: bool, rnd: random.Random):
    # Dot-sourcing via variable path
    run = root/"Run.ps1"
    mod = root/"Utils.ps1"
    write(mod, f"function Invoke-Work {{ Write-Host \"{rnd.choice(ECHOES)}\" }}\n# {salted(rnd,'ps-mod2')}\n")
    body = (
        "$m = Join-Path $PSScriptRoot 'Utils.ps1'\n"
        ". $m\n"
        "Invoke-Work\n"
        f"# {salted(rnd,'ps-run2')}\n"
    )
    write(run, body)
    src = canon(run,root,windows); dst = canon(mod,root,windows)
    feats = {"dot-sourcing","var-indirection"}
    return {src,dst}, [{"src":src,"dst":dst,"kind":"source","dge": dge_ps_source("$m")}], [run], feats

def mk_cmd_chain_vars(root: Path, windows: bool, rnd: random.Random):
    # Chain of env vars with delayed expansion, final CALL through !TARGET!
    run = root / "Run.cmd"
    dirn = rnd.choice(["bin", "steps", "tasks"])
    sub  = root / dirn / "step.cmd"
    write(sub, "@echo off\r\necho " + rnd.choice(ECHOES) + "\r\n")
    body = (
        "@echo off\r\nsetlocal EnableDelayedExpansion\r\n"
        f"set D1={dirn}\r\n"
        "set NAME=step.cmd\r\n"
        "set D2=!D1!\r\n"
        "set TARGET=!D2!\\!NAME!\r\n"
        "call \"!TARGET!\"\r\n"
    )
    write(run, body)
    src = canon(run, root, windows); dst = canon(sub, root, windows)
    feats = {"var-indirection", "delayed-expansion"}
    return {src, dst}, [{"src": src, "dst": dst, "kind": "call", "dge": dge_cmd_call("!TARGET!")}], [run], feats

def mk_ps_amp_invoke_var(root: Path, windows: bool, rnd: random.Random):
    # Ampersand invocation of a variable-resolved path (& $m)
    run = root / "Run.ps1"
    mod = root / "Utils.ps1"
    write(mod, f"function Invoke-Work {{ Write-Host \"{rnd.choice(ECHOES)}\" }}\n# {salted(rnd,'ps-mod3')}\n")
    body = (
        "$m = Join-Path $PSScriptRoot 'Utils.ps1'\n"
        "& $m\n"
        "Invoke-Work\n"
        f"# {salted(rnd,'ps-run3')}\n"
    )
    write(run, body)
    src = canon(run, root, windows); dst = canon(mod, root, windows)
    feats = {"var-indirection", "dot-sourcing"}
    return {src, dst}, [{"src": src, "dst": dst, "kind": "call", "dge": "& $m"}], [run], feats

def mk_bundle(root: Path, hard: bool, platform: str, rnd: random.Random, seen_hashes: set, min_hard_features: int):
    windows = platform in ("windows","mixed") and (platform=="windows" or rnd.random()<0.5)
    # pattern library
    makers_easy = [mk_shell_linear, mk_shell_dispatch, mk_ps_dotsource]
    makers_hard = [
        # bash / shell families
        mk_shell_varind,
        mk_shell_varcall,               
        mk_shell_dot_source,
        mk_shell_source_varprefix,      
        mk_shell_interp_python,
        mk_shell_interp_python_var,     
        mk_shell_interp_perl,
        mk_cmd_varind,
        mk_cmd_for_loop,
        mk_ps_dotsource,
        mk_ps_var_dotsource,
        mk_cmd_chain_vars,
        mk_ps_amp_invoke_var
    ]
    maker = rnd.choice(makers_hard if hard else makers_easy)

    # generate, then ensure textual uniqueness via salt if needed
    attempts = 0
    while True:
        nodes, edges, seeds, feats = maker(root, windows, rnd)
        # enforce difficulty for hard bundles: at least N features and at least one dynamic feature
        if hard:
            dynamic = {"var-indirection","delayed-expansion","dot-sourcing","interpreter-hop-bash",
                       "interpreter-hop-python","interpreter-hop-perl","for-loop","cross-language"}
            if len(feats & dynamic) < 1 or len(feats) < min_hard_features:
                maker = rnd.choice(makers_hard)  # try another pattern
                # clean any files created so far
                for p in sorted(root.rglob("*"), reverse=True):
                    if p.is_file(): p.unlink()
                    elif p.is_dir(): 
                        try: p.rmdir()
                        except OSError: pass
                attempts += 1
                if attempts > 6:
                    # fall through with whatever we have after several tries
                    pass
                continue
        # fingerprint all file contents
        buf = []
        for p in sorted(root.rglob("*")):
            if p.is_file():
                buf.append(p.read_text(encoding="utf-8", errors="ignore"))
        h = hashlib.sha256(("||".join(buf)).encode("utf-8")).hexdigest()
        if h not in seen_hashes: 
            seen_hashes.add(h)
            break
        # collide: append a tiny salt comment to run file and try again
        attempts += 1
        run_candidates = [s for s in seeds if s.exists()]
        if run_candidates:
            rp = run_candidates[0]
            rp.write_text(rp.read_text(encoding="utf-8") + f"\n# uniq-{attempts}-{rnd.randrange(10**6)}\n", encoding="utf-8")
        if attempts > 3:  # give up after a few tries
            break

    # write seeds.txt, truth.yaml, meta.json
    (root).mkdir(parents=True, exist_ok=True)
    write(root/"seeds.txt", "\n".join([p.name for p in seeds]) + "\n")
    truth = {"nodes": sorted(nodes), "edges": edges}
    write(root/"truth.yaml", textwrap.dedent(
        "nodes:\n" + "".join([f"  - {n}\n" for n in truth["nodes"]]) +
        "edges:\n" + "".join([f"  - src: {e['src']}\n    dst: {e['dst']}\n    kind: {e['kind']}\n    dge: {e['dge']}\n" for e in truth["edges"]])
    ))
    meta = {
        "platform": "windows" if windows else "linux",
        "hard": hard,
        "pattern": maker.__name__,
        "features": sorted(list(feats))
    }
    write(root/"meta.json", json.dumps(meta, indent=2))
    return nodes, edges, seeds

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/bundles")
    ap.add_argument("--kind", choices=KINDS, required=True)
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--seed", type=int, default=13)
    ap.add_argument("--platform", choices=["linux","windows","mixed"], default="mixed")
    ap.add_argument("--min-hard-features", type=int, default=2,
                    help="minimum number of difficulty features required for a hard bundle")
    args = ap.parse_args()
    rnd = random.Random(args.seed)
    base = Path(args.out)/args.kind
    seen = set()
    for i in range(1, args.count+1):
        root = base/ f"{i:03d}"
        if root.exists():
            # clean existing to avoid stale files influencing hashes
            for p in sorted(root.rglob("*"), reverse=True):
                if p.is_file(): p.unlink()
                else: p.rmdir()
        mk_bundle(root, hard=(args.kind=="hard"), platform=args.platform, rnd=rnd, seen_hashes=seen,
                  min_hard_features=args.min_hard_features)
    print(f"wrote {args.count} {args.kind} bundles under {base}")

if __name__ == "__main__":
    main()