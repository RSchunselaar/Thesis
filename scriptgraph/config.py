from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any
import hashlib
import io
import yaml


@dataclass
class RuntimeCfg:
    egress: bool = False
    sqlite_path: str = "./out/runlog.sqlite"


@dataclass
class LlmCfg:
    # providers: disabled | local | openai | azure
    provider: str = "disabled"
    model: str = ""
    model_path: str = ""  # for local models later
    openai: Dict[str, Any] = field(default_factory=dict)
    azure: Dict[str, Any] = field(default_factory=dict)
    temperature: float | None = None
    max_tokens: int | None = None


@dataclass
class PrivacyCfg:
    log_prompts: bool = False
    redact_paths: bool = True
    redact_ips: bool = True
    redact_emails: bool = True


@dataclass
class ParsingCfg:
    include_ext: List[str] = field(
        default_factory=lambda: [
            ".sh",
            ".bash",
            ".ksh",
            ".bat",
            ".cmd",
            ".ps1",
            ".pl",
            ".py",
        ]
    )

@dataclass
class AgentsCfg:
    reader_hints: bool = False

@dataclass
class Config:
    llm: LlmCfg = field(default_factory=LlmCfg)
    runtime: RuntimeCfg = field(default_factory=RuntimeCfg)
    privacy: PrivacyCfg = field(default_factory=PrivacyCfg)
    parsing: ParsingCfg = field(default_factory=ParsingCfg)
    agents: AgentsCfg = field(default_factory=AgentsCfg)

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        # Merge unknown keys into known defaults (keeps forward-compatibility)
        def merged(default_obj, src: dict):
            base = {**default_obj.__dict__}
            base.update(src or {})
            return base

        llm = LlmCfg(**merged(LlmCfg(), data.get("llm", {})))
        runtime = RuntimeCfg(**merged(RuntimeCfg(), data.get("runtime", {})))
        privacy = PrivacyCfg(**merged(PrivacyCfg(), data.get("privacy", {})))
        parsing = ParsingCfg(**merged(ParsingCfg(), data.get("parsing", {})))
        agents  = AgentsCfg(**merged(AgentsCfg(),  data.get("agents", {})))
        return Config(llm=llm, runtime=runtime, privacy=privacy, parsing=parsing, agents=agents)

    def hash(self) -> str:
        buf = io.StringIO()
        yaml.safe_dump(
            {
                "llm": self.llm.__dict__,
                "runtime": self.runtime.__dict__,
                "privacy": self.privacy.__dict__,
                "parsing": self.parsing.__dict__,
            },
            buf,
            sort_keys=True,
        )
        return hashlib.sha256(buf.getvalue().encode()).hexdigest()[:12]
