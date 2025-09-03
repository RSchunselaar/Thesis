from __future__ import annotations
import sqlite3
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  cmd TEXT NOT NULL,
  config_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  ts TEXT NOT NULL,
  level TEXT NOT NULL,
  msg TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS llm_calls(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  ts TEXT NOT NULL,
  role TEXT NOT NULL,
  model TEXT,
  endpoint TEXT,
  prompt_chars INTEGER,
  input_tokens INTEGER,
  output_tokens INTEGER,
  total_tokens INTEGER,
  latency_ms REAL,
  src TEXT,
  command_snippet TEXT,
  targets_count INTEGER,
  status TEXT,
  reasoning TEXT,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS llm_prompts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  ts TEXT NOT NULL,
  role TEXT NOT NULL,
  prompt TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS role_latencies(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  role TEXT NOT NULL,
  seconds REAL NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);
"""

@dataclass
class Run:
    id: int

class RunLogger:
    def __init__(self, path: str, echo: bool = False):
        self.path = path
        self.echo = echo
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._run_id: int | None = None

    @property
    def run_id(self) -> int | None:
        return self._run_id

    def start(self, cmd: str, config_hash: str) -> Run:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO runs(started_at, cmd, config_hash) VALUES (?, ?, ?)",
            (datetime.utcnow().isoformat(), cmd, config_hash),
        )
        self.conn.commit()
        self._run_id = cur.lastrowid
        return Run(id=self._run_id)

    def log(self, level: str, msg: str):
        assert self._run_id is not None
        ts = datetime.utcnow().isoformat()
        self.conn.execute(
            "INSERT INTO events(run_id, ts, level, msg) VALUES (?, ?, ?, ?)",
            (self._run_id, ts, level.upper(), msg),
        )
        self.conn.commit()
        if self.echo:                        # <-- NEW
            print(f"[{ts}] {level.upper():5s} {msg}")

    def log_llm(self, *, role: str, model: str = "", endpoint: str = "",
                prompt_chars: int = 0, input_tokens: int | None = None,
                output_tokens: int | None = None, total_tokens: int | None = None,
                latency_ms: float = 0.0, status: str = "ok", src: str | None = None,
                command_snippet: str | None = None, targets_count: int | None = None,
                reasoning: str | None = None):
        """Record one LLM call (tokens may be None if API doesn't return them)."""
        assert self._run_id is not None
        self.conn.execute(
            "INSERT INTO llm_calls(run_id, ts, role, model, endpoint, prompt_chars, "
            "input_tokens, output_tokens, total_tokens, latency_ms, src, command_snippet, "
            "targets_count, status, reasoning) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self._run_id,
                datetime.utcnow().isoformat(),
                role, model, endpoint,
                int(prompt_chars),
                input_tokens, output_tokens, total_tokens,
                float(latency_ms),
                src,
                (command_snippet or "")[:400],
                targets_count,
                status,
                (reasoning or "")[:1000],
            ),
        )
        self.conn.commit()

    def log_prompt(self, *, role: str, prompt: str):
        """Record the exact prompt (pre-redacted upstream). Controlled by cfg.privacy.log_prompts."""
        assert self._run_id is not None
        self.conn.execute(
            "INSERT INTO llm_prompts(run_id, ts, role, prompt) VALUES (?, ?, ?, ?)",
            (self._run_id, datetime.utcnow().isoformat(), role, prompt[:4000]),
        )
        self.conn.commit()

    def log_role_latency(self, role: str, seconds: float):
        assert self._run_id is not None
        self.conn.execute(
            "INSERT INTO role_latencies(run_id, role, seconds) VALUES (?, ?, ?)",
            (self._run_id, role, float(seconds)),
        )
        self.conn.commit()

    def finish(self):
        if self._run_id is not None:
            ts = datetime.utcnow().isoformat()
            self.conn.execute(
                "UPDATE runs SET finished_at=? WHERE id=?",
                (ts, self._run_id),
            )
            self.conn.commit()
            if self.echo:
                print(f"[{ts}] FINISH run_id={self._run_id}")
            self.conn.close()
