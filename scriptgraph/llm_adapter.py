from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class LLMConfig:
    provider: str
    model: str | None = None 
    openai_base: str = "https://api.openai.com"
    json_mode: bool = True
    temperature: float | None = None
    max_tokens: int | None = None


class LLMClient:
    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg

    def chat(self, system: str, user: str, *, return_meta: bool = False, **extra):
        if self.cfg.provider == "disabled":
            return ("{\"targets\":[],\"reasoning\":\"LLM disabled\"}", {}) if return_meta else "{\"targets\":[],\"reasoning\":\"LLM disabled\"}"
        if self.cfg.provider == "openai":
            content, meta = self._openai_chat(system, user, **extra)
        else:
            raise ValueError(f"Unknown provider {self.cfg.provider}")
        return (content, meta) if return_meta else content

    def _openai_chat(self, system: str, user: str, **extra) -> tuple[str, dict]:
        import os, requests, json, time
        api_key = os.environ.get("OPENAI_API_KEY"); assert api_key, "OPENAI_API_KEY not set"
        url = f"{self.cfg.openai_base}/v1/chat/completions"
        model = self.cfg.model or "gpt-5-mini"
        payload = {
            "model": model,
            "messages": [{"role":"system","content":system},{"role":"user","content":user}],
        }
        # GPT-5 family: omit temperature + response_format
        if not model.startswith("gpt-5") and (self.cfg.temperature is not None):
            payload["temperature"] = self.cfg.temperature
        if self.cfg.json_mode and not model.startswith("gpt-5"):
            payload["response_format"] = {"type":"json_object"}
        payload.update(extra)

        t0 = time.monotonic()
        resp = requests.post(url, headers={"Authorization": f"Bearer {api_key}"}, json=payload, timeout=60)
        dt = (time.monotonic() - t0) * 1000.0
        try:
            body = resp.json()
        except Exception:
            body = {"error": {"message": resp.text}}
        if resp.status_code >= 400:
            raise RuntimeError(f"OpenAI API error {resp.status_code}: {body}")
        usage = body.get("usage", {}) or {}
        meta = {
            "model": model,
            "endpoint": self.cfg.openai_base,
            "latency_ms": dt,
            "prompt_tokens": usage.get("prompt_tokens") or usage.get("input_tokens"),
            "completion_tokens": usage.get("completion_tokens") or usage.get("output_tokens"),
            "total_tokens": usage.get("total_tokens"),
        }
        content = body["choices"][0]["message"]["content"]
        return content, meta