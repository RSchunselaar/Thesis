from __future__ import annotations
import re
from pathlib import Path

IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PATH_RE = re.compile(r"([A-Za-z]:\\\\[^\s]+|/[^\s]+)")


class Redactor:
    def __init__(self, redact_paths=True, redact_ips=True, redact_emails=True):
        self.redact_paths = redact_paths
        self.redact_ips = redact_ips
        self.redact_emails = redact_emails

    def redact(self, s: str) -> str:
        out = s
        if self.redact_ips:
            out = IP_RE.sub("<IP>", out)
        if self.redact_emails:
            out = EMAIL_RE.sub("<EMAIL>", out)
        if self.redact_paths:
            out = PATH_RE.sub("<PATH>", out)
        return out
