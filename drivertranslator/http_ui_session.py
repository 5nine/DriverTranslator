from __future__ import annotations

import contextlib
import secrets
import time
import urllib.parse
from typing import Dict, Tuple

_TTL_SEC = 30 * 60
_sessions: Dict[str, float] = {}


def parse_path_params(path: str) -> Tuple[str, Dict[str, str]]:
    if "?" not in path:
        return path, {}
    base, qs = path.split("?", 1)
    params: Dict[str, str] = {}
    for part in qs.split("&"):
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        params[k] = urllib.parse.unquote_plus(v)
    return base, params


def issue_session_token() -> str:
    now = time.time()
    for k, ts in list(_sessions.items()):
        if now - ts > _TTL_SEC:
            del _sessions[k]
    tok = secrets.token_urlsafe(24)
    _sessions[tok] = now
    return tok


def valid_session_token(tok: str) -> bool:
    if not tok:
        return False
    ts = _sessions.get(tok)
    if ts is None:
        return False
    if time.time() - ts > _TTL_SEC:
        with contextlib.suppress(KeyError):
            del _sessions[tok]
        return False
    return True
