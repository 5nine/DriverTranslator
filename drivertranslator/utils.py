from __future__ import annotations

import random
import re
from typing import Any, Optional, Tuple


def as_int(v: Any, *, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def bind_addr(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def opt_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def as_bool(v: Any, *, default: bool) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def clamp_int(v: Any, *, default: int, min_v: int, max_v: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = default
    return max(min_v, min(max_v, x))


def retry_delay_seconds(*, attempt_index: int, initial_ms: int, max_ms: int) -> float:
    """
    attempt_index: 1..N (1 is first retry delay)
    Exponential backoff with small jitter.
    """
    if initial_ms <= 0 or max_ms <= 0:
        return 0.0
    base_ms = min(max_ms, int(initial_ms * (2 ** max(0, attempt_index - 1))))
    jitter_ms = int(base_ms * random.uniform(0.0, 0.2))
    return (base_ms + jitter_ms) / 1000.0


_RX_ALIAS_RE = re.compile(r"^OUT(\d+)\b", re.IGNORECASE)
_TX_ALIAS_RE = re.compile(r"^IN(\d+)\b", re.IGNORECASE)


def rx_alias_sort_key(alias: str) -> Tuple[int, str]:
    """
    Natural sort for RX aliases like OUT1-TV1, OUT10-TV10, ...
    """
    m = _RX_ALIAS_RE.match(alias.strip())
    if not m:
        return (10**9, alias)
    try:
        return (int(m.group(1)), alias)
    except Exception:
        return (10**9, alias)


def tx_alias_sort_key(alias: str) -> Tuple[int, str]:
    """
    Natural sort for TX aliases like IN1-BOX1, IN10-BOX10, ...
    """
    m = _TX_ALIAS_RE.match(alias.strip())
    if not m:
        return (10**9, alias)
    try:
        return (int(m.group(1)), alias)
    except Exception:
        return (10**9, alias)
