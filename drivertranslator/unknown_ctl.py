from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

LOG = logging.getLogger("drivertranslator")

_MAX_KEYS = 400
_entries: Dict[str, Dict[str, Any]] = {}
_lock = threading.Lock()
_file: Optional[Path] = None


def configure(*, enabled: bool, config_dir: Path, persist_path: Optional[str]) -> None:
    global _file
    if not enabled:
        _file = None
        return
    if persist_path and str(persist_path).strip():
        _file = Path(persist_path).expanduser().resolve()
    else:
        _file = (config_dir / "unknown_ctl.json").resolve()


def persist_file() -> Optional[Path]:
    """Path to JSON file when persistence is enabled, else None."""
    return _file


def load_from_disk() -> None:
    global _entries
    path = _file
    if path is None or not path.is_file():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        LOG.warning("unknown_ctl: could not load %s: %s", path, e)
        return
    entries = raw.get("entries") if isinstance(raw, dict) else None
    if not isinstance(entries, dict):
        return
    loaded: Dict[str, Dict[str, Any]] = {}
    for k, v in entries.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        try:
            c = int(v.get("count", 1))
            first = str(v.get("first", ""))
            last = str(v.get("last", ""))
        except (TypeError, ValueError):
            continue
        if c < 1 or len(k) > 2000:
            continue
        loaded[k] = {"count": c, "first": first or "?", "last": last or "?"}
        if len(loaded) >= _MAX_KEYS:
            break
    with _lock:
        _entries.clear()
        _entries.update(loaded)
    LOG.info("unknown_ctl: loaded %d entr%s from %s", len(loaded), "y" if len(loaded) == 1 else "ies", path)


def _save_to_disk() -> None:
    path = _file
    if path is None:
        return
    with _lock:
        payload = {"v": 1, "entries": dict(_entries)}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
    except OSError as e:
        LOG.warning("unknown_ctl: save failed %s: %s", path, e)


def clear_persisted() -> None:
    with _lock:
        _entries.clear()
    _save_to_disk()


def record(line: str) -> None:
    key = line.strip()
    if not key or len(key) > 2000:
        return
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with _lock:
        if key in _entries:
            e = _entries[key]
            e["count"] = int(e["count"]) + 1
            e["last"] = now
        else:
            if len(_entries) >= _MAX_KEYS:
                victim = min(
                    _entries.items(),
                    key=lambda kv: (int(kv[1]["count"]), kv[1]["last"]),
                )[0]
                del _entries[victim]
            _entries[key] = {"count": 1, "first": now, "last": now}
    _save_to_disk()


def page_text() -> str:
    with _lock:
        items = list(_entries.items())
    if not items:
        return (
            "(No unrecognized commands yet.)\n\n"
            "When the WyreStorm/RTI driver sends a line the emulator does not handle, "
            "it appears here with a count."
        )
    items.sort(key=lambda x: (-int(x[1]["count"]), x[1]["last"]))
    lines = [
        "# DriverTranslator — unrecognized NHD-CTL / RTI TCP command lines",
        "# How to read this:",
        "#   - These are EXACT lines sent TO this service (RTI port, e.g. 2323).",
        "#   - Server replied: unknown command",
        "#   - COUNT = how many times that same line was sent (deduplicated).",
        "#   - Times are UTC. Copy from the dashed line down and paste into support chat.",
        "# ---------------------------------------------------------------------------",
        "",
    ]
    for cmd, meta in items:
        lines.append(
            f"{int(meta['count'])}× | first: {meta['first']} | last: {meta['last']}"
        )
        lines.append(f"    {cmd}")
        lines.append("")
    return "\n".join(lines)
