from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

LOG = logging.getLogger("drivertranslator")

_lock = threading.Lock()
_text: str = ""
_updated_at: float = 0.0
_file: Optional[Path] = None


def configure(*, config_dir: Path) -> None:
    global _file
    _file = (config_dir / "staff_notes.json").resolve()


def load_from_disk() -> None:
    global _text, _updated_at
    path = _file
    if path is None or not path.is_file():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        LOG.warning("staff_notes: could not load %s: %s", path, e)
        return
    if not isinstance(raw, dict):
        return
    text = raw.get("text", "")
    updated = raw.get("updated_at", 0.0)
    if not isinstance(text, str):
        return
    try:
        updated_f = float(updated)
    except (TypeError, ValueError):
        updated_f = 0.0
    with _lock:
        _text = text
        _updated_at = updated_f
    LOG.info("staff_notes: loaded from %s", path)


def _save_to_disk() -> None:
    path = _file
    if path is None:
        return
    with _lock:
        payload = {"v": 1, "text": _text, "updated_at": _updated_at}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
    except OSError as e:
        LOG.warning("staff_notes: save failed %s: %s", path, e)


def snapshot() -> Dict[str, Any]:
    with _lock:
        return {"text": _text, "updated_at": _updated_at}


def set_text(text: str) -> Dict[str, Any]:
    global _text, _updated_at
    if not isinstance(text, str):
        text = str(text)
    now = time.time()
    with _lock:
        _text = text
        _updated_at = now
    _save_to_disk()
    return {"text": _text, "updated_at": _updated_at}


def clear() -> Dict[str, Any]:
    return set_text("")


def handle_api(method: str, body: bytes) -> Tuple[int, str, bytes]:
    if method == "GET":
        payload = snapshot()
        raw = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        return 200, "application/json; charset=utf-8", raw

    if method == "POST":
        try:
            data = json.loads(body.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            return 400, "text/plain; charset=utf-8", b"invalid json"
        if not isinstance(data, dict):
            return 400, "text/plain; charset=utf-8", b"expected object"
        if data.get("action") == "clear":
            payload = clear()
        else:
            payload = set_text(data.get("text", ""))
        raw = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        return 200, "application/json; charset=utf-8", raw

    return 405, "text/plain; charset=utf-8", b"method not allowed"


def page_html() -> bytes:
    return """<!doctype html>
<html lang="sv">
<head>
  <meta charset="utf-8"/>
  <title>Anteckningar</title>
  <style>
    :root {
      color-scheme: light;
      --frame: #c4a24a;
      --panel: #fff6d8;
      --ink: #2e2618;
      --muted: #7a6b4f;
      --placeholder: #a89472;
      --btn-bg: #f5e8bc;
      --btn-border: #c4a24a;
      --btn-hover: #edd9a0;
      --ok: #2d6a3e;
      --warn: #9a3412;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      padding: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
      background: var(--panel);
      color: var(--ink);
      font-size: clamp(18px, 1.65vw, 28px);
    }
    .page {
      position: absolute;
      inset: 0;
      display: flex;
      flex-direction: column;
    }
    .frame {
      flex: 1;
      min-height: 0;
      border: 3px solid var(--frame);
      background: var(--panel);
      padding: 0;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    textarea {
      flex: 1;
      width: 100%;
      min-height: 0;
      resize: none;
      border: none;
      padding: 1.65% 1.88%;
      font: inherit;
      font-size: 1em;
      line-height: 1.55;
      color: var(--ink);
      background: var(--panel);
    }
    textarea:focus {
      outline: none;
    }
    textarea::placeholder {
      color: var(--placeholder);
    }
    .toolbar {
      display: flex;
      align-items: center;
      gap: 1%;
      flex-shrink: 0;
      padding: 1% 1.2%;
      border-top: 1px solid rgba(196, 162, 74, 0.35);
      background: var(--panel);
    }
    button {
      font: inherit;
      cursor: pointer;
      border: 1px solid var(--btn-border);
      padding: 0.55em 1.2em;
      font-weight: 600;
      font-size: 0.85em;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--ink);
      background: var(--btn-bg);
      transition: background 0.15s;
    }
    button:hover { background: var(--btn-hover); }
    button:active { transform: scale(0.98); }
    .status {
      flex: 1;
      font-size: 0.8em;
      color: var(--muted);
      text-align: right;
    }
    .status.ok { color: var(--ok); }
    .status.warn { color: var(--warn); }
  </style>
</head>
<body>
  <div class="page">
    <div class="frame">
      <textarea id="notes" placeholder="Skriv anteckningar här" spellcheck="true"></textarea>
      <div class="toolbar">
        <button type="button" id="clearBtn">Rensa</button>
        <div class="status" id="status" aria-live="polite">Laddar…</div>
      </div>
    </div>
  </div>
  <script>
    (function () {
      const notesEl = document.getElementById('notes');
      const statusEl = document.getElementById('status');
      const clearBtn = document.getElementById('clearBtn');
      const apiUrl = '/anteckningar/api';

      let localVersion = 0;
      let dirty = false;
      let saveTimer = null;
      let saving = false;

      function setStatus(msg, cls) {
        statusEl.textContent = msg;
        statusEl.className = 'status' + (cls ? ' ' + cls : '');
      }

      function formatTime(ts) {
        if (!ts) return '';
        try {
          return new Date(ts * 1000).toLocaleString('sv-SE', {
            day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit'
          });
        } catch (_e) {
          return '';
        }
      }

      async function fetchNotes() {
        const r = await fetch(apiUrl, { cache: 'no-store' });
        if (!r.ok) throw new Error('Kunde inte hämta');
        return r.json();
      }

      async function saveNotes(text) {
        saving = true;
        setStatus('Sparar…', '');
        try {
          const r = await fetch(apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: text })
          });
          if (!r.ok) throw new Error('Kunde inte spara');
          const data = await r.json();
          localVersion = data.updated_at || 0;
          dirty = false;
          setStatus('Sparat ' + formatTime(localVersion), 'ok');
        } catch (_e) {
          setStatus('Kunde inte spara — försök igen', 'warn');
        } finally {
          saving = false;
        }
      }

      function scheduleSave() {
        dirty = true;
        setStatus('Skriver…', '');
        if (saveTimer) clearTimeout(saveTimer);
        saveTimer = setTimeout(function () {
          saveNotes(notesEl.value);
        }, 600);
      }

      async function poll() {
        if (document.hidden || dirty || saving || document.activeElement === notesEl) return;
        try {
          const data = await fetchNotes();
          const serverTs = data.updated_at || 0;
          if (serverTs > localVersion && data.text !== notesEl.value) {
            notesEl.value = data.text || '';
            localVersion = serverTs;
            setStatus('Uppdaterad ' + formatTime(serverTs), 'ok');
          }
        } catch (_e) {
          // tyst vid tillfälliga nätverksfel
        }
      }

      clearBtn.addEventListener('click', async function () {
        if (!confirm('Rensa alla anteckningar för alla som använder sidan?')) return;
        if (saveTimer) clearTimeout(saveTimer);
        dirty = false;
        saving = true;
        setStatus('Rensar…', '');
        try {
          const r = await fetch(apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'clear' })
          });
          if (!r.ok) throw new Error('Kunde inte rensa');
          const data = await r.json();
          notesEl.value = '';
          localVersion = data.updated_at || 0;
          setStatus('Rensat', 'ok');
        } catch (_e) {
          setStatus('Kunde inte rensa', 'warn');
        } finally {
          saving = false;
        }
      });

      notesEl.addEventListener('input', scheduleSave);

      fetchNotes().then(function (data) {
        notesEl.value = data.text || '';
        localVersion = data.updated_at || 0;
        setStatus('Senast sparad ' + formatTime(localVersion), 'ok');
      }).catch(function () {
        setStatus('Kunde inte ladda anteckningar', 'warn');
      });

      setInterval(poll, 2500);
    })();
  </script>
</body>
</html>
""".encode("utf-8")
