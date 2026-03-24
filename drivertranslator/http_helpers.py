from __future__ import annotations

import html
import json
import time
from typing import Any, Dict, List, Optional

from .models import Config, HealthState


def http_response(status: str, content_type: str, body: bytes) -> bytes:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]
    return "\r\n".join(headers).encode("ascii") + body


def params_want_html(params: Dict[str, str]) -> bool:
    return (params.get("html") or "").lower() in ("1", "true", "yes", "on")


def control_feedback_html(
    *,
    ok: bool,
    headline: str,
    paragraphs: List[str],
    pre_json: Optional[Any] = None,
) -> bytes:
    status_cls = "banner-ok" if ok else "banner-bad"
    paras = "".join(f"<p class=\"detail\">{html.escape(p)}</p>" for p in paragraphs)
    pre_block = ""
    if pre_json is not None:
        pre_block = (
            "<p class=\"detail\"><b>Details (JSON)</b></p><pre>"
            + html.escape(json.dumps(pre_json, indent=2))
            + "</pre>"
        )
    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(headline)} — DriverTranslator</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      margin: 0; min-height: 100vh;
      padding: 28px 20px 48px;
      background: #f4f6f9; color: #0f172a;
      line-height: 1.5;
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #0f1218; color: #e8eaef; }}
    }}
    .wrap {{ max-width: 560px; margin: 0 auto; }}
    .banner {{
      padding: 22px 24px; border-radius: 14px; margin-bottom: 20px;
      font-size: 1.2rem; font-weight: 700; letter-spacing: -0.02em;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }}
    .banner-ok {{ background: linear-gradient(135deg, #dcfce7 0%, #bbf7d0 100%); color: #14532d; border: 1px solid #86efac; }}
    .banner-bad {{ background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%); color: #7f1d1d; border: 1px solid #fca5a5; }}
    @media (prefers-color-scheme: dark) {{
      .banner-ok {{ background: linear-gradient(135deg, #14532d 0%, #166534 100%); color: #bbf7d0; border-color: #22c55e; }}
      .banner-bad {{ background: linear-gradient(135deg, #7f1d1d 0%, #991b1b 100%); color: #fecaca; border-color: #ef4444; }}
    }}
    .detail {{ margin: 0 0 14px 0; color: #64748b; font-size: 15px; }}
    @media (prefers-color-scheme: dark) {{ .detail {{ color: #9aa3b2; }} }}
    pre {{
      background: #0f172a; color: #e2e8f0; padding: 16px 18px; border-radius: 12px;
      overflow-x: auto; font-size: 12px; line-height: 1.45; border: 1px solid #334155;
    }}
    a {{
      display: inline-block; margin-top: 8px; color: #2563eb; font-weight: 600;
      text-decoration: none; padding: 10px 0;
    }}
    a:hover {{ text-decoration: underline; }}
    @media (prefers-color-scheme: dark) {{ a {{ color: #7aa2ff; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="banner {status_cls}">{html.escape(headline)}</div>
    {paras}
    {pre_block}
    <p><a href="/">← Back to status</a></p>
  </div>
</body>
</html>"""
    return page.encode("utf-8")


def http_unauthorized() -> bytes:
    # Basic auth (password-only semantics; username ignored)
    headers = [
        "HTTP/1.1 401 Unauthorized",
        'WWW-Authenticate: Basic realm="DriverTranslator"',
        "Content-Type: text/plain",
        "Content-Length: 12",
        "Connection: close",
        "",
        "",
    ]
    return "\r\n".join(headers).encode("ascii") + b"unauthorized"


def parse_basic_auth_password(data: bytes) -> Optional[str]:
    try:
        text = data.decode("iso-8859-1", errors="replace")
    except Exception:
        return None
    # Look for Authorization header in the initial read buffer
    for line in text.split("\r\n"):
        if line.lower().startswith("authorization:"):
            v = line.split(":", 1)[1].strip()
            if not v.lower().startswith("basic "):
                return None
            import base64

            b64 = v.split(None, 1)[1].strip()
            try:
                raw = base64.b64decode(b64).decode("utf-8", errors="replace")
            except Exception:
                return None
            # raw is "user:pass"
            if ":" in raw:
                return raw.split(":", 1)[1]
            return ""
    return None


def format_uptime(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{secs:02d}s")
    return " ".join(parts)


def build_status_snapshot(*, cfg: Config, health: HealthState, amx: Any, started_at: float) -> Dict[str, Any]:
    now = time.monotonic()
    mode = "dry_run" if cfg.amx_dry_run else ("persistent" if cfg.amx_persistent else "connect_close")

    amx_connected = None
    amx_total_known = None
    if hasattr(amx, "connection_summary"):
        try:
            amx_connected, amx_total_known = amx.connection_summary()
        except Exception:
            amx_connected, amx_total_known = None, None

    uptime_seconds = int(now - started_at)
    return {
        "uptime_seconds": uptime_seconds,
        "mode": mode,
        "rti_clients": health.rti_clients,
        "tx_configured": len(cfg.tx_by_alias),
        "rx_configured": len(cfg.rx_by_alias),
        "amx_connected": amx_connected,
        "amx_total_known": amx_total_known,
    }
