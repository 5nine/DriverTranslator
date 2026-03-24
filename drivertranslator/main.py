from __future__ import annotations

import argparse
import asyncio
import logging
from typing import List, Optional

from .config_loader import load_config, validate_config as _validate_config
from .log_ring import RingBufferLogHandler as _RingBufferLogHandler
from .server import run_server

# CLI entrypoint only; see server.py for listener bootstrap.


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="RTI -> WyreStorm NHD-CTL emulator -> AMX AVoIP translator")
    parser.add_argument("--config", required=True, help="Path to config.json")
    parser.add_argument("--listen", default="0.0.0.0", help="Address to bind (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=2323, help="TCP port to listen on (default 2323)")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ring = _RingBufferLogHandler()
    ring.setLevel(logging.DEBUG)
    ring.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(ring)

    cfg = load_config(args.config)
    _validate_config(cfg)

    try:
        asyncio.run(
            run_server(cfg=cfg, config_path=args.config, listen=args.listen, port=args.port)
        )
    except KeyboardInterrupt:
        return 130
    return 0
