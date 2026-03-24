from __future__ import annotations

import asyncio
from typing import Optional, Tuple


async def open_connection(
    host: str,
    port: int,
    *,
    timeout: float,
    local_addr: Optional[Tuple[str, int]] = None,
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open TCP connection, optionally binding to a specific local address (e.g. AVoIP NIC)."""
    if local_addr is None:
        return await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await asyncio.wait_for(
        loop.create_connection(lambda: protocol, host, port, local_addr=local_addr),
        timeout=timeout,
    )
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


def crlf_line(line: str) -> bytes:
    return (line + "\r\n").encode("utf-8", errors="replace")
