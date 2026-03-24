from __future__ import annotations

import collections
import logging
from typing import List

_LOG_RING: "collections.deque[str]" = collections.deque(maxlen=500)


class RingBufferLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        _LOG_RING.append(msg)


def get_log_tail(n: int) -> List[str]:
    n = max(0, min(int(n), 500))
    if n == 0:
        return []
    return list(_LOG_RING)[-n:]
