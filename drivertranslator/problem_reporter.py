from __future__ import annotations

import contextlib
import logging
import time
from typing import Dict, Optional

from .models import ProblemState

LOG = logging.getLogger("drivertranslator")


class LocalProblemReporter:
    def __init__(
        self,
        *,
        min_interval_seconds: int = 10,
        repeat_suppression_seconds: int = 300,
    ) -> None:
        self._min_interval = max(0, int(min_interval_seconds))
        self._repeat_suppression = max(0, int(repeat_suppression_seconds))
        self._last_sent_at: Dict[str, float] = {}
        self._last_sent_msg: Dict[str, str] = {}
        self._problems: Optional[ProblemState] = None

    def attach_problem_state(self, problems: ProblemState) -> None:
        self._problems = problems

    async def problem(self, key: str, message: str) -> None:
        """
        Problems-only notification with anti-spam:
        - per-key minimum interval
        - suppress identical messages for a longer window
        """
        now = time.monotonic()
        last_at = self._last_sent_at.get(key)
        last_msg = self._last_sent_msg.get(key)

        msg = message.strip()
        if last_msg == msg and last_at is not None and (now - last_at) < self._repeat_suppression:
            return
        if last_at is not None and (now - last_at) < self._min_interval:
            return

        self._last_sent_at[key] = now
        self._last_sent_msg[key] = msg
        # Always log locally (and on the status page log tail).
        LOG.error("%s", msg)
        if self._problems is not None:
            with contextlib.suppress(Exception):
                await self._problems.record(key=key, message=msg)
