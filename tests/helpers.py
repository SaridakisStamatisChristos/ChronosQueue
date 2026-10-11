from __future__ import annotations

import contextlib
import tempfile
from pathlib import Path
from typing import Iterator

from chronosqueue.clock import ManualClock
from chronosqueue.service import QueueService


@contextlib.contextmanager
def service_at(now_us: int = 1_700_000_000_000_000) -> Iterator[tuple[QueueService, ManualClock, Path]]:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "chronosqueue.db"
        clock = ManualClock(now_us)
        yield QueueService(str(path), clock=clock), clock, path
