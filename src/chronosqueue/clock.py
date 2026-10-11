from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


class Clock:
    def now_us(self) -> int:
        raise NotImplementedError


class SystemClock(Clock):
    def now_us(self) -> int:
        return time.time_ns() // 1_000


@dataclass
class FakeClock(Clock):
    _now_us: int

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def now_us(self) -> int:
        with self._lock:
            return self._now_us

    def set_us(self, value: int) -> None:
        with self._lock:
            self._now_us = value

    def advance_us(self, delta: int) -> None:
        if delta < 0:
            raise ValueError("clock cannot move backwards")
        with self._lock:
            self._now_us += delta


def parse_timestamp_us(value: str) -> int:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    utc = dt.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def format_timestamp_us(value: int | None) -> str | None:
    if value is None:
        return None
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (epoch + timedelta(microseconds=value)).isoformat(timespec="microseconds").replace("+00:00", "Z")
