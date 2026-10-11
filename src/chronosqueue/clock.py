from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now_us(self) -> int: ...


class SystemClock:
    def now_us(self) -> int:
        return time.time_ns() // 1_000


class ManualClock:
    """Thread-safe deterministic clock for boundary and concurrency tests."""

    def __init__(self, initial_us: int = 0) -> None:
        self._value = initial_us
        self._lock = threading.Lock()

    def now_us(self) -> int:
        with self._lock:
            return self._value

    def set_us(self, value: int) -> None:
        with self._lock:
            self._value = value

    def advance_us(self, delta: int) -> int:
        if delta < 0:
            raise ValueError("clock cannot move backwards")
        with self._lock:
            self._value += delta
            return self._value


def us_to_rfc3339(value: int | None) -> str | None:
    if value is None:
        return None
    dt = datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def rfc3339_to_us(value: str) -> int:
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp must be a non-empty RFC3339 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return int(dt.timestamp() * 1_000_000)
