from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class JobState(StrEnum):
    SCHEDULED = "scheduled"
    READY = "ready"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead-lettered"
    CANCELLED = "cancelled"


class RetryKind(StrEnum):
    FIXED = "fixed"
    EXPONENTIAL = "exponential"


@dataclass(frozen=True)
class RetryPolicy:
    kind: RetryKind = RetryKind.FIXED
    delay_ms: int = 1_000
    max_delay_ms: int = 86_400_000

    def delay_for_attempt(self, attempt_count: int) -> int:
        if attempt_count < 1:
            raise ValueError("attempt_count must be >= 1")
        if self.kind == RetryKind.FIXED:
            return self.delay_ms
        multiplier = 1 << min(attempt_count - 1, 30)
        return min(self.delay_ms * multiplier, self.max_delay_ms)


JSON = dict[str, Any] | list[Any] | str | int | float | bool | None
