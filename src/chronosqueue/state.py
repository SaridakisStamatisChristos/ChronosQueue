from __future__ import annotations

from .errors import Conflict
from .models import JobState


_ALLOWED: dict[JobState, set[JobState]] = {
    JobState.SCHEDULED: {JobState.READY, JobState.CANCELLED},
    JobState.READY: {JobState.LEASED, JobState.CANCELLED},
    JobState.LEASED: {JobState.SUCCEEDED, JobState.FAILED, JobState.READY, JobState.DEAD_LETTERED, JobState.CANCELLED},
    JobState.FAILED: {JobState.SCHEDULED, JobState.DEAD_LETTERED},
    JobState.SUCCEEDED: set(),
    JobState.DEAD_LETTERED: {JobState.SCHEDULED},
    JobState.CANCELLED: {JobState.SCHEDULED},
}

TERMINAL_STATES = {JobState.SUCCEEDED, JobState.DEAD_LETTERED, JobState.CANCELLED}


def validate_transition(before: JobState, after: JobState, *, administrative: bool = False) -> None:
    if after not in _ALLOWED[before]:
        raise Conflict(f"illegal job state transition: {before} -> {after}")
    if before in {JobState.DEAD_LETTERED, JobState.CANCELLED} and after == JobState.SCHEDULED and not administrative:
        raise Conflict("terminal requeue requires explicit administrative operation")
