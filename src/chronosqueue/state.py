from __future__ import annotations

from .errors import InvalidTransition

STATES = frozenset({
    "scheduled",
    "ready",
    "leased",
    "succeeded",
    "failed",
    "dead-lettered",
    "cancelled",
})

# Normal runtime transitions. Administrative resurrection is intentionally separate.
TRANSITIONS = {
    "scheduled": frozenset({"ready", "cancelled"}),
    "ready": frozenset({"leased", "cancelled"}),
    "leased": frozenset({"succeeded", "failed", "ready", "dead-lettered", "cancelled"}),
    "failed": frozenset({"ready", "cancelled"}),
    "succeeded": frozenset(),
    "dead-lettered": frozenset(),
    "cancelled": frozenset(),
}

ADMIN_TRANSITIONS = {
    "dead-lettered": frozenset({"ready", "scheduled"}),
    "cancelled": frozenset({"ready", "scheduled"}),
}


def ensure_transition(old: str, new: str, *, administrative: bool = False) -> None:
    if old not in STATES or new not in STATES:
        raise InvalidTransition(f"unknown state transition {old!r} -> {new!r}")
    allowed = TRANSITIONS[old]
    if administrative:
        allowed = allowed | ADMIN_TRANSITIONS.get(old, frozenset())
    if new not in allowed:
        kind = "administrative" if administrative else "runtime"
        raise InvalidTransition(f"illegal {kind} transition {old} -> {new}")
