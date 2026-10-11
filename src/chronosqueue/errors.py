from __future__ import annotations


class ChronosError(Exception):
    status = 400
    code = "invalid_request"

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFound(ChronosError):
    status = 404
    code = "not_found"


class Conflict(ChronosError):
    status = 409
    code = "conflict"


class StaleLease(Conflict):
    code = "stale_lease"


class PayloadTooLarge(ChronosError):
    status = 413
    code = "payload_too_large"


class StorageUnavailable(ChronosError):
    status = 503
    code = "storage_unavailable"
