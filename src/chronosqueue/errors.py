class ChronosError(Exception):
    status = 400
    code = "chronos_error"


class NotFound(ChronosError):
    status = 404
    code = "not_found"


class Conflict(ChronosError):
    status = 409
    code = "conflict"


class StaleLease(Conflict):
    code = "stale_lease"


class InvalidTransition(Conflict):
    code = "invalid_transition"


class ValidationError(ChronosError):
    status = 422
    code = "validation_error"


class StorageUnavailable(ChronosError):
    status = 503
    code = "storage_unavailable"
