from .lease import LeaseMixin
from .outcome import OutcomeMixin


class WorkerMixin(LeaseMixin, OutcomeMixin):
    pass
