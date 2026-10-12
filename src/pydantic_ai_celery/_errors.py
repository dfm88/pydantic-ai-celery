"""Failures the durability wrapper raises."""


class NonRetryableDurabilityError(Exception):
    """Terminal durability failure. Excluded from autoretry."""


class DurabilityDivergence(NonRetryableDurabilityError):
    """The journaled operation does not match the operation about to run."""


class RunInProgress(Exception):
    """Resume was requested while the owner still holds the lease."""


class LeaseHeld(Exception):
    """Another delivery holds the run lease."""


class LeaseLost(Exception):
    """This delivery no longer holds the lease it writes under."""


class UnitTimeout(Exception):
    """A model or tool unit exceeded its timeout."""
