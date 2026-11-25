"""Deterministic logical clock used by rehearsals and tests."""

from __future__ import annotations


class LogicalClock:
    """A manually advanced integer clock.

    The trace files use integer logical seconds rather than wall time.  Keeping
    this clock explicit makes replay deterministic.
    """

    def __init__(self, now: int = 0):
        if type(now) is not int or isinstance(now, bool) or now < 0:
            raise ValueError("logical time must be a non-negative integer")
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance(self, seconds: int = 1) -> int:
        if type(seconds) is not int or isinstance(seconds, bool) or seconds < 0:
            raise ValueError("advance must be a non-negative integer")
        self.now += seconds
        return self.now

    def set(self, now: int) -> None:
        if type(now) is not int or isinstance(now, bool) or now < 0:
            raise ValueError("logical time must be a non-negative integer")
        self.now = now
