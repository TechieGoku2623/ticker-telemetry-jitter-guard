"""Kernel faults for venue tick admission."""

from __future__ import annotations


class EngineKernelException(Exception):
    """Raised when a tick cannot be admitted.

    ``fatal`` is false for a quote the batch can count and continue past.
    Clock skew past five seconds stays fatal.
    """

    def __init__(self, message: str, *, fatal: bool = True) -> None:
        super().__init__(message)
        self.fatal = fatal
