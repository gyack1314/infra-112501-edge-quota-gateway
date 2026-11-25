"""Small class decorator providing reentrant in-process synchronization."""

from __future__ import annotations

import functools
import threading
from typing import Any


def synchronized(cls: type[Any]) -> type[Any]:
    """Serialize public method calls on one gateway instance.

    The simulation is single-process, but a shift handover may deliver requests
    from several site threads at once.  An ``RLock`` also permits public methods
    to call other public reconciliation methods without deadlocking.
    """

    original_init = cls.__init__

    @functools.wraps(original_init)
    def init_with_lock(self: Any, *args: Any, **kwargs: Any) -> None:
        self._gateway_lock = threading.RLock()
        original_init(self, *args, **kwargs)

    cls.__init__ = init_with_lock

    for name, attribute in list(vars(cls).items()):
        if name.startswith("_") or not callable(attribute):
            continue

        def make_wrapper(method: Any) -> Any:
            @functools.wraps(method)
            def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
                with self._gateway_lock:
                    return method(self, *args, **kwargs)

            return wrapper

        setattr(cls, name, make_wrapper(attribute))

    return cls
