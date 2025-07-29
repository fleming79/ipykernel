# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Generic, Literal, Self

import anyio

from async_kernel.typing import T
from async_kernel.utils import wait_thread_event

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["PendingResult"]


class PendingResult(Generic[T]):
    """An anyio compatible synchronization primitive for awaiting a result.

    thread: The thread where the result must be set. Defaults to the current thread.
    """

    __slots__ = ["_anyio_event_done", "_done_callbacks", "_event_done", "_exception", "result", "thread"]
    result: T

    def __init__(self, thread: threading.Thread | None = None) -> None:
        self._event_done = threading.Event()
        self._exception = None
        self._anyio_event_done = None
        self.thread = thread or threading.current_thread()
        self._done_callbacks = []

    async def wait(self) -> T:
        "Wait for the result (thread-safe)."
        if not self._event_done.is_set():
            if threading.current_thread() is self.thread:
                if not self._anyio_event_done:
                    self._anyio_event_done = anyio.Event()
                await self._anyio_event_done.wait()
            else:
                await wait_thread_event(self._event_done)
        if self._exception:
            raise self._exception
        return self.result

    def wait_sync(self) -> T:
        "Synchronously wait for the result."
        if threading.current_thread() is self.thread:
            raise RuntimeError
        self._event_done.wait()
        if self._exception:
            raise self._exception
        return self.result

    def set_result(self, value: T):
        self._set_value("result", value)

    def set_exception(self, exception: BaseException):
        self._set_value("exception", exception)

    def _set_value(self, mode: Literal["result", "exception"], value):
        if self._event_done.is_set() or threading.current_thread() is not self.thread:
            raise RuntimeError
        if mode == "exception":
            self._exception = value
        else:
            self.result = value
        self._event_done.set()
        if self._anyio_event_done:
            self._anyio_event_done.set()
        for cb in reversed(self._done_callbacks):
            try:
                cb(self)
            except Exception:
                pass

    def done(self):
        return self._event_done.is_set()

    def add_done_callback(self, callback: Callable[[Self], None]):
        self._done_callbacks.append(callback)
