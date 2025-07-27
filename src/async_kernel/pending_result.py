# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import threading
from collections import deque
from typing import TYPE_CHECKING, Generic

import anyio

from async_kernel.typing import T
from async_kernel.utils import wait_thread_event

if TYPE_CHECKING:
    from collections.abc import Iterable

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
        self._done_callbacks = set()

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

    def set_result(self, value):
        if self._event_done.is_set() or threading.current_thread() is not self.thread:
            raise RuntimeError
        self.result = value
        self._event_done.set()
        if self._anyio_event_done:
            self._anyio_event_done.set()
        while self._done_callbacks:
            self._done_callbacks.pop()(self)

    def set_exception(self, exception: BaseException | type[BaseException]):
        if self._event_done.is_set() or threading.current_thread() is not self.thread:
            raise RuntimeError
        self._exception = exception
        self._event_done.set()
        if self._anyio_event_done:
            self._anyio_event_done.set()
        while self._done_callbacks:
            self._done_callbacks.pop()(self)

    def done(self):
        return self._event_done.is_set()

    @classmethod
    async def as_completed(cls, items: Iterable[PendingResult[T]]):
        "An iterator to wait for pending results to complete."
        event_pending_done = threading.Event()
        has_result: deque[PendingResult[T]] = deque()
        n = 0

        def _on_done(pending_):
            has_result.append(pending_)
            event_pending_done.set()

        for pending in items:
            n += 1
            if pending.done():
                has_result.append(pending)
            else:
                pending._done_callbacks.add(_on_done)

        for _ in range(n):
            if has_result:
                event_pending_done.clear()
                yield has_result.popleft()
                continue
            await wait_thread_event(event_pending_done)
