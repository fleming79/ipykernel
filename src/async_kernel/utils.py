# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import errno
import inspect
import logging
import sys
import threading
import weakref
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, ParamSpec, Self, TypeVar

import anyio
import anyio.to_thread
from anyio.abc import TaskStatus
from zmq import Socket, ZMQError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from anyio.abc import TaskStatus

__all__ = ["PendingResult", "ThreadSafeCaller", "bind_socket", "do_not_debug_this_thread", "mark_thread_debugpy_ignore"]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules

P = ParamSpec("P")
T = TypeVar("T")


def bind_socket(socket: Socket, transport: Literal["tcp", "ipc"], ip: str, port: int = 0, max_attempts=100) -> int:
    def _try_bind_socket(port: int):
        if transport == "tcp":
            if port <= 0:
                port = socket.bind_to_random_port(f"tcp://{ip}")
            else:
                socket.bind(f"tcp://{ip}:{port}")
        elif transport == "ipc":
            if port <= 0:
                port = 1
                while True:
                    port = port + 1
                    path = f"{ip}-{port}"
                    if not Path(path).exists():
                        break
            else:
                path = f"{ip}-{port}"
            socket.bind(f"ipc://{path}")
        return port

    try:
        win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
    except AttributeError:
        win_in_use = None
    # Try up to 100 times to bind a port when in conflict to avoid
    # infinite attempts in bad setups
    max_attempts = 1 if port else max_attempts
    for attempt in range(max_attempts):
        try:
            return _try_bind_socket(port)
        except ZMQError as e:
            # Raise if we have any error not related to socket binding
            if e.errno not in {errno.EADDRINUSE, win_in_use}:
                raise
            if attempt == max_attempts - 1:
                raise
    msg = f"Failed to bind a {socket}:{port}"
    raise RuntimeError(msg)


def mark_thread_debugpy_ignore(thread: threading.Thread, name="", *, unhide=False):
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread.pydev_do_not_trace = not unhide  # type: ignore[attr-defined]
    if name:
        thread.name = name


@contextlib.contextmanager
def do_not_debug_this_thread(name=""):
    "A context to mark the thread for debugpy to not debug."
    mark_thread_debugpy_ignore(threading.current_thread(), name)
    try:
        yield
    finally:
        mark_thread_debugpy_ignore(threading.current_thread(), unhide=True)


class ThreadSafeCaller:
    """
    ThreadSafeCaller provides a mechanism to safely schedule and execute functions
    or coroutines from multiple threads within an async context.

    This class manages a queue of jobs that can be submitted from any thread,
    ensuring that all scheduled calls are executed in the context of a dedicated
    thread and async task group. It is particularly useful for integrating
    synchronous and asynchronous code, or for safely invoking async operations
    from non-async threads.
    """

    _instances: ClassVar[weakref.WeakSet[Self]] = weakref.WeakSet()
    thread: threading.Thread
    __stack = None

    def __init__(self, *, log: logging.LoggerAdapter | None = None) -> None:
        self.log = log or logging.LoggerAdapter(logging.getLogger())

    async def __aenter__(self) -> Self:
        self._instances.add(self)
        self.thread = threading.current_thread()
        self._jobs = deque()
        self._jobs_added = threading.Event()
        async with contextlib.AsyncExitStack() as stack:
            self.tg = await stack.enter_async_context(anyio.create_task_group())
            await self.tg.start(self._server_loop)
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self.__stack is not None:
            self.tg.cancel_scope.cancel()
            self._jobs_added.set()
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _server_loop(self, task_status: TaskStatus):
        def wait_threading_event():
            mark_thread_debugpy_ignore(threading.current_thread())
            self._jobs_added.wait()
            mark_thread_debugpy_ignore(threading.current_thread(), unhide=True)

        task_status.started()
        while True:
            while len(self._jobs):
                self.tg.start_soon(self.wrap_call, *self._jobs.popleft())
                self._jobs_added.clear()

            await anyio.to_thread.run_sync(wait_threading_event)

    def call_later(self, func: Callable[P, Any | Awaitable], delay=0.0, /, *args: P.args, **kwargs: P.kwargs):
        """Schedules a function or coroutine for execution in the thread that owns it."""
        if threading.current_thread() is self.thread:
            self.tg.start_soon(self.wrap_call, func, delay, args, kwargs)
        else:
            self._jobs.append((func, delay, args, kwargs))
            self._jobs_added.set()

    def call_soon(self, func: Callable[P, Any | Awaitable], *args: P.args, **kwargs: P.kwargs):
        return self.call_later(func, 0.0, *args, **kwargs)

    async def wrap_call(self, func: Callable[..., Any | Awaitable], delay: float, args: tuple, kwargs: dict):
        """Asynchronously calls the given function with provided arguments, awaiting the result if it is awaitable.

        **Not intended to be called directly.**

        Overwrite this method as required.
        """
        try:
            if delay:
                await anyio.sleep(float(delay))
            result = func(*args, **kwargs) if callable(func) else func
            while inspect.isawaitable(result):
                result = await result
        except Exception as e:
            self.log.exception("Exception occurred while running %s", func, exc_info=e)

    @classmethod
    def get_instance(cls, thread: None | threading.Thread = None) -> Self:
        thread = thread or threading.current_thread()
        for instance in cls._instances:
            if instance.thread is thread:
                return instance
        msg = "A threadsafe caller was not found for this thread"
        raise RuntimeError(msg)


class PendingResult(Generic[T]):
    """An anyio compatible synchronization primitive for awaiting a result."""

    __slots__ = ["_event_done", "_exception", "result"]

    def __init__(self) -> None:
        self._exception = None
        self._event_done = anyio.Event()

    async def wait(self) -> T:
        await self._event_done.wait()
        if self._exception:
            raise self._exception
        return self.result

    def set_result(self, value):
        if self._event_done.is_set():
            raise RuntimeError
        self.result = value
        self._event_done.set()

    def set_exception(self, exception: Exception):
        if self._event_done.is_set():
            raise RuntimeError
        self._exception = exception
        self._event_done.set()
