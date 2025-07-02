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
from typing import TYPE_CHECKING, Any, ClassVar, Literal, ParamSpec, Self

import anyio
import anyio.to_thread
import sniffio
import zmq

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules

P = ParamSpec("P")


def wait_threading_event(event: threading.Event):
    """Wait for the given threading.Event, marking the thread as a PyDev daemon and
    disabling tracing during the wait."""
    thread = threading.current_thread()
    thread.pydev_do_not_trace = True  # type: ignore[attr-defined]
    thread.is_pydev_daemon_thread = True  # type: ignore[attr-defined]
    event.wait()
    thread.pydev_do_not_trace = False  # type: ignore[attr-defined]
    thread.is_pydev_daemon_thread = False  # type: ignore[attr-defined]


def start_anyio_thread(
    func: Callable[[TaskStatus], CoroutineType],
    stop_event: threading.Event,
    tg: TaskGroup,
    *,
    backend: Literal["anyio", "trio", ""] = "",
    name="",
    pydev_do_not_trace=not LAUNCHED_BY_DEBUGPY,
    is_pydev_daemon_thread=not LAUNCHED_BY_DEBUGPY,
):
    """Run a coroutine function in a separate thread (and event loop) and manage its lifecycle using AnyIO.

    This function takes an asynchronous function, a stop event, and a task group,
    and starts the function in a separate thread. It ensures that the function
    is properly started and can be stopped gracefully.

    This coroutine returns once `task_status.started()`is called inside `func`
    running until the `stop_event` is set.

    Args:
        func: The asynchronous function to run in a separate thread. It should
            accept a TaskStatus object as an argument and return a coroutine.
        stop_event: A threading.Event that signals when the function should stop.
        tg: The AnyIO TaskGroup to use for managing the function's task.
        task_status: An AnyIO TaskStatus object to signal when the function has started.
    """

    backend = backend or sniffio.current_async_library()  # type: ignore[no-any-return]
    ready_event = threading.Event()

    def run_func():
        thread = threading.current_thread()
        if name:
            thread.name = name
        thread.pydev_do_not_trace = pydev_do_not_trace  # type: ignore[attr-defined]
        thread.is_pydev_daemon_thread = is_pydev_daemon_thread  # type: ignore[attr-defined]

        async def run_until_stop_event():
            async with anyio.create_task_group() as tg:
                await tg.start(func)
                ready_event.set()
                await anyio.to_thread.run_sync(wait_threading_event, stop_event)
                tg.cancel_scope.cancel()

        anyio.run(run_until_stop_event, backend=backend)

    tg.start_soon(anyio.to_thread.run_sync, run_func)
    return anyio.to_thread.run_sync(ready_event.wait)


def bind_socket(socket: zmq.Socket, transport: Literal["tcp", "ipc"], ip: str, port: int = 0, max_attempts=100) -> int:
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
                    if Path(path).exists():
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
        except zmq.ZMQError as e:
            # Raise if we have any error not related to socket binding
            if e.errno != errno.EADDRINUSE and e.errno != win_in_use:
                raise
            if attempt == max_attempts - 1:
                raise
    msg = f"Failed to bind a {socket}:{port}"
    raise RuntimeError(msg)


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

    _instances: ClassVar = weakref.WeakSet()
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
        task_status.started()
        while True:
            while len(self._jobs):
                self.tg.start_soon(self.wrap_call, *self._jobs.popleft())
                self._jobs_added.clear()
            await anyio.to_thread.run_sync(wait_threading_event, self._jobs_added)

    def call_later(self, func: Callable[P, Any | Awaitable], delay=0.0, /, *args: P.args, **kwargs: P.kwargs):
        """Schedules a function or coroutine for execution in the thread that owns it."""
        if threading.current_thread() is self.thread:
            self.tg.start_soon(self.wrap_call, func, delay, args, kwargs)
        else:
            self._jobs.append((func, delay, args, kwargs))
            self._jobs_added.set()

    async def wrap_call(self, func: Callable[..., Any | Awaitable], delay: float, args: tuple, kwargs: dict):
        """Asynchronously calls the given function with provided arguments, awaiting the result if it is awaitable.

        **Not intended to be called directly.**

        Overwrite this method as requried.
        """
        if delay:
            await anyio.sleep(delay)
        result = func(*args, **kwargs) if callable(func) else func
        try:
            while inspect.isawaitable(result):
                result = await result
        except Exception as e:
            self.log.exception("Exception occurred while running %s", func, exc_info=e)

    @classmethod
    def get_instance(cls, thread: None|threading.Thread) -> Self:
        thread = thread or threading.current_thread()
        for instance in cls._instances:
            if instance.thread is thread:
                return instance
        msg = "A threadsafe caller was not found for this thread"
        raise RuntimeError(msg)