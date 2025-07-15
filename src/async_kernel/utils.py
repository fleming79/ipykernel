# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import errno
import inspect
import logging
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, ParamSpec, Self, TypeVar, cast

import anyio
import anyio.to_thread
import sniffio
from zmq import Socket, ZMQError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

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


def new_event_loop(*, backend="", log: logging.LoggerAdapter | None = None, name: str | None = None):
    "Start a new event loop with a thread safe caller"

    def run_event_loop():
        async def run_event_loop_():
            nonlocal threadsafe_caller
            async with ThreadSafeCaller(log=log) as threadsafe_caller:
                ready_event.set()
                with contextlib.suppress(anyio.get_cancelled_exc_class()):
                    await anyio.sleep_forever()

        anyio.run(run_event_loop_, backend=backend)

    backend = backend or sniffio.current_async_library()
    threadsafe_caller = cast("ThreadSafeCaller", None)
    ready_event = threading.Event()
    thread = threading.Thread(target=run_event_loop, name=name, daemon=True)
    thread.start()
    ready_event.wait(10)
    return threadsafe_caller



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

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    thread: threading.Thread
    backend = ""
    log: logging.LoggerAdapter
    __stack = None
    _outstanding = 0
    _tsc_pool: ClassVar[deque[Self]] = deque()
    MAX_IDLE_EVENT_THREADS = 10

    def __new__(cls, thread: threading.Thread | None = None, *, log: logging.LoggerAdapter | None = None) -> Self:
        thread = thread or threading.current_thread()
        if not (inst := cls._instances.get(thread)):
            inst = super().__new__(cls)
            inst.thread = thread
            inst.log = log or logging.LoggerAdapter(logging.getLogger())
            cls._instances[thread] = inst
        return inst

    async def __aenter__(self) -> Self:
        self._jobs = deque()
        self._jobs_added = threading.Event()
        async with contextlib.AsyncExitStack() as stack:
            self.taskgroup = await stack.enter_async_context(anyio.create_task_group())
            await self.taskgroup.start(self._server_loop)
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self.__stack is not None:
            self.taskgroup.cancel_scope.cancel()
            self._jobs_added.set()
            self._instances.pop(self.thread, self)
            if self in self._tsc_pool:
                self._tsc_pool.remove(self)
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _server_loop(self, task_status: TaskStatus):
        def wait_threading_event():
            mark_thread_debugpy_ignore(threading.current_thread())
            self._jobs_added.wait()
            mark_thread_debugpy_ignore(threading.current_thread(), unhide=True)

        task_status.started()
        with contextlib.suppress(anyio.get_cancelled_exc_class()):
            while True:
                while len(self._jobs):
                    self.taskgroup.start_soon(self._wrap_call, *self._jobs.popleft())
                    self._jobs_added.clear()
                await anyio.to_thread.run_sync(wait_threading_event, abandon_on_cancel=True)

    def shutdown(self):
        self.call_soon(self.taskgroup.cancel_scope.cancel)

    def _to_thread_on_done(self, _):
        if len(self._tsc_pool) < self.MAX_IDLE_EVENT_THREADS or self._outstanding:
            self._tsc_pool.append(self)
        else:
            self.shutdown()

    def call_later(
        self, func: Callable[P, T | Awaitable[T]], delay=0.0, /, *args: P.args, **kwargs: P.kwargs
    ) -> PendingResult[T]:
        """Schedules a function or coroutine for execution."""
        pending = PendingResult(thread=self.thread)
        if threading.current_thread() is self.thread:
            self.taskgroup.start_soon(self._wrap_call, pending, func, delay, args, kwargs)
        else:
            self._jobs.append((pending, func, delay, args, kwargs))
            self._jobs_added.set()
        self._outstanding += 1
        return pending

    def call_soon(self, func: Callable[P, T | Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> PendingResult[T]:
        "Calls call_later with delay=0.0."
        return self.call_later(func, 0.0, *args, **kwargs)

    async def _wrap_call(
        self, pending: PendingResult, func: Callable[..., Any | Awaitable], delay: float, args: tuple, kwargs: dict
    ):
        try:
            if delay:
                await anyio.sleep(float(delay))
            result = func(*args, **kwargs) if callable(func) else func
            while inspect.isawaitable(result):
                result = await result
            self._outstanding -= 1
            pending.set_result(result)
        except Exception as e:
            self.log.exception("Exception occurred while running %s", func, exc_info=e)
            pending.set_exception(e)

    @classmethod
    def _shutdown_all_instances(cls):
        for tsc in cls._instances.values():
            tsc.shutdown()
        while cls._instances:
            time.sleep(0.01)

    @classmethod
    def get_instance(cls, thread: threading.Thread | None = None) -> Self:
        thread = thread or threading.current_thread()
        if instance := cls._instances.get(thread):
            return instance
        msg = "A threadsafe caller was not found for this thread"
        raise RuntimeError(msg)

    @classmethod
    def to_thread(cls, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> PendingResult[T]:
        "Call func in a separate thread."
        try:
            tsc = cls._tsc_pool.popleft()
        except IndexError:
            tsc = new_event_loop()
        pending = tsc.call_soon(func, *args, **kwargs)
        pending._done_callbacks.add(tsc._to_thread_on_done)
        return pending


class PendingResult(Generic[T]):
    """An anyio compatible synchronization primitive for awaiting a result.

    thread: The thread where the result must be set. Defaults to the current thread.
    """

    __slots__ = ["_anyio_event_done", "_done_callbacks", "_event_done", "_exception", "result", "thread"]

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

                def wait_event_done():
                    with do_not_debug_this_thread():
                        self._event_done.wait()

                await anyio.to_thread.run_sync(wait_event_done)

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

    def set_exception(self, exception: Exception):
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

        def wait_threading_event():
            mark_thread_debugpy_ignore(threading.current_thread())
            event_pending_done.wait()
            mark_thread_debugpy_ignore(threading.current_thread(), unhide=True)

        for _ in range(n):
            if has_result:
                event_pending_done.clear()
                yield has_result.popleft()
                continue
            await anyio.to_thread.run_sync(wait_threading_event)
