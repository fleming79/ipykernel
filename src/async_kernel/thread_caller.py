# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import contextvars
import inspect
import logging
import threading
import time
import weakref
from collections import deque
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

import anyio
import sniffio
from zmq import Context, Socket, SocketType

from async_kernel.pending_result import PendingResult
from async_kernel.utils import wait_thread_event

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel.typing import P, T

__all__ = ["ThreadCaller"]


class ThreadCaller:
    """
    ThreadCaller provides a mechanism to safely schedule and execute functions
    or coroutines from multiple threads within an async context.

    This class manages a queue of jobs that can be submitted from any thread,
    ensuring that all scheduled calls are executed in the context of a dedicated
    thread and async task group. It is particularly useful for integrating
    synchronous and asynchronous code, or for safely invoking async operations
    from non-async threads.

    Only one instance per thread will be created and the instance must be open
    within an async context for call_soon and call_later to be processed.
    """

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    thread: threading.Thread
    backend = ""
    log: logging.LoggerAdapter
    __stack = None
    _outstanding = 0
    _to_thread_pool: ClassVar[deque[Self]] = deque()
    _to_thread_instances: ClassVar[weakref.WeakSet[Self]] = weakref.WeakSet()
    MAX_IDLE_EVENT_THREADS = 10
    _taskgroup: TaskGroup | None = None
    _jobs: deque[tuple[contextvars.Context, tuple[PendingResult, float, float, Callable, tuple, dict]]]
    _jobs_added: threading.Event
    _closed = False
    iopub_sockets: ClassVar[weakref.WeakKeyDictionary[threading.Thread, Socket]] = weakref.WeakKeyDictionary()
    iopub_url: ClassVar = "inproc://iopub"

    def __new__(cls, thread: threading.Thread | None = None, *, log: logging.LoggerAdapter | None = None) -> Self:
        thread = thread or threading.current_thread()
        if not (inst := cls._instances.get(thread)):
            inst = super().__new__(cls)
            inst.thread = thread
            inst.log = log or logging.LoggerAdapter(logging.getLogger())
            inst._jobs = deque()
            inst._jobs_added = threading.Event()
            cls._instances[thread] = inst
        return inst

    def __repr__(self) -> str:
        return f"ThreadCaller<{self.thread}>"

    async def __aenter__(self) -> Self:
        self._cancelled_exception_class = anyio.get_cancelled_exc_class()
        async with contextlib.AsyncExitStack() as stack:
            self._taskgroup = tg = await stack.enter_async_context(anyio.create_task_group())
            await tg.start(self._server_loop, tg)
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self.__stack is not None:
            self.close()
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _server_loop(self, tg: TaskGroup, task_status: TaskStatus):
        thread = threading.current_thread()
        socket = Context.instance().socket(SocketType.PUB)
        socket.linger = 500
        socket.connect(self.iopub_url)
        try:
            self.iopub_sockets[thread] = socket
            task_status.started()
            while not self._closed:
                while len(self._jobs):
                    context, args = self._jobs.popleft()
                    context.run(tg.start_soon, self._wrap_call, *args)
                    self._jobs_added.clear()
                await wait_thread_event(self._jobs_added)
        finally:
            socket.close(linger=500)
            self.iopub_sockets.pop(thread, None)
            self.taskgroup.cancel_scope.cancel()

    async def _wrap_call(
        self,
        pending: PendingResult,
        starttime: float,
        delay: float,
        func: Callable[..., Any | Awaitable],
        args: tuple,
        kwargs: dict,
    ):
        try:
            if (delay_ := delay - time.monotonic() + starttime) > 0:
                await anyio.sleep(float(delay_))
            result = func(*args, **kwargs) if callable(func) else func
            while inspect.isawaitable(result):
                result = await result
            self._outstanding -= 1
            pending.set_result(result)
        except (self._cancelled_exception_class, Exception) as e:
            e.add_note(f"{self} {func=}")
            self.log.exception("Exception occurred while running %s", func, exc_info=e)
            self._outstanding -= 1
            pending.set_exception(e)

    def _to_thread_on_done(self, _):
        if not self._closed:
            if len(self._to_thread_pool) < self.MAX_IDLE_EVENT_THREADS or self._outstanding:
                self._to_thread_pool.append(self)
            else:
                self.close()

    @classmethod
    def _shutdown_to_thread_instances(cls):
        "Shutdown currently open instance created via 'to_thread'."
        for tsc in set(cls._to_thread_instances):
            tsc.close()
        while cls._instances:
            time.sleep(0.01)

    @classmethod
    def get_instance(cls, thread: threading.Thread | None = None) -> Self:
        thread = thread or threading.current_thread()
        if instance := cls._instances.get(thread):
            return instance
        msg = f"A ThreadCaller was not found for {thread=}."
        raise RuntimeError(msg)

    @classmethod
    def to_thread(cls, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> PendingResult[T]:
        "Call func in a separate thread."
        try:
            tsc = cls._to_thread_pool.popleft()
        except IndexError:
            tsc = cls.start_new()
            cls._to_thread_instances.add(tsc)
        pending = tsc.call_soon(func, *args, **kwargs)
        pending._done_callbacks.add(tsc._to_thread_on_done)
        return pending

    @classmethod
    def start_new(cls, *, backend="", log: logging.LoggerAdapter | None = None, name: str | None = None):
        "Start a new thread, open a ThreadCaller in a new event loop  returning the ThreadCaller instance."

        def run_event_loop():
            async def run_event_loop_():
                nonlocal tsc
                async with cls(log=log) as tsc:
                    ready_event.set()
                    with contextlib.suppress(anyio.get_cancelled_exc_class()):
                        await anyio.sleep_forever()

            anyio.run(run_event_loop_, backend=backend)

        backend = backend or sniffio.current_async_library()
        tsc = cast("Self", None)
        ready_event = threading.Event()
        thread = threading.Thread(target=run_event_loop, name=name, daemon=True)
        thread.start()
        ready_event.wait()
        assert isinstance(tsc, cls)
        return tsc

    @property
    def taskgroup(self) -> TaskGroup:
        if tg := self._taskgroup:
            return tg
        msg = f"{self}  is not currently open in an asnyc context."
        raise RuntimeError(msg)

    def close(self):
        "Once closed it can not be reopened."
        self._closed = True
        self._jobs_added.set()
        self._instances.pop(self.thread, None)
        if self in self._to_thread_pool:
            self._to_thread_pool.remove(self)

    def call_later(
        self, func: Callable[P, T | Awaitable[T]], delay=0.0, /, *args: P.args, **kwargs: P.kwargs
    ) -> PendingResult[T]:
        """Schedules a function or coroutine for execution.

        If the instance is not open in an async context, the function will be queued and
        executed once the async context is open.

        The delay is calculated from the submission time.
        """
        if self._closed:
            msg = f"{self} is closed!"
            raise RuntimeError(msg)
        pending = PendingResult(thread=self.thread)
        if threading.current_thread() is self.thread and (tg := self._taskgroup):
            tg.start_soon(self._wrap_call, pending, time.monotonic(), delay, func, args, kwargs)
        else:
            self._jobs.append((contextvars.copy_context(), (pending, time.monotonic(), delay, func, args, kwargs)))
            self._jobs_added.set()
        self._outstanding += 1
        return pending

    def call_soon(self, func: Callable[P, T | Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> PendingResult[T]:
        "Calls call_later with delay=0.0."
        return self.call_later(func, 0.0, *args, **kwargs)
