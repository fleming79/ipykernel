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
import weakref
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, ParamSpec, Self, TypeVar, cast

import anyio
import anyio.to_thread
import sniffio
from zmq import Socket, ZMQError

from async_kernel.typing import ExecuteContent, ExecuteJobInfo, ExecuteMode

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from anyio.abc import TaskGroup, TaskStatus

__all__ = [
    "PendingResult",
    "ThreadCaller",
    "bind_socket",
    "do_not_debug_this_thread",
    "mark_thread_pydev_do_not_trace",
    "wait_thread_event",
]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules


P = ParamSpec("P")
T = TypeVar("T")


def bind_socket(socket: Socket, transport: Literal["tcp", "ipc"], ip: str, port: int = 0, max_attempts=100) -> int:
    def _try_bind_socket(port: int):
        if transport == "tcp":
            if not port:
                port = socket.bind_to_random_port(f"tcp://{ip}")
            else:
                socket.bind(f"tcp://{ip}:{port}")
        elif transport == "ipc":
            if not port:
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
    e = None
    for _ in range(max_attempts):
        try:
            return _try_bind_socket(port)
        except ZMQError as e_:
            # Raise if we have any error not related to socket binding
            if e_.errno in {errno.EADDRINUSE, win_in_use}:
                e = e_
                break
    msg = f"Failed to bind {socket} for {transport=}" + f" to {port=}!" if max_attempts == 1 else "!"
    raise RuntimeError(msg) from e


def get_execute_info(content: ExecuteContent) -> ExecuteJobInfo:
    """Extract ExecuteJobInfo from the content.

    If the top line of the code starts with '#@'; the execute mode and
    namespace will be extacted from that line.

    Whitespace is stripped from


    code:
    ``` python
    # @<execute_mode>, namespace=<namespace>
    ```
    """
    mode = ExecuteMode.task if content.get("silent", True) else ExecuteMode.queue
    namespace = ""
    if (code := content["code"].strip()).startswith("#@") and (header := code.split("\n", maxsplit=1)[0]):
        match header.split(",")[0].strip().removeprefix("#@").lower():
            case "task":
                mode = ExecuteMode.task
            case "thread":
                mode = ExecuteMode.thread
        if len(s := header.split("namespace=", maxsplit=1)) == 2:
            namespace = s[1].strip().strip("'\"")
            assert "," not in namespace, "Reserved symbol detected!"
    return ExecuteJobInfo(execute_mode=mode, namespace=namespace)


def mark_thread_pydev_do_not_trace(thread: threading.Thread, name="", *, remove=False):
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread.pydev_do_not_trace = not remove  # type: ignore[attr-defined]
    if name:
        thread.name = name


@contextlib.contextmanager
def do_not_debug_this_thread(name=""):
    "A context to mark the thread for debugpy to not debug."
    if not LAUNCHED_BY_DEBUGPY:
        mark_thread_pydev_do_not_trace(threading.current_thread(), name)
    try:
        yield
    finally:
        if not LAUNCHED_BY_DEBUGPY:
            mark_thread_pydev_do_not_trace(threading.current_thread(), remove=True)


async def wait_thread_event(event: threading.Event):
    """Wait for the threading event in a separate thread.

    The event will be set event if the coroutine is cancelled to ensure the thread is cleared.
    """

    def _in_thread_call():
        with do_not_debug_this_thread():
            event.wait()

    try:
        await anyio.to_thread.run_sync(_in_thread_call, abandon_on_cancel=True)
    finally:
        event.set()


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
    _jobs: deque
    _jobs_added: threading.Event
    _closed = False

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
        task_status.started()
        with contextlib.suppress(anyio.get_cancelled_exc_class()):
            while True:
                while len(self._jobs):
                    tg.start_soon(self._wrap_call, *self._jobs.popleft())
                    self._jobs_added.clear()
                await wait_thread_event(self._jobs_added)

    def __repr__(self) -> str:
        return f"ThreadCaller<{self.thread}>"

    @property
    def taskgroup(self) -> TaskGroup:
        if tg := self._taskgroup:
            return tg
        msg = f"{self}  is not currently open in an asnyc context."
        raise RuntimeError(msg)

    def close(self):
        "Once closed it can not be reopened."
        if not self._closed:
            if tg := self._taskgroup:
                self._taskgroup = None
                if threading.current_thread() is self.thread:
                    tg.cancel_scope.cancel()
                else:
                    self.call_soon(tg.cancel_scope.cancel)
            self._closed = True
            self._jobs_added.set()
            self._instances.pop(self.thread, None)
            if self in self._to_thread_pool:
                self._to_thread_pool.remove(self)

    def _to_thread_on_done(self, _):
        if not self._closed:
            if len(self._to_thread_pool) < self.MAX_IDLE_EVENT_THREADS or self._outstanding:
                self._to_thread_pool.append(self)
            else:
                self.close()

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
            self._jobs.append((pending, time.monotonic(), delay, func, args, kwargs))
            self._jobs_added.set()
        self._outstanding += 1
        return pending

    def call_soon(self, func: Callable[P, T | Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> PendingResult[T]:
        "Calls call_later with delay=0.0."
        return self.call_later(func, 0.0, *args, **kwargs)

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
        ready_event.wait(10)
        assert isinstance(tsc, cls)
        return tsc


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

    def set_exception(self, exception: BaseException):
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
