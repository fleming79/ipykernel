# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import logging
import threading
import time
import weakref
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast

import anyio
import sniffio
from typing_extensions import override
from zmq import Context, Socket, SocketType

from async_kernel.typing import NoValue, T
from async_kernel.utils import wait_thread_event

if TYPE_CHECKING:
    from collections.abc import Iterable

    from anyio._core._synchronization import Event
    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel.typing import P

__all__ = ["Caller", "CancelledError", "Future"]


class CancelledError(anyio.ClosedResourceError):
    "Used to indicate a future is cancelled."


class InvalidStateError(RuntimeError):
    pass


class Future(Awaitable[T]):
    """
    A class representing a future result of an asynchronous operation.

    This class provides a way to wait for the result of a computation
    that may be running in another thread. It supports setting a result
    or an exception, adding callbacks to be executed when the future is
    done, and canceling the future.

    The set_result/set_exception methods must be called from inside the thread
    specified when the instance was created.

    Attributes:
        thread (threading.Thread | None): The thread associated with the future.

    Methods:
        result(): Wait for the result (thread-safe).
        wait_sync(): Synchronously wait for the result.
        set_result(value): Set the result of the future.
        set_exception(exception): Set the exception for the future.
        done(): Return True if the Future is done.
        add_done_callback(fn): Add a callback to be called when the future is done (not thread-safe).
        cancel(): Cancel the Future and schedule callbacks (thread-safe).
        cancelled(): Return True if the future has been cancelled.
        exception(): Return the exception that was set on this Future.
        remove_done_callback(fn): Remove all instances of a callback from the callbacks list.
    """

    __slots__ = [
        "_anyio_event_done",
        "_cancel_scope",
        "_cancelled",
        "_done_callbacks",
        "_event_done",
        "_exception",
        "_result",
        "_setting_value",
        "thread",
    ]
    _result: T

    def __init__(self, thread: threading.Thread | None = None) -> None:
        self._event_done = threading.Event()
        self._exception = None
        self._anyio_event_done = None
        self.thread = thread or threading.current_thread()
        self._done_callbacks = []
        self._cancelled = False
        self._cancel_scope: anyio.CancelScope | None = None
        self._setting_value = False

    @override
    def __await__(self) -> Generator[Any, None, T]:
        return self.result().__await__()

    async def result(self) -> T:
        "Wait for the result (thread-safe)."
        try:
            if not self._event_done.is_set():
                if threading.current_thread() is self.thread:
                    if not self._anyio_event_done:
                        self._anyio_event_done = anyio.Event()
                    await self._anyio_event_done.wait()
                else:
                    await wait_thread_event(self._event_done)
        except anyio.get_cancelled_exc_class():
            self.cancel()
            raise
        if self._exception:
            raise self._exception
        return self._result

    def wait_sync(self) -> T:
        "Synchronously wait for the result."
        if threading.current_thread() is self.thread:
            raise RuntimeError
        self._event_done.wait()
        if self._exception:
            raise self._exception
        return self._result

    def set_result(self, value: T):
        self._set_value("result", value)

    def set_exception(self, exception: BaseException):
        self._set_value("exception", exception)

    def _set_value(self, mode: Literal["result", "exception"], value):
        if self._setting_value:
            raise InvalidStateError
        self._setting_value = True

        def set_value():
            if mode == "exception":
                self._exception = value
            else:
                self._result = value
            self._event_done.set()
            if self._anyio_event_done:
                self._anyio_event_done.set()
            for cb in reversed(self._done_callbacks):
                try:
                    cb(self)
                except Exception:
                    pass

        if threading.current_thread() is not self.thread:
            try:
                Caller(self.thread).call_no_context(func=set_value)
            except RuntimeError:
                msg = (
                    f"The current thread is not {self.thread.name} and a caller does not exist for that thread either."
                )
                raise RuntimeError(msg) from None
        else:
            set_value()

    def done(self):
        """Return True if the Future is done.

        Done means either that a result / exception are available."""
        return self._event_done.is_set()

    def add_done_callback(self, fn: Callable[[Self], object]):
        """Add a callback for when the callback is done (not thread-safe).

        The result of the future and done callbacks are always called for the futures thread.
        Callbacks are called in the reverse order in which they were added.
        """
        self._done_callbacks.append(fn)

    def cancel(self) -> bool:
        """Cancel the Future and schedule callbacks.

        Returns if it has been cancelled.
        """
        if not self.done():
            self._cancelled = True
            if scope := self._cancel_scope:
                if threading.current_thread() is self.thread:
                    scope.cancel()
                else:
                    Caller(self.thread).call_no_context(self.cancel)
        return self.cancelled()

    def cancelled(self) -> bool:
        return self._cancelled

    def exception(self) -> BaseException | None:
        "Return the exception that was set on this Future."
        return self._exception

    def remove_done_callback(self, fn: Callable[[Self], object], /) -> int:
        """Remove all instances of a callback from the callbacks list.

        Returns the number of callbacks removed.
        """
        n = 0
        while fn in self._done_callbacks:
            n += 1
            self._done_callbacks.remove(fn)
        return n

    def set_cancel_scope(self, scope: anyio.CancelScope):
        "Provide a cancel scope for cancellation"
        if self._cancelled:
            scope.cancel()
        self._cancel_scope = scope


class Caller:
    """
    A class to manage calls to functions and coroutines in a separate thread,
    utilizing AnyIO for asynchronous operations.

    The `Caller` class provides a mechanism to execute functions and coroutines
    in a dedicated thread, leveraging AnyIO for asynchronous task management.
    It supports scheduling calls with delays, executing them immediately,
    and running them without a context.  It also provides a means to manage
    a pool of threads for general purpose offloading of tasks.

    The class maintains a registry of instances, associating each with a specific
    thread. It uses a task group to manage the execution of scheduled tasks and
    provides methods to start, stop, and query the status of the caller.

    Attributes:
        thread (threading.Thread): The thread associated with this `Caller` instance.
        backend (str): The AnyIO backend used by this `Caller` instance.
        log (logging.LoggerAdapter): A logger adapter for logging messages.
        active (bool): A flag indicating whether the `Caller` is active.
        iopub_sockets (weakref.WeakKeyDictionary[threading.Thread, Socket]): A class-level
            weak key dictionary mapping threads to ZeroMQ sockets for inter-process
            communication.
        iopub_url (str): The URL for the ZeroMQ IOPub socket.

    Methods:
        __new__(cls, thread: threading.Thread | None = None, *, log: logging.LoggerAdapter | None = None, create=False, protected=False) -> Self:
            Creates a new `Caller` instance or returns an existing one for the given thread.
        taskgroup (property):
            Returns the AnyIO task group associated with this `Caller` instance.
        stopped (property):
            Returns True if the `Caller` is stopped, False otherwise.
        stop(self, *, force=False):
            Stops the `Caller` instance, closing the event loop and releasing resources.
        call_later(self, func: Callable[P, T | Awaitable[T]], delay=0.0, /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
            Schedules a function or coroutine for execution after a delay.
        call_soon(self, func: Callable[P, T | Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
            Schedules a function or coroutine for immediate execution.
        call_no_context(self, func: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
            Schedules a function for execution without a context.
    Classmethods:
        stop_all(cls, **kwgs):
            Stops all active `Caller` instances.
        get_instance(cls, name: str | None = "MainThread", *, create=False) -> Self:
            Gets an instance of `Caller` for the given thread name.
        to_thread(cls, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
            Calls a function in a separate thread using a thread pool.
        to_thread_by_name(cls, name: str | None, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
            Calls a function in a separate thread, creating a new thread if necessary.
        start_new(cls, *, backend: Literal["asyncio", "trio"] | str = "", log: logging.LoggerAdapter | None = None, name: str | None = None, protected=False):
            Starts a new `Caller` in a separate thread.
        as_completed(cls, items: Iterable[Future[T]] | AsyncGenerator[Future[T]], *, max_concurrent: NoValue | int = NoValue):
            An asynchronous iterator that yields futures as they complete.
        list_active(cls) -> list[str]:
            Lists the names of all active callers.
    """

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    thread: threading.Thread
    backend = ""
    log: logging.LoggerAdapter[Any]
    __stack = None
    _outstanding = 0
    _to_thread_pool: ClassVar[deque[Self]] = deque()
    _pool_instances: ClassVar[weakref.WeakSet[Self]] = weakref.WeakSet()
    MAX_IDLE_POOL_INSTANCES = 10
    _taskgroup: TaskGroup | None = None
    _jobs: deque[tuple[contextvars.Context, tuple[Future, float, float, Callable, tuple, dict]] | Callable[[], Any]]
    _jobs_added: threading.Event
    _stopped = False
    _protected = False
    active = False
    iopub_sockets: ClassVar[weakref.WeakKeyDictionary[threading.Thread, Socket]] = weakref.WeakKeyDictionary()
    iopub_url: ClassVar = "inproc://iopub"

    def __new__(
        cls,
        thread: threading.Thread | None = None,
        *,
        log: logging.LoggerAdapter | None = None,
        create=False,
        protected=False,
    ) -> Self:
        thread = thread or threading.current_thread()
        if not (inst := cls._instances.get(thread)):
            if not create:
                msg = f"A caller is not provided for {thread=}"
                raise RuntimeError(msg)
            inst = super().__new__(cls)
            inst.thread = thread
            inst.log = log or logging.LoggerAdapter(logging.getLogger())
            inst._jobs = deque()
            inst._jobs_added = threading.Event()
            inst._protected = protected
            cls._instances[thread] = inst
        return inst

    @override
    def __repr__(self) -> str:
        return f"Caller<{self.thread.name}>"

    async def __aenter__(self) -> Self:
        self._cancelled_exception_class = anyio.get_cancelled_exc_class()
        async with contextlib.AsyncExitStack() as stack:
            self.active = True
            self._taskgroup = tg = await stack.enter_async_context(anyio.create_task_group())
            await tg.start(self._server_loop, tg)
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self.__stack is not None:
            self.stop()
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _server_loop(self, tg: TaskGroup, task_status: TaskStatus[None]):
        thread = threading.current_thread()
        socket = Context.instance().socket(SocketType.PUB)
        socket.linger = 500
        socket.connect(self.iopub_url)
        try:
            self.iopub_sockets[thread] = socket
            task_status.started()
            while not self._stopped:
                while len(self._jobs):
                    job = self._jobs.popleft()
                    if isinstance(job, Callable):
                        try:
                            job()
                        except Exception as e:
                            self.log.exception("Simple call failed", exc_info=e)
                    else:
                        context, args = job
                        context.run(tg.start_soon, self._wrap_call, *args)
                    self._jobs_added.clear()
                await wait_thread_event(self._jobs_added)
        finally:
            self.active = False
            for job in self._jobs:
                if not callable(job):
                    job[1][0].set_exception(CancelledError())
            socket.close()
            self.iopub_sockets.pop(thread, None)
            self.taskgroup.cancel_scope.cancel()

    async def _wrap_call(
        self,
        fut: Future[T],
        starttime: float,
        delay: float,
        func: Callable[..., T | Awaitable[T]],
        args: tuple,
        kwargs: dict,
    ) -> None:
        try:
            with anyio.CancelScope() as scope:
                fut.set_cancel_scope(scope)
                try:
                    if (delay_ := delay - time.monotonic() + starttime) > 0:
                        await anyio.sleep(float(delay_))
                    result = func(*args, **kwargs) if callable(func) else func  # pyright: ignore[reportAssignmentType]
                    while inspect.isawaitable(result):
                        result: T = await result
                    if fut.cancelled() and not scope.cancel_called:
                        scope.cancel()
                    if scope.cancel_called:
                        # await here to allow the cancel scope to be raised/caught.
                        await anyio.sleep(0)
                    self._outstanding -= 1  # update first for _to_thread_on_done
                    fut.set_result(result)  # type: ignore[call-arg]
                except (self._cancelled_exception_class, Exception) as e:
                    self._outstanding -= 1  # # update first for _to_thread_on_done
                    if not fut.done():
                        if isinstance(e, self._cancelled_exception_class):
                            e = CancelledError()
                        else:
                            self.log.exception("Exception occurred while running %s", func, exc_info=e)
                        fut.set_exception(e)
        except Exception as e:
            self.log.exception("Calling func %s failed", func, exc_info=e)

    def _to_thread_on_done(self, _) -> None:
        if not self._stopped:
            if (len(self._to_thread_pool) < self.MAX_IDLE_POOL_INSTANCES) or self._outstanding:
                self._to_thread_pool.append(self)
            else:
                self.stop()

    @property
    def taskgroup(self) -> TaskGroup:
        if tg := self._taskgroup:
            return tg
        msg = f"{self}  is not currently open in an async context."
        raise RuntimeError(msg)

    @property
    def stopped(self):
        return self._stopped

    def stop(self, *, force=False):
        "Once closed it can not be reopened."
        if self._protected and not force:
            return
        self._stopped = True
        self._jobs_added.set()
        self._instances.pop(self.thread, None)
        if self in self._to_thread_pool:
            self._to_thread_pool.remove(self)

    def call_later(
        self, func: Callable[P, T | Awaitable[T]], delay=0.0, /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """Schedules a function or coroutine for execution.

        If the instance is not open in an async context, the function will be queued and
        executed once the async context is open.

        The delay is calculated from the submission time.
        """
        if self._stopped:
            raise anyio.ClosedResourceError
        fut: Future[T] = Future(thread=self.thread)
        if threading.current_thread() is self.thread and (tg := self._taskgroup):
            tg.start_soon(self._wrap_call, fut, time.monotonic(), delay, func, args, kwargs)
        else:
            self._jobs.append((contextvars.copy_context(), (fut, time.monotonic(), delay, func, args, kwargs)))
            self._jobs_added.set()
        self._outstanding += 1
        return fut

    def call_soon(self, func: Callable[P, T | Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        "Calls call_later with delay=0.0."
        return self.call_later(func, 0.0, *args, **kwargs)

    def call_no_context(self, func: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        """Call func in the thread event loop."""
        self._jobs.append(functools.partial(func, *args, **kwargs))
        self._jobs_added.set()

    @classmethod
    def stop_all(cls, **kwgs) -> None:
        "Stop all instances."
        force = kwgs.get("_stop_protected", False)
        for caller in tuple(reversed(cls._instances.values())):
            caller.stop(force=force)

    @classmethod
    def get_instance(cls, name: str | None = "MainThread", *, create=False) -> Self:
        """Gets an instance of Caller.
        name: str | None
        If the

        create: bool
            If the Caller instance does not exist a new thread is created.
        """
        for thread in cls._instances:
            if thread.name == name:
                return cls._instances[thread]
        if create:
            return cls.start_new(name=name)
        msg = f"A Caller was not found for {name=}."
        raise RuntimeError(msg)

    @classmethod
    def to_thread(cls, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Call func in a separate thread.

        A pool of 'workers' is are used to provide an event loop
        """
        return cls.to_thread_by_name(None, func, *args, **kwargs)

    @classmethod
    def to_thread_by_name(
        cls, name: str | None, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """Call the function in the Caller's thread.

        name: name of the caller's thread. passing an empty string will provide a caller from the pool.

        func:
            The function (awaitables permitted, though discouraged).
        *args, **kwargs: for func.

        If a caller thread is not found a new one is created with the specified name."""
        caller = (
            cls._to_thread_pool.popleft()
            if not name and cls._to_thread_pool
            else cls.get_instance(name=name, create=True)
        )
        fut = caller.call_soon(func, *args, **kwargs)
        if not name:
            cls._pool_instances.add(caller)
            fut.add_done_callback(caller._to_thread_on_done)
        return fut

    @classmethod
    def start_new(
        cls,
        *,
        backend: Literal["asyncio", "trio"] | str = "",  # noqa: PYI051
        log: logging.LoggerAdapter | None = None,
        name: str | None = None,
        protected=False,
    ) -> Self:
        """Start a new thread with a new Caller open in the context of anyio event loop.

        A new thread and caller is always started and ready to start new jobs as soon as it is returned.

        Args:
            backend: The backend to use for the anyio event loop (anyio.run).
            log: A logging adapter to use for debug messages.
            protected: When True, the caller will not shutdown unless shutdown is called with `force=True`.
        """

        def anyio_run_caller() -> None:
            async def caller_context() -> None:
                nonlocal caller
                async with cls(log=log, create=True, protected=protected) as caller:
                    ready_event.set()
                    with contextlib.suppress(anyio.get_cancelled_exc_class()):
                        await anyio.sleep_forever()

            anyio.run(caller_context, backend=backend_)

        assert name not in [t.name for t in cls._instances], f"{name=} already exists!"
        backend_ = backend or sniffio.current_async_library()
        caller = cast("Self", object)
        ready_event = threading.Event()
        thread = threading.Thread(target=anyio_run_caller, name=name, daemon=True)
        thread.start()
        ready_event.wait()
        assert isinstance(caller, cls)
        return caller

    @classmethod
    async def as_completed(
        cls,
        items: Iterable[Future[T]] | AsyncGenerator[Future[T]],
        *,
        max_concurrent: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
    ):
        """An iterator to get Futures as they complete.

        Pass a generator should you wish to limit the number future jobs when calling to_thread/to_task etc.
        Pass a set/list/tuple to ensure all get monitored at once.

        Args:
            items: Either a container with existing futures or generator of Futures.
            max_concurrent: The maximum number of concurrent futures to monitor at a time.
            This is useful when `items` is a generator utilising Caller.to_thread. By default this will
            limit to `Caller.MAX_IDLE_POOL_INSTANCES`.
        """
        event_future_ready = threading.Event()
        has_result: deque[Future[T]] = deque()
        futures: set[Future[T]] = set()
        done = False
        resume: Event | None = cast("anyio.Event | None", None)

        def _on_done(fut: Future[T]) -> None:
            has_result.append(fut)
            event_future_ready.set()

        async def iter_items(task_status: TaskStatus[None]):
            nonlocal done, resume
            if isinstance(items, set | list | tuple):
                max_concurrent_ = 0
            else:
                max_concurrent_ = cls.MAX_IDLE_POOL_INSTANCES if max_concurrent is NoValue else int(max_concurrent)

            gen = items if isinstance(items, AsyncGenerator) else iter(items)
            task_status.started()
            try:
                while True:
                    fut = await anext(gen) if isinstance(gen, AsyncGenerator) else next(gen)
                    futures.add(fut)
                    if fut.done():
                        has_result.append(fut)
                        event_future_ready.set()
                    else:
                        fut.add_done_callback(_on_done)
                    if max_concurrent_ and len(futures) == max_concurrent_:
                        resume = anyio.Event()
                        await resume.wait()
            except (StopAsyncIteration, StopIteration):
                return
            finally:
                done = True
                event_future_ready.set()

        try:
            async with anyio.create_task_group() as tg:
                await tg.start(iter_items)
                while futures or not done:
                    if has_result:
                        event_future_ready.clear()
                        fut = has_result.popleft()
                        futures.discard(fut)
                        yield fut
                        if resume:
                            resume.set()
                        continue
                    if not has_result:
                        await wait_thread_event(event_future_ready)
        finally:
            for fut in futures:
                fut.cancel()

    @classmethod
    def all_callers(cls, active_only=True):
        "Get a list of the callers."
        return [caller for caller in Caller._instances.values() if caller.active or not active_only]
