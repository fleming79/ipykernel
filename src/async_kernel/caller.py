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

from async_kernel.kernelspec import Backend
from async_kernel.typing import NoValue, PosArgsT, T
from async_kernel.utils import wait_thread_event

if TYPE_CHECKING:
    from collections.abc import Iterable

    from anyio._core._synchronization import Event
    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel.typing import P

__all__ = ["Caller", "Future", "FutureCancelledError", "InvalidStateError"]


class FutureCancelledError(anyio.ClosedResourceError):
    "Used to indicate a `Future` is cancelled."


class InvalidStateError(RuntimeError):
    "An invalid state of a [Future][async_kernel.caller.Future]."


class Future(Awaitable[T]):
    """
    A class representing a future result modelled on Asyncio's [`Future`](https://docs.python.org/3/library/asyncio-future.html#futures).

    This class provides an anyio compatible Future primitive. It is designed
    to work with `Caller` to enable thread-safe calling, setting and awaiting
    execution results.
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

    def set_result(self, value: T) -> None:
        "Set the result (thread-safe using Caller)."
        self._set_value("result", value)

    def set_exception(self, exception: BaseException) -> None:
        "Set the exception (thread-safe using Caller)."
        self._set_value("exception", exception)

    def _set_value(self, mode: Literal["result", "exception"], value) -> None:
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
                Caller(thread=self.thread).call_no_context(set_value)
            except RuntimeError:
                msg = f"The current thread is not {self.thread.name} and a `Caller` does not exist for that thread either."
                raise RuntimeError(msg) from None
        else:
            set_value()

    def done(self) -> bool:
        """Return True if the Future is done.

        Done means either that a result / exception is available."""
        return self._event_done.is_set()

    def add_done_callback(self, fn: Callable[[Self], object]) -> None:
        """Add a callback for when the callback is done (not thread-safe).

        If the Future is already done it will be scheduled for calling.

        The result of the future and done callbacks are always called for the futures thread.
        Callbacks are called in the reverse order in which they were added in the owning thread.
        """
        if not self.done():
            self._done_callbacks.append(fn)
        else:
            self.get_caller().call_no_context(fn, self)

    def cancel(self) -> bool:
        """Cancel the Future and schedule callbacks (thread-safe using Caller).

        Returns if it has been cancelled.
        """
        if not self.done():
            self._cancelled = True
            if scope := self._cancel_scope:
                if threading.current_thread() is self.thread:
                    scope.cancel()
                else:
                    Caller(thread=self.thread).call_no_context(self.cancel)
        return self.cancelled()

    def cancelled(self) -> bool:
        """Return True if the Future is cancelled."""
        return self._cancelled

    def exception(self) -> BaseException | None:
        """Return the exception that was set on the Future.

        If the Future has been cancelled, this method raises a [FutureCancelledError][async_kernel.caller.FutureCancelledError] exception.

        If the Future isn't done yet, this method raises an [InvalidStateError][async_kernel.caller.InvalidStateError] exception.
        """
        if self._cancelled:
            raise FutureCancelledError
        if not self.done():
            raise InvalidStateError
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

    def set_cancel_scope(self, scope: anyio.CancelScope) -> None:
        "Provide a cancel scope for cancellation."
        if self._cancelled:
            scope.cancel()
        self._cancel_scope = scope

    def get_caller(self) -> Caller:
        "The the Caller the Future's thread corresponds."
        return Caller(thread=self.thread)


class Caller:
    """A class to enable calling functions and coroutines between anyio event loops.

    The `Caller` class provides a mechanism to execute functions and coroutines
    in a dedicated thread, leveraging AnyIO for asynchronous task management.
    It supports scheduling calls with delays, executing them immediately,
    and running them without a context.  It also provides a means to manage
    a pool of threads for general purpose offloading of tasks.

    The class maintains a registry of instances, associating each with a specific
    thread. It uses a task group to manage the execution of scheduled tasks and
    provides methods to start, stop, and query the status of the caller.
    """

    MAX_IDLE_POOL_INSTANCES = 10
    "The number of `pool` instances to leave idle (See also[to_thread][async_kernel.Caller.to_thread])."
    MAX_BUFFER_SIZE = 1000
    "The default  maximum_buffer_size used in [queue_call][async_kernel.Caller.queue_call]."
    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    __stack = None
    _outstanding = 0
    _to_thread_pool: ClassVar[deque[Self]] = deque()
    _pool_instances: ClassVar[weakref.WeakSet[Self]] = weakref.WeakSet()
    _executor_queue: dict
    _taskgroup: TaskGroup | None = None
    _callers: deque[tuple[contextvars.Context, tuple[Future, float, float, Callable, tuple, dict]] | Callable[[], Any]]
    _callers_added: threading.Event
    _stopped = False
    _protected = False
    _running = False
    thread: threading.Thread
    "The thread in which the caller will run."
    backend: Backend
    "The `anyio` backend the caller is running in."
    log: logging.LoggerAdapter[Any]
    ""
    iopub_sockets: ClassVar[weakref.WeakKeyDictionary[threading.Thread, Socket]] = weakref.WeakKeyDictionary()
    iopub_url: ClassVar = "inproc://iopub"

    def __new__(
        cls,
        *,
        thread: threading.Thread | None = None,
        log: logging.LoggerAdapter | None = None,
        create: bool = False,
        protected: bool = False,
    ) -> Self:
        """Create the `Caller` instance for the current thread or retrieve an existing instance
            by passing the thread.

        The caller provides a way to execute synchronous code in a separate
        thread, and to call asynchronous code from synchronous code.

        Args:
            thread:
            log: Logger to use for logging messages.
            create: Whether to create a new instance if one does not exist for the current thread.
            protected : Whether the caller is protected from having its event loop closed.

        Returns
        -------
        Caller
            The `Caller` instance for the current thread.

        Raises
        ------
        RuntimeError
            If `create` is False and a `Caller` instance does not exist.
        """

        thread = thread or threading.current_thread()
        if not (inst := cls._instances.get(thread)):
            if not create:
                msg = f"A caller is not provided for {thread=}"
                raise RuntimeError(msg)
            inst = super().__new__(cls)
            inst.backend = Backend(sniffio.current_async_library())
            inst.thread = thread
            inst.log = log or logging.LoggerAdapter(logging.getLogger())
            inst._callers = deque()
            inst._callers_added = threading.Event()
            inst._protected = protected
            inst._executor_queue = {}
            cls._instances[thread] = inst
        return inst

    @override
    def __repr__(self) -> str:
        return f"Caller<{self.thread.name}>"

    async def __aenter__(self) -> Self:
        self._cancelled_exception_class = anyio.get_cancelled_exc_class()
        async with contextlib.AsyncExitStack() as stack:
            self._running = True
            self._taskgroup = tg = await stack.enter_async_context(anyio.create_task_group())
            await tg.start(self._server_loop, tg)
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb) -> None:
        if self.__stack is not None:
            self.stop()
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _server_loop(self, tg: TaskGroup, task_status: TaskStatus[None]) -> None:
        socket = Context.instance().socket(SocketType.PUB)
        socket.linger = 500
        socket.connect(self.iopub_url)
        try:
            self.iopub_sockets[self.thread] = socket
            task_status.started()
            while not self._stopped:
                while len(self._callers):
                    job = self._callers.popleft()
                    if isinstance(job, Callable):
                        try:
                            job()
                        except Exception as e:
                            self.log.exception("Simple call failed", exc_info=e)
                    else:
                        context, args = job
                        context.run(tg.start_soon, self._wrap_call, *args)
                    self._callers_added.clear()
                await wait_thread_event(self._callers_added)
        finally:
            self._running = False
            for job in self._callers:
                if not callable(job):
                    job[1][0].set_exception(FutureCancelledError())
            socket.close()
            self.iopub_sockets.pop(self.thread, None)
            tg.cancel_scope.cancel()

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
                    if inspect.isawaitable(result):
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
                            e = FutureCancelledError()
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

    def _check_in_thread(self):
        if self.thread is not threading.current_thread():
            msg = "This function must be called from its own thread. Tip: Use `call_no_context` to call this method from another thread."
            raise RuntimeError(msg)

    @property
    def protected(self) -> bool:
        "Returns `True` if the caller is protected from stopping."
        return self._protected

    @property
    def running(self):
        "Returns `True` when the caller is available to run requests."
        return self._running

    @property
    def stopped(self) -> bool:
        "Returns  `True` if the caller is stopped."
        return self._stopped

    def stop(self, *, force=False) -> None:
        """Stop the caller, cancelling all pending tasks and close the thread.

        If the instance is protected, this is no-op unless force is used.
        """
        if self._protected and not force:
            return
        self._stopped = True
        self._callers_added.set()
        self._instances.pop(self.thread, None)
        if self in self._to_thread_pool:
            self._to_thread_pool.remove(self)

    def call_later(
        self, func: Callable[P, T | Awaitable[T]], delay: float = 0.0, /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """Schedule func to be called in this instances event loop using the current contextvars context.

        Args:
            func: The function (awaitables permitted, though discouraged).
            delay: The minimum delay to add between submission and execution.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.
        """
        if self._stopped:
            raise anyio.ClosedResourceError
        fut: Future[T] = Future(thread=self.thread)
        if threading.current_thread() is self.thread and (tg := self._taskgroup):
            tg.start_soon(self._wrap_call, fut, time.monotonic(), delay, func, args, kwargs)
        else:
            self._callers.append((contextvars.copy_context(), (fut, time.monotonic(), delay, func, args, kwargs)))
            self._callers_added.set()
        self._outstanding += 1
        return fut

    def call_soon(self, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Schedule func to be called in this instances event loop using the current contextvars context.

        Args:
            func: The function (awaitables permitted, though discouraged).
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.
        """
        return self.call_later(func, 0.0, *args, **kwargs)

    def call_no_context(self, func: Callable[P, Any], /, *args: P.args, **kwargs: P.kwargs) -> None:
        """Call func in the thread event loop.

        Args:
            func: The function (awaitables permitted, though discouraged).
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.
        """
        self._callers.append(functools.partial(func, *args, **kwargs))
        self._callers_added.set()

    def has_execution_queue(self, func: Callable) -> bool:
        "Returns True if an execution queue exists for `func`."
        return func in self._executor_queue

    async def queue_call(
        self,
        func: Callable[[*PosArgsT], Awaitable[Any]],
        /,
        *args: *PosArgsT,
        max_buffer_size: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
    ) -> None:
        """Queue the execution of func in queue specific to the function (not thread-safe).

        The args are added to a queue associated with the provided `func`. If queue does not already exist for
        func, a new queue is created with a specified maximum buffer size. The arguments are then sent to the queue,
        and an `execute_loop` coroutine is started to consume the queue and execute the function with the received
        arguments.  Exceptions during execution are caught and logged.

        Args:
            func: The asynchronous function to execute.
            *args: The arguments to pass to the function.
            max_buffer_size: The maximum buffer size for the queue. If NoValue, defaults to [async_kernel.Caller.MAX_BUFFER_SIZE].
        """
        self._check_in_thread()
        if not self.has_execution_queue(func):
            max_buffer_size = self.MAX_BUFFER_SIZE if max_buffer_size is NoValue else max_buffer_size
            sender, queue = anyio.create_memory_object_stream[tuple[*PosArgsT]](max_buffer_size=max_buffer_size)

            async def execute_loop():
                try:
                    with contextlib.suppress(anyio.get_cancelled_exc_class()):
                        async with queue as receive_stream:
                            async for args in receive_stream:
                                try:
                                    await func(*args)
                                except Exception as e:
                                    self.log.exception("Execution %f failed", func, exc_info=e)
                finally:
                    self._executor_queue.pop(execute_loop, None)

            self._executor_queue[func] = {"queue": sender, "future": self.call_soon(execute_loop)}
        await self._executor_queue[func]["queue"].send(args)

    async def queue_close(self, func: Callable, *, force: bool = False) -> bool:
        """Close the execution queue associated with func (not thread-safe).

        Args:
            func: The queue of the function to close.
            force: Shutdown without waiting pending tasks to complete.

        Returns:
            True if a queue was closed.
        """
        self._check_in_thread()
        if queue_map := self._executor_queue.pop(func, None):
            if force:
                queue_map["future"].cancel()
            else:
                await queue_map["queue"].aclose()
            with contextlib.suppress(FutureCancelledError):
                await queue_map["future"]
            return True
        return False

    @classmethod
    def stop_all(cls, *, _stop_protected: bool = False) -> None:
        """A classmethod to stop all un-protected callers.

        Args:
            _stop_protected: A private argument to shutdown protected instances.
        """
        for caller in tuple(reversed(cls._instances.values())):
            caller.stop(force=_stop_protected)

    @classmethod
    def get_instance(cls, name: str | None = "MainThread", *, create: bool = False) -> Self:
        """A classmethod that gets an instance by name, possibly starting a new instance.

        Args:
            name: The name to identify the caller.
            create: Create a new instance if one with the corresponding name does not already exist.
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
        """A classmethod to call func in a separate thread see also [to_thread_by_name][async_kernel.Caller.to_thread_by_name]."""
        return cls.to_thread_by_name(None, func, *args, **kwargs)

    @classmethod
    def to_thread_by_name(
        cls, name: str | None, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """A classmethod to call func in the thread specified by name.

        Args:
            name: The name of the `Caller`. A new `Caller` is created if an instance corresponding to name  [^notes].

                [^notes]:  'MainThread' is special name corresponding to the main thread.
                    A `RuntimeError` will be raised if a Caller does not exist for the main thread.

            func: The function to call. If it returns an awaitable, the awaitable will be awaited.
                Passing a coroutine as `func` discourage, but will be awaited.

            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Returns:
            A future that can be awaited for the  result of func.
        """
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
        protected: bool = False,
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
    ) -> AsyncGenerator[Future[T], Any]:
        """An iterator to get [Futures][async_kernel.caller.Future] as they complete.

        Args:
            items: Either a container with existing futures or generator of Futures.
            max_concurrent: The maximum number of concurrent futures to monitor at a time.
                This is useful when `items` is a generator utilising Caller.to_thread.
                By default this will limit to `Caller.MAX_IDLE_POOL_INSTANCES`.

        !!! tip

            1. Pass a generator should you wish to limit the number future jobs when calling to_thread/to_task etc.
            2. Pass a set/list/tuple to ensure all get monitored at once.
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
    def all_callers(cls, running_only: bool = True) -> list[Caller]:
        """A classmethod to get a list of the callers.

        Args:
            running_only: Restrict the list to callers that are active (running in an async context).
        """
        return [caller for caller in Caller._instances.values() if caller._running or not running_only]
