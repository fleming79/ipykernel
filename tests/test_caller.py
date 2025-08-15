"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import contextlib
import inspect
import threading
import time
from contextvars import ContextVar
from random import random
from typing import Literal, cast

import anyio
import anyio.to_thread
import pytest
import sniffio
import zmq
from anyio.abc import TaskStatus

from async_kernel.caller import Caller, Future, FutureCancelledError


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="module", params=["tcp", "ipc"] if zmq.has("ipc") else ["tcp"])
def transport(request):
    return request.param


@pytest.mark.anyio
class TestFuture:
    async def test_set_and_wait_result(self):
        fut = Future[int]()
        assert inspect.isawaitable(fut)
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is fut
            done_called = True

        fut.add_done_callback(callback)
        fut.set_result(42)
        result = await fut
        assert result == 42
        assert done_called

    async def test_set_and_wait_exception(self):
        fut = Future()
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is fut
            done_called = True

        fut.add_done_callback(callback)
        assert not fut.done()
        exc = ValueError("fail")
        fut.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await fut
        assert e.value is exc
        assert fut.done()
        assert done_called
        assert fut.remove_done_callback(callback) == 1

    async def test_set_result_twice_raises(self):
        fut = Future()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_result(2)

    async def test_set_exception_twice_raises(self):
        fut = Future()
        fut.set_exception(ValueError())
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError())

    async def test_set_result_after_exception_raises(self):
        fut = Future()
        assert fut.exception() is None
        fut.set_exception(ValueError())
        assert isinstance(fut.exception(), ValueError)
        with pytest.raises(RuntimeError):
            fut.set_result(1)

    async def test_set_exception_after_result_raises(self):
        fut = Future()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError())

    def test_cancel(self):
        fut = Future()
        assert fut.cancel()

    def test_error_from_non_thread(self):
        fut = Future(thread=threading.Thread())
        with pytest.raises(RuntimeError):
            fut.set_result(None)

    async def test_set_from_non_thread(self, anyio_backend):
        caller = Caller.start_new(backend=anyio_backend)
        try:
            fut = Future(thread=caller.thread)
            assert fut.thread is not threading.current_thread()
            fut.set_result(value=123)
            assert (await fut) == 123
        finally:
            caller.stop()


@pytest.mark.anyio
class TestCaller:
    def setup_method(self, test_method):
        Caller.stop_all()

    def teardown_method(self, test_method):
        Caller.stop_all()

    async def test_sync(self):
        async with Caller(create=True) as caller:
            is_called = anyio.Event()
            caller.call_later(is_called.set)
            await is_called.wait()

    def test_caller_no_thread(self):
        with pytest.raises(RuntimeError):
            Caller()

    def test_caller_protected(self):
        caller = Caller(create=True, protected=True)
        caller.stop()
        assert not caller.stopped
        caller.stop(force=True)

    @pytest.mark.parametrize("args_kwargs", [((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: anyio.Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller(create=True) as caller:
            is_called = anyio.Event()
            fut = caller.call_later(my_func, 0.2, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs
            assert (await fut) == args_kwargs

    async def test_anyio_to_thread(self):
        # Test the call works from another thread
        async with Caller(create=True) as caller:
            assert caller.active
            assert caller in Caller.all_callers()

            def _in_thread():
                def my_func(*args, **kwargs):
                    return args, kwargs

                async def runner():
                    fut = caller.call_soon(my_func, 1, 2, 3, a=10)
                    result = await fut
                    assert result == ((1, 2, 3), {"a": 10})

                anyio.run(runner)

            await anyio.to_thread.run_sync(_in_thread)
        assert caller not in Caller.all_callers()

    async def test_cancels_on_exit(self):
        is_cancelled = False
        async with Caller(create=True) as caller:

            async def my_test():
                nonlocal is_cancelled
                started.set()
                exception_ = anyio.get_cancelled_exc_class()
                try:
                    await anyio.sleep_forever()
                except exception_:
                    is_cancelled = True

            started = anyio.Event()
            caller.call_later(my_test)
            await started.wait()
        assert is_cancelled

    @pytest.mark.parametrize("check_result", ["result", "exception"])
    @pytest.mark.parametrize("check_mode", ["main", "local", "asyncio", "trio", "wait_sync"])
    async def test_wait_from_threads(self, anyio_backend, check_mode: str, check_result: str):
        finished_event = cast("anyio.Event", object)
        ready = threading.Event()

        def _thread_task():
            nonlocal the_thread
            nonlocal finished_event
            finished_event = anyio.Event()

            async def _run():
                async with Caller(create=True):
                    ready.set()
                    await finished_event.wait()

            anyio.run(_run, backend=anyio_backend)

        the_thread = threading.Thread(target=_thread_task, daemon=True)
        the_thread.start()
        ready.wait()
        assert isinstance(finished_event, anyio.Event)
        caller = Caller.get_instance(the_thread.name)
        if check_result == "result":
            expr = "10"
            context = contextlib.nullcontext()
        else:
            expr = "invalid call"
            context = pytest.raises(SyntaxError)
        fut = caller.call_later(eval, 0.2, expr)
        with context:
            match check_mode:
                case "main":
                    assert (await fut) == 10
                case "local":
                    fut_local = caller.call_soon(fut.result)
                    result = await fut_local
                    assert result == 10
                case "wait_sync":
                    assert fut.wait_sync() == 10
                case "asyncio" | "trio":

                    def another_thread():
                        async def waiter():
                            result = await fut
                            assert result == 10
                            return result

                        return anyio.run(waiter, backend=check_mode)

                    result = await anyio.to_thread.run_sync(another_thread)
                    assert result == 10
                case _:
                    raise NotImplementedError

        caller.call_soon(finished_event.set)
        the_thread.join()

    async def test_get_instance_no_instance(self, anyio_backend):
        with pytest.raises(RuntimeError):
            Caller.get_instance(None, create=False)

    async def test_error_wait_sync(self):
        async with Caller(create=True) as caller:
            fut = caller.call_later(anyio.sleep, 0.1, 0.1)
            with pytest.raises(RuntimeError):
                fut.wait_sync()

    @pytest.mark.parametrize("mode", ["restricted", "surge"])
    async def test_as_completed(self, anyio_backend, mode: Literal["restricted", "surge"], mocker):
        mocker.patch.object(Caller, "MAX_IDLE_POOL_INSTANCES", new=2)

        async def func():
            assert sniffio.current_async_library() == anyio_backend
            n = random()
            if n < 0.2:
                time.sleep(n / 10)
            elif n < 0.6:
                await anyio.sleep(n / 10)
            return threading.current_thread()

        threads = set[threading.Thread]()
        n = 40
        fut = Caller.to_thread(time.sleep, 0)
        await fut
        # check can handle completed future okay first
        async for fut_ in Caller.as_completed([fut]):
            assert fut_.done()
        # work directly with iterator
        n_ = 0
        max_concurrent = Caller.MAX_IDLE_POOL_INSTANCES if mode == "restricted" else n / 2
        async for fut in Caller.as_completed((Caller.to_thread(func) for _ in range(n)), max_concurrent=max_concurrent):
            assert fut.done()
            n_ += 1
            thread = await fut
            threads.add(thread)
        assert n_ == n
        if mode == "restricted":
            assert len(threads) == 2
        else:
            assert len(threads) > 2
        assert len(Caller._to_thread_pool) == 2  # pyright: ignore[reportPrivateUsage]

    async def test_as_completed_error(self, anyio_backend):
        def func():
            raise RuntimeError()

        async for fut in Caller.as_completed((Caller.to_thread(func) for _ in range(6)), max_concurrent=4):
            with pytest.raises(RuntimeError):
                await fut

    async def test_as_completed_cancelled(self, anyio_backend):
        items = {Caller.to_thread(anyio.sleep, 100) for _ in range(4)}

        async def cancelled(task_status: TaskStatus[None]):
            with pytest.raises(anyio.get_cancelled_exc_class()):  # noqa: PT012
                task_status.started()
                async for _ in Caller.as_completed(items):
                    pass

        async with anyio.create_task_group() as tg:
            await tg.start(cancelled)
            tg.cancel_scope.cancel()
        for item in items:
            with pytest.raises(FutureCancelledError):
                await item

    async def test_call_early(self, anyio_backend):
        caller = Caller(create=True)
        with pytest.raises(RuntimeError, match=".*not currently open in an async context"):
            caller.taskgroup  # noqa: B018
        fut = caller.call_soon(time.sleep, 0.1)
        await anyio.sleep(0.1)
        assert not fut.done()
        async with caller:
            await fut

    async def test_call_coroutine(self, anyio_backend):
        # Test we can await a coroutine, note that it is not permitted with the type hints,
        # but should probably be discouraged anyway since there is no way of knowing
        # (with type hints) if a coroutine has already been awaited.
        my_contextvar = ContextVar[int]("my_contextvar")
        my_contextvar.set(1)

        async def my_func():
            await anyio.sleep(0)
            assert my_contextvar.get() == 1
            return True

        # Discouraged
        fut = Caller.to_thread(my_func())  # pyright: ignore[reportCallIssue, reportArgumentType]
        val = await fut
        assert val is True
        # This the preferred way of calling.
        fut = Caller.to_thread(my_func)
        val = await fut
        assert val is True

    async def test_closed_in_call_soon(self, anyio_backend):
        ready = threading.Event()
        proceed = threading.Event()

        async def close_tsc():
            caller = Caller(create=True)
            ready.set()
            proceed.wait()
            caller.stop()
            await anyio.sleep_forever()

        fut = Caller.to_thread(close_tsc)
        caller = Caller.get_instance(fut.thread.name)
        ready.wait()
        never_called_future = caller.call_later(str, 10)
        proceed.set()
        with pytest.raises(FutureCancelledError):
            await fut
        assert fut.done()
        assert caller.stopped
        with pytest.raises(anyio.ClosedResourceError):
            caller.call_soon(time.sleep, 0)
        with pytest.raises(FutureCancelledError):
            await never_called_future

    @pytest.mark.parametrize("mode", ["async", "blocking"])
    @pytest.mark.parametrize("cancel_mode", ["local", "thread"])
    async def test_cancel(
        self, anyio_backend, mode: Literal["async", "blocking"], cancel_mode: Literal["local", "thread"]
    ):
        async def async_func():
            await anyio.sleep(10)
            raise RuntimeError

        def blocking_func():
            import time  # noqa: PLC0415

            time.sleep(0.5)

        my_func = blocking_func
        match mode:
            case "async":
                my_func = async_func
            case "blocking":
                my_func = blocking_func

        async with Caller(create=True) as caller:
            fut = caller.call_soon(my_func)
            if cancel_mode == "local":
                fut.cancel()
            else:
                caller.to_thread(fut.cancel)

            with pytest.raises(anyio.ClosedResourceError):
                await fut

    async def test_cancelled_waiter(self, anyio_backend):
        # Cancelling the waiter should also cancel call soon operation.
        async def async_func():
            await anyio.sleep(10)
            raise RuntimeError

        async with Caller(create=True) as caller:
            async with anyio.create_task_group() as tg:
                fut = caller.call_soon(async_func)
                tg.start_soon(fut.result)
                await anyio.sleep(0)
                tg.cancel_scope.cancel()
            await anyio.sleep(0)
            assert isinstance(fut.exception(), FutureCancelledError)  # pyright: ignore[reportPossiblyUnboundVariable]
