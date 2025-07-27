"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import contextlib
import threading
import time
from random import random
from typing import Literal, cast

import anyio
import anyio.to_thread
import pytest
import sniffio

from async_kernel.caller import Caller
from async_kernel.pending_result import PendingResult


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param


@pytest.mark.anyio
class TestCaller:
    def setup_method(self, test_method):
        Caller._shutdown_all()

    def teardown_method(self, test_method):
        Caller._shutdown_all()

    async def test_sync(self):
        async with Caller() as caller:
            is_called = anyio.Event()
            caller.call_later(is_called.set)
            await is_called.wait()

    @pytest.mark.parametrize("args_kwargs", [((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: anyio.Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller() as caller:
            is_called = anyio.Event()
            pr = caller.call_later(my_func, 0.2, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs
            assert (await pr.wait()) == args_kwargs

    async def test_anyio_to_thread(self):
        # Test the call works from another thread
        async with Caller() as caller:

            def _in_thread():
                def my_func(*args, **kwargs):
                    return args, kwargs

                async def runner():
                    pr = caller.call_soon(my_func, 1, 2, 3, a=10)
                    result = await pr.wait()
                    assert result == ((1, 2, 3), {"a": 10})

                anyio.run(runner)

            await anyio.to_thread.run_sync(_in_thread)

    async def test_cancels_on_exit(self):
        is_cancelled = False
        async with Caller() as caller:

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
        finished_event = cast("anyio.Event", None)
        ready = threading.Event()

        def _thread_task():
            nonlocal the_thread
            nonlocal finished_event
            finished_event = anyio.Event()

            async def _run():
                async with Caller():
                    ready.set()
                    await finished_event.wait()

            anyio.run(_run, backend=anyio_backend)

        the_thread = threading.Thread(target=_thread_task, daemon=True)
        the_thread.start()
        ready.wait()
        assert finished_event
        caller = Caller.get_instance(the_thread.name)
        if check_result == "result":
            expr = "10"
            context = contextlib.nullcontext()
        else:
            expr = "invalid call"
            context = pytest.raises(SyntaxError)
        pending = caller.call_later(eval, 0.2, expr)
        with context:
            match check_mode:
                case "main":
                    assert (await pending.wait()) == 10
                case "local":
                    pending_local = caller.call_soon(pending.wait)
                    result = await pending_local.wait()
                    assert result == 10
                case "wait_sync":
                    assert pending.wait_sync() == 10
                case "asyncio" | "trio":

                    def another_thread():
                        async def waiter():
                            result = await pending.wait()
                            assert result == 10
                            return result

                        return anyio.run(waiter, backend=check_mode)

                    result = await anyio.to_thread.run_sync(another_thread)
                    assert result == 10

        caller.call_soon(finished_event.set)
        the_thread.join()

    async def test_error_wait_sync(self):
        async with Caller() as caller:
            pending = caller.call_later(anyio.sleep, 0.1, 0.1)
            with pytest.raises(RuntimeError):
                pending.wait_sync()

    async def test_to_thread(self, anyio_backend, mocker):
        mocker.patch.object(Caller, "MAX_IDLE_EVENT_THREADS", new=2)

        async def func():
            assert sniffio.current_async_library() == anyio_backend
            n = random()
            if n < 0.2:
                time.sleep(0.01)
            elif n < 0.6:
                await anyio.sleep(0.01)
            return threading.current_thread()

        threads = set()
        n = 40

        pending = Caller.to_thread(time.sleep, 0)
        await pending.wait()
        # check can handle completed pending okay first
        async for pending_ in PendingResult.as_completed([pending]):
            assert pending_.done()
        # work directly with iterator
        async for pending in PendingResult.as_completed(Caller.to_thread(func) for _ in range(n)):
            assert pending.done()
            thread = await pending.wait()
            threads.add(thread)
        assert len(threads) > Caller.MAX_IDLE_EVENT_THREADS
        assert len(Caller._to_thread_pool) <= Caller.MAX_IDLE_EVENT_THREADS
        Caller._shutdown_all()

    async def test_call_early(self, anyio_backend):
        caller = Caller()
        with pytest.raises(RuntimeError, match=".*not currently open in an async context"):
            caller.taskgroup  # noqa: B018
        pr = caller.call_soon(time.sleep, 0.1)
        with anyio.move_on_after(0.1):
            await pr.wait()
        assert not pr.done()
        async with caller:
            await pr.wait()

    async def test_closed_in_call_soon(self, anyio_backend):
        async def close_tsc():
            caller = Caller()
            caller.close()
            await anyio.sleep_forever()

        pr = Caller.to_thread(close_tsc)
        caller = Caller.get_instance(pr.thread.name)
        cancelled_ = anyio.get_cancelled_exc_class()
        with pytest.raises(cancelled_):
            await pr.wait()
        assert pr.done()
        assert caller._closed
        with pytest.raises(RuntimeError):
            caller.call_soon(time.sleep, 0)

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

        async with Caller() as caller:
            pr = caller.call_soon(my_func)
            if cancel_mode == "local":
                pr.cancel()
            else:
                caller.to_thread(pr.cancel)

            with pytest.raises(anyio.get_cancelled_exc_class()):
                await pr.wait()

    async def test_subshell(self, anyio_backend):
        subshell_id = Caller.start_subshell()
        assert subshell_id in Caller.list_subshells()
        Caller.delete_subshell(subshell_id)
        assert not Caller.list_subshells()
