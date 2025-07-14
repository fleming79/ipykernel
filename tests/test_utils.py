"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import contextlib
import threading
from typing import cast

import anyio
import anyio.to_thread
import pytest

from async_kernel.utils import PendingResult, ThreadSafeCaller


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param


@pytest.mark.anyio
class TestPendingResult:
    async def test_set_and_wait_result(self):
        pr = PendingResult()
        pr.set_result(42)
        result = await pr.wait()
        assert result == 42

    async def test_set_and_wait_exception(self):
        pr = PendingResult()
        assert not pr.done()
        exc = ValueError("fail")
        pr.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await pr.wait()
        assert e.value is exc
        assert pr.done()

    async def test_set_result_twice_raises(self):
        pr = PendingResult()
        pr.set_result(1)
        with pytest.raises(RuntimeError):
            pr.set_result(2)

    async def test_set_exception_twice_raises(self):
        pr = PendingResult()
        pr.set_exception(ValueError())
        with pytest.raises(RuntimeError):
            pr.set_exception(ValueError())

    async def test_set_result_after_exception_raises(self):
        pr = PendingResult()
        pr.set_exception(ValueError())
        with pytest.raises(RuntimeError):
            pr.set_result(1)

    async def test_set_exception_after_result_raises(self):
        pr = PendingResult()
        pr.set_result(1)
        with pytest.raises(RuntimeError):
            pr.set_exception(ValueError())


@pytest.mark.anyio
class TestThreadSafeCaller:
    async def test_sync(self):
        async with ThreadSafeCaller() as ts_caller:
            is_called = anyio.Event()
            ts_caller.call_later(is_called.set)
            await is_called.wait()

    @pytest.mark.parametrize("args_kwargs", [((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: anyio.Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with ThreadSafeCaller() as ts_caller:
            is_called = anyio.Event()
            pr = ts_caller.call_later(my_func, 0, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs
            assert (await pr.wait()) == args_kwargs

    async def test_to_thread(self):
        # Test the call works from another thread
        async with ThreadSafeCaller() as ts_caller:

            def _in_thread():
                def my_func(*args, **kwargs):
                    return args, kwargs

                async def runner():
                    pr = ts_caller.call_soon(my_func, 1, 2, 3, a=10)
                    result = await pr.wait()
                    assert result == ((1, 2, 3), {"a": 10})

                anyio.run(runner)

            await anyio.to_thread.run_sync(_in_thread)

    async def test_cancels_on_exit(self):
        is_cancelled = False
        async with ThreadSafeCaller() as ts_caller:

            async def my_test():
                nonlocal is_cancelled
                started.set()
                try:
                    await anyio.sleep_forever()
                except anyio.get_cancelled_exc_class():
                    is_cancelled = True

            started = anyio.Event()
            ts_caller.call_later(my_test)
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
                async with ThreadSafeCaller():
                    ready.set()
                    await finished_event.wait()

            anyio.run(_run, backend=anyio_backend)

        the_thread = threading.Thread(target=_thread_task, daemon=True)
        the_thread.start()
        ready.wait()
        assert finished_event
        tsc = ThreadSafeCaller.get_instance(the_thread)
        if check_result == "result":
            expr = "10"
            context = contextlib.nullcontext()
        else:
            expr = "invalid call"
            context = pytest.raises(SyntaxError)
        pending = tsc.call_later(eval, 0.2, expr)
        with context:
            match check_mode:
                case "main":
                    assert (await pending.wait()) == 10
                case "local":
                    pending_local = tsc.call_soon(pending.wait)
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

        tsc.call_soon(finished_event.set)

    async def test_error_wait_sync(self):
        async with ThreadSafeCaller() as tsc:
            pending = tsc.call_later(anyio.sleep, 0.1, 0.1)
            with pytest.raises(RuntimeError):
                pending.wait_sync()

    async def test_not_available_for_thread(self):
        with pytest.raises(RuntimeError):
            ThreadSafeCaller.get_instance(threading.Thread())
