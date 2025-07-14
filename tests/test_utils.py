"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import anyio
import anyio.to_thread
import pytest

from async_kernel.utils import PendingResult, ThreadSafeCaller


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param


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

        async with ThreadSafeCaller() as ts_caller:
            is_called = anyio.Event()
            ts_caller.call_later(my_func, 0, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs

    async def test_to_thread(self):
        # Test the call works from another thread
        async with ThreadSafeCaller() as ts_caller:
            is_called = anyio.Event()
            await anyio.to_thread.run_sync(ts_caller.call_later, is_called.set)
            await is_called.wait()

    async def test_sleep_forever(self):
        async with ThreadSafeCaller() as ts_caller:
            ts_caller.call_later(anyio.sleep_forever)


@pytest.mark.anyio
class TestPendingResult:
    async def test_set_and_wait_result(self):
        pr = PendingResult()
        pr.set_result(42)
        result = await pr.wait()
        assert result == 42

    async def test_set_and_wait_exception(self):
        pr = PendingResult()
        exc = ValueError("fail")
        pr.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await pr.wait()
        assert e.value is exc

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
