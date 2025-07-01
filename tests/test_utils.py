"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.


import anyio.to_thread
import pytest

from async_kernel.utils import ThreadSafeCaller


class TestThreadSafeCaller:
    async def test_sync(self, anyio_backend):
        ts_caller = ThreadSafeCaller()
        async with ts_caller.begin():
            is_called = anyio.Event()
            ts_caller.call(is_called.set)
            await is_called.wait()

    @pytest.mark.parametrize("args_kwargs", [((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, anyio_backend, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: anyio.Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()

        ts_caller = ThreadSafeCaller()
        async with ts_caller.begin():
            is_called = anyio.Event()
            ts_caller.call(my_func, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs


    async def test_to_thread(self, anyio_backend):
        # Test the call works from another thread
        ts_caller = ThreadSafeCaller()
        async with ts_caller.begin():
            is_called = anyio.Event()
            await anyio.to_thread.run_sync(ts_caller.call, is_called.set)
            await is_called.wait()