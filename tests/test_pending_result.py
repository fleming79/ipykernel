"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import pytest
import zmq

from async_kernel.pending_result import PendingResult


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="module", params=["tcp", "ipc"] if zmq.has("ipc") else ["tcp"])
def transport(request):
    return request.param


@pytest.mark.anyio
class TestPendingResult:
    async def test_set_and_wait_result(self):
        pr = PendingResult()
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is pr
            done_called = True

        pr._done_callbacks.add(callback)
        pr.set_result(42)
        result = await pr.wait()
        assert result == 42
        assert done_called

    async def test_set_and_wait_exception(self):
        pr = PendingResult()
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is pr
            done_called = True

        pr._done_callbacks.add(callback)
        assert not pr.done()
        exc = ValueError("fail")
        pr.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await pr.wait()
        assert e.value is exc
        assert pr.done()
        assert done_called

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
