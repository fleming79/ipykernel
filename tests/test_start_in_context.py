# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import pytest

from async_kernel import Kernel
from async_kernel.kernelspec import KernelName
from async_kernel.typing import SocketID


@pytest.fixture(scope="module", params=list(KernelName))
def kernel_name(request):
    return request.param


@pytest.fixture(scope="module")
def anyio_backend(kernel_name: KernelName):
    return "trio" if kernel_name is KernelName.trio else "asyncio"


async def test_start_kernel_in_context(anyio_backend, kernel_name):
    async with Kernel().start_in_context() as kernel:
        assert kernel.kernel_name == kernel_name
        connection_file = kernel.connection_file
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="Already started"):
            async with kernel.start_in_context():
                pass
        # Test prevents binding socket more than once.
        with (
            pytest.raises(RuntimeError, match=".*is already loaded"),
            kernel._bind_socket(SocketID.shell, None),  # pyright: ignore[reportArgumentType, reportPrivateUsage]
        ):
            pass
    async with Kernel(connection_file=connection_file).start_in_context():
        # Test we can re-enter the kernel.
        pass
