# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import os
import subprocess
import sys
from typing import TYPE_CHECKING

import anyio
import pytest
import zmq
from jupyter_client.asynchronous.client import AsyncKernelClient

import async_kernel.utils
from async_kernel.kernel import Kernel
from async_kernel.kernelspec import KernelName, make_argv
from tests import utils

if TYPE_CHECKING:
    pytest_plugins = ["anyio.pytest_plugin"]

pytestmark = pytest.mark.anyio

if sys.platform.startswith("win"):
    import asyncio

    # needed for `jupyter_client.AsyncKernelClient` messaging only
    # ref: https://github.com/zeromq/pyzmq/issues/1423
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]


@pytest.hookimpl
def pytest_configure(config):
    os.environ["PYTEST_TIMEOUT"] = str(1e6) if async_kernel.utils.LAUNCHED_BY_DEBUGPY else str(utils.TIMEOUT)


@pytest.fixture(scope="module")
def anyio_backend(request):
    return "asyncio"


@pytest.fixture(scope="module")
def transport():
    return 'tcp'
    return "ipc" if zmq.has("ipc") else "tcp"


@pytest.fixture(scope="module")
async def kernel(anyio_backend, transport: str, tmp_path_factory):
    # Set a blank connection_file
    utils.clear_kernel()
    connection_file = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))
    kernel = Kernel()
    kernel.connection_file = str(connection_file.resolve())
    kernel.transport = transport
    try:
        async with kernel.start_in_context():
            yield kernel
    finally:
        utils.clear_kernel()


@pytest.fixture(scope="module")
async def client(kernel: Kernel):
    if kernel.kernel_name is KernelName.trio:
        pytest.skip("AsyncKernelClient needs asyncio")
    client = AsyncKernelClient()
    client.load_connection_info(kernel.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        client.stop_channels()
        await anyio.sleep(0)


@pytest.fixture(scope="module", params=KernelName)
def kernel_name(request):
    return request.param


@pytest.fixture(scope="module")
async def subprocess_kernels_client(anyio_backend, tmp_path_factory, kernel_name: KernelName):
    """Starts a kernel in a subprocess and returns an AsyncKernelCient that is connected to it.

    This is primarily provided for testing the debugger making it more convenient
    connect a debugger to the test.
    """
    assert anyio_backend == "asyncio", "Asyncio is required for the client"
    connection_file = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    command = make_argv(connection_file=connection_file, kernel_name=kernel_name)
    process = subprocess.Popen(command)
    try:
        client = AsyncKernelClient()
        while not connection_file.exists() or not connection_file.stat().st_size:
            await anyio.sleep(0.1)
        client.load_connection_file(connection_file)
        client.start_channels()
        msg_id = client.kernel_info()
        await utils.get_reply(client, msg_id)
        try:
            yield client
        finally:
            client.shutdown()
            client.stop_channels()
            assert process.wait(10) == 0
    finally:
        if process.returncode is None:
            process.kill()

    assert not connection_file.exists(), "cleanup_connection_file not called by atexit ..."
