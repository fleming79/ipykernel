import os
import sys
from typing import TYPE_CHECKING

import anyio
import pytest
from jupyter_client.asynchronous.client import AsyncKernelClient

from async_kernel.kernel import Kernel

if TYPE_CHECKING:
    pytest_plugins = ["anyio.pytest_plugin"]

pytestmark = pytest.mark.anyio

if sys.platform.startswith("win"):
    import asyncio

    # needed for `jupyter_client.AsyncKernelClient` messaging only
    # ref: https://github.com/zeromq/pyzmq/issues/1423
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.hookimpl
def pytest_configure(config):
    os.environ["PYTEST_TIMEOUT"] = str(1e6) if "debugpy" in sys.modules else str(60)


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
async def kernel(anyio_backend, tmp_path_factory):
    # Set a blank connection_file
    connection_file = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))
    kernel = Kernel()
    kernel.connection_file = str(connection_file.resolve())
    async with kernel.start_in_context():
        yield kernel


@pytest.fixture(scope="session")
async def client(kernel: Kernel):
    client = AsyncKernelClient()
    client.load_connection_info(kernel.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        client.stop_channels()
        await anyio.sleep(0)
