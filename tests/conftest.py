import os
import sys
from typing import TYPE_CHECKING

import pytest
from jupyter_client.asynchronous.client import AsyncKernelClient

from ipykernel.kernel import Kernel

if TYPE_CHECKING:
    pytest_plugins = ["anyio.pytest_plugin"]

pytestmark = pytest.mark.anyio


@pytest.hookimpl
def pytest_configure(config):
    os.environ["PYTEST_TIMEOUT"] = str(1e6) if "debugpy" in sys.modules else str(60)


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
async def kernel(anyio_backend, tmp_path_factory):
    # Set a blank connection_file
    connection_file = tmp_path_factory.mktemp("ipykernel") / "temp_connection.json"
    kernel = Kernel()
    kernel.connection_file = str(connection_file.resolve())
    async with kernel.start_in_context():
        yield kernel


@pytest.fixture(scope="session")
async def client(kernel: Kernel):
    client: AsyncKernelClient = AsyncKernelClient()
    client.load_connection_info(kernel.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        client.stop_channels()
