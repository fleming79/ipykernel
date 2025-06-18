import gc
import os
import sys
import warnings
from typing import TYPE_CHECKING

import pytest
from jupyter_client.asynchronous.client import AsyncKernelClient

from ipykernel.kernelapp import MainKernel

if TYPE_CHECKING:
    pytest_plugins = ["anyio.pytest_plugin"]


@pytest.fixture(scope="module", autouse=True)
def _garbage_collection(request):
    gc.collect()


try:
    import resource
except ImportError:
    # Windows
    resource = None  # type:ignore  # noqa: PGH003

try:
    import tracemalloc
except ModuleNotFoundError:
    tracemalloc = None

pytestmark = pytest.mark.anyio


@pytest.hookimpl
def pytest_configure(config):
    os.environ["PYTEST_TIMEOUT"] = str(1e6) if "debugpy" in sys.modules else str(60)


# Handle resource limit
# Ensure a minimal soft limit of DEFAULT_SOFT if the current hard limit is at least that much.
if resource is not None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    DEFAULT_SOFT = 4096
    if hard >= DEFAULT_SOFT:
        soft = DEFAULT_SOFT

    if hard < soft:
        hard = soft

    resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
async def app(anyio_backend, tmp_path_factory):
    # Set a blank connection_file
    connection_file = tmp_path_factory.mktemp("ipykernel") / "temp_connection.json"
    kernel = MainKernel()
    kernel.connection_file = str(connection_file.resolve())
    async with kernel.start_in_context():
        yield kernel


@pytest.fixture(scope="session")
async def kernel(app: MainKernel, client: AsyncKernelClient):
    # We require client for it to send shutdown
    return app


@pytest.fixture(scope="session")
async def client(app: MainKernel):
    client: AsyncKernelClient = AsyncKernelClient()
    client.load_connection_info(app.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        client.stop_channels()

@pytest.fixture
def tracemalloc_resource_warning(recwarn, N=10):
    """fixture to enable tracemalloc for a single test, and report the
    location of the leaked resource

    We cannot only enable tracemalloc, as otherwise it is stopped just after the
    test, the frame cache is cleared by tracemalloc.stop() and thus the warning
    printing code get None when doing
    `tracemalloc.get_object_traceback(r.source)`.

    So we need to both filter the warnings to enable ResourceWarning, and loop
    through it print the stack before we stop tracemalloc and continue.

    """
    if tracemalloc is None:
        yield
        return

    tracemalloc.start(N)
    with warnings.catch_warnings():
        warnings.simplefilter("always", category=ResourceWarning)
        yield None
    try:
        for r in recwarn:
            if r.category is ResourceWarning and r.source is not None:
                tb = tracemalloc.get_object_traceback(r.source)
                if tb:
                    info = f"Leaking resource:{r}\n |" + "\n |".join(tb.format())
                    # technically an Error and not a failure as we fail in the fixture
                    # and not the test
                    pytest.fail(info)
    finally:
        tracemalloc.stop()
