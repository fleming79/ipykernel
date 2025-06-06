import gc
import os
import sys
import warnings
from typing import TYPE_CHECKING

import pytest
from anyio import create_task_group
from jupyter_client.asynchronous.client import AsyncKernelClient

from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelapp import IPKernelApp
from ipykernel.zmqshell import ZMQInteractiveShell

if TYPE_CHECKING:
    from ipykernel.kernelbase import Kernel

    pytest_plugins = ["anyio.pytest_plugin"]


@pytest.fixture(scope="module", autouse=True)
def _garbage_collection(request):
    gc.collect()


@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


try:
    import resource
except ImportError:
    # Windows
    resource = None  # type:ignore

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


@pytest.fixture(scope="module")
async def app(anyio_backend):
    async with create_task_group() as tg:
        try:
            app: IPKernelApp = IPKernelApp()
            app.initialize()
            kernel: Kernel = app.kernel
            tg.start_soon(kernel.start)
            await kernel._main_subshell_ready.wait()
        except Exception as e:
            e.add_note("Failed to setup IPKernelApp or AsyncKernelClient")
            raise e from None
        try:
            yield app
        finally:
            kernel.stop()
            kernel.clear_instance()
            ZMQInteractiveShell.clear_instance()


@pytest.fixture(scope="module")
async def kernel(app: IPKernelApp) -> IPythonKernel:
    return app.kernel


@pytest.fixture(scope="module")
async def client(app: IPKernelApp):
    kc: AsyncKernelClient = AsyncKernelClient()
    kc.load_connection_info(app.get_connection_info())
    kc.start_channels()
    try:
        yield kc
    finally:
        kc.shutdown()


@pytest.fixture()
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
