"""Test IPythonKernel directly"""

import os
import time

import pytest
from comm import _create_comm
from IPython.core.history import DummyDB

from ipykernel.comm.comm import BaseComm
from tests.utils import ka_kc_kernel, shell_message, test_control_message

if os.name == "nt":
    pytest.skip("skipping tests on windows", allow_module_level=True)


class user_mod:
    __dict__ = {}


async def test_properties(client, kernel) -> None:
    ka, client, kernel = ka_kc_kernel(client, kernel)
    kernel.user_module = user_mod()
    kernel.user_ns = {}


async def test_direct_kernel_info_request(client, kernel):
    reply = await shell_message(client, kernel, "kernel_info_request")
    assert reply["header"]["msg_type"] == "kernel_info_reply"
    assert (
        "supported_features" not in reply["content"] or "kernel subshells" not in reply["content"]["supported_features"]
    )


async def test_direct_execute_request(client, kernel) -> None:
    ka, client, kernel = ka_kc_kernel(client, kernel)
    reply = await shell_message(client, kernel, "execute_request", code="invalid_call()", silent=False)
    assert reply["header"]["msg_type"] == "execute_reply"
    kernel._aborted_time += 10
    reply = await shell_message(client, kernel, "execute_request", code="trigger_error", silent=False)
    assert reply["content"]["status"] == "aborted"
    kernel._aborted_time = time.monotonic()
    reply = await shell_message(client, kernel, "execute_request", code="okay=True", silent=False)
    assert reply["header"]["msg_type"] == "execute_reply"


async def test_direct_execute_request_aborting(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    kernel._aborted_time = time.monotonic() + 10  # Set in the future
    reply = await shell_message(client, kernel, "execute_request", code="hello", silent=False)
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "aborted"


async def test_complete_request(client, kernel, tracemalloc_resource_warning):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    reply = await shell_message(client, kernel, "complete_request", code="hello", cursor_pos=0)
    assert reply["header"]["msg_type"] == "complete_reply"
    kernel.use_experimental_completions = False
    reply = await shell_message(client, kernel, "complete_request", code="hello", cursor_pos=None)
    assert reply["header"]["msg_type"] == "complete_reply"


async def test_inspect_request(client, kernel):
    reply = await shell_message(client, kernel, "inspect_request", code="hello", cursor_pos=0)
    assert reply["header"]["msg_type"] == "inspect_reply"


async def test_history_request(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    assert kernel.shell
    assert kernel.shell.history_manager
    kernel.shell.history_manager.db = DummyDB()
    reply = await shell_message(client, kernel, "history_request", hist_access_type="", output="", raw="")
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await shell_message(client, kernel, "history_request", hist_access_type="tail", output="", raw="")
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await shell_message(client, kernel, "history_request", hist_access_type="range", output="", raw="")
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await shell_message(client, kernel, "history_request", hist_access_type="search", output="", raw="")
    assert reply["header"]["msg_type"] == "history_reply"


async def test_comm_info_request(client, kernel):
    reply = await shell_message(client, kernel, "comm_info_request")
    assert reply["header"]["msg_type"] == "comm_info_reply"


async def test_direct_interrupt_request(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    reply = await test_control_message(client, kernel, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}

    # test failure on interrupt request
    def raiseOSError():
        msg = "evalue"
        raise OSError(msg)

    kernel._send_interrupt_children = raiseOSError
    reply = await test_control_message(client, kernel, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "OSError"
    assert reply["content"]["evalue"] == "evalue"
    assert len(reply["content"]["traceback"]) > 0


# TODO: this causes deadlock
# async def test_direct_shutdown_request(kc, kernel):
#     reply = await shell_message(kc, kernel, "shutdown_request", restart=False))
#     assert reply["header"]["msg_type"] == "shutdown_reply"
#     reply = await shell_message(kc, kernel, "shutdown_request", restart=True))
#     assert reply["header"]["msg_type"] == "shutdown_reply"

# TODO: this causes deadlock
# async def test_direct_usage_request(kernel):
#     reply = await test_control_message("usage_request")
#     assert reply['header']['msg_type'] == 'usage_reply'


async def test_is_complete_request(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    reply = await shell_message(client, kernel, "is_complete_request", code="hello")
    assert reply["header"]["msg_type"] == "is_complete_reply"
    setattr(kernel, "shell.input_transformer_manager", None)
    reply = await shell_message(client, kernel, "is_complete_request", code="hello")
    assert reply["header"]["msg_type"] == "is_complete_reply"


async def test_direct_debug_request(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    reply = await test_control_message(client, kernel, "debug_request")
    assert reply["header"]["msg_type"] == "debug_reply"


async def test_direct_clear(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    kernel.do_clear()


async def test_dispatch_debugpy(client, kernel) -> None:
    ka, client, kernel = ka_kc_kernel(client, kernel)
    msg = kernel.session.msg("debug_request")
    msg_list = kernel.session.serialize(msg)
    await kernel.receive_debugpy_message(msg_list)


async def test_create_comm(client, kernel):
    assert isinstance(_create_comm(), BaseComm)


async def test_do_debug_request(client, kernel):
    ka, client, kernel = ka_kc_kernel(client, kernel)
    msg = kernel.session.msg("debug_request")
    kernel.session.serialize(msg)
    await kernel.do_debug_request(msg)


@pytest.mark.parametrize("mode", ["main", "external"])
@pytest.mark.parametrize("exception", [True, False])
async def test_start_soon(mode, exception: bool, client, kernel):
    # Test we can start coroutines from various scopes

    import anyio
    from anyio import to_thread

    ka, client, kernel = ka_kc_kernel(client, kernel)

    async def my_test(event: anyio.Event):
        event.set()
        if exception:
            raise ValueError

    events = []

    async def start():
        event = anyio.Event()
        if mode == "main":
            kernel.start_soon(my_test, event)
        else:
            await to_thread.run_sync(kernel.start_soon, my_test, event)
        events.append(event)

    for _ in range(50):
        await start()

    for event in events:
        await event.wait()
