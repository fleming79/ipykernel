"""Test IPythonAKernel directly"""


# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import pytest
from IPython.core.history import DummyDB

from tests.utils import send_control_message, send_shell_message


class user_mod:
    __dict__ = {}


async def test_properties(kernel) -> None:
    kernel.user_module = user_mod()
    kernel.user_ns = {}


async def test_direct_kernel_info_request(
    client,
):
    reply = await send_shell_message(client, "kernel_info_request")
    assert reply["header"]["msg_type"] == "kernel_info_reply"


async def test_complete_request(client, kernel, tracemalloc_resource_warning):
    reply = await send_shell_message(client, "complete_request", {"code": "hello", "cursor_pos": 0})
    assert reply["header"]["msg_type"] == "complete_reply"


async def test_inspect_request(
    client,
):
    reply = await send_shell_message(client, "inspect_request", {"code": "hello", "cursor_pos": 0})
    assert reply["header"]["msg_type"] == "inspect_reply"


async def test_history_request(client, kernel):
    assert kernel.shell
    assert kernel.shell.history_manager
    kernel.shell.history_manager.db = DummyDB()
    reply = await send_shell_message(client, "history_request", {"hist_access_type": "", "output": "", "raw": ""})
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await send_shell_message(client, "history_request", {"hist_access_type": "tail", "output": "", "raw": ""})
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await send_shell_message(client, "history_request", {"hist_access_type": "range", "output": "", "raw": ""})
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await send_shell_message(client, "history_request", {"hist_access_type": "search", "output": "", "raw": ""})
    assert reply["header"]["msg_type"] == "history_reply"


async def test_comm_info_request(
    client,
):
    reply = await send_shell_message(client, "comm_info_request")
    assert reply["header"]["msg_type"] == "comm_info_reply"


async def test_direct_interrupt_request(client, kernel):
    reply = await send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}

    # test failure on interrupt request
    def raiseOSError():
        msg = "evalue"
        raise OSError(msg)

    kernel._send_interrupt_children = raiseOSError
    reply = await send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "OSError"
    assert reply["content"]["evalue"] == "evalue"
    assert len(reply["content"]["traceback"]) > 0


async def test_is_complete_request(client, kernel):
    reply = await send_shell_message(client, "is_complete_request", {"code": "hello"})
    assert reply["header"]["msg_type"] == "is_complete_reply"
    setattr(kernel, "shell.input_transformer_manager", None)
    reply = await send_shell_message(client, "is_complete_request", {"code": "hello"})
    assert reply["header"]["msg_type"] == "is_complete_reply"


async def test_direct_clear(kernel):
    kernel.do_clear()


@pytest.mark.parametrize("mode", ["main", "external"])
@pytest.mark.parametrize("exception", [True, False])
async def test_start_soon(mode, exception: bool, client, kernel):
    # Test we can start coroutines from various scopes

    import anyio
    from anyio import to_thread

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
