"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import threading
import time

import pytest

from tests import utils


async def test_direct_kernel_info_request(client):
    reply = await utils.send_shell_message(client, "kernel_info_request")
    assert reply["header"]["msg_type"] == "kernel_info_reply"
    supported_features = reply["content"]["supported_features"]
    assert supported_features == ["kernel subshells"]


async def test_direct_execute_request(client):
    reply = await utils.send_shell_message(client, "execute_request", {"code": "hello", "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"


async def test_direct_execute_request_aborting(client, kernel):
    kernel._stop_on_error_time = time.monotonic() + 10
    reply = await utils.send_shell_message(client, "execute_request", {"code": "hello", "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"
    kernel._stop_on_error_time = time.monotonic()


async def test_complete_request(client):
    reply = await utils.send_shell_message(client, "complete_request", {"code": "hello", "cursor_pos": 0})
    assert reply["header"]["msg_type"] == "complete_reply"


async def test_inspect_request(client):
    reply = await utils.send_shell_message(client, "inspect_request", {"code": "hello", "cursor_pos": 0})
    assert reply["header"]["msg_type"] == "inspect_reply"


async def test_history_request(client, kernel):
    assert kernel.shell
    # assert kernel.shell.history_manager
    # kernel.shell.history_manager.db = DummyDB()
    reply = await utils.send_shell_message(client, "history_request", {"hist_access_type": "", "output": "", "raw": ""})
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await utils.send_shell_message(
        client, "history_request", {"hist_access_type": "tail", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await utils.send_shell_message(
        client, "history_request", {"hist_access_type": "range", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await utils.send_shell_message(
        client, "history_request", {"hist_access_type": "search", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"


async def test_comm_info_request(client):
    reply = await utils.send_shell_message(client, "comm_info_request")
    assert reply["header"]["msg_type"] == "comm_info_reply"


async def test_direct_interrupt_request(client, kernel, mocker):
    await utils.clear_pub_message(client)
    event = threading.Event()
    kernel.shell_interrupt.add(event)
    reply = await utils.send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}
    assert event.is_set()


async def test_shutdown_request(client, kernel, mocker):
    # Apply patches
    shutdown_request = mocker.patch.object(kernel, "do_shutdown", return_value={"restart": False, "status": "ok"})
    await utils.send_control_message(client, "shutdown_request", {"restart": False})
    assert shutdown_request.call_count == 1


async def test_is_complete_request(client):
    reply = await utils.send_shell_message(client, "is_complete_request", {"code": "hello"})
    assert reply["header"]["msg_type"] == "is_complete_reply"


async def test_publish_debug_event(kernel):
    kernel._publish_debug_event({})


async def test_properties(kernel) -> None:
    class user_mod:
        __dict__ = {}

    kernel.user_module = user_mod()
    kernel.user_ns = {}


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

    for _ in range(2):
        event = anyio.Event()
        if mode == "main":
            kernel.start_soon(my_test, event)
        else:
            await to_thread.run_sync(kernel.start_soon, my_test, event)
        events.append(event)

    for event in events:
        await event.wait()
