"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import time

import pytest

from tests import utils


async def test_direct_kernel_info_request(client):
    reply = await utils.send_shell_message(client, "kernel_info_request")
    assert reply["header"]["msg_type"] == "kernel_info_reply"
    supported_features = reply["content"]["supported_features"]
    assert supported_features == ["kernel subshells", "debugger"]


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


@pytest.mark.parametrize("hist_access_type", ["", "tail", "range", "search"])
async def test_history_request(client, hist_access_type):
    content = {"hist_access_type": hist_access_type, "output": "", "raw": ""}
    reply = await utils.send_shell_message(client, "history_request", content)
    assert reply["header"]["msg_type"] == "history_reply"


async def test_comm_info_request(client):
    reply = await utils.send_shell_message(client, "comm_info_request")
    assert reply["header"]["msg_type"] == "comm_info_reply"


async def test_direct_interrupt_request(client, kernel, mocker):
    await utils.clear_pub_message(client)
    mocker.patch.object(kernel, "interrupt_request")
    reply = await utils.send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}
    # test failure on interrupt request
    def raiseOSError():
        msg = "evalue"
        raise OSError(msg)

    _obj = kernel._send_interrupt_children
    try:
        kernel._send_interrupt_children = raiseOSError
        reply = await utils.send_control_message(client, "interrupt_request")
        assert reply["header"]["msg_type"] == "interrupt_reply"
        assert reply["content"]["status"] == "error"
        assert reply["content"]["ename"] == "OSError"
        assert reply["content"]["evalue"] == "evalue"
        assert len(reply["content"]["traceback"]) > 0
    finally:
        kernel._send_interrupt_children = _obj


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

