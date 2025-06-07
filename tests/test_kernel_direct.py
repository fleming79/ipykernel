"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import time

from jupyter_client.session import DELIM

from tests import utils

# if os.name == "nt":
#     pytest.skip("skipping tests on windows", allow_module_level=True)


async def test_direct_kernel_info_request(client, kernel):
    reply = await utils.send_shell_message(client, "kernel_info_request")
    assert reply["header"]["msg_type"] == "kernel_info_reply"
    assert (
        "supported_features" not in reply["content"] or "kernel subshells" not in reply["content"]["supported_features"]
    )


async def test_direct_execute_request(client, kernel):
    reply = await utils.send_shell_message(client, "execute_request", dict(code="hello", silent=False))
    assert reply["header"]["msg_type"] == "execute_reply"


async def test_direct_execute_request_aborting(client, kernel):
    kernel._aborted_time = time.monotonic() + 10
    reply = await utils.send_shell_message(client, "execute_request", dict(code="hello", silent=False))
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "aborted"


async def test_direct_execute_request_error(kernel):
    await kernel.execute_request(None, None, None)


async def test_complete_request(client, kernel):
    reply = await utils.send_shell_message(client, "complete_request", dict(code="hello", cursor_pos=0))
    assert reply["header"]["msg_type"] == "complete_reply"


async def test_inspect_request(client, kernel):
    reply = await utils.send_shell_message(client, "inspect_request", dict(code="hello", cursor_pos=0))
    assert reply["header"]["msg_type"] == "inspect_reply"


async def test_history_request(client, kernel):
    reply = await utils.send_shell_message(client, "history_request", dict(hist_access_type="", output="", raw=""))
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await utils.send_shell_message(client, "history_request", dict(hist_access_type="tail", output="", raw=""))
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await utils.send_shell_message(client, "history_request", dict(hist_access_type="range", output="", raw=""))
    assert reply["header"]["msg_type"] == "history_reply"
    reply = await utils.send_shell_message(
        client, "history_request", dict(hist_access_type="search", output="", raw="")
    )
    assert reply["header"]["msg_type"] == "history_reply"


async def test_comm_info_request(client, kernel):
    reply = await utils.send_shell_message(client, "comm_info_request")
    assert reply["header"]["msg_type"] == "comm_info_reply"


async def test_direct_interrupt_request(client, kernel):
    reply = await utils.send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}

    # test failure on interrupt request
    def raiseOSError():
        msg = "evalue"
        raise OSError(msg)

    kernel._send_interrupt_children = raiseOSError
    reply = await utils.send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "OSError"
    assert reply["content"]["evalue"] == "evalue"
    assert len(reply["content"]["traceback"]) > 0


async def test_direct_shutdown_request(client, kernel):
    reply = await utils.send_shell_message(client, "shutdown_request", dict(restart=False))
    assert reply["header"]["msg_type"] == "shutdown_reply"
    reply = await utils.send_shell_message(client, "shutdown_request", dict(restart=True))
    assert reply["header"]["msg_type"] == "shutdown_reply"


async def test_is_complete_request(client, kernel):
    reply = await utils.send_shell_message(client, "is_complete_request", dict(code="hello"))
    assert reply["header"]["msg_type"] == "is_complete_reply"


async def test_process_control(kernel):
    await kernel.process_control_message([DELIM, 1])
    msg = utils._prep_msg(kernel, msg_type="does_not_exist")
    await kernel.process_control_message(msg)


async def test_dispatch_shell(client, kernel):
    from jupyter_client.session import DELIM

    await kernel.process_shell_message([DELIM, 1])
    msg = kernel._prep_msg("does_not_exist")
    await kernel.process_shell_message(msg)


async def test_publish_debug_event(kernel):
    kernel._publish_debug_event({})


async def test_connect_request(kernel):
    await kernel.connect_request(kernel.shell_socket, b"foo", None)


async def test_send_interrupt_children(kernel):
    kernel._send_interrupt_children()
