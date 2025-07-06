"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import inspect
import threading
import time
from typing import Literal

import anyio
import pytest
import zmq

from async_kernel.kernel import Kernel, SocketID
from async_kernel.kernelspec import KernelName
from tests import utils


@pytest.mark.parametrize("mode", ["direct", "proxy"])
async def test_iopub(kernel, mode: Literal["direct", "proxy"]):
    n = 10

    info = kernel.get_connection_info()
    url = f"{info['transport']}://{info['ip']}:{info['iopub_port']}"
    socket = kernel._sockets[SocketID.iopub]
    assert url == socket.get_string(zmq.LAST_ENDPOINT)

    def pubio_subscribe():
        """Consume messages"""
        ctx = zmq.Context()
        s = ctx.socket(zmq.SUB)
        s.connect(url)
        s.setsockopt(zmq.SUBSCRIBE, b"")
        try:
            i = 0
            while i < n:
                msg = s.recv_multipart()
                if msg[0] == b"0":
                    assert int(msg[1]) == i
                    i += 1
        finally:
            s.close()
            ctx.term()

    thread = threading.Thread(target=pubio_subscribe)
    thread.start()
    time.sleep(0.05)
    if mode == "proxy":
        socket = kernel._iopub_sockets.get(threading.current_thread())
    for i in range(n):
        socket.send_multipart([b"0", f"{i}".encode()])
    thread.join()


async def test_simple_print(client):
    """simple print statement in kernel"""
    await utils.clear_pub_message(client)
    client.execute("print('hi')")
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == "hi\n"
    assert stderr == ""


async def test_raw_input(client):
    """test input"""

    pytest.skip("Blocks forever")

    input_f = "input"
    theprompt = "prompt> "
    code = f'print({input_f}("{theprompt}"))'
    client.execute(code, allow_stdin=True)
    await anyio.sleep(0.1)
    msg = await client.get_stdin_msg()
    assert msg["header"]["msg_type"] == "input_request"
    content = msg["content"]
    assert content["prompt"] == theprompt
    text = "some text"
    client.input(text)
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "ok"
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == text + "\n"


async def test_save_history(client, tmp_path):
    file = tmp_path.joinpath("hist.out")
    client.execute("a=1")
    await utils.wait_for_idle(client)
    client.execute('b="abcþ"')
    await utils.wait_for_idle(client)
    _, reply = await utils.execute(client, f"%hist -f {file}")
    assert reply["status"] == "ok"
    with open(file, encoding="utf-8") as f:
        content = f.read()
    assert "a=1" in content
    assert 'b="abcþ"' in content


async def test_is_complete(client):
    # There are more test cases for this in core - here we just check
    # that the kernel exposes the interface correctly.
    client.is_complete("2+2")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "complete"

    # SyntaxError
    client.is_complete("raise = 2")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "invalid"

    client.is_complete("a = [1,\n2,")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "incomplete"
    assert reply["content"]["indent"] == ""

    # Cell magic ends on two blank lines for console UIs
    client.is_complete("%%timeit\na\n\n")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "complete"


async def test_message_order(client):
    N = 100  # number of messages to test

    _, reply = await utils.execute(client, "a = 1")
    offset = reply["execution_count"] + 1
    cell = "a += 1\na"
    msg_ids = []
    # submit N executions as fast as we can
    for _ in range(N):
        msg_ids.append(client.execute(cell))
    # check message-handling order
    for i, msg_id in enumerate(msg_ids, offset):
        reply = await client.get_shell_msg()
        assert reply["content"]["execution_count"] == i
        assert reply["parent_header"]["msg_id"] == msg_id


async def test_execute_request(client):
    reply = await utils.send_shell_message(client, "execute_request", {"code": "hello", "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"


async def test_execute_request_stop_on_error(client, kernel):
    kernel._stop_on_error_time = time.monotonic() + 10
    reply = await utils.send_shell_message(client, "execute_request", {"code": "hello", "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"
    kernel._stop_on_error_time = 0


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


async def test_interrupt_request(client, kernel, mocker):
    await utils.clear_pub_message(client)
    event = threading.Event()
    kernel._interrupt_events.add(event)
    reply = await utils.send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}
    assert event.is_set()


async def test_user_exit(client, kernel, mocker):
    stop = mocker.patch.object(kernel, "stop")
    raw_input = mocker.patch.object(kernel, "raw_input", return_value="y")
    await utils.execute(client, "quit()")
    assert raw_input.call_count == 1
    assert stop.call_count == 1
    kernel.exit_now = False


async def test_shutdown_request(client, kernel, mocker):
    # Apply patches
    shutdown_request = mocker.patch.object(kernel, "do_shutdown", return_value={"restart": False, "status": "ok"})
    await utils.send_control_message(client, "shutdown_request", {"restart": False})
    assert shutdown_request.call_count == 1


async def test_is_complete_request(client):
    reply = await utils.send_shell_message(client, "is_complete_request", {"code": "hello"})
    assert reply["header"]["msg_type"] == "is_complete_reply"


async def test_properties(kernel) -> None:
    class user_mod:
        __dict__ = {}

    kernel.user_module = user_mod()
    kernel.user_ns = {}


async def test_clear(kernel):
    kernel.do_clear()


@pytest.mark.parametrize("kernel_name", list(KernelName))
def test_kernel_start(kernel_name: KernelName):
    anyio_run = anyio.run
    anyio_sleep_forever = anyio.sleep_forever

    def _patch_run(coro, backend):
        nonlocal _start
        _start = coro
        if kernel_name.startswith("async"):
            assert backend == "asyncio"
        else:
            assert backend == "trio"

    anyio.run = _patch_run
    try:
        _start = None
        Kernel.start(connection_file="test.json", kernel_name=kernel_name)
        assert inspect.iscoroutinefunction(_start)
    finally:
        anyio.run = anyio_run
        anyio.sleep_forever = anyio_sleep_forever
