"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Literal, cast

import anyio
import pytest
import zmq

import async_kernel.utils
from async_kernel.caller import Caller
from async_kernel.comm import Comm
from async_kernel.kernel import ExecuteMode, SocketID
from tests import utils


@pytest.fixture(scope="module", params=["tcp", "ipc"])
def transport(request):
    return request.param


@pytest.mark.parametrize("mode", ["direct", "proxy"])
async def test_iopub(kernel, mode: Literal["direct", "proxy"]):
    def pubio_subscribe():
        """Consume messages"""
        with ctx.socket(zmq.SocketType.SUB) as socket:
            socket.linger = 0
            socket.connect(url)
            socket.setsockopt(zmq.SocketOption.SUBSCRIBE, b"")
            i = 0
            while i < n:
                msg = socket.recv_multipart()
                if msg[0] == b"0":
                    assert int(msg[1]) == i
                    i += 1

    n = 10
    socket = kernel._sockets[SocketID.iopub]
    url = socket.get_string(zmq.SocketOption.LAST_ENDPOINT)
    assert url.endswith(str(kernel.iopub_port))
    ctx = zmq.Context()
    thread = threading.Thread(target=pubio_subscribe)
    thread.start()
    try:
        time.sleep(0.05)
        if mode == "proxy":
            socket = Caller.iopub_sockets[threading.current_thread()]
        for i in range(n):
            socket.send_multipart([b"0", f"{i}".encode()])
        thread.join()
    finally:
        ctx.term()


@pytest.mark.parametrize("quiet", [True, False])
async def test_simple_print(kernel, client, quiet: bool):
    """simple print statement in kernel"""
    kernel.quiet = quiet
    try:
        client.execute("print('test_simple_print')")
        stdout, stderr = await utils.assemble_output(client)
        assert stdout == "test_simple_print\n"
        assert stderr == ""
        await utils.clear_iopub(client)
    finally:
        kernel.quiet = True
        await utils.clear_iopub(client)


@pytest.mark.parametrize("test_mode", ["interrupt", "reply", "allow_stdin=False"])
@pytest.mark.parametrize("mode", ["input", "password"])
async def test_input(
    subprocess_kernels_client,
    mode: Literal["input", "password"],
    test_mode: Literal["interrupt", "reply", "allow_stdin=False"],
):
    client = subprocess_kernels_client
    client.input("Some input that should be discardes")
    theprompt = "Enter a value >"
    match mode:
        case "input":
            code = f"response = input('{theprompt}')"
        case "password":
            code = f"import getpass;response = getpass.getpass('{theprompt}')"
    # allow_stdin=False
    if test_mode == "allow_stdin=False":
        _, reply = await utils.execute(client, code, allow_stdin=False)
        assert reply["status"] == "error"
        assert reply["ename"] == "StdinNotImplementedError"
        return
    msg_id = client.execute(code, allow_stdin=True, user_expressions={"response": "response"})
    msg = await client.get_stdin_msg()
    assert msg["header"]["msg_type"] == "input_request"
    content = msg["content"]
    assert content["prompt"] == theprompt
    # interrupt
    if test_mode == "interrupt":
        await utils.send_control_message(client, "interrupt_request")
        reply = await utils.get_reply(client, msg_id, clear_pub=False)
        assert reply["content"]["status"] == "error"
        return
    # reply
    text = "some text"
    client.input(text)
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "ok"
    assert text in reply["content"]["user_expressions"]["response"]["data"]["text/plain"]


async def test_unraisablehook(kernel, mocker):
    handler = logging.Handler()
    kernel.log.logger.addHandler(handler)

    class Unraiseable:
        def __init__(self) -> None:
            self.exc_type = BaseException
            self.exc_value = BaseException()
            self.exc_traceback = None
            self.err_msg = "my error message"
            self.object = ""

    emit = mocker.patch.object(handler, "emit")
    kernel.unraisablehook(Unraiseable())
    assert emit.call_count == 1
    kernel.log.logger.removeHandler(handler)


async def test_save_history(client, tmp_path):
    file = tmp_path.joinpath("hist.out")
    client.execute("a=1")
    await utils.wait_for_idle(client)
    client.execute('b="abcþ"')
    await utils.wait_for_idle(client)
    _, reply = await utils.execute(client, f"%hist -f {file}")
    assert reply["status"] == "ok"
    with file.open("r", encoding="utf-8") as f:
        content = f.read()
    assert "a=1" in content
    assert 'b="abcþ"' in content
    await utils.clear_iopub(client)


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("2+2", "complete"),
        ("raise = 2", "invalid"),
        ("a = [1,\n2,", "incomplete"),
        ("%%timeit\na\n\n", "complete"),
    ],
)
async def test_is_complete(client, code: str, status: str):
    # There are more test cases for this in core - here we just check
    # that the kernel exposes the interface correctly.
    client.is_complete(code)
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == status
    await utils.clear_iopub(client)


async def test_message_order(client):
    N = 100  # number of messages to test

    _, reply = await utils.execute(client, "a = 1")
    offset = reply["execution_count"] + 1
    cell = "a += 1\na"

    # submit N executions as fast as we can
    msg_ids = [client.execute(cell) for _ in range(N)]
    # check message-handling order
    for i, msg_id in enumerate(msg_ids, offset):
        reply = await client.get_shell_msg()
        assert reply["content"]["execution_count"] == i
        assert reply["parent_header"]["msg_id"] == msg_id
    await utils.clear_iopub(client)


async def test_execute_request_success(client):
    reply = await utils.send_shell_message(client, "execute_request", {"code": "1 + 1", "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "ok"
    await utils.clear_iopub(client)


async def test_execute_request_error(client):
    reply = await utils.send_shell_message(client, "execute_request", {"code": "some invalid code", "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"
    await utils.clear_iopub(client)


async def test_execute_request_stop_on_error(client, kernel):
    kernel._stop_on_error_time = time.monotonic() + 10
    reply = await utils.send_shell_message(client, "execute_request", {"code": "some invalid code", "silent": False})
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


async def test_comm_open_msg_close(client, kernel, mocker):
    comm = None

    def cb(comm_, msg):
        nonlocal comm
        comm = comm_

    kernel.comm_manager.register_target("my target", cb)
    # open a comm
    with anyio.move_on_after(0.1):
        await utils.send_shell_message(
            client, "comm_open", {"content": {}, "comm_id": "comm id", "target_name": "my target"}
        )
    assert isinstance(comm, Comm)
    comm = cast("Comm", comm)
    reply = await utils.send_shell_message(client, "comm_info_request")
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"]["comms"].get("comm id") == {"target_name": "my target"}

    msg_received = mocker.patch.object(comm, "handle_msg")
    with anyio.move_on_after(0.1):
        await utils.send_shell_message(client, "comm_msg", {"comm_id": comm.comm_id})
    assert msg_received.call_count == 1
    # close comm
    closed = mocker.patch.object(comm, "handle_close")
    with anyio.move_on_after(0.1):
        await utils.send_shell_message(client, "comm_close", {"comm_id": comm.comm_id})
    assert closed.call_count == 1
    kernel.comm_manager.unregister_target("my target", cb)


async def test_interrupt_request(client, kernel):
    event = threading.Event()
    kernel._interrupt_events.add(event)
    reply = await utils.send_control_message(client, "interrupt_request")
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}
    assert event.is_set()


async def test_interrupt_request_async_request(subprocess_kernels_client):
    client = subprocess_kernels_client
    msg_id = client.execute("await anyio.sleep(100)")
    await anyio.sleep(0.1)
    reply = await utils.send_control_message(client, "interrupt_request")
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"


async def test_interrupt_request_blocking_exec_request(subprocess_kernels_client):
    client = subprocess_kernels_client
    msg_id = client.execute("import time;time.sleep(100)")
    await anyio.sleep(0.1)
    reply = await utils.send_control_message(client, "interrupt_request")
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "KernelInterruptError"


async def test_interrupt_request_blocking_task(subprocess_kernels_client):
    code = """
async def test():
    import time
    started.set()
    await anyio.sleep(0.01)
    try:
        time.sleep(100)
    except KernelInterruptError:
        print("KernelInterruptError")
    print("Failed")
import anyio
started = anyio.Event()
caller.call_soon(test)
await started.wait()
"""
    client = subprocess_kernels_client
    _, reply = await utils.execute(client, code)
    assert reply["status"] == "ok"
    await anyio.sleep(0.011)
    for _ in range(2):  # Blocking calls in tasks need to be interrupted twice
        await utils.send_control_message(client, "interrupt_request", clear_pub=False)
    stdout, _ = await utils.assemble_output(client, timeout=1)
    assert "KernelInterruptError" in stdout
    await utils.clear_iopub(client)


@pytest.mark.parametrize("response", ["y", ""])
async def test_user_exit(client, kernel, mocker, response: Literal["y", ""]):
    stop = mocker.patch.object(kernel, "stop")
    raw_input = mocker.patch.object(kernel, "raw_input", return_value=response)
    await utils.execute(client, "quit()")
    assert raw_input.call_count == 1
    assert stop.call_count == (1 if response == "y" else 0)
    kernel.exit_now = False


async def test_is_complete_request(client):
    reply = await utils.send_shell_message(client, "is_complete_request", {"code": "hello"})
    assert reply["header"]["msg_type"] == "is_complete_reply"


@pytest.mark.parametrize("command", ["debugInfo", "inspectVariables", "modules", "dumpCell", "source"])
async def test_debug_static(kernel, client, command: str, mocker):
    # These are tests on the debugger that don't required the debugger to be connected.
    code = "my_variable=123"
    if command == "debugInfo":
        mocker.patch.object(async_kernel.utils, "LAUNCHED_BY_DEBUGPY", new=True)
        assert async_kernel.utils.LAUNCHED_BY_DEBUGPY
    reply = await utils.send_control_message(
        client, "debug_request", {"type": "request", "seq": 1, "command": command, "arguments": {"code": code}}
    )
    assert reply["content"]["status"] == "ok"
    if command == "dumpCell":
        path = reply["content"]["body"]["sourcePath"]
        reply = await utils.send_control_message(
            client,
            "debug_request",
            {"type": "request", "seq": 1, "command": "source", "arguments": {"source": {"path": path}}},
        )
        assert reply["content"]["status"] == "ok"
        assert reply["content"]["body"] == {"content": code}


async def test_debug_not_connected(kernel, client):
    reply = await utils.send_control_message(
        client, "debug_request", {"type": "request", "seq": 1, "command": "disconnect", "arguments": {}}
    )
    assert reply["content"]["status"] == "ok"
    with pytest.raises(RuntimeError, match=".*not available until debugpy is listening"):
        kernel.debugger.debugpy_client.get_host_port()


@pytest.mark.parametrize("variable_name", ["my_variable", "invalid variable name", "special variables"])
async def test_debug_static_richInspectVariables(kernel, client, variable_name):
    # These are tests on the debugger that don't required the debugger to be connected.
    reply = await utils.send_control_message(
        client,
        "debug_request",
        {
            "type": "request",
            "seq": 1,
            "command": "richInspectVariables",
            "arguments": {"code": "my_variable=123", "variableName": variable_name},
        },
    )
    assert reply["content"]["status"] == "ok"


async def test_properties(kernel) -> None:
    class user_mod:
        __dict__ = {}

    kernel.user_module = user_mod()
    kernel.user_ns = {}


async def test_matplotlib_inline_on_import(kernel, client):
    pytest.importorskip("matplotlib", reason="this test requires matplotlib")
    code = "\n".join(["import matplotlib, matplotlib.pyplot as plt", "backend = matplotlib.get_backend()"])
    _, reply = await utils.execute(client, code, user_expressions={"backend": "backend"})
    backend_bundle = reply["user_expressions"]["backend"]
    assert "backend_inline" in backend_bundle["data"]["text/plain"]
    await utils.clear_iopub(client)


@pytest.mark.parametrize("code", ["%connect_info", "%matplotlib --list"])
async def test_magic(client, code: str):
    assert code
    _, reply = await utils.execute(client, code, clear_pub=False)
    assert reply["status"] == "ok"
    stdout, _ = await utils.assemble_output(client)
    assert stdout
    await utils.clear_iopub(client)


async def test_shell_required_properites(kernel):
    # used by ipython AutoMagicChecker via is_shadowed (requires 'builitin')
    assert set(kernel.shell.ns_table) == {"user_global", "user_local", "builtin"}
    # U
    kernel.shell.enable_gui()


async def test_shell_can_set_namespace(kernel):
    kernel.shell.user_ns = {}
    assert set(kernel.shell.user_ns).intersection(kernel.shell._user_ns_builtin)


@pytest.mark.parametrize("mode", ExecuteMode)
async def test_header_mode(client, mode: ExecuteMode):
    code = f"""
#@{mode.name}
import time
time.sleep(0.1)
print("{mode.name}")
"""
    _, reply = await utils.execute(client, code, clear_pub=False)
    assert reply["status"] == "ok"
    stdout, _ = await utils.assemble_output(client)
    assert mode.name in stdout
    await utils.clear_iopub(client)


@pytest.mark.parametrize(
    "code",
    [
        "caller.call_later(str, 0, 123)",
        "caller.call_soon(print, 'hello')",
    ],
)
async def test_namespace_default(client, code: str):
    assert code
    _, reply = await utils.execute(client, code)
    assert reply["status"] == "ok"
    await anyio.sleep(0.02)
    await utils.clear_iopub(client)


@pytest.mark.parametrize("channel", ["shell", "control"])
async def test_invalid_message(client, channel):
    f = utils.send_control_message if channel == "control" else utils.send_shell_message
    response = None
    with anyio.move_on_after(0.1):
        response = await f(client, "invalid-message-type")
    assert response is None
    await utils.clear_iopub(client)


@pytest.mark.parametrize("namespace_id", ["", "my namespace_id"])
@pytest.mark.parametrize("mode", ["", *ExecuteMode])
async def test_run_thread_ns(client, kernel, namespace_id, mode: ExecuteMode):
    symbol = str(uuid.uuid4())
    kernel.shell.namespace_id = namespace_id
    kernel.shell.user_ns["my_local_variable"] = symbol
    header = f'#@{mode} namespace_id="{namespace_id}'
    if mode is ExecuteMode.thread:
        header += ",thread_name=My thread 1324"

    code = f"""{header}"\n
def test():
    assert anyio
    import threading
    assert my_local_variable == "{symbol}"
    assert threading.current_thread() is {"not" if mode is ExecuteMode.thread else ""} threading.main_thread()
    return True
test()
    """
    _, reply = await utils.execute(client, code, user_expressions={"symbol": "my_local_variable"})
    assert reply["user_expressions"]["symbol"]["status"] == "ok"
    symbol_ = eval(reply["user_expressions"]["symbol"]["data"]["text/plain"])
    assert symbol_ == symbol
    if mode is ExecuteMode.thread:
        assert any(inst for inst in Caller._instances if inst.name == "My thread 1324")
