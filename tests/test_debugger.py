from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

import async_kernel.utils
from tests import utils

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient

    from async_kernel import Kernel


if async_kernel.utils.LAUNCHED_BY_DEBUGPY:
    msg = "This test module tests debugy. Debugging tests in this module WILL NOT WORK."
    raise RuntimeError(msg)


# Tests support debugpy not being installed, in which case the tests don't do anything useful
# functionally as the debug message replies are usually empty dictionaries, but they confirm that
# ipykernel doesn't block, or segfault, or raise an exception.
try:
    import debugpy
except ImportError:
    debugpy = None



async def wait_for_debug_request(
    kernel: Kernel, client: AsyncKernelClient, command, arguments: dict | None = None, full_reply=False
):
    """Carry out a debug request and return the reply content.

    It does not check if the request was successful.
    """

    msg = kernel.session.msg(
        "debug_request",
        {
            "type": "request",
            "seq": 1,
            "command": command,
            "arguments": arguments or {},
        },
    )
    assert client.control_channel
    client.control_channel.send(msg)
    reply = await utils.get_reply(client, msg["header"]["msg_id"], channel="control")
    return reply if full_reply else reply["content"]


@pytest.fixture
async def debug_kernel(kernel, client):
    # Initialize
    await wait_for_debug_request(
        kernel=kernel,
        client=client,
        command="initialize",
        arguments={
            "clientID": "test-client",
            "clientName": "testClient",
            "adapterID": "",
            "pathFormat": "path",
            "linesStartAt1": True,
            "columnsStartAt1": True,
            "supportsVariableType": True,
            "supportsVariablePaging": True,
            "supportsRunInTerminalRequest": True,
            "locale": "en",
        },
    )
    # Attach
    await wait_for_debug_request(kernel, client, "attach")
    try:
        yield kernel
    finally:
        # Detach
        await wait_for_debug_request(
            kernel=kernel,
            client=client,
            command="disconnect",
            arguments={"restart": False, "terminateDebuggee": False},
        )


async def test_debug_initialize(debug_kernel, client):
    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="initialize",
        arguments={
            "clientID": "test-client",
            "clientName": "testClient",
            "adapterID": "",
            "pathFormat": "path",
            "linesStartAt1": True,
            "columnsStartAt1": True,
            "supportsVariableType": True,
            "supportsVariablePaging": True,
            "supportsRunInTerminalRequest": True,
            "locale": "en",
        },
    )
    if debugpy:
        assert reply["success"]
    else:
        assert reply == {}


async def test_attach_debug(debug_kernel, client):
    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="evaluate",
        arguments={"expression": "'a' + 'b'", "context": "repl"},
    )
    if debugpy:
        assert reply["success"]
        assert reply["body"]["result"] == ""
    else:
        assert reply == {}


async def test_set_breakpoints(debug_kernel, client):
    code = """def f(a, b):
    c = a + b
    return c

f(2, 3)"""

    r = await wait_for_debug_request(debug_kernel, client, "dumpCell", {"code": code})
    if debugpy:
        source = r["body"]["sourcePath"]
    else:
        assert r == {}
        source = "non-existent path"

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="setBreakpoints",
        arguments={
            "breakpoints": [{"line": 2}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    if debugpy:
        assert reply["success"]
        assert len(reply["body"]["breakpoints"]) == 1
        assert reply["body"]["breakpoints"][0]["verified"]
        assert reply["body"]["breakpoints"][0]["source"]["path"] == source
    else:
        assert reply == {}

    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="debugInfo",
    )

    def func(b):
        return b["source"]

    if debugpy:
        assert source in map(func, r["body"]["breakpoints"])
    else:
        assert r == {}


async def test_convert_to_long_pathname(debug_kernel, client):
    if sys.platform == "win32":
        from async_kernel import compiler  # noqa: PLC0415

        compiler._convert_to_long_pathname(__file__)
