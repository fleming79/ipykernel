# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests import utils

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient

import async_kernel.utils

if async_kernel.utils.LAUNCHED_BY_DEBUGPY:
    import debugpy.server.api

    if debugpy.server.api._config["subProcess"]:
        msg = 'Sub-process debugging is enabled! First set `"subProcess"=false` in .vscode.launch.json and try again.'
        raise RuntimeError(msg)


@pytest.fixture(scope="module")
async def client(subprocess_kernels_client):
    """This client is connected to a kernel in a subprocess.

    Notes:
    - Trying debug the subprocess will fail because only one debug client is allowed and ipykernel is running its own client.
    - To Debug this module in vscode: set `"subProcess"=false` in '.vscode.launch.json'.
    """
    client = subprocess_kernels_client
    await send_debug_request(
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
    reply = await send_debug_request(client, "attach")
    assert reply["status"] == "ok"
    return client


async def send_debug_request(client: AsyncKernelClient, command: str, arguments: dict | None = None):
    """Carry out a debug request and return the reply content.

    It does not check if the request was successful.
    """

    send_debug_request._seq = seq = getattr(send_debug_request, "_seq", 0) + 1  # type: ignore[assignment]
    # DAP Ref: https://microsoft.github.io/debug-adapter-protocol/specification
    reply = await utils.send_control_message(
        client,
        "debug_request",
        {
            "type": "request",
            "seq": seq,
            "command": command,
            "arguments": arguments or {},
        },
    )
    return reply["content"]


async def test_debug_disconnect_initialize(client):
    reply = await send_debug_request(client, "disconnect")
    assert reply["success"]
    await send_debug_request(
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
    await send_debug_request(client, "attach")
    await send_debug_request(client, "configurationDone")  # An invalid call to absorb.


async def test_set_breakpoints(client):
    code = """
my_variable = 'has a value'

def f(a, b):
    c = a + b
    return c

f(2, 3)"""

    reply = await send_debug_request(client, "dumpCell", {"code": code})
    source = reply["body"]["sourcePath"]
    line = 4
    reply = await send_debug_request(
        client=client,
        command="setBreakpoints",
        arguments={
            "breakpoints": [{"line": line}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    assert reply["success"]
    assert len(reply["body"]["breakpoints"]) == 1
    assert reply["body"]["breakpoints"][0]["verified"]
    assert reply["body"]["breakpoints"][0]["source"]["path"] == source
    reply = await send_debug_request(client, "debugInfo")
    assert source in reply["body"]["breakpoints"][0]["source"]
    assert reply["body"]["breakpoints"][0]["breakpoints"][0]["line"] == line
    return code


async def test_stop_on_breakpoint(client):
    code = await test_set_breakpoints(client)
    msg_id = client.execute(code)
    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_id") != msg_id and msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg(timeout=5)
    assert msg["content"]["body"]["reason"] == "breakpoint"
    assert msg["content"]["body"]["allThreadsStopped"]
    thread_id = msg["content"]["body"]["threadId"]
    # stackTrace
    reply = await send_debug_request(client, "stackTrace", {"threadId": thread_id})
    stacks = reply["body"]["stackFrames"]
    # scopes
    reply = await send_debug_request(client, "scopes", {"frameId": stacks[0]["id"]})
    scopes = reply["body"]["scopes"]
    # evaluate
    reply = await send_debug_request(
        client=client,
        command="evaluate",
        arguments={"expression": "print(my_variable)", "context": "repl", "frameId": stacks[0]["id"]},
    )
    assert reply["success"]
    # variables
    v_ref = next(filter(lambda s: s["name"] == "Locals", scopes))["variablesReference"]
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": v_ref},
    )
    # copyToGlobals
    reply = await send_debug_request(
        client=client,
        command="copyToGlobals",
        arguments={"dstVariableName": "my_copy", "srcVariableName": "my_variable", "srcFrameId": stacks[0]["id"]},
    )
    assert reply["success"]
    # richInspectVariables
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable", "frameId": stacks[0]["id"]},
    )
    assert reply["success"]
    assert set(reply["body"]) == {"metadata", "data"}

    await send_debug_request(client, "continue", {"threadId": thread_id})

    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "ok"
    # variables
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable", "frameId": stacks[0]["id"]},
    )
    assert reply["success"]
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": v_ref},
    )
    assert reply["success"]
