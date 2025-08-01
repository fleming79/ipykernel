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

    # setBreakpoints
    reply = await send_debug_request(client, "dumpCell", {"code": code})
    source = reply["body"]["sourcePath"]
    reply = await send_debug_request(
        client=client,
        command="setBreakpoints",
        arguments={
            "breakpoints": [{"line": 2}, {"line": 8}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    assert reply["success"]
    assert len(reply["body"]["breakpoints"]) == 2
    assert reply["body"]["breakpoints"][0]["verified"]
    assert reply["body"]["breakpoints"][0]["source"]["path"] == source

    # debugInfo
    reply = await send_debug_request(client, "debugInfo")
    assert source in reply["body"]["breakpoints"][0]["source"]
    assert reply["body"]["breakpoints"][0]["breakpoints"] == [{"line": 2}, {"line": 8}]
    return code


async def test_stop_on_breakpoint(client):
    # Debugger needs to be stopped on a breakpoint

    code = await test_set_breakpoints(client)
    # Executing code will run till a breakpoint is reached
    msg_id = client.execute(code)

    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_id") != msg_id and msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg(timeout=5)
    assert msg["content"]["body"]["reason"] == "breakpoint"
    assert msg["content"]["body"]["allThreadsStopped"]
    thread_id = msg["content"]["body"]["threadId"]

    reply = await send_debug_request(client, "debugInfo")
    assert reply["body"]["stoppedThreads"] == [1]

    # next
    await utils.clear_iopub(client)
    reply = await send_debug_request(client, "next", {"threadId": thread_id})
    msg = await client.get_iopub_msg()
    msg = await client.get_iopub_msg()
    assert msg["content"]["event"] == "stopped"
    assert msg["content"]["body"]["allThreadsStopped"]

    # stackTrace
    reply = await send_debug_request(client, "stackTrace", {"threadId": thread_id})
    stacks = reply["body"]["stackFrames"]
    assert stacks

    # source
    reply = await send_debug_request(client, "source", {"source": stacks[0]["source"]})
    assert reply["success"]
    assert reply["body"]["content"] == code

    # scopes
    reply = await send_debug_request(client, "scopes", {"frameId": stacks[0]["id"]})
    assert reply["success"]

    # variables
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": reply["body"]["scopes"][0]["variablesReference"]},
    )
    assert reply["success"]
    assert reply["body"]["variables"]

    # evaluate
    reply = await send_debug_request(
        client=client,
        command="evaluate",
        arguments={
            "expression": "a=10;b=20",
            "context": "repl",
            "frameId": stacks[0]["id"],
        },
    )
    assert reply["success"]

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

    # inspectVariables
    reply = await send_debug_request(client=client, command="inspectVariables", arguments={"frameId": stacks[0]["id"]})
    assert reply["success"]

    # continue
    await utils.clear_iopub(client)
    reply = await send_debug_request(client, "continue", {"threadId": thread_id})
    assert reply["success"]
    assert reply["body"] == {"allThreadsContinued": True}
    msg = await client.get_iopub_msg()
    assert msg["content"]["event"] == "stopped"
    assert msg["content"]["body"]["allThreadsStopped"]

    reply = await send_debug_request(client, "continue", {"threadId": thread_id})
    msg = await client.get_iopub_msg()
    assert msg["content"]["event"] == "continued"
    assert reply["body"]["allThreadsContinued"]

    # debugInfo
    reply = await send_debug_request(client, "debugInfo")
    assert reply["body"]["stoppedThreads"] == []
    # richInspectVariables (again whilst continued)
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable"},
    )
    assert reply["success"]
    assert reply["body"] == {"data": {"text/plain": "'has a value'"}, "metadata": {}}
