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
    # The next line is an 'invalid call' (replicates a bug in jupyterlab see: https://github.com/jupyterlab/jupyterlab/issues/17673).
    await send_debug_request(client, "configurationDone")


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
    # The steps below expect the 'debugger' to be in a various state (stopped or running)

    code = await test_set_breakpoints(client)
    # Executing code will run till a breakpoint is reached
    msg_id = client.execute(code)

    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_id") != msg_id and msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg(timeout=5)
    assert msg["content"]["body"]["reason"] == "breakpoint"
    thread_id = msg["content"]["body"]["threadId"]

    reply = await send_debug_request(client, "debugInfo")
    assert reply["body"]["stoppedThreads"] == [1]

    # next
    reply = await send_debug_request(client, "next", {"threadId": thread_id})
    while (msg := await client.get_iopub_msg()) and msg["content"]["event"] != "stopped":
        pass

    # stackTrace (stopped)
    reply = await send_debug_request(client, "stackTrace", {"threadId": thread_id})
    stacks = reply["body"]["stackFrames"]
    assert stacks

    # source (stopped)
    reply = await send_debug_request(client, "source", {"source": stacks[0]["source"]})
    assert reply["success"]
    assert reply["body"]["content"] == code

    # scopes (stopped)
    reply = await send_debug_request(client, "scopes", {"frameId": stacks[0]["id"]})
    assert reply["success"]
    variables_reference = reply["body"]["scopes"][0]["variablesReference"]

    # variables (stopped)
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": variables_reference},
    )
    assert reply["success"]
    assert reply["body"]["variables"]

    # evaluate (stopped)
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

    # copyToGlobals (stopped)
    reply = await send_debug_request(
        client=client,
        command="copyToGlobals",
        arguments={"dstVariableName": "my_copy", "srcVariableName": "my_variable", "srcFrameId": stacks[0]["id"]},
    )
    assert reply["success"]

    # richInspectVariables (stopped)
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable", "frameId": stacks[0]["id"]},
    )
    assert reply["success"]
    assert set(reply["body"]) == {"metadata", "data"}

    # inspectVariables (stopped)
    reply = await send_debug_request(client=client, command="inspectVariables", arguments={"frameId": stacks[0]["id"]})
    assert reply["success"]

    # continue
    await utils.clear_iopub(client)
    reply = await send_debug_request(client, "continue", {"threadId": thread_id})
    assert reply["success"]
    assert reply["body"] == {"allThreadsContinued": True}
    while (msg := await client.get_iopub_msg()) and msg["content"]["event"] != "stopped":
        pass

    reply = await send_debug_request(client, "continue", {"threadId": thread_id})
    while (msg := await client.get_iopub_msg()) and msg["content"]["event"] != "continued":
        pass

    # debugInfo (running)
    reply = await send_debug_request(client, "debugInfo")
    assert reply["body"]["stoppedThreads"] == []

    # richInspectVariables (running)
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable"},
    )
    assert reply["success"]
    assert reply["body"] == {"data": {"text/plain": "'has a value'"}, "metadata": {}}

    # variables (running)
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": variables_reference},
    )
    assert reply["success"]
