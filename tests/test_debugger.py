# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from tests import utils

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient

import async_kernel.utils

if async_kernel.utils.LAUNCHED_BY_DEBUGPY:
    import debugpy.server.api

    if debugpy.server.api._config["subProcess"]:  # pyright: ignore[reportPrivateUsage]
        msg = 'Sub-process debugging is enabled! First set `"subProcess"=false` in .vscode.launch.json and try again.'
        raise RuntimeError(msg)


initialize_args = {
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
    # "subProcess": False,
}


async def send_debug_request(client: AsyncKernelClient, command: str, arguments: dict | None = None):
    """Carry out a debug request and return the reply content.

    It does not check if the request was successful.
    """

    send_debug_request._seq = seq = getattr(send_debug_request, "_seq", 0) + 1  # pyright: ignore[reportFunctionMemberAccess]
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


async def test_debugger(subprocess_kernels_client):
    client = subprocess_kernels_client
    reply = await send_debug_request(client=client, command="initialize", arguments=initialize_args)
    assert reply["status"] == "ok"
    await send_debug_request(client, "disconnect")
    await send_debug_request(client=client, command="initialize", arguments=initialize_args)
    reply = await send_debug_request(client, "attach")
    assert reply["status"] == "ok"
    assert reply["success"]

    reply = await send_debug_request(client, "configurationDone")
    assert reply["status"] == "ok"

    # Debugger needs to be stopped on a breakpoint
    # The steps below expect the 'debugger' to be in a various state (stopped or running)
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
            "breakpoints": [{"line": 2}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    # debugInfo
    reply = await send_debug_request(client, "debugInfo")
    assert source in reply["body"]["breakpoints"][0]["source"]
    assert reply["body"]["breakpoints"][0]["breakpoints"] == [{"line": 2}]

    # Executing code will run till a breakpoint is reached
    client.execute(code)

    while True:
        reply = await send_debug_request(client, "debugInfo")
        if reply["body"]["stoppedThreads"]:
            break

    # next
    reply = await send_debug_request(client, "next", {"threadId": 1})
    await anyio.sleep(0.5)

    # stackTrace
    reply = await send_debug_request(client, "stackTrace", {"threadId": 1})
    stacks = reply["body"]["stackFrames"]
    assert stacks

    # source
    reply = await send_debug_request(client, "source", {"source": stacks[0]["source"]})
    assert reply["success"]
    assert reply["body"]["content"] == code

    # scopes
    reply = await send_debug_request(client, "scopes", {"frameId": stacks[0]["id"]})
    assert reply["success"]
    variables_reference = reply["body"]["scopes"][0]["variablesReference"]

    frameId = stacks[0]["id"]

    # variables
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": variables_reference},
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
            "frameId": frameId,
        },
    )
    assert reply["success"]

    # copyToGlobals
    reply = await send_debug_request(
        client=client,
        command="copyToGlobals",
        arguments={
            "dstVariableName": "my_copy",
            "srcVariableName": "my_variable",
            "srcFrameId": frameId,
        },
    )
    assert reply["success"]

    # richInspectVariables
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable", "frameId": frameId},
    )
    assert reply["success"]
    assert set(reply["body"]) == {"metadata", "data"}

    # inspectVariables
    reply = await send_debug_request(client=client, command="inspectVariables", arguments={"frameId": frameId})
    assert reply["success"]
    # continue
    reply = await send_debug_request(client, "continue", {"threadId": 1})

    # debugInfo
    while True:
        reply = await send_debug_request(client, "debugInfo")
        if not reply["body"]["stoppedThreads"]:
            break

    # richInspectVariables
    reply = await send_debug_request(
        client=client,
        command="richInspectVariables",
        arguments={"variableName": "my_variable"},
    )
    assert reply["success"]
    assert reply["body"] == {"data": {"text/plain": "'has a value'"}, "metadata": {}}

    # variables
    reply = await send_debug_request(
        client=client,
        command="variables",
        arguments={"variablesReference": variables_reference},
    )
    assert reply["success"]
