# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import anyio
import pytest

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


@pytest.fixture(scope="module")
async def client(subprocess_kernels_client):
    """This client is connected to a kernel in a subprocess.

    Notes:
    - Trying debug the subprocess will fail because only one debug client is allowed and ipykernel is running its own client.
    - To Debug this module in vscode: set `"subProcess"=false` in '.vscode.launch.json'.
    """
    client = subprocess_kernels_client
    reply = await send_debug_request(client=client, command="initialize", arguments=initialize_args)
    assert reply["status"] == "ok"
    return client


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


async def get_iopub_event(client: AsyncKernelClient, event: str):
    with anyio.fail_after(1):
        while (msg := await client.get_iopub_msg()) and msg["content"].get("event") != event:
            pass
        return msg


class TestDebugger:
    @contextlib.asynccontextmanager
    async def connected(self, client):
        reply = await send_debug_request(client, "debugInfo")
        if reply["body"]["isStarted"]:
            reply = await send_debug_request(client, "disconnect")
        reply = await send_debug_request(client=client, command="initialize", arguments=initialize_args)
        reply = await send_debug_request(client, "attach")
        assert reply["status"] == "ok"
        assert reply["success"]
        try:
            yield client
        finally:
            reply = await send_debug_request(client, "disconnect")

    async def test_debug_disconnect_initialize(self, client):
        async with self.connected(client):
            # The next line is an 'invalid call' (replicates a bug in jupyterlab see: https://github.com/jupyterlab/jupyterlab/issues/17673).
            await send_debug_request(client, "configurationDone")

    async def stop_on_breakpoint(self, client):
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
                "breakpoints": [{"line": 2}, {"line": 8}],
                "source": {"path": source},
                "sourceModified": False,
            },
        )
        # debugInfo
        reply = await send_debug_request(client, "debugInfo")
        assert source in reply["body"]["breakpoints"][0]["source"]
        assert reply["body"]["breakpoints"][0]["breakpoints"] == [{"line": 2}, {"line": 8}]

        # Executing code will run till a breakpoint is reached
        client.execute(code)

        # Wait for stop on breakpoint
        msg = await get_iopub_event(client, "stopped")
        assert msg["content"]["body"]["reason"] == "breakpoint"
        thread_id = msg["content"]["body"]["threadId"]

        reply = await send_debug_request(client, "debugInfo")
        assert reply["body"]["stoppedThreads"] == [1]

        # next
        reply = await send_debug_request(client, "next", {"threadId": thread_id})
        await get_iopub_event(client, "continued")
        await get_iopub_event(client, "stopped")

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
        variables_reference = reply["body"]["scopes"][0]["variablesReference"]
        return {"variables_reference": variables_reference, "frameId": stacks[0]["id"], "thread_id": thread_id}

    async def test_while_stopped(self, client):
        async with self.connected(client):
            info = await self.stop_on_breakpoint(client)
            # variables
            reply = await send_debug_request(
                client=client,
                command="variables",
                arguments={"variablesReference": info["variables_reference"]},
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
                    "frameId": info["frameId"],
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
                    "srcFrameId": info["frameId"],
                },
            )
            assert reply["success"]

            # richInspectVariables
            reply = await send_debug_request(
                client=client,
                command="richInspectVariables",
                arguments={"variableName": "my_variable", "frameId": info["frameId"]},
            )
            assert reply["success"]
            assert set(reply["body"]) == {"metadata", "data"}

            # inspectVariables
            reply = await send_debug_request(
                client=client, command="inspectVariables", arguments={"frameId": info["frameId"]}
            )
            assert reply["success"]

    async def test_request_while_running(self, client):
        async with self.connected(client):
            info = await self.stop_on_breakpoint(client)
            # continue
            reply = await send_debug_request(client, "continue", {"threadId": info["thread_id"]})
            assert reply["success"]
            assert reply["body"] == {"allThreadsContinued": True}
            await get_iopub_event(client, "continued")
            await get_iopub_event(client, "stopped")

            reply = await send_debug_request(client, "continue", {"threadId": info["thread_id"]})
            await get_iopub_event(client, "continued")

            # debugInfo
            reply = await send_debug_request(client, "debugInfo")
            assert reply["body"]["stoppedThreads"] == []

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
                arguments={"variablesReference": info["variables_reference"]},
            )
            assert reply["success"]
