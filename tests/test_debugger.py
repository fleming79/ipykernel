from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from tests.utils import TIMEOUT, execute, get_reply

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient

    from async_kernel import Kernel


# Tests support debugpy not being installed, in which case the tests don't do anything useful
# functionally as the debug message replies are usually empty dictionaries, but they confirm that
# ipykernel doesn't block, or segfault, or raise an exception.
try:
    import debugpy
except ImportError:
    debugpy = None


if True:
    pytest.skip("skipping tests until debug is implemented", allow_module_level=True)


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
    reply = await get_reply(client, msg["header"]["msg_id"], channel="control")
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
            arguments={"restart": False, "terminateDebuggee": True},
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


async def test_supported_features(debug_kernel, client):
    msg_id = client.kernel_info()
    reply = await get_reply(client, msg_id)
    supported_features = reply["content"]["supported_features"]

    if debugpy:
        assert "debugger" in supported_features
    else:
        assert "debugger" not in supported_features


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

    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="configurationDone",
    )
    if debugpy:
        assert r["success"]
    else:
        assert r == {}


async def test_stop_on_breakpoint(debug_kernel, client):
    code = """def f(a, b):
    c = a + b
    return c

f(2, 3)"""

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="dumpCell",
        arguments={
            "code": code,
        },
    )
    if debugpy:
        source = reply["body"]["sourcePath"]
    else:
        assert reply == {}
        source = "some path"

    await wait_for_debug_request(debug_kernel, client, "debugInfo")
    await wait_for_debug_request(
        debug_kernel,
        client,
        "setBreakpoints",
        {
            "breakpoints": [{"line": 2}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="configurationDone",
        full_reply=True,
    )
    client.execute(code)

    if not debugpy:
        # Cannot stop on breakpoint if debugpy not installed
        return
    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_type") != "debug_event" or msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg(timeout=5)
    assert msg["content"]["body"]["reason"] == "breakpoint"


async def test_breakpoint_in_cell_with_leading_empty_lines(debug_kernel, client):
    code = """
def f(a, b):
    c = a + b
    return c

f(2, 3)"""

    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="dumpCell",
        arguments={"code": code},
    )
    if debugpy:
        source = r["body"]["sourcePath"]
    else:
        assert r == {}
        source = "some path"
    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="debugInfo",
    )
    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="setBreakpoints",
        arguments={
            "breakpoints": [{"line": 6}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="configurationDone",
        full_reply=True,
    )

    await execute(client, code)

    if not debugpy:
        # Cannot stop on breakpoint if debugpy not installed
        return

    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_type") != "debug_event" or msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg(timeout=TIMEOUT)

    assert msg["content"]["body"]["reason"] == "breakpoint"


async def test_rich_inspect_not_at_breakpoint(debug_kernel, client):
    var_name = "text"
    value = "Hello the world"
    code = f"""{var_name}='{value}'
print({var_name})
"""

    msg_id = await execute(client(code))
    await get_reply(client, msg_id)

    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="inspectVariables",
    )

    def func(v):
        return v["name"]

    if debugpy:
        assert var_name in list(map(func, r["body"]["variables"]))
    else:
        assert r == {}

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="richInspectVariables",
        arguments={"variableName": var_name},
    )

    if debugpy:
        assert reply["body"]["data"] == {"text/plain": f"'{value}'"}
    else:
        assert reply == {}


async def test_rich_inspect_at_breakpoint(debug_kernel, client):
    code = """def f(a, b):
    c = a + b
    return c

f(2, 3)"""
    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="dumpCell",
        arguments={"code": code},
    )
    if debugpy:
        source = r["body"]["sourcePath"]
    else:
        assert r == {}
        source = "some path"
    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="setBreakpoints",
        arguments={
            "breakpoints": [{"line": 2}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )
    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="debugInfo",
    )
    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="configurationDone",
    )

    await execute(client, code)

    if not debugpy:
        # Cannot stop on breakpoint if debugpy not installed
        return
    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_type") != "debug_event" or msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg()

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="stackTrace",
        arguments={
            "threadId": 1,
        },
    )
    stacks = reply["body"]["stackFrames"]

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="scopes",
        arguments={
            "frameId": stacks[0]["id"],
        },
    )
    scopes = reply["body"]["scopes"]

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="variables",
        arguments={"variablesReference": next(filter(lambda s: s["name"] == "Locals", scopes))["variablesReference"]},
    )
    locals_ = reply["body"]["variables"]

    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="richInspectVariables",
        arguments={"variableName": locals_[0]["name"], "frameId": stacks[0]["id"]},
    )

    assert reply["body"]["data"] == {"text/plain": locals_[0]["value"]}


async def test_convert_to_long_pathname(debug_kernel, client):
    if sys.platform == "win32":
        from ipykernel.compiler import _convert_to_long_pathname

        _convert_to_long_pathname(__file__)


async def test_copy_to_globals(debug_kernel, client):
    local_var_name = "var"
    global_var_name = "var_copy"
    code = f"""from IPython.core.display import HTML
def my_test():
    {local_var_name} = HTML('<p>test content</p>')
    pass
a = 2
my_test()"""

    # Init debugger and set breakpoint
    r = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="dumpCell",
        arguments={"code": code},
    )
    if debugpy:
        source = r["body"]["sourcePath"]
    else:
        assert r == {}
        source = "some path"

    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="setBreakpoints",
        arguments={
            "breakpoints": [{"line": 4}],
            "source": {"path": source},
            "sourceModified": False,
        },
    )

    await wait_for_debug_request(kernel=debug_kernel, client=client, command="debugInfo")

    await wait_for_debug_request(kernel=debug_kernel, client=client, command="configurationDone")

    # Execute code
    client.execute(code)

    if not debugpy:
        # Cannot stop on breakpoint if debugpy not installed
        return

    # Wait for stop on breakpoint
    msg: dict = {"msg_type": "", "content": {}}
    while msg.get("msg_type") != "debug_event" or msg["content"].get("event") != "stopped":
        msg = await client.get_iopub_msg(timeout=TIMEOUT)

    reply = await wait_for_debug_request(debug_kernel, client, "stackTrace", {"threadId": 1})
    stacks = reply["body"]["stackFrames"]

    # Get local frame id
    frame_id = stacks[0]["id"]

    # Copy the variable
    await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="copyToGlobals",
        arguments={
            "srcVariableName": local_var_name,
            "dstVariableName": global_var_name,
            "srcFrameId": frame_id,
        },
    )

    # Get the scopes
    reply = wait_for_debug_request(debug_kernel, client, "scopes", {"frameId": frame_id})
    scopes = reply["body"]["scopes"]

    # Get the local variable
    reply = wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="variables",
        arguments={"variablesReference": next(filter(lambda s: s["name"] == "Locals", scopes))["variablesReference"]},
    )
    locals_ = reply["body"]["variables"]

    local_var = None
    for variable in locals_:
        if local_var_name in variable["evaluateName"]:
            local_var = variable
    assert local_var is not None

    # Get the global variable (copy of the local variable)
    reply = await wait_for_debug_request(
        kernel=debug_kernel,
        client=client,
        command="variables",
        arguments={"variablesReference": next(filter(lambda s: s["name"] == "Globals", scopes))["variablesReference"]},
    )
    globals_ = reply["body"]["variables"]

    global_var = None
    for variable in globals_:
        if global_var_name in variable["evaluateName"]:
            global_var = variable
    assert global_var is not None

    # Compare local and global variable
    assert global_var["value"] == local_var["value"] and global_var["type"] == local_var["type"]  # noqa: PT018
