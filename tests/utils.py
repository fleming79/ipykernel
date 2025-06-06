"""utilities for testing IPython kernels"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

import anyio
from jupyter_client.asynchronous.client import AsyncKernelClient

if TYPE_CHECKING:
    from ipykernel.ipkernel import IPythonKernel
    from ipykernel.kernelapp import IPKernelApp

    OwnerType = AsyncKernelClient | tuple[IPKernelApp, AsyncKernelClient]


STARTUP_TIMEOUT = 60 if "debugpy" not in sys.modules else 1e6
TIMEOUT = 100 if "debugpy" not in sys.modules else 1e6


class ContentType(TypedDict):
    code: NotRequired[str]
    silent: NotRequired[bool]
    store_history: NotRequired[bool]
    user_expressions: NotRequired[dict[str, str]]
    allow_stdin: NotRequired[bool]
    stop_on_error: NotRequired[bool]


def ka_kc_kernel(
    kc, kernel: tuple[IPKernelApp, AsyncKernelClient]
) -> tuple[IPKernelApp, AsyncKernelClient, IPythonKernel]:
    ka, kc = kc, kernel
    return ka, kc, ka.kernel


async def get_reply(
    owner: AsyncKernelClient, msg_id: str, *, channel: Literal["shell", "control"] = "shell", timeout=TIMEOUT
):
    kc = owner[1] if isinstance(owner, tuple) else owner

    while True:
        with anyio.fail_after(timeout):
            match channel:
                case "shell":
                    reply = await kc.get_shell_msg(timeout=timeout)
                case "control":
                    reply = await kc.get_control_msg(timeout=timeout)
            if reply["parent_header"]["msg_id"] == msg_id:
                break
        # Allow debugging ignored replies
        print(f"Ignoring reply not to {msg_id}: {reply}")
    return reply


async def execute(client: AsyncKernelClient, /, code="", **kwargs):
    """wrapper for doing common steps for validating an execution request"""
    from tests.test_message_spec import validate_message

    assert isinstance(client, AsyncKernelClient)

    with anyio.fail_after(TIMEOUT):
        msg_id = client.execute(code=code, **kwargs)
        reply = await get_reply(client, msg_id)
        validate_message(reply, "execute_reply", msg_id)
        busy = await client.get_iopub_msg()
        validate_message(busy, "status", msg_id)
        assert busy["content"]["execution_state"] == "busy"

        if not kwargs.get("silent"):
            execute_input = await client.get_iopub_msg()
            validate_message(execute_input, "execute_input", msg_id)
            assert execute_input["content"]["code"] == code

        # show tracebacks if present for debugging
        if reply["content"].get("traceback"):
            print("\n".join(reply["content"]["traceback"]), file=sys.stderr)

    return msg_id, reply["content"]


async def assemble_output(owner: OwnerType, *, timeout=1.0):
    """Assemble stdout/err from an execution"""
    kc = owner[1] if isinstance(owner, tuple) else owner
    stdout = ""
    stderr = ""
    with anyio.move_on_after(timeout):
        while True:
            msg = await kc.get_iopub_msg()
            msg_type = msg["msg_type"]
            content = msg["content"]
            if msg_type == "status" and content["execution_state"] == "idle":
                # idle message signals end of output
                break
            elif msg["msg_type"] == "stream":
                if content["name"] == "stdout":
                    stdout += content["text"]
                elif content["name"] == "stderr":
                    stderr += content["text"]
                else:
                    raise KeyError("bad stream: %r" % content["name"])
    return stdout, stderr


async def wait_for_idle(kc: AsyncKernelClient, *, wait=1):
    with anyio.fail_after(wait):
        while True:
            msg = await kc.get_iopub_msg()
            msg_type = msg["msg_type"]
            content = msg["content"]
            if msg_type == "status" and content["execution_state"] == "idle":
                break


async def do_debug_request(kernel: IPythonKernel, msg: str):
    return {}


def _prep_msg(kernel: IPythonKernel, *, msg_type: str, **content: Unpack[ContentType]):
    assert kernel.session
    msg = kernel.session.msg(msg_type, content)  # type: ignore
    return kernel.session.serialize(msg)


async def _test_message(
    kc,
    kernel: tuple[IPKernelApp, AsyncKernelClient],
    channel: Literal["shell", "control"],
    /,
    msg_type: str,
    **kwargs,
):
    ka, kc = kc, kernel
    kernel = ka.kernel
    msg_list = _prep_msg(kernel, msg_type=msg_type, **kwargs)
    await kernel.process_control_message(msg_list)
    msg_id = msg_list
    kc.control_channel.send()
    return await get_reply(kc, msg_id, channel="control")


async def shell_message(kc, kernel: tuple[IPKernelApp, AsyncKernelClient], msg_type: str, **kwargs):
    return await _test_message(kc, kernel, "shell", msg_type=msg_type, **kwargs)


async def test_control_message(kc, kernel: tuple[IPKernelApp, AsyncKernelClient], msg_type: str, **kwargs):
    return await _test_message(kc, kernel, "control", msg_type=msg_type, **kwargs)
