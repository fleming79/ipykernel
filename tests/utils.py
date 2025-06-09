"""utilities for testing IPython kernels"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

import anyio
from jupyter_client.asynchronous.client import AsyncKernelClient

from tests.references import RMessage, references

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ipykernel.kernelapp import IPKernelApp

    OwnerType = AsyncKernelClient | tuple[IPKernelApp, AsyncKernelClient]


STARTUP_TIMEOUT = 60 if "debugpy" not in sys.modules else 1e6
TIMEOUT = 10 if "debugpy" not in sys.modules else 1e6


class ExecuteContentType(TypedDict):
    code: NotRequired[str]
    silent: NotRequired[bool]
    store_history: NotRequired[bool]
    user_expressions: NotRequired[dict[str, str]]
    allow_stdin: NotRequired[bool]
    stop_on_error: NotRequired[bool]


async def get_reply(
    client: AsyncKernelClient,
    msg_id: str,
    *,
    channel: Literal["shell", "control"] = "shell",
    timeout=TIMEOUT,
) -> Mapping[str, Mapping[str, Any]]:
    "Gets the first revieved reply correspond to the msg_id."
    with anyio.fail_after(timeout):
        while True:
            match channel:
                case "shell":
                    reply = await client.get_shell_msg(timeout=timeout)
                case "control":
                    reply = await client.get_control_msg(timeout=timeout)
            if reply["parent_header"]["msg_id"] == msg_id:
                return reply


# -----------------------------------------------------------------------------
# Specifications of `content` part of the reply messages.
# -----------------------------------------------------------------------------


def validate_message(msg: Mapping[str, Any], msg_type=None, parent=None):
    """validate a message.

    If msg_type and/or parent are given, the msg_type and/or parent msg_id
    are compared with the given values.
    """
    RMessage().check(msg)
    if msg_type and msg["msg_type"] != msg_type:
        msg_ = f"Expected {msg_type=} but got '{msg['msg_type']}'  for {msg=}"
        raise ValueError(msg_)
    if parent and msg["parent_header"]["msg_id"] != parent:
        raise RuntimeError(f"This parent 'msg_id' does not match {msg=} {parent=}")
    content = msg["content"]
    ref = references[msg["msg_type"]]
    try:
        ref.check(content)
    except Exception as e:
        e.add_note(f"\n{msg_type=}\n{parent=}\n{content=}")
        raise e


async def execute(client: AsyncKernelClient, /, code="", **kwargs):
    """wrapper for doing common steps for validating an execution request"""

    assert isinstance(client, AsyncKernelClient)

    with anyio.fail_after(TIMEOUT):
        msg_id = client.execute(code=code, **kwargs)
        reply = await get_reply(client, msg_id)
        validate_message(reply, "execute_reply", msg_id)
    return msg_id, reply["content"]


async def assemble_output(client: AsyncKernelClient, timeout=TIMEOUT):
    """Assemble stdout/err from an execution"""
    assert isinstance(client, AsyncKernelClient)
    stdout = ""
    stderr = ""
    done = False
    with anyio.move_on_after(timeout):
        while True:
            msg = await client.get_iopub_msg()
            msg_type = msg["msg_type"]
            content = msg["content"]
            if not done:
                done = bool(msg_type == "status" and content["execution_state"] == "idle")
            if done and (stdout or stderr):
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


async def send_shell_message(client: AsyncKernelClient, msg_type: str, content: Mapping[str, Any] | None = None):
    msg = client.session.msg(msg_type, content=dict(content) if content is not None else None)
    client.shell_channel.send(msg)
    return await get_reply(client, msg["header"]["msg_id"], channel="shell")


async def send_control_message(client: AsyncKernelClient, msg_type: str, content: Mapping[str, Any] | None = None):
    msg = client.session.msg(msg_type, content=dict(content) if content is not None else None)
    client.control_channel.send(msg)
    return await get_reply(client, msg["header"]["msg_id"], channel="control")


async def check_pub_message(client: AsyncKernelClient, msg_id: str, *, msg_type="status", **content_checks):
    msg = await client.get_iopub_msg()
    validate_message(msg, msg_type, msg_id)
    content = msg["content"]
    for k, v in content_checks.items():
        assert content[k] == v
    return msg


async def get_shell_message(client: AsyncKernelClient, msg_id: str, msg_type: str):
    msg = await client.get_shell_msg()
    validate_message(msg, msg_type, msg_id)
    return msg["content"]
