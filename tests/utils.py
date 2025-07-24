"""utilities for testing IPython kernels"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

import anyio
from jupyter_client.asynchronous.client import AsyncKernelClient

import async_kernel.utils
from async_kernel import Caller, Kernel
from async_kernel.asyncshell import AsyncInteractiveShell
from tests.references import RMessage, references

if TYPE_CHECKING:
    from collections.abc import Mapping


TIMEOUT = 10 if not async_kernel.utils.LAUNCHED_BY_DEBUGPY else 1e6


class ExecuteContentType(TypedDict):
    code: NotRequired[str]
    silent: NotRequired[bool]
    store_history: NotRequired[bool]
    user_expressions: NotRequired[dict[str, str]]
    allow_stdin: NotRequired[bool]
    stop_on_error: NotRequired[bool]


def clear_kernel():
    "Clear the kernel so it can be started fresh."
    if kernel := Kernel._instance:
        kernel.stop()
    Kernel._instance = None
    AsyncInteractiveShell.clear_instance()
    Caller._shutdown_all()


async def get_reply(
    client: AsyncKernelClient,
    msg_id: str,
    *,
    channel: Literal["shell", "control"] = "shell",
    timeout=TIMEOUT,
    clear_pub=True,
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
                if clear_pub:
                    await clear_iopub(client)
                return reply


def validate_message(msg: Mapping[str, Any], msg_type="", parent=None):
    """validate a message.

    If msg_type and/or parent are given, the msg_type and/or parent msg_id
    are compared with the given values.
    """
    RMessage().check(msg)
    if msg_type and msg["msg_type"] != msg_type:
        msg_ = f"Expected {msg_type=} but got '{msg['msg_type']}'  for {msg=}"
        raise ValueError(msg_)
    if parent and msg["parent_header"]["msg_id"] != parent:
        msg_ = f"This parent 'msg_id' does not match {msg=} {parent=}"
        raise RuntimeError(msg_)
    content = msg["content"]
    ref = references[msg["msg_type"]]
    try:
        ref.check(content)
    except Exception as e:
        e.add_note(f"\n{msg_type=}\n{parent=}\n{content=}")
        raise


async def execute(client: AsyncKernelClient, /, code="", clear_pub=True, **kwargs):
    """wrapper for doing common steps for validating an execution request"""

    assert isinstance(client, AsyncKernelClient)

    with anyio.fail_after(TIMEOUT):
        msg_id = client.execute(code=code, **kwargs)
        reply = await get_reply(client, msg_id, clear_pub=clear_pub)
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
            if msg["msg_type"] == "stream":
                if content["name"] == "stdout":
                    stdout += content["text"]
                elif content["name"] == "stderr":
                    stderr += content["text"]
                else:
                    msg = f"bad stream: {content['name']}"
                    raise KeyError(msg)
    return stdout, stderr


async def wait_for_idle(client: AsyncKernelClient, *, wait=1.0):
    with anyio.move_on_after(wait):
        while True:
            msg = await client.get_iopub_msg()
            msg_type = msg["msg_type"]
            content = msg["content"]
            if msg_type == "status" and content["execution_state"] == "idle":
                break


async def clear_iopub(client):
    "Ensure there are no further iopub messages waiting."
    await assemble_output(client, timeout=0.01)


async def send_shell_message(client: AsyncKernelClient, msg_type: str, content: Mapping[str, Any] | None = None):
    msg = client.session.msg(msg_type, content=dict(content) if content is not None else None)
    client.shell_channel.send(msg)
    return await get_reply(client, msg["header"]["msg_id"], channel="shell")


async def send_control_message(
    client: AsyncKernelClient, msg_type: str, content: Mapping[str, Any] | None = None, clear_pub=True
):
    msg = client.session.msg(msg_type, content=dict(content) if content is not None else None)
    client.control_channel.send(msg)
    return await get_reply(client, msg["header"]["msg_id"], channel="control", clear_pub=clear_pub)


async def check_pub_message(client: AsyncKernelClient, msg_id: str = "", *, msg_type="status", **content_checks):
    msg = await client.get_iopub_msg()
    validate_message(msg, msg_type, msg_id)
    content = msg["content"]
    for k, v in content_checks.items():
        if content[k] != v:
            msg = f"Failed content check for {msg_type=}  {k}!={v}"
            raise ValueError(msg)
    return msg


async def get_shell_message(client: AsyncKernelClient, msg_id: str, msg_type: str):
    msg = await client.get_shell_msg()
    validate_message(msg, msg_type, msg_id)
    return msg["content"]
