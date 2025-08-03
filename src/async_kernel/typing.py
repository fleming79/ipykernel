# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, ParamSpec, TypedDict, TypeVar

from typing_extensions import Sentinel

if TYPE_CHECKING:
    import zmq

__all__ = ["DebugMessage", "Job", "Message", "MsgHeader", "SocketID"]


NoValue = Sentinel("NoValue")


T = TypeVar("T")
D = TypeVar("D", bound=dict)
P = ParamSpec("P")


class SocketID(enum.StrEnum):
    heartbeat = "hb"
    shell = "shell"
    stdin = "stdin"
    control = "control"
    iopub = "iopub"


class ExecuteMode(enum.StrEnum):
    task = "task"
    thread = "thread"
    queue = "queue"


EXECUTE_MODE_PREFIX: Final = "# @"


class MsgType(enum.StrEnum):
    kernel_info_request = "kernel_info_request"
    comm_info_request = "comm_info_request"
    execute_request = "execute_request"
    interrupt_request = "interrupt_request"
    complete_request = "complete_request"
    is_complete_request = "is_complete_request"
    inspect_request = "inspect_request"
    history_request = "history_request"
    comm_open = "comm_open"
    comm_msg = "comm_msg"
    comm_close = "comm_close"
    # Control
    shutdown_request = "shutdown_request"
    debug_request = "debug_request"


class MsgHeader(TypedDict):
    # https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header
    msg_id: str
    session: str
    username: str
    date: str
    msg_type: str
    version: str


class Message(TypedDict, Generic[T]):
    header: MsgHeader
    parent_header: MsgHeader
    metadata: dict[str, Any]
    content: T
    buffers: list[bytearray | bytes]


class Job(TypedDict, Generic[T]):
    "A message bundled with its details."

    msg: Message[T]
    socket_id: Literal[SocketID.control, SocketID.shell]
    socket: zmq.Socket
    ident: bytes | list[bytes]
    msg_type: MsgType


class ExecuteContent(TypedDict):
    # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute
    code: str
    silent: bool
    store_history: bool
    user_expressions: dict[str, str]
    allow_stdin: bool
    stop_on_error: bool
    execute_mode: ExecuteMode | None  # Added by the kernel when the message is received


DebugMessage = dict[str, Any]
