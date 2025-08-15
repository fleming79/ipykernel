# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, ParamSpec, TypedDict, TypeVar

from typing_extensions import Sentinel

if TYPE_CHECKING:
    from collections.abc import Mapping

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


EXECUTE_MODE_PREFIX: Final = "##"


class ExecuteMode(enum.StrEnum):
    task = f"{EXECUTE_MODE_PREFIX}task"
    thread = f"{EXECUTE_MODE_PREFIX}thread"
    queue = f"{EXECUTE_MODE_PREFIX}queue"


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


class MetadataKeys(enum.StrEnum):
    """This is an enum of keys for [metadata in kernel messages](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)
    that are used in async_kernel.

    !!! Note
        Metadata can be edited in Jupyter lab "Advanced tools" and Tags can be added using "common tools" in the [right side bar](https://jupyterlab.readthedocs.io/en/stable/user/interface.html#left-and-right-sidebar).
    """

    tags = "tags"
    """The `tags` metadata key corresponds to is a list of strings. 
    
    The list can be edited by the user in a notebook.
    see also: [Tags][async_kernel.typing.Tags].
    """
    timeout = "timeout"
    """The `timeout` metadata key is used to specify a timeout for execution of the code.
    
    The value should be a floating point value of the timeout in seconds.
    """


class Tags(enum.StrEnum):
    """Tags recognised by the kernel"""

    suppress_error = "suppress-error"
    """Ignore`stop_on_error` in context of the `execute request`."""
    do_not_publish_error = "do-not-publish-error"
    """Prevent the shell from publishing error messages in context of the `execute request`."""


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
    metadata: Mapping[MetadataKeys | str, Any]
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
