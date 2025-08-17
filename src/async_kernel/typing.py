# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, ParamSpec, TypedDict, TypeVar, TypeVarTuple

from typing_extensions import Sentinel

if TYPE_CHECKING:
    from collections.abc import Mapping

    import zmq

__all__ = [
    "DebugMessage",
    "ExecuteMode",
    "Job",
    "Message",
    "MetadataKeys",
    "MsgHeader",
    "MsgType",
    "SocketID",
    "Tags",
]

NoValue = Sentinel("NoValue")


T = TypeVar("T")
D = TypeVar("D", bound=dict)
P = ParamSpec("P")
PosArgsT = TypeVarTuple("PosArgsT")


class SocketID(enum.StrEnum):
    "Mapping of `Kernel.port_<id>` for sockets. [Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introduction)."

    heartbeat = "hb"
    ""
    shell = "shell"
    ""
    stdin = "stdin"
    ""
    control = "control"
    ""
    iopub = "iopub"
    ""


EXECUTE_MODE_PREFIX: Final = "##"
"The Prefix used for [ExecuteMode][async_kernel.typing.ExecuteMode] identifiers."


class ExecuteMode(enum.StrEnum):
    "An Enum of the Execute modes available for altering how [execute requests](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute) are handled."

    queue = f"{EXECUTE_MODE_PREFIX}queue"
    "Add to the execute_request queue (default)."
    task = f"{EXECUTE_MODE_PREFIX}task"
    "Execute as a task in the MainThread."
    thread = f"{EXECUTE_MODE_PREFIX}thread"
    "Execute in a caller worker thread."


class MsgType(enum.StrEnum):
    """An enumeration of Message `msg_type` for [shell and control messages]( https://jupyter-client.readthedocs.io/en/stable/messaging.html#messages-on-the-shell-router-dealer-channel).



    [Control channel](https://jupyter-client.readthedocs.io/en/stable/messaging.html#messages-on-the-control-router-dealer-channel) only
    """

    kernel_info_request = "kernel_info_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info)"
    comm_info_request = "comm_info_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-info)"
    execute_request = "execute_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)"
    complete_request = "complete_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)"
    is_complete_request = "is_complete_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)"
    inspect_request = "inspect_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)"
    history_request = "history_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)"
    comm_open = "comm_open"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm)"
    comm_msg = "comm_msg"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages)"
    comm_close = "comm_close"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms)"
    # Control
    interrupt_request = "interrupt_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt) (control only)"
    shutdown_request = "shutdown_request"
    "[ref](shutdown_request) (control only)"
    debug_request = "debug_request"
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request) (control only)"


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
    ""

    # https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header
    msg_id: str
    session: str
    username: str
    date: str
    msg_type: MsgType
    version: str


class Message(TypedDict, Generic[T]):
    "A [message](https://jupyter-client.readthedocs.io/en/stable/messaging.html#general-message-format)."

    header: MsgHeader
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header)"
    parent_header: MsgHeader
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#parent-header)"
    metadata: Mapping[MetadataKeys | str, Any]
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)"
    content: T
    """[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)
    
    See also:

    - [ExecuteContent][async_kernel.typing.ExecuteContent]
    """
    buffers: list[bytearray | bytes]
    ""


class Job(TypedDict, Generic[T]):
    "An [async_kernel.typing.Message][] bundled with sockit_id, socket and ident."

    msg: Message[T]
    ""
    socket_id: Literal[SocketID.control, SocketID.shell]
    ""
    socket: zmq.Socket
    ""
    ident: bytes | list[bytes]
    ""
    received_time: float
    "The time the message was received."


class ExecuteContent(TypedDict):
    "[Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute).  see also: [Message][async_kernel.typing.Message]"

    code: str
    "The code to execute."
    silent: bool
    "Modifies how code is executed. See also [get_execute_mode][async_kernel.kernel.get_execute_mode]."
    store_history: bool
    "See ref."
    user_expressions: dict[str, str]
    "See ref."
    allow_stdin: bool
    "See ref."
    stop_on_error: bool
    "See ref."
    execute_mode: ExecuteMode
    """The execute mode. See also [get_execute_mode][async_kernel.kernel.get_execute_mode]."""


DebugMessage = dict[str, Any]
