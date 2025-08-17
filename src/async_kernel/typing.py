# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
from collections.abc import Callable
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, NotRequired, ParamSpec, TypedDict, TypeVar, TypeVarTuple

from typing_extensions import Sentinel

if TYPE_CHECKING:
    from collections.abc import Mapping

    import zmq

__all__ = [
    "CODE_MODE_MAPPINGS",
    "RUN_MODE_PREFIX",
    "DebugMessage",
    "Job",
    "Message",
    "MetadataKeys",
    "MsgHeader",
    "MsgType",
    "RunMode",
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


RUN_MODE_PREFIX: Final = "##"  # "The Prefix used for [RunMode][async_kernel.typing.RunMode] identifiers."


class RunMode(enum.StrEnum):
    "An Enum of the Run modes available for altering how jobs are(https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute) are handled."

    queue = "queue"
    "Add to the execute_request queue."
    task = "task"
    "Execute as a task in the MainThread."
    thread = "thread"
    "Execute in a caller worker thread."
    wait = "wait"
    """Wait for the message to execute.

    This blocks the message loop"""


CODE_MODE_MAPPINGS: Final[dict[str, RunMode]] = {f"{RUN_MODE_PREFIX}{mode}": mode for mode in RunMode}


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
    "A [message header](https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header)."

    msg_id: str
    ""
    session: str
    ""
    username: str
    ""
    date: str
    ""
    msg_type: MsgType
    ""
    version: str
    ""


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
    run_mode: NotRequired[RunMode]
    """The run mode."""


class ExecuteContent(TypedDict):
    "[Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute).  see also: [Message][async_kernel.typing.Message]"

    code: str
    "The code to execute."
    silent: bool
    "Modifies how code is executed. See also [get_run_mode][async_kernel.kernel.get_run_mode]."
    store_history: bool
    "See ref."
    user_expressions: dict[str, str]
    "See ref."
    allow_stdin: bool
    "See ref."
    stop_on_error: bool
    "See ref."


DebugMessage = dict[str, Any]
HandlerType = Callable[[Job], CoroutineType]
