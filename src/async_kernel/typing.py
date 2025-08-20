# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Literal, ParamSpec, TypedDict, TypeVar, TypeVarTuple

from typing_extensions import Sentinel, override

if TYPE_CHECKING:
    from collections.abc import Mapping

    import zmq

__all__ = [
    "DebugMessage",
    "Job",
    "KernelConcurrencyMode",
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


class RunMode(enum.StrEnum):
    """An Enum of the [kernel run modes][async_kernel.Kernel.handle_message_request] available for
    altering how message requests are run.

    !!! hint "String match options"

        Each of these options will give a match.

        - `<value>`
        - `<##value>`
        - '`RunMode.<value>`.

    !!! note "special usage"

        Run mode can be used in [execute requests](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute).
        Add it at the top line (or use the string equivalent "##<run mode>") of a code cell.
    """

    "The prefix for each run mode."

    @override
    def __str__(self):
        return f"##{self.name}"

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in (self.name, str(self), repr(self))

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @classmethod
    def get_mode(cls, code: str) -> RunMode | None:
        "Get a RunMode from the code if it is found."
        try:
            if (code := code.strip().split("\n")[0].strip()).startswith("##"):
                return RunMode(code.removeprefix("##"))
            if code.startswith("RunMode."):
                return RunMode(code.removeprefix("RunMode."))
        except ValueError:
            return None

    queue = "queue"
    "The message for the [handler][async_kernel.typing.MsgType] is run sequentially with other messages that are queued."

    task = "task"
    "The message for the [handler][async_kernel.typing.MsgType] are run concurrently in task (starting immediately)."

    thread = "thread"
    "Messages for the [handler][async_kernel.typing.MsgType] are run concurrently in a thread (starting immediately)."

    blocking = "blocking"
    """Run the handler directly as soon as it is received.
    
    !!! warning 
    
        **This mode blocks the message loop.** 
        
        Use this for short running messages that should be processed as soon as it is received.
        """


class KernelConcurrencyMode(enum.StrEnum):
    ""

    default = "default"
    "The default concurrency mode"
    blocking = "blocking"
    "All handlers are run with the [blocking][async_kernel.typing.RunMode.blocking]."


class MsgType(enum.StrEnum):
    """An enumeration of Message `msg_type` for [shell and control messages]( https://jupyter-client.readthedocs.io/en/stable/messaging.html#messages-on-the-shell-router-dealer-channel).

    Some message types are on the [control channel](https://jupyter-client.readthedocs.io/en/stable/messaging.html#messages-on-the-control-router-dealer-channel) only.
    """

    kernel_info_request = "kernel_info_request"
    "[async_kernel.Kernel.kernel_info_request][]"

    comm_info_request = "comm_info_request"
    "[async_kernel.Kernel.comm_info_request][]"

    execute_request = "execute_request"
    "[async_kernel.Kernel.execute_request][]"

    complete_request = "complete_request"
    "[async_kernel.Kernel.complete_request][]"

    is_complete_request = "is_complete_request"
    "[async_kernel.Kernel.is_complete_request][]"

    inspect_request = "inspect_request"
    "[async_kernel.Kernel.inspect_request][]"

    history_request = "history_request"
    "[async_kernel.Kernel.history_request][]"

    comm_open = "comm_open"
    "[async_kernel.Kernel.comm_open][]"

    comm_msg = "comm_msg"
    "[async_kernel.Kernel.comm_msg][]"

    comm_close = "comm_close"
    "[async_kernel.Kernel.comm_close][]"

    # Control
    interrupt_request = "interrupt_request"
    "[async_kernel.Kernel.interrupt_request][] (control channel only)"

    shutdown_request = "shutdown_request"
    "[async_kernel.Kernel.shutdown_request][] (control channel only)"

    debug_request = "debug_request"
    "[async_kernel.Kernel.debug_request][] (control channel only)"


class MetadataKeys(enum.StrEnum):
    """This is an enum of keys for [metadata in kernel messages](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)
    that are used in async_kernel.

    !!! Note
        Metadata can be edited in Jupyter lab "Advanced tools" and Tags can be added using "common tools" in the [right side bar](https://jupyterlab.readthedocs.io/en/stable/user/interface.html#left-and-right-sidebar).
    """

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in (self.name, str(self))

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    tags = "tags"
    """The `tags` metadata key corresponds to is a list of strings. 
    
    The list can be edited by the user in a notebook.
    see also: [Tags][async_kernel.typing.Tags].
    """
    timeout = "timeout"
    """The `timeout` metadata key is used to specify a timeout for execution of the code.
    
    The value should be a floating point value of the timeout in seconds.
    """
    suppress_error_message = "suppress-error-message"
    """A message to print when the error has been suppressed using [async_kernel.typing.Tags.suppress_error][]. 
    
    ???+ note

        The default message is '⚠'.
    """


class Tags(enum.StrEnum):
    """Tags recognised by the kernel.

    ??? info
        Tags are can be added per cell.

        - Jupyter: via the [right side bar](https://jupyterlab.readthedocs.io/en/stable/user/interface.html#left-and-right-sidebar).
        - VScode: via [Jupyter variables explorer](https://code.visualstudio.com/docs/python/jupyter-support-py#_variables-explorer-and-data-viewer)/
    """

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in (self.name, str(self))

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    suppress_error = "suppress-error"
    """Suppress exceptions that occur during execution of the code cell.
    
    !!! note "Warning"
    
        The code block will return as 'ok' and there will be no message recorded.
    """


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

    content: T | Content
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

    run_mode: RunMode
    """The run mode."""


class ExecuteContent(TypedDict):
    "[Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute).  see also: [Message][async_kernel.typing.Message]"

    code: str
    "The code to execute."
    silent: bool
    ""
    store_history: bool
    ""
    user_expressions: dict[str, str]
    ""
    allow_stdin: bool
    ""
    stop_on_error: bool
    ""


DebugMessage = dict[str, Any]
Content = dict[str, Any]
HandlerType = Callable[[Job], Awaitable[Content | None]]
