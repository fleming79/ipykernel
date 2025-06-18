"""An Application for launching a kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import asyncio
import atexit
import builtins
import errno
import getpass
import logging
import os
import sys
import threading
import time
import traceback
import typing as t
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import anyio
import anyio.from_thread
import zmq
import zmq_anyio
from anyio import create_memory_object_stream, create_task_group, to_thread
from anyio.abc import TaskGroup
from anyio.from_thread import BlockingPortal
from IPython.core.completer import provisionalcompleter as _provisionalcompleter
from IPython.core.completer import rectify_completions as _rectify_completions
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import (
    Any,
    Bool,
    Container,
    Dict,
    DottedObjectName,
    Float,
    Instance,
    Set,
    Type,
    Unicode,
    default,
    observe,
)
from traitlets.config import SingletonConfigurable
from traitlets.config.configurable import LoggingConfigurable
from traitlets.utils.importstring import import_item

from ipykernel._version import kernel_protocol_version
from ipykernel.compiler import XCachingCompiler
from ipykernel.iostream import OutStream, SocketID
from ipykernel.kernelspec import AsyncMode
from ipykernel.zmqshell import ZMQInteractiveShell

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from anyio.abc import TaskStatus
    from IPython.core.interactiveshell import ExecutionResult

    from ipykernel.comm import CommManager
    from ipykernel.debugger import Debugger
    from ipykernel.kernel import Kernel


def run_in_thread(
    func: Callable[[TaskStatus], CoroutineType],
    stop_event: threading.Event,
    tg: TaskGroup,
    *,
    backend: Literal["anyio", "trio", ""] = "",
    name="",
    pydev_do_not_trace=False,
    is_pydev_daemon_thread=False,
):
    """Run a coroutine function in a separate thread (and event loop) and manage its lifecycle using AnyIO.

    This function takes an asynchronous function, a stop event, and a task group,
    and starts the function in a separate thread. It ensures that the function
    is properly started and can be stopped gracefully.

    This coroutine returns once `task_status.started()`is called inside `func`
    running until the `stop_event` is set.

    Args:
        func: The asynchronous function to run in a separate thread. It should
            accept a TaskStatus object as an argument and return a coroutine.
        stop_event: A threading.Event that signals when the function should stop.
        tg: The AnyIO TaskGroup to use for managing the function's task.
        task_status: An AnyIO TaskStatus object to signal when the function has started.
    """

    import sniffio

    backend = backend or sniffio.current_async_library()  # type: ignore[no-any-return]

    ready_event = threading.Event()

    def run_func():
        thread = threading.current_thread()
        if name:
            thread.name = name
        thread.pydev_do_not_trace = pydev_do_not_trace  # type: ignore  # noqa: PGH003
        thread.is_pydev_daemon_thread = is_pydev_daemon_thread  # type: ignore  # noqa: PGH003

        async def run_until_stop_event():
            async with anyio.create_task_group() as tg:
                await tg.start(func)
                ready_event.set()
                await to_thread.run_sync(stop_event.wait)
                tg.cancel_scope.cancel()

        anyio.run(run_until_stop_event, backend=backend)

    tg.start_soon(to_thread.run_sync, run_func)
    return to_thread.run_sync(ready_event.wait)


class Subkernel(LoggingConfigurable):
    """The base kernel class."""

    _stop_on_error_time: float = time.monotonic()

    # ---------------------------------------------------------------------------
    # Kernel interface
    # ---------------------------------------------------------------------------
    session: Instance[Session] = Instance(Session)
    profile_dir = Instance("IPython.core.profiledir.ProfileDir", allow_none=True)

    implementation: str
    implementation_version: str

    execution_count = 0
    main_kernel: Instance[Kernel] = Instance("ipykernel.kernel.Kernel", ())
    asyncio_event_loop = Instance(asyncio.AbstractEventLoop, allow_none=True, read_only=True)  # type:ignore[call-overload]
    _portal = Instance(BlockingPortal)

    log = Instance(logging.LoggerAdapter)
    ident = Unicode()
    shell_handlers = Dict()

    @default("log")
    def _default_log(self):
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @default("ident")
    def _default_ident(self):
        return str(uuid.uuid4())

    language_info = {
        "name": "python",
        "version": sys.version.split()[0],
        "mimetype": "text/x-python",
        "codemirror_mode": {"name": "ipython", "version": sys.version_info[0]},
        "pygments_lexer": "ipython%d" % 3,
        "nbconvert_exporter": "python",
        "file_extension": ".py",
    }

    # Experimental option to break in non-user code.
    # The ipykernel source is in the call stack, so the user
    # has to manipulate the step-over and step-into in a wize way.
    debug_just_my_code = Bool(
        True,
        help="""Set to False if you want to debug python standard and dependent libraries.
        """,
    ).tag(config=True)

    # track associations with current request
    _allow_stdin = Bool(False)

    # Frequency of the kernel's event loop.
    # Units are in seconds, kernel subclasses for GUI toolkits may need to
    # adapt to milliseconds.
    _poll_interval = Float(0.01).tag(config=True)

    stop_on_error_timeout = Float(
        0.0,
        config=True,
        help="""time (in seconds) to wait for messages to arrive
        when aborting queued requests after an error.

        Requests that arrive within this window after an error
        will be cancelled.

        Increase in the event of unusually slow network
        causing significant delays,
        which can manifest as e.g. "Run all" in a notebook
        aborting some, but not all, messages after an error.
        """,
    )

    shell = Instance(ZMQInteractiveShell)
    shell_class = Type(ZMQInteractiveShell)

    # use fully-qualified name to ensure lazy import and prevent the issue from
    # https://github.com/ipython/ipykernel/issues/1198
    debugger_class: Type[type[Debugger], type[Debugger]] = Type("ipykernel.debugger.Debugger")

    compiler_class = Type(XCachingCompiler)

    debugpy_socket = Instance(zmq.Socket)

    user_module = Any()

    @observe("user_module")
    def _user_module_changed(self, change):
        if self.shell is not None:
            self.shell.user_module = change["new"]

    user_ns = Dict()

    @observe("user_ns")
    def _user_ns_changed(self, change):
        if self.trait_has_value("shell"):
            self.shell.user_ns = change["new"]
            self.shell.init_user_ns()
            self.shell.set_completer_frame()

    comm_manager: Instance[CommManager] = Instance("ipykernel.comm.CommManager")

    @default("comm_manager")
    def _default_comm_manager(self):
        import ipykernel.comm

        ipykernel.comm.set_comm()
        return ipykernel.comm.get_comm_manager()

    # Kernel info fields
    implementation = "asynckernel"
    implementation_version = " 0.1"

    # A reference to the Python builtin 'raw_input' function.
    # (i.e., __builtin__.raw_input for Python 2.7, builtins.input for Python 3)
    _sys_raw_input = Any()
    _sys_eval_input = Any()

    def __init__(self, **kwargs):
        """Initialize the kernel."""
        super().__init__(**kwargs)
        self.shell_handlers = {
            "execute_request": self.execute_request,
            "complete_request": self.complete_request,
            "inspect_request": self.inspect_request,
            "history_request": self.history_request,
            "comm_info_request": self.comm_info_request,
            "kernel_info_request": self.kernel_info_request,
            "is_complete_request": self.is_complete_request,
            "interrupt_request": self.interrupt_request,
            "comm_open": self.comm_open,
            "comm_msg": self.comm_msg,
            "comm_close": self.comm_close,
        }
        self._exec_send_stream, self._exec_receive_stream = create_memory_object_stream[
            tuple[float, zmq_anyio.Socket, list[bytes | bytearray], dict]
        ](max_buffer_size=1000)
        self.shell = self.shell_class.instance(
            parent=self,
            profile_dir=self.profile_dir,
            user_module=self.user_module,
            user_ns=self.user_ns,
            kernel=self,
            compiler_class=self.compiler_class,
        )
        jupyter_session_name = os.environ.get("JPY_SESSION_NAME")
        if jupyter_session_name:
            self.shell.user_ns["__session__"] = jupyter_session_name

    # ---------------------------------------------------------------------------
    # Kernel request handlers
    # ---------------------------------------------------------------------------

    def _publish_status(self, status: Literal["busy", "idle"], parent):
        """send status (busy/idle) on IOPub"""
        self.main_kernel.pubio_send(
            msg_or_type="status",
            content={"execution_state": status},
            parent=parent,
            ident=self._topic("status"),
        )

    def _publish_debug_event(self, event):
        self.main_kernel.pubio_send(
            msg_or_type="debug_event",
            content=event,
            parent=self.parent_msg,
            ident=self._topic("debug_event"),
        )

    def _set_parent_ident(self, parent, ident):
        self._parent_msg = parent
        self._parent_ident = ident
        self.shell.set_parent(parent)

    @property
    def parent_msg(self):
        """The message of the most recent execution request.

        .. versionadded:: 7
        """
        try:
            return self._parent_msg
        except AttributeError:
            return {}

    @property
    def parent_ident(self):
        """The ident of the most recent execution request.

        .. versionadded:: 7
        """
        try:
            return self._parent_ident
        except AttributeError:
            return []

    async def execute_request(self, socket, ident, parent):
        content = parent["content"]
        silent = content["silent"]
        if not silent:
            await self._exec_send_stream.send((time.monotonic(), socket, ident, parent))
        else:
            self.main_kernel.start_soon(self._execute_request, socket, ident, parent)

    async def _shell_execute_request_loop(self):
        async with self._exec_receive_stream as receive_stream:
            async for received_time, socket, idents, msg in receive_stream:
                try:
                    if received_time < self._stop_on_error_time:
                        self.log.info("Aborting execute_request: %s", msg["header"]["msg_id"])
                        self._send_error_reply(
                            socket=socket,
                            idents=idents,
                            msg=msg,
                            evalue="Aborting due to prior exception",
                        )
                        continue
                    await self._execute_request(socket, idents, msg)
                except BaseException as e:
                    self.log.exception("Execute request", exc_info=e)
                    self._send_error_reply(
                        socket=socket,
                        idents=idents,
                        msg=msg,
                        ename=str(type(e).__name__),
                        evalue=str(e),
                        traceback=traceback.format_stack(),
                    )
                finally:
                    self._publish_status("idle", msg)

    async def _execute_request(self, socket, ident, parent):
        """handle an execute_request"""
        content = parent["content"]
        silent = content["silent"]
        stop_on_error = content.pop("stop_on_error", True)

        self._publish_status("busy", parent)
        try:
            # Re-broadcast our input for the benefit of listening clients, and
            # start computing output
            if not silent:
                self._set_parent_ident(parent, ident)
                self.execution_count += 1
                self.main_kernel.pubio_send(
                    msg_or_type="execute_input",
                    content={"code": content["code"], "execution_count": self.execution_count},
                    parent=parent,
                    ident=self._topic("execute_input"),
                )
            # Call do_execute with the appropriate arguments
            reply_content = await self._do_execute(**content)
            if not silent and stop_on_error and reply_content.get("status") == "error":
                self._stop_on_error_time = time.monotonic()
                self.log.info("An error occurred in a non-silent execution request at %s", self._stop_on_error_time)

            # Send the reply.
            self.session.send(
                stream=socket,
                msg_or_type="execute_reply",
                content=reply_content,
                parent=parent,
                ident=ident,
            )
        except Exception as e:
            self._send_error_reply(socket, parent, ident, ename=e.__class__.__name__, evalue=str(e))
        finally:
            self._publish_status("idle", parent)

    async def _do_execute(
        self,
        code: str,
        silent: bool,
        store_history=True,
        user_expressions: dict | None = None,
        allow_stdin=False,
    ):
        """Handle code execution."""
        # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute
        if not (shell := self.shell):
            msg = "shell is missing!"
            raise RuntimeError(msg)
        self._forward_input(allow_stdin)
        reply_content: dict[str, t.Any] = {}
        result: list[ExecutionResult] = []
        interrupt = threading.Event()
        if not silent:
            self.main_kernel.shell_interrupt.add(interrupt)
        try:

            async def run() -> None:
                result_ = await shell.run_cell_async(
                    raw_cell=code,
                    store_history=store_history,
                    silent=silent,
                    transformed_cell=shell.transform_cell(code),
                )
                result.append(result_)
                interrupt.set()

            async with create_task_group() as tg:
                tg.start_soon(run)
                await to_thread.run_sync(interrupt.wait)
                if not result:
                    tg.cancel_scope.cancel()
        finally:
            self.main_kernel.shell_interrupt.discard(interrupt)
            self._restore_input()

        reply_content = {}
        if result and (res := result[0]):
            err = res.error_before_exec if res.error_before_exec is not None else res.error_in_exec
        else:
            err = KeyboardInterrupt("Interruped by client request")
        if not err:
            reply_content["status"] = "ok"
        else:
            if traceback := shell._last_traceback:
                self.log.info("Exception in execute request:\n%s", "\n".join(traceback))
            reply_content["status"] = "error"
            reply_content["traceback"] = traceback or []
            reply_content["ename"] = str(type(err).__name__)
            reply_content["evalue"] = str(err)

        reply_content["execution_count"] = shell.execution_count - 1
        reply_content["user_expressions"] = (
            shell.user_expressions(user_expressions) if not err and user_expressions else {}
        )
        return reply_content

    def _send_error_reply(
        self,
        socket: zmq_anyio.Socket,
        idents,
        msg: dict,
        *,
        ename="RuntimeError",
        evalue="",
        traceback: list[str] | None = None,
    ):
        "Send a reply to the request"
        msg_or_type = msg["header"]["msg_type"].replace("request", "reply")
        content = {
            "status": "error",
            "execution_count": self.execution_count,
            "ename": ename,
            "evalue": evalue,
            "traceback": traceback or [],
        }
        self.session.send(stream=socket, msg_or_type=msg_or_type, content=content, ident=idents, parent=msg)

    async def interrupt_request(self, socket, ident, parent):
        """Handle an interrupt request."""

        content: dict[str, t.Any] = {"status": "ok"}
        for event in self.main_kernel.shell_interrupt:
            event.set()
        self.session.send(
            stream=socket,
            msg_or_type="interrupt_reply",
            content=content,
            parent=parent,
            ident=ident,
        )
        return

    async def comm_open(self, socket, ident, parent):
        self.comm_manager.comm_open(socket, ident, parent)

    async def comm_msg(self, socket, ident, parent):
        self.comm_manager.comm_msg(socket, ident, parent)

    async def comm_close(self, socket, ident, parent):
        self.comm_manager.comm_close(socket, ident, parent)

    async def is_complete_request(self, socket, ident, parent):
        """Handle an is_complete request."""
        content = parent["content"]
        code = content["code"]

        reply_content = await self.do_is_complete(code)
        reply_msg = self.session.send(
            stream=socket,
            msg_or_type="is_complete_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", reply_msg)

    async def do_is_complete(self, code):
        """Handle an is_complete request."""
        status, indent_spaces = self.shell.input_transformer_manager.check_complete(code)
        r = {"status": status}
        if status == "incomplete":
            r["indent"] = " " * indent_spaces
        return r

    async def do_complete(self, code, cursor_pos):
        """
        Completions from IPython, using Jedi.
        """
        if cursor_pos is None:
            cursor_pos = len(code)
        with _provisionalcompleter():
            raw_completions = self.shell.Completer.completions(code, cursor_pos)
            completions = list(_rectify_completions(code, raw_completions))

            comps = []
            for comp in completions:
                comps.append(
                    {
                        "start": comp.start,
                        "end": comp.end,
                        "text": comp.text,
                        "type": comp.type,
                        "signature": comp.signature,
                    }
                )

        if completions:
            s = completions[0].start
            e = completions[0].end
            matches = [c.text for c in completions]
        else:
            s = cursor_pos
            e = cursor_pos
            matches = []

        return {
            "matches": matches,
            "cursor_end": e,
            "cursor_start": s,
            "metadata": {"_jupyter_types_experimental": comps},
            "status": "ok",
        }

    async def complete_request(self, socket, ident, parent):
        """Handle a completion request."""

        content = parent["content"]
        code = content["code"]
        cursor_pos = content["cursor_pos"]

        matches = await self.do_complete(code, cursor_pos)
        self.session.send(
            stream=socket,
            msg_or_type="complete_reply",
            content=matches,
            parent=parent,
            ident=ident,
        )

    async def inspect_request(self, socket, ident, parent):
        """Handle an inspect request."""

        content = parent["content"]
        reply_content = await self.do_inspect(
            content["code"],
            content["cursor_pos"],
            content.get("detail_level", 0),
            set(content.get("omit_sections", [])),
        )
        msg = self.session.send(
            stream=socket,
            msg_or_type="inspect_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", msg)

    async def do_inspect(self, code, cursor_pos, detail_level=0, omit_sections=()):
        """Handle code inspection."""
        name = token_at_cursor(code, cursor_pos)

        reply_content: dict[str, t.Any] = {"status": "ok"}
        reply_content["data"] = {}
        reply_content["metadata"] = {}
        try:
            bundle = self.shell.object_inspect_mime(name, detail_level=detail_level, omit_sections=omit_sections)
            reply_content["data"].update(bundle)
            if not self.shell.enable_html_pager:
                reply_content["data"].pop("text/html")
            reply_content["found"] = True
        except KeyError:
            reply_content["found"] = False

        return reply_content

    async def history_request(self, socket, ident, parent):
        """Handle a history request."""

        content = parent["content"]
        reply_content = await self.do_history(**content)
        msg = self.session.send(
            stream=socket, msg_or_type="history_reply", content=reply_content, parent=parent, ident=ident
        )
        self.log.debug("%s", msg)

    async def do_history(
        self,
        hist_access_type,
        output,
        raw,
        session=0,
        start=0,
        stop=None,
        n=None,
        pattern=None,
        unique=False,
    ):
        """Handle code history."""

        history_manager = self.shell.history_manager
        assert history_manager
        if hist_access_type == "tail":
            hist = history_manager.get_tail(n, raw=raw, output=output, include_latest=True)

        elif hist_access_type == "range":
            hist = history_manager.get_range(session, start, stop, raw=raw, output=output)

        elif hist_access_type == "search":
            hist = history_manager.search(pattern, raw=raw, output=output, n=n, unique=unique)
        else:
            hist = []

        return {
            "status": "ok",
            "history": list(hist),
        }

    @property
    def kernel_info(self):
        from ipykernel.debugger import _is_debugpy_available

        supported_features: list[str] = []
        if self._supports_kernel_subshells:
            supported_features.append("kernel subshells")
        if _is_debugpy_available:
            supported_features.append("debugger")

        return {
            "protocol_version": kernel_protocol_version,
            "implementation": self.implementation,
            "implementation_version": self.implementation_version,
            "language_info": self.language_info,
            "banner": self.shell.banner,
            "supported_features": supported_features,
        }

    async def kernel_info_request(self, socket, ident, parent):
        """Handle a kernel info request."""

        content = {"status": "ok"}
        content.update(self.kernel_info)
        msg = self.session.send(
            stream=socket,
            msg_or_type="kernel_info_reply",
            content=content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", msg)

    async def comm_info_request(self, socket, ident, parent):
        """Handle a comm info request."""

        content = parent["content"]
        target_name = content.get("target_name", None)

        # Should this be moved to kernelbase?
        if hasattr(self, "comm_manager"):
            comms = {
                k: {"target_name": v.target_name}
                for (k, v) in self.comm_manager.comms.items()
                if v.target_name == target_name or target_name is None
            }
        else:
            comms = {}
        reply_content = {"comms": comms, "status": "ok"}
        msg = self.session.send(
            stream=socket,
            msg_or_type="comm_info_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", msg)

    async def do_shutdown(self, restart):
        """Handle kernel shutdown."""
        if self.shell:
            self.shell.exit_now = True
        return {"status": "ok", "restart": restart}

    def do_clear(self):
        """Clear the kernel."""
        if self.shell:
            self.shell.reset(False)
        return {"status": "ok"}

    async def debug_request(self, socket, ident, parent):
        """Handle a debug request."""

        content = parent["content"]
        reply_content = await self.do_debug_request(content)
        reply_msg = self.session.send(
            stream=socket,
            msg_or_type="debug_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", reply_msg)

    async def do_debug_request(self, msg):
        """Handle a debug request."""
        # from ipykernel.debugger import _is_debugpy_available

        # if _is_debugpy_available:
        #     return await self.debugger.process_request(msg)
        return {}

    # ---------------------------------------------------------------------------
    # Protected interface
    # ---------------------------------------------------------------------------

    def _topic(self, topic):
        """prefixed topic for IOPub messages"""
        return (f"kernel.{self.ident}.{topic}").encode()

    def _no_raw_input(self):
        """Raise StdinNotImplementedError if active frontend doesn't support
        stdin."""
        msg = "raw_input was called, but this frontend does not support stdin."
        raise StdinNotImplementedError(msg)

    def getpass(self, prompt="", stream=None):
        """Forward getpass to frontends

        Raises
        ------
        StdinNotImplementedError if active frontend doesn't support stdin.
        """
        if not self._allow_stdin:
            msg = "getpass was called, but this frontend does not support input requests."
            raise StdinNotImplementedError(msg)
        if stream is not None:
            import warnings

            warnings.warn(
                "The `stream` parameter of `getpass.getpass` will have no effect when using ipykernel",
                UserWarning,
                stacklevel=2,
            )
        return self._input_request(prompt, password=True)

    def raw_input(self, prompt=""):
        """Forward raw_input to frontends

        Raises
        ------
        StdinNotImplementedError if active frontend doesn't support stdin.
        """
        if not self._allow_stdin:
            msg = "raw_input was called, but this frontend does not support input requests."
            raise StdinNotImplementedError(msg)
        return self._input_request(str(prompt), password=False)

    def _input_request(self, prompt, *, password=False):
        # Flush output before making the request.
        if threading.current_thread() is not threading.main_thread():
            msg = "Input request is only allowed from the main thread (eg: not from the control thread)."
            raise RuntimeError(msg)
        if sys.stdout is not None:
            sys.stdout.flush()
        if sys.stderr is not None:
            sys.stderr.flush()

        # flush the stdin socket, to purge stale replies
        socket = self.main_kernel.sockets[SocketID.stdin]
        while True:
            try:
                socket.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
                raise

        # Send the input request.
        assert self is not None
        self.session.send(
            stream=socket,
            msg_or_type="input_request",
            content={"prompt": prompt, "password": password},
            parent=self.parent_msg,
            ident=self.parent_ident,
        )

        # Await a response.
        while True:
            try:
                # Use polling with select() so KeyboardInterrupts can get
                # through; doing a blocking recv() means stdin reads are
                # uninterruptible on Windows. We need a timeout because
                # zmq.select() is also uninterruptible, but at least this
                # way reads get noticed immediately and KeyboardInterrupts
                # get noticed fairly quickly by human response time standards.
                rlist, _, xlist = zmq.select([socket], [], [socket], 0.01)
                if rlist or xlist:
                    ident, reply = self.session.recv(socket)
                    if (ident, reply) != (None, None):
                        break
            except KeyboardInterrupt:
                # re-raise KeyboardInterrupt, to truncate traceback
                msg = "Interrupted by user"
                raise KeyboardInterrupt(msg) from None
            except Exception:
                self.log.warning("Invalid Message:", exc_info=True)

        try:
            value = reply["content"]["value"]  # type:ignore[index]
        except Exception:
            self.log.error("Bad input_reply: %s", self.parent_msg)
            value = ""
        if value == "\x04":
            # EOF
            raise EOFError
        return value

    @property
    def _supports_kernel_subshells(self):
        return True

    async def process_debugpy(self):
        async with self.debug_shell_socket, self.debugpy_socket, create_task_group() as tg:
            tg.start_soon(self.receive_debugpy_messages)
            tg.start_soon(self.poll_stopped_queue)
            self.debugpy_stop = threading.Event()
            await to_thread.run_sync(self.debugpy_stop.wait)
            tg.cancel_scope.cancel()

    async def receive_debugpy_messages(self):
        from ipykernel.debugger import _is_debugpy_available

        if not _is_debugpy_available:
            return

        while True:
            await self.receive_debugpy_message()

    async def receive_debugpy_message(self, msg=None):
        from ipykernel.debugger import _is_debugpy_available

        if not _is_debugpy_available:
            return

        if msg is None:
            assert self.debugpy_socket is not None
            msg = await self.debugpy_socket.arecv_multipart().wait()
        # The first frame is the socket id, we can drop it
        if msg and isinstance(data := msg[1], bytes):
            frame = data.decode("utf-8")
            self.log.debug("Debugpy received: %s", frame)
            self.debugger.tcp_client.receive_dap_frame(frame)

    async def poll_stopped_queue(self):
        """Poll the stopped queue."""
        while True:
            await self.debugger.handle_stopped_event()

    def _forward_input(self, allow_stdin=False):
        """Forward raw_input and getpass to the current frontend.

        via input_request
        """
        self._allow_stdin = allow_stdin

        self._sys_raw_input = builtins.input
        builtins.input = self.raw_input

        self._save_getpass = getpass.getpass
        getpass.getpass = self.getpass

    def _restore_input(self):
        """Restore raw_input, getpass"""
        builtins.input = self._sys_raw_input

        getpass.getpass = self._save_getpass


class Kernel(SingletonConfigurable, ConnectionFileMixin, Subkernel):
    """The IPYKernel application class."""

    _io_modified = Bool(False)
    _log_map: Dict[int, t.Any] = Dict()
    _portal = Instance(BlockingPortal)
    _socket_threads: Dict[zmq_anyio.Socket, threading.Thread] = Dict()
    _ports = Dict()
    _stopped = Instance(anyio.Event, ())
    _stop_event = Instance(threading.Event, ())
    _tg_main = Instance(TaskGroup)

    sockets: Dict[SocketID, zmq_anyio.Socket] = Dict()
    zmq_context = Instance(zmq.Context)
    shell_interrupt: Container[set[threading.Event]] = Set()

    def __new__(cls, **kwargs) -> Self:
        #  There is only one instance.
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, **kwargs):
        if self.shell_handlers:
            return  # Only initialize once
        super().__init__(**kwargs)
        self.control_handlers = {
            "shutdown_request": self.shutdown_request,
            "debug_request": self.debug_request,
            "execute_request": self._execute_request,  # bypass
        }
        self.init_crash_handler()

    @property
    def ports(self):
        return {
            "shell": self.shell_port,
            "iopub": self.iopub_port,
            "stdin": self.stdin_port,
            "hb": self.hb_port,
            "control": self.control_port,
        }

    quiet = Bool(True, help="Only send stdout/stderr to output stream").tag(config=True)
    outstream_class = DottedObjectName(
        "ipykernel.iostream.OutStream",
        help="The importstring for the OutStream factory",
        allow_none=True,
    ).tag(config=True)
    displayhook_class = DottedObjectName(
        "ipykernel.displayhook.ZMQDisplayHook", help="The importstring for the DisplayHook factory"
    ).tag(config=True)

    def init_crash_handler(self):
        """Initialize the crash handler."""
        sys.excepthook = self.excepthook

    def excepthook(self, etype, evalue, tb):
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def init_pubio(self):
        """Redirect input streams."""
        if threading.current_thread() != threading.main_thread():
            msg = "pubio expects to be running in the main thread"
            raise RuntimeError(msg)
        self._save_io()
        if self.outstream_class:
            cls: type[OutStream] = import_item(self.outstream_class)
            for name in ["stdout", "stderr"]:
                echo = getattr(sys, name)

                def flusher(string: str, name=name, echo=echo):
                    "Publish stdio or stderr when flush is called"
                    self.pubio_send(
                        msg_or_type="stream",
                        content={"name": name, "text": string},
                        ident=self._topic("status"),
                    )
                    if not self.quiet and echo:
                        echo.write(string)
                        echo.flush()

                wrapper = cls(name=name, flusher=flusher)  # type: ignore[call-arg]
                setattr(sys, name, wrapper)

    def _save_io(self):
        if not self._io_modified:
            self._original_io = sys.stdout, sys.stderr, sys.displayhook
            self._log_map = {}
            self._io_modified = True

    def reset_io(self):
        """restore original io

        restores state after init_io
        """
        if not self._io_modified:
            return
        stdout, stderr, displayhook = sys.stdout, sys.stderr, sys.displayhook
        sys.stdout, sys.stderr, sys.displayhook = self._original_io
        self._io_modified = False
        if finish_displayhook := getattr(displayhook, "finish_displayhook", None):
            finish_displayhook()
            self._log_map = {}
        if self.outstream_class:
            outstream_factory = import_item(str(self.outstream_class))
            if isinstance(stderr, outstream_factory):
                stderr.close()
            if isinstance(stdout, outstream_factory):
                stdout.close()

    def pubio_send(
        self,
        msg_or_type: dict[str, t.Any] | str,
        content: dict[str, t.Any] | None = None,
        metadata: dict[str, t.Any] | None = None,
        parent: dict[str, t.Any] | None = None,
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes | bytearray] | None = None,
    ):
        "Send the message on the iopub socket"
        if threading.current_thread() is not threading.main_thread():
            # Send the message from the main thread
            try:
                anyio.from_thread.run_sync(self.pubio_send, msg_or_type, content, metadata, parent, ident, buffers)
            except RuntimeError:
                pass
            return
        self.session.send(
            stream=self.sockets[SocketID.iopub],
            msg_or_type=msg_or_type,
            content=content,
            metadata=metadata,
            parent=parent or self.parent_msg,
            ident=ident,
            buffers=buffers,
        )

    def start_soon(self, func, *args, name: str | None = None):
        "Run a coroutine in the main thread taskgroup."
        try:
            if threading.current_thread() is threading.main_thread():

                async def _run_coro():
                    try:
                        await func(*args)
                    except Exception:
                        pass

                self._tg_main.start_soon(_run_coro, name=name)
            else:
                self._portal.start_task_soon(func, *args, name=name)
        except Exception:
            self.log.exception("portal call failed")
            raise

    async def init_pdb(self):
        """Replace pdb with IPython's version that is interruptible.

        With the non-interruptible version, stopping pdb() locks up the kernel in a
        non-recoverable state.
        """
        import pdb

        from IPython.core import debugger

        if hasattr(debugger, "InterruptiblePdb"):
            # Only available in newer IPython releases:
            debugger.Pdb = debugger.InterruptiblePdb  # type:ignore[misc]
            pdb.Pdb = debugger.Pdb  # type:ignore[assignment,misc]
            pdb.set_trace = debugger.set_trace

    @asynccontextmanager
    async def start_in_context(self):
        """Start inside the current anyio event loop.

        ``` python
        kernel = Kernel.instance()
        async kernel.start_in_context():
            await anyio.sleep_forever()
        ```
        """
        if self.sockets:  # TODO: Make change to cls._instance
            msg = "Already started"
            raise RuntimeError(msg)
        if self.connection_file and Path(self.connection_file).exists():
            self.load_connection_file()
        self.zmq_context = zmq.Context()
        try:
            async with (
                anyio.create_task_group() as tg,
                BlockingPortal() as portal,
                self.get_socket(SocketID.shell, zmq.SocketType.ROUTER, 1000) as shell_socket,
                self.get_socket(SocketID.iopub, zmq.SocketType.PUB, 1000),
                self.get_socket(SocketID.stdin, zmq.SocketType.ROUTER, 1000),
            ):
                self._portal = portal
                tg.cancel_scope.shield = True
                self._tg_main = tg
                try:
                    self.init_pubio()
                    await run_in_thread(self._heartbeat, self._stop_event, tg, name="Heartbeat")
                    await run_in_thread(self._run_control_loop, self._stop_event, tg, name="Control")
                    if not self.connection_file:
                        self.connection_file = str(Path(jupyter_runtime_dir()).joinpath(f"kernel-{self.ident}.json"))
                    self.write_connection_file()
                    atexit.register(self.cleanup_connection_file)
                    # Log connection info after writing connection file, so that the connection
                    # file is definitely available at the time someone reads the log.
                    print(
                        'To connect another client to this kernel, use:\n      --existing "%s"', {self.connection_file}
                    )
                    tg.start_soon(self._receive_msg_loop, self._process_shell, shell_socket)
                    tg.start_soon(self._shell_execute_request_loop)
                    tg.start_soon(self.init_pdb)
                    self.comm_manager.kernel = self
                    yield
                finally:
                    # enable a message to be sent incase anyone is waiting
                    self.stop()
                    self.comm_manager.kernel = None
                    tg.cancel_scope.cancel()
        finally:
            self.zmq_context.destroy(linger=0)
            self.reset_io()

    @classmethod
    def start(cls, connection_file="", async_mode=AsyncMode.asyncio) -> int:
        """Start the application.

        Other options:

        Using stored config.

        ``` python
        Kernel.launch_instance()
        ```

        Or if there is already an anyio event loop running you can use

        ``` python
        async with Kernel.instance().start_in_context() as kernel:
        ...

        """
        async_mode = AsyncMode(async_mode)

        async def _start() -> None:
            """ """
            if async_mode is AsyncMode.asyncio_eager and sys.version_info >= (3, 11):
                import asyncio

                loop = asyncio.get_running_loop()
                loop.set_task_factory(asyncio.eager_task_factory)

            async with kernel.start_in_context():
                await anyio.sleep_forever()

        kernel = cls.instance()
        kernel.connection_file = connection_file

        if async_mode in [AsyncMode.asyncio, AsyncMode.asyncio_eager] and sys.platform == "win32":
            import asyncio

            policy = asyncio.get_event_loop_policy()
            if policy.__class__.__name__ == "WindowsProactorEventLoopPolicy":
                from anyio._core._asyncio_selector_thread import get_selector

                selector = get_selector()
                selector._thread.pydev_do_not_trace = True
        try:
            anyio.run(_start, backend="trio" if async_mode is AsyncMode.trio else "asyncio")
        finally:
            pass
        return 0

    def stop(self):
        self._stop_event.set()

    def get_socket(
        self,
        socket_id: SocketID,
        socket_type: zmq.SocketType,
        linger: int,
        max_attempts=100,
        context: zmq.Context | None = None,
    ):
        "Open the socket for comms, it will be closed when the context is exited"
        # Create socket
        assert socket_id not in self.sockets
        socket = zmq_anyio.Socket(context or self.zmq_context, socket_type)
        socket.linger = linger
        # Bind port
        port_name = f"{socket_id}_port"
        port = self._bind_socket(socket, getattr(self, port_name, 0), max_attempts)
        setattr(self, port_name, port)
        self.sockets[socket_id] = socket
        self._socket_threads[socket] = threading.current_thread()
        self.log.debug("{%} {%} Channel on port: %i", socket_id, socket_type.name, port)
        return socket

    def _bind_socket(self, socket: zmq.Socket, port: int, max_attempts=100):
        try:
            win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
        except AttributeError:
            win_in_use = None

        def _try_bind_socket(port: int):
            if self.transport == "tcp":
                if port <= 0:
                    port = socket.bind_to_random_port(f"{self.transport}://{self.ip}")
                else:
                    socket.bind(f"tcp://{self.ip}:{port}")
            elif self.transport == "ipc":
                if port <= 0:
                    port = 1
                    while True:
                        port = port + 1
                        path = f"{self.ip}-{port}"
                        if Path(path).exists():
                            break
                else:
                    path = f"{self.ip}-{port}"
                socket.bind(f"ipc://{path}")
            return port

        # Try up to 100 times to bind a port when in conflict to avoid
        # infinite attempts in bad setups
        max_attempts = 1 if port else max_attempts
        for attempt in range(max_attempts):
            try:
                return _try_bind_socket(port)
            except zmq.ZMQError as e:
                # Raise if we have any error not related to socket binding
                if e.errno != errno.EADDRINUSE and e.errno != win_in_use:
                    raise
                if attempt == max_attempts - 1:
                    raise
        msg = f"Failed to bind a {socket}:{port}"
        raise RuntimeError(msg)

    async def _process_shell(self, socket, idents, msg):
        msg_type = msg["header"]["msg_type"]

        self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
        self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

        if msg_type == "execute_request":
            await self.execute_request(socket, idents, msg)
        else:
            handler = self.shell_handlers.get(msg_type)
            if handler is None:
                self.log.error("Unknown message type: %r", msg_type)
            else:
                try:
                    self._publish_status("busy", msg)
                    await handler(socket, idents, msg)
                except Exception as e:
                    self.log.error("Exception in message handler:", exc_info=e)
                except KeyboardInterrupt:
                    # Ctrl-c shouldn't crash the kernel here.
                    self.log.error("KeyboardInterrupt caught in kernel.")
                finally:
                    self._publish_status("idle", msg)

    async def _receive_msg_loop(
        self,
        process_message: Callable[[zmq.Socket, list[bytes | bytearray], dict | None], CoroutineType],
        socket: zmq_anyio.Socket,
    ):
        """Receive messages from the socket, unpack them and pass them to be processed with process_message.

        Intended to be used with:
         - shell socket
         - control socket
        """
        while True:
            if not (msg_ := await socket.arecv_multipart(copy=False).wait()):
                self.log.error("Empty message received on socket %", socket)
                await anyio.sleep(0.1)
                continue
            copy = not isinstance(msg_[0], zmq.Message)
            idents, msg_ = self.session.feed_identities(msg_, copy=copy)
            msg = self.session.deserialize(msg_, content=True, copy=copy)
            await process_message(socket, idents, msg)

    async def shutdown_request(self, socket, ident, parent):
        """Handle a shutdown request."""
        content = await self.do_shutdown(parent["content"]["restart"])
        self.session.send(
            stream=socket,
            msg_or_type="shutdown_reply",
            content=content,
            parent=parent,
            ident=ident,
        )

    async def do_shutdown(self, restart):
        """Handle kernel shutdown."""
        self.shell.exit_now = True
        await self._stopped.wait()
        return {"status": "ok", "restart": restart}

    async def _run_control_loop(self, task_status: TaskStatus):
        # Inside control thread
        # This code runs in a different thread having its own event loop
        async with self.get_socket(SocketID.control, zmq.SocketType.ROUTER, 1000) as control_socket:
            task_status.started()
            await self._receive_msg_loop(self._process_control, socket=control_socket)

    async def _process_control(self, socket, idents, msg):
        # Inside control thread
        msg_type = msg["header"]["msg_type"]

        self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
        self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

        # Execute_requests
        handler = self.control_handlers.get(msg_type) or self.shell_handlers.get(msg_type)
        if not handler:
            self.log.error("Unknown message type: %r", msg_type)
        else:
            try:
                self._publish_status("busy", msg)
                await handler(socket, idents, msg)
            except Exception as e:
                self.log.error("Exception in message handler:", exc_info=e)
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
            finally:
                self._publish_status("idle", msg)

    async def _heartbeat(self, task_status: TaskStatus):
        """The heartbeat.

        Reference: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
        """
        # Inside heartbeat thread
        socket = self.get_socket(SocketID.heartbeat, zmq.SocketType.ROUTER, 1000)
        await anyio.sleep(1)
        async with socket:
            task_status.started()
            while True:
                data = await socket.arecv_multipart(copy=False).wait()
                socket.send_multipart(data)
