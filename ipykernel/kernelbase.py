"""The Kernel kernel implementation"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import asyncio
import builtins
import getpass
import inspect
import logging
import os
import sys
import threading
import time
import typing as t
import uuid
from dataclasses import dataclass
from signal import SIGINT
from typing import TYPE_CHECKING, Literal, Unpack

import zmq
from anyio import create_memory_object_stream, create_task_group, to_thread
from anyio.abc import TaskGroup
from anyio.from_thread import BlockingPortal
from IPython.core.completer import provisionalcompleter as _provisionalcompleter
from IPython.core.completer import rectify_completions as _rectify_completions
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.session import Session
from traitlets import Any, Bool, Dict, Float, Instance, List, Type, Unicode, default, observe
from traitlets.config.configurable import LoggingConfigurable

from ipykernel._version import kernel_protocol_version
from ipykernel.compiler import XCachingCompiler
from ipykernel.iostream import SendKwgs, SocketID
from ipykernel.zmqshell import ZMQInteractiveShell

if TYPE_CHECKING:
    import zmq_anyio

    from ipykernel.comm import CommManager
    from ipykernel.kernelapp import MainKernel


class Kernel(LoggingConfigurable):
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
    main_kernel: Instance[MainKernel] = Instance("ipykernel.kernelapp.MainKernel", ())
    asyncio_event_loop = Instance(asyncio.AbstractEventLoop, allow_none=True, read_only=True)  # type:ignore[call-overload]
    _tg_main = Instance(TaskGroup)
    _portal = Instance(BlockingPortal)

    log = Instance(logging.LoggerAdapter)
    ident = Unicode()
    shell_handlers = Dict()
    sockets: Dict[SocketID, zmq_anyio.Socket] = Dict()

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

    # any links that should go in the help menu
    help_links = List([
        {
            "text": "Python Reference",
            "url": "https://docs.python.org/%i.%i" % sys.version_info[:2],
        },
        {
            "text": "IPython Reference",
            "url": "https://ipython.org/documentation.html",
        },
        {
            "text": "NumPy Reference",
            "url": "https://docs.scipy.org/doc/numpy/reference/",
        },
        {
            "text": "SciPy Reference",
            "url": "https://docs.scipy.org/doc/scipy/reference/",
        },
        {
            "text": "Matplotlib Reference",
            "url": "https://matplotlib.org/contents.html",
        },
        {
            "text": "SymPy Reference",
            "url": "http://docs.sympy.org/latest/index.html",
        },
        {
            "text": "pandas Reference",
            "url": "https://pandas.pydata.org/pandas-docs/stable/",
        },
    ]).tag(config=True)

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

    # If the shutdown was requested over the network, we leave here the
    # necessary reply message so it can be sent by our registered atexit
    # handler.  This ensures that the reply is only sent to clients truly at
    # the end of our shutdown process (which happens after the underlying
    # IPython shell's own shutdown).
    _shutdown_message = None

    # This is a dict of port number that the kernel is listening on. It is set
    # by record_ports and used by connect_request.
    _recorded_ports = Dict()

    shell = Instance(ZMQInteractiveShell)
    shell_class = Type(ZMQInteractiveShell)

    # use fully-qualified name to ensure lazy import and prevent the issue from
    # https://github.com/ipython/ipykernel/issues/1198
    debugger_class = Type("ipykernel.debugger.Debugger")

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
            "connect_request": self.connect_request,
            "is_complete_request": self.is_complete_request,
            "interrupt_request": self.interrupt_request,
            "comm_open": self.comm_manager.comm_open,
            "comm_msg": self.comm_manager.comm_msg,
            "comm_close": self.comm_manager.comm_close,
        }
        self._exec_send_stream, self._exec_receive_stream = create_memory_object_stream[
            tuple[float, zmq.Socket, list[bytes | bytearray], dict | None]
        ](max_buffer_size=1000)

    def init_shell(self):
        # Initialize the InteractiveShell subclass
        shell = self.shell_class.instance(
            parent=self,
            profile_dir=self.profile_dir,
            user_module=self.user_module,
            user_ns=self.user_ns,
            kernel=self,
            compiler_class=self.compiler_class,
        )
        shell.displayhook.session = self.session
        self.set_trait("shell", shell)
        jupyter_session_name = os.environ.get("JPY_SESSION_NAME")
        if jupyter_session_name:
            self.shell.user_ns["__session__"] = jupyter_session_name

    # async def shell_channel_thread_main(self):
    #     """Main loop for shell channel thread.

    #     Listen for incoming messages on kernel shell_socket.  For each message
    #     received, extract the subshell_id from the message header and forward the
    #     message to the correct subshell via ZMQ inproc pair socket.
    #     """
    #     async with self.shell_socket, create_task_group() as tg:
    #         try:
    #             while True:
    #                 msg = await self.shell_socket.arecv_multipart(copy=False).wait()
    #                 # deserialize only the header to get subshell_id
    #                 # Keep original message to send to subshell_id unmodified.
    #                 _, msg2 = self.feed_identities(msg, copy=False)
    #                 try:
    #                     msg3 = self.deserialize(msg2, content=False, copy=False)
    #                     subshell_id = msg3["header"].get("subshell_id")

    #                     # Find inproc pair socket to use to send message to correct subshell.
    #                     socket = self.shell_channel_thread.manager.get_shell_channel_socket(subshell_id)
    #                     assert socket is not None
    #                     if not socket.started.is_set():
    #                         await tg.start(socket.start)
    #                     await socket.asend_multipart(msg, copy=False).wait()
    #                 except Exception:
    #                     self.log.error("Invalid message", exc_info=True)
    #         except BaseException:
    #             if self.shell_stop.is_set():
    #                 return
    # raise

    # ---------------------------------------------------------------------------
    # Kernel request handlers
    # ---------------------------------------------------------------------------

    def _publish_status(self, status: Literal["busy", "idle"], parent):
        """send status (busy/idle) on IOPub"""
        self.send(
            stream=self.sockets[SocketID.iopub],
            msg_or_type="status",
            content={"execution_state": status},
            parent=parent,
            ident=self._topic("status"),
        )

    def _publish_debug_event(self, event):
        self.send(
            stream=self.sockets[SocketID.iopub],
            msg_or_type="debug_event",
            content=event,
            parent=self.parent_msg,
            ident=self._topic("debug_event"),
        )

    def send(self, **kwgs: Unpack[SendKwgs]) -> dict[str, t.Any] | None:
        # if self is self.main_kernel:
        #     super().send(**kwgs)
        # else:
        #     self.main_kernel.send(**kwgs)
        self.session.send(**kwgs)

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

    # def send_response(
    #     self,
    #     socket,
    #     msg_or_type,
    #     content=None,
    #     ident=None,
    #     buffers=None,
    #     track=False,
    #     header=None,
    #     metadata=None,
    # ):
    #     """Send a response to the message we're currently processing.

    #     This accepts all the parameters of :meth:`jupyter_client.session.Session.send`
    #     except ``parent``.
    #     """
    #     return self.send(
    #         socket,
    #         msg_or_type,
    #         content=content,
    #         parent=self.parent_msg,
    #         ident=ident,
    #         buffers=buffers,
    #         track=track,
    #         header=header,
    #         metadata=metadata,
    #     )


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
                        if session := self:
                            session.send(
                                stream=socket,
                                msg_or_type="execute_reply",
                                content={
                                    "status": "error",
                                    "execution_count": self.execution_count,
                                    "ename": "RuntimeError",
                                    "evalue": "An exception occurred whilst this execute request was queued!",
                                    "traceback": [],
                                },
                                parent=msg,
                                ident=idents,
                            )
                        continue
                    await self._execute_request(socket, idents, msg)
                except BaseException as e:
                    self.log.exception("Execute request", exc_info=e)
                finally:
                    self._publish_status("idle", msg)

    async def _execute_request(self, socket, ident, parent):
        """handle an execute_request"""
        content = parent["content"]
        silent = content["silent"]
        stop_on_error = content.pop("stop_on_error", True)

        self._set_parent_ident(parent, ident)
        self._publish_status("busy", parent)

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            self.execution_count += 1
            self.send(
                stream=self.sockets[SocketID.iopub],
                msg_or_type="execute_input",
                content={"code": content["code"], "execution_count": self.execution_count},
                parent=parent,
                ident=self._topic("execute_input"),
            )
        # Call do_execute with the appropriate arguments
        reply_content = await self.do_execute(**content)

        # Send the reply.
        reply_msg = self.send(
            stream=socket,
            msg_or_type="execute_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )

        self.log.debug("%s", reply_msg)

        assert reply_msg is not None
        if reply_msg["content"]["status"] == "error":
            self.log.exception("Execution failed %s", reply_msg)
            if not silent and stop_on_error:
                self._stop_on_error_time = time.monotonic()
                self.log.info("Rejecting non-silent execute request")

        self._publish_status("idle", parent)

    async def do_execute(
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
        try:

            @dataclass
            class Execution:
                interrupt: bool = False
                result: t.Any = None

            async def run(execution: Execution) -> None:
                execution.result = await shell.run_cell_async(
                    raw_cell=code,
                    store_history=store_history,
                    silent=silent,
                    transformed_cell=shell.transform_cell(code),
                )
                if not execution.interrupt:
                    self.main_kernel.shell_interrupt.put(False)

            res = None
            try:
                async with create_task_group() as tg:
                    execution = Execution()
                    self.shell_is_awaiting = True
                    tg.start_soon(run, execution)
                    execution.interrupt = await to_thread.run_sync(self.main_kernel.shell_interrupt.get)
                    self.shell_is_awaiting = False
                    if execution.interrupt:
                        tg.cancel_scope.cancel()
                    res = execution.result
            finally:
                shell.events.trigger("post_execute")
                if not silent:
                    shell.events.trigger("post_run_cell", res)
        finally:
            self._restore_input()

        if res is not None:
            err = res.error_before_exec if res.error_before_exec is not None else res.error_in_exec
        else:
            err = KeyboardInterrupt()
        if res is not None and res.success:
            reply_content["status"] = "ok"
        else:
            reply_content["status"] = "error"
            reply_content.update({
                "traceback": shell._last_traceback or [],
                "ename": str(type(err).__name__),
                "evalue": str(err),
            })

        # Return the execution counter so clients can display prompts
        reply_content["execution_count"] = shell.execution_count - 1
        if "traceback" in reply_content:
            self.log.info(
                "Exception in execute request:\n%s",
                "\n".join(reply_content["traceback"]),
            )
        # At this point, we can tell whether the main code execution succeeded
        # or not.  If it did, we proceed to evaluate user_expressions
        if reply_content["status"] == "ok":
            reply_content["user_expressions"] = shell.user_expressions(user_expressions or {})
        else:
            # If there was an error, don't even try to compute expressions
            reply_content["user_expressions"] = {}

        # Payloads should be retrieved regardless of outcome, so we can both
        # recover partial output (that could have been generated early in a
        # block, before an error) and always clear the payload system.
        reply_content["payload"] = shell.payload_manager.read_payload()
        # Be aggressive about clearing the payload because we don't want
        # it to sit in memory until the next execute_request comes in.
        shell.payload_manager.clear_payload()
        return reply_content

    async def interrupt_request(self, socket_id, ident, parent):
        """Handle an interrupt request."""

        content: dict[str, t.Any] = {"status": "ok"}
        try:
            self._send_interrupt_children()
        except OSError as err:
            import traceback

            content = {
                "status": "error",
                "traceback": traceback.format_stack(),
                "ename": str(type(err).__name__),
                "evalue": str(err),
            }

        self.send(
            stream=socket_id,
            msg_or_type="interrupt_reply",
            content=content,
            parent=parent,
            ident=ident,
        )
        return

    async def is_complete_request(self, socket, ident, parent):
        """Handle an is_complete request."""
        content = parent["content"]
        code = content["code"]

        reply_content = await self.do_is_complete(code)
        reply_msg = self.send(
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
                comps.append({
                    "start": comp.start,
                    "end": comp.end,
                    "text": comp.text,
                    "type": comp.type,
                    "signature": comp.signature,
                })

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
        self.send(
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
        msg = self.send(
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
        msg = self.send(stream=socket, msg_or_type="history_reply", content=reply_content, parent=parent, ident=ident)
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

    async def connect_request(self, socket, ident, parent):
        """Handle a connect request."""

        content = self._recorded_ports.copy() if self._recorded_ports else {}
        content["status"] = "ok"
        msg = self.send(
            stream=socket,
            msg_or_type="connect_reply",
            content=content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", msg)

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
            "help_links": self.help_links,
            "supported_features": supported_features,
        }

    async def kernel_info_request(self, socket, ident, parent):
        """Handle a kernel info request."""

        content = {"status": "ok"}
        content.update(self.kernel_info)
        msg = self.send(
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
        msg = self.send(
            stream=socket,
            msg_or_type="comm_info_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", msg)

    def _send_interrupt_children(self):
        if os.name == "nt":
            self.log.error("Interrupt message not supported on Windows")
        else:
            pid = os.getpid()
            pgid = os.getpgid(pid)
            # Prefer process-group over process
            # but only if the kernel is the leader of the process group
            if pgid and pgid == pid and hasattr(os, "killpg"):
                try:
                    os.killpg(pgid, SIGINT)
                except OSError:
                    os.kill(pid, SIGINT)
                    raise
            else:
                os.kill(pid, SIGINT)

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
        reply_content = self.do_debug_request(content)
        if inspect.isawaitable(reply_content):
            reply_content = await reply_content
        reply_msg = self.send(
            stream=socket,
            msg_or_type="debug_reply",
            content=reply_content,
            parent=parent,
            ident=ident,
        )
        self.log.debug("%s", reply_msg)

    async def do_debug_request(self, msg):
        """Handle a debug request."""
        from ipykernel.debugger import _is_debugpy_available

        if _is_debugpy_available:
            return await self.debugger.process_request(msg)
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
        self.send(
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
                    ident, reply = self.recv(socket)
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
