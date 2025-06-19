# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import asyncio
import builtins
import enum
import getpass
import logging
import os
import sys
import threading
import time
import traceback
import typing as t
import uuid
from collections.abc import Callable
from typing import Literal

import anyio.from_thread
import anyio.to_thread
import zmq
import zmq_anyio
from anyio import create_task_group, to_thread
from IPython.core.completer import provisionalcompleter as _provisionalcompleter
from IPython.core.completer import rectify_completions as _rectify_completions
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.session import Session
from traitlets import Any, Dict, HasTraits, Instance, Type, Unicode, default, observe

from ipykernel._version import kernel_protocol_version
from ipykernel.zmqshell import ZMQInteractiveShell

if t.TYPE_CHECKING:
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus
    from IPython.core.interactiveshell import ExecutionResult

    from ipykernel.comm import CommManager
    from ipykernel.kernel import Kernel


class SocketID(enum.StrEnum):
    heartbeat = "hb"
    shell = "shell"
    iopub = "iopub"
    stdin = "stdin"
    control = "control"


class Subkernel(HasTraits):
    """A kernel without sockets."""

    _stop_on_error_time: float = 0

    session: Instance[Session] = Instance(Session)
    profile_dir = Instance("IPython.core.profiledir.ProfileDir", allow_none=True)

    main_kernel: Instance[Kernel] = Instance("ipykernel.kernel.Kernel", ())
    asyncio_event_loop = Instance(asyncio.AbstractEventLoop, allow_none=True, read_only=True)  # type:ignore[call-overload]
    _stop_event = Instance(anyio.Event, ())

    log = Instance(logging.LoggerAdapter)
    ident = Unicode()
    shell_handlers = Dict()

    language_info = {
        "name": "python",
        "version": sys.version.split()[0],
        "mimetype": "text/x-python",
        "codemirror_mode": {"name": "ipython", "version": sys.version_info[0]},
        "pygments_lexer": "ipython%d" % 3,
        "nbconvert_exporter": "python",
        "file_extension": ".py",
    }

    # Kernel info fields
    implementation = "asynckernel"
    implementation_version = " 0.1"

    shell = Instance(ZMQInteractiveShell)
    shell_class = Type(ZMQInteractiveShell)

    user_module = Any()
    user_ns = Dict()
    comm_manager: Instance[CommManager] = Instance("ipykernel.comm.CommManager")

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
        self.main_kernel._subkernels[self.ident] = self
        self._exec_send_stream, self._exec_receive_stream = anyio.create_memory_object_stream[
            tuple[float, zmq_anyio.Socket, list[bytes | bytearray], dict]
        ](max_buffer_size=1000)
        self.shell = self.shell_class.instance(
            parent=self,
            profile_dir=self.profile_dir,
            user_module=self.user_module,
            user_ns=self.user_ns,
            kernel=self,
        )
        jupyter_session_name = os.environ.get("JPY_SESSION_NAME")
        if jupyter_session_name:
            self.shell.user_ns["__session__"] = jupyter_session_name

    @property
    def parent_msg(self):
        """The message of the most recent execution request."""
        try:
            return self._parent_msg
        except AttributeError:
            return {}

    @property
    def parent_ident(self):
        """The ident of the most recent execution request."""
        try:
            return self._parent_ident
        except AttributeError:
            return []

    @property
    def kernel_info(self):
        return {
            "protocol_version": kernel_protocol_version,
            "implementation": self.implementation,
            "implementation_version": self.implementation_version,
            "language_info": self.language_info,
            "banner": self.shell.banner,
            "supported_features": ["kernel subshells"],
        }

    @observe("user_module")
    def _user_module_changed(self, change):
        self.shell.user_module = change["new"]

    @observe("user_ns")
    def _user_ns_changed(self, change):
        if self.trait_has_value("shell"):
            self.shell.user_ns = change["new"]
            self.shell.init_user_ns()
            self.shell.set_completer_frame()

    @default("log")
    def _default_log(self):
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @default("ident")
    def _default_ident(self):
        return str(uuid.uuid4())

    @default("comm_manager")
    def _default_comm_manager(self):
        from ipykernel import comm

        comm.set_comm()
        return comm.get_comm_manager()

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
            "execution_count": self.shell.execution_count,
            "ename": ename,
            "evalue": evalue,
            "traceback": traceback or [],
        }
        self.session.send(stream=socket, msg_or_type=msg_or_type, content=content, ident=idents, parent=msg)

    async def process_shell(self, socket, idents, msg, msg_type):
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
                    # Ctrl-c shouldn't crash the self here.
                    self.log.error("KeyboardInterrupt caught in kernel.")
                finally:
                    self._publish_status("idle", msg)

    async def execute_request(self, socket, ident, parent):
        content = parent["content"]
        silent = content["silent"]
        if not silent:
            await self._exec_send_stream.send((time.monotonic(), socket, ident, parent))
        else:
            self.main_kernel.start_soon(self._execute_request, socket, ident, parent)

    async def _shell_execute_request_loop(self, *, task_status: TaskStatus):
        async with self._exec_receive_stream as receive_stream:
            task_status.started()
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
                self.main_kernel.pubio_send(
                    msg_or_type="execute_input",
                    content={"code": content["code"], "execution_count": self.shell.execution_count},
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

    async def kernel_info_request(self, socket, ident, parent):
        """Handle a kernel info request."""

        msg = self.session.send(
            stream=socket,
            msg_or_type="kernel_info_reply",
            content={"status": "ok"} | self.kernel_info,
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
        self.shell.exit_now = True
        return {"status": "ok", "restart": restart}

    def do_clear(self):
        """Clear the kernel."""
        self.shell.reset(False)
        return {"status": "ok"}

    async def _start_async(self):
        async with anyio.create_task_group() as tg:
            await tg.start(self._shell_execute_request_loop)
            await tg.start(self._start_soon_scheduler, tg)
            await self._stop_event.wait()
            tg.cancel_scope.cancel()

    def stop(self):
        self.main_kernel._subkernels.pop(self.ident, None)
        if not self._stop_event.is_set():
            if threading.current_thread() is threading.main_thread():
                self._stop_event.set()
            else:
                self.start_soon(self._stop_event.set)

    async def _start_soon_scheduler(self, tg: TaskGroup, *, task_status: TaskStatus):
        "Orderly schedule starting a coroutine"
        self._start_soon_stream, scheduled = anyio.create_memory_object_stream[tuple[Callable, tuple, str | None]](
            max_buffer_size=1000
        )
        task_status.started()
        while True:
            func, args, name = await scheduled.receive()
            tg.start_soon(self._wrap_start_soon, func, args, name=name)

    async def _wrap_start_soon(self, func: Callable[..., CoroutineType], args: tuple):
        try:
            await func(*args)
        except Exception as e:
            self.log.exception("Coroutine execution failed", exc_info=e)

    def start_soon(self, func, *args, name: str | None = None):
        "Run a coroutine in the main thread."
        to_start = (func, args, name)
        if threading.current_thread() is threading.main_thread():
            self._start_soon_stream.send_nowait(to_start)
        else:
            anyio.from_thread.run_sync(self._start_soon_stream.send_nowait, to_start)

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

    def getpass(self, prompt=""):
        """Forward getpass to frontends"""
        if not self._allow_stdin:
            msg = "getpass was called, but this frontend does not support input requests."
            raise StdinNotImplementedError(msg)
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
